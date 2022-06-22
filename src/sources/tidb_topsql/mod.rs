use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{StreamExt, TryFutureExt};
use http::uri::InvalidUri;
use ordered_float::NotNan;
use prost::Message;
use proto::{
    resource_metering_pubsub::{
        resource_metering_pub_sub_client::ResourceMeteringPubSubClient,
        resource_usage_record::RecordOneof, GroupTagRecord, ResourceMeteringRequest,
        ResourceUsageRecord,
    },
    topsql_pubsub::{
        top_sql_pub_sub_client::TopSqlPubSubClient, top_sql_sub_response::RespOneof, PlanMeta,
        ResourceGroupTag, SqlMeta, TopSqlRecord, TopSqlSubRequest, TopSqlSubResponse,
    },
};
use serde::{Deserialize, Serialize};
use snafu::{Error, ResultExt, Snafu};
use tokio_stream::wrappers::IntervalStream;
use tonic::transport::{Channel, Endpoint};
use vector_common::byte_size_of::ByteSizeOf;

use crate::internal_events::{BytesReceived, EventsReceived};
use crate::{
    config::{self, GenerateConfig, Output, SourceConfig, SourceContext, SourceDescription},
    event::{EventMetadata, LogEvent, Value},
    internal_events::{
        StreamClosedError, TopSQLPubSubConnectError, TopSQLPubSubReceiveError,
        TopSQLPubSubSubscribeError,
    },
    shutdown::ShutdownSignal,
    sources,
    tls::TlsConfig,
    SourceSender,
};

#[allow(clippy::clone_on_ref_ptr)]
mod proto {
    pub mod topsql_pubsub {
        include!(concat!(env!("OUT_DIR"), "/tipb.rs"));

        use top_sql_sub_response::RespOneof;
        use vector_core::ByteSizeOf;

        impl ByteSizeOf for TopSqlSubResponse {
            fn allocated_bytes(&self) -> usize {
                self.resp_oneof.as_ref().map_or(0, ByteSizeOf::size_of)
            }
        }

        impl ByteSizeOf for RespOneof {
            fn allocated_bytes(&self) -> usize {
                match self {
                    RespOneof::Record(record) => {
                        record.items.size_of() + record.sql_digest.len() + record.plan_digest.len()
                    }
                    RespOneof::SqlMeta(sql_meta) => {
                        sql_meta.sql_digest.len() + sql_meta.normalized_sql.len()
                    }
                    RespOneof::PlanMeta(plan_meta) => {
                        plan_meta.plan_digest.len() + plan_meta.normalized_plan.len()
                    }
                }
            }
        }

        impl ByteSizeOf for TopSqlRecordItem {
            fn allocated_bytes(&self) -> usize {
                self.stmt_kv_exec_count.size_of()
            }
        }
    }

    pub mod resource_metering_pubsub {
        include!(concat!(env!("OUT_DIR"), "/resource_usage_agent.rs"));

        use resource_usage_record::RecordOneof;
        use vector_core::ByteSizeOf;

        impl ByteSizeOf for ResourceUsageRecord {
            fn allocated_bytes(&self) -> usize {
                self.record_oneof.as_ref().map_or(0, ByteSizeOf::size_of)
            }
        }

        impl ByteSizeOf for RecordOneof {
            fn allocated_bytes(&self) -> usize {
                match self {
                    RecordOneof::Record(record) => {
                        record.resource_group_tag.len() + record.items.size_of()
                    }
                }
            }
        }

        impl ByteSizeOf for GroupTagRecordItem {
            fn allocated_bytes(&self) -> usize {
                0
            }
        }
    }
}

#[derive(Debug, Snafu)]
enum ConfigError {
    #[snafu(display("Unsupported instance type {:?}", instance_type))]
    UnsupportedInstanceType { instance_type: String },
    #[snafu(display("Could not create endpoint: {}", source))]
    Endpoint { source: InvalidUri },
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct TopSQLPubSubConfig {
    instance: String,
    instance_type: String,
    tls: Option<TlsConfig>,

    /// The amount of time, in seconds, to wait between retry attempts after an error.
    #[serde(default = "default_retry_delay")]
    pub retry_delay_seconds: f64,
}

const fn default_retry_delay() -> f64 {
    1.0
}

inventory::submit! {
    SourceDescription::new::<TopSQLPubSubConfig>("topsql")
}

impl GenerateConfig for TopSQLPubSubConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            instance: "127.0.0.1:10080".to_owned(),
            instance_type: "tidb".to_owned(),
            tls: None,
            retry_delay_seconds: default_retry_delay(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "topsql_pubsub")]
impl SourceConfig for TopSQLPubSubConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        match self.instance_type.as_str() {
            "tidb" | "tikv" => {}
            _ => {
                return Err(ConfigError::UnsupportedInstanceType {
                    instance_type: self.instance_type.clone(),
                }
                .into());
            }
        }
        let source = TopSQLSource::new(
            self.clone(),
            cx.shutdown,
            cx.out,
            Duration::from_secs_f64(self.retry_delay_seconds),
        )
        .run()
        .map_err(|error| error!(message = "Source failed.", %error));

        Ok(Box::pin(source))
    }

    fn outputs(&self) -> Vec<Output> {
        vec![Output::default(config::DataType::Log)]
    }

    fn source_type(&self) -> &'static str {
        "topsql_pubsub"
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

struct TopSQLSource {
    config: TopSQLPubSubConfig,
    shutdown: ShutdownSignal,
    out: SourceSender,
    retry_delay: Duration,
}

enum State {
    RetryNow,
    RetryDelay,
    Shutdown,
}

impl TopSQLSource {
    fn new(
        config: TopSQLPubSubConfig,
        shutdown: ShutdownSignal,
        out: SourceSender,
        retry_delay: Duration,
    ) -> Self {
        Self {
            config,
            shutdown,
            out,
            retry_delay,
        }
    }

    async fn run(mut self) -> crate::Result<()> {
        let channel = Channel::from_shared(format!("http://{}", self.config.instance))
            .context(EndpointSnafu)?;

        loop {
            match match self.config.instance_type.as_str() {
                "tidb" => self.run_once_tidb(&channel).await,
                "tikv" => self.run_once_tikv(&channel).await,
                _ => unreachable!(),
            } {
                State::RetryNow => debug!("Retrying immediately."),
                State::RetryDelay => {
                    info!(
                        timeout_secs = self.retry_delay.as_secs_f64(),
                        "Retrying after timeout."
                    );
                    tokio::time::sleep(self.retry_delay).await;
                }
                State::Shutdown => break,
            }
        }

        Ok(())
    }

    async fn run_once_tidb(&mut self, endpoint: &Endpoint) -> State {
        let connection = tokio::select! {
            _ = &mut self.shutdown => return State::Shutdown,
            connection = endpoint.connect() => match connection {
                Ok(connection) => connection,
                Err(error) => {
                    emit!(TopSQLPubSubConnectError { error });
                    return State::RetryDelay;
                }
            }
        };
        let mut client = TopSqlPubSubClient::new(connection);

        let stream = tokio::select! {
            _ = &mut self.shutdown => return State::Shutdown,
            result = client.subscribe(TopSqlSubRequest {}) => match result {
                Ok(stream) => stream,
                Err(error) => {
                    emit!(TopSQLPubSubSubscribeError { error });
                    return State::RetryDelay;
                }
            }
        };
        let mut response_stream = stream.into_inner();
        let mut instance_stream =
            IntervalStream::new(tokio::time::interval(Duration::from_secs(30)));

        loop {
            tokio::select! {
                _ = &mut self.shutdown => break State::Shutdown,
                _ = instance_stream.next() => self.handle_instance().await,
                response = response_stream.next() => match response {
                    Some(Ok(response)) => self.handle_tidb_response(response).await,
                    Some(Err(error)) => break translate_error(error),
                    None => break State::RetryNow,
                }
            }
        }
    }

    async fn run_once_tikv(&mut self, endpoint: &Endpoint) -> State {
        let connection = tokio::select! {
            _ = &mut self.shutdown => return State::Shutdown,
            connection = endpoint.connect() => match connection {
                Ok(connection) => connection,
                Err(error) => {
                    emit!(TopSQLPubSubConnectError { error });
                    return State::RetryDelay;
                }
            }
        };
        let mut client = ResourceMeteringPubSubClient::new(connection);

        let stream = tokio::select! {
            _ = &mut self.shutdown => return State::Shutdown,
            result = client.subscribe(ResourceMeteringRequest {}) => match result {
                Ok(stream) => stream,
                Err(error) => {
                    emit!(TopSQLPubSubSubscribeError { error });
                    return State::RetryDelay;
                }
            }
        };
        let mut response_stream = stream.into_inner();
        let mut instance_stream =
            IntervalStream::new(tokio::time::interval(Duration::from_secs(30)));

        loop {
            tokio::select! {
                _ = &mut self.shutdown => break State::Shutdown,
                _ = instance_stream.next() => self.handle_instance().await,
                response = response_stream.next() => match response {
                    Some(Ok(response)) => self.handle_tikv_response(response).await,
                    Some(Err(error)) => break translate_error(error),
                    None => break State::RetryNow,
                }
            }
        }
    }

    async fn handle_instance(&mut self) {
        let event = make_metric_like_log_event(
            &[
                ("__name__", "instance".to_owned()),
                ("instance", self.config.instance.clone()),
                ("instance_type", self.config.instance_type.clone()),
            ],
            &[Utc::now()],
            &[1.0],
        );

        if let Err(error) = self.out.send_event(event).await {
            emit!(StreamClosedError { error, count: 1 })
        }
    }

    async fn handle_tidb_response(&mut self, response: TopSqlSubResponse) {
        emit!(BytesReceived {
            byte_size: response.size_of(),
            protocol: "http", // TODO: or https
        });

        let events = self.parse_tidb_response(response);
        let count = events.len();
        emit!(EventsReceived {
            byte_size: events.size_of(),
            count: count,
        });
        if let Err(error) = self.out.send_batch(events).await {
            emit!(StreamClosedError { error, count })
        }
    }

    async fn handle_tikv_response(&mut self, response: ResourceUsageRecord) {
        emit!(BytesReceived {
            byte_size: response.size_of(),
            protocol: "http", // TODO: or https
        });

        let events = self.parse_tikv_response(response);
        let count = events.len();
        emit!(EventsReceived {
            byte_size: events.size_of(),
            count: count,
        });
        if let Err(error) = self.out.send_batch(events).await {
            emit!(StreamClosedError { error, count })
        }
    }

    fn parse_tidb_response(&self, response: TopSqlSubResponse) -> Vec<LogEvent> {
        match response.resp_oneof {
            Some(RespOneof::Record(record)) => self.parse_tidb_record(record),
            Some(RespOneof::SqlMeta(sql_meta)) => self.parse_tidb_sql_meta(sql_meta),
            Some(RespOneof::PlanMeta(plan_meta)) => self.parse_tidb_plan_meta(plan_meta),
            None => vec![],
        }
    }

    fn parse_tikv_response(&self, response: ResourceUsageRecord) -> Vec<LogEvent> {
        match response.record_oneof {
            Some(RecordOneof::Record(record)) => self.parse_tikv_record(record),
            None => vec![],
        }
    }

    fn parse_tidb_record(&self, record: TopSqlRecord) -> Vec<LogEvent> {
        let timestamps = record
            .items
            .iter()
            .map(|item| {
                DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp(item.timestamp_sec as i64, 0),
                    Utc,
                )
            })
            .collect::<Vec<_>>();
        let mut labels = vec![
            ("__name__", String::new()),
            ("instance", String::new()),
            ("instance_type", String::new()),
            ("sql_digest", String::new()),
            ("plan_digest", String::new()),
        ];
        const NAME_INDEX: usize = 0;
        const INSTANCE_INDEX: usize = 1;
        const INSTANCE_TYPE_INDEX: usize = 2;
        const SQL_DIGEST_INDEX: usize = 3;
        const PLAN_DIGEST_INDEX: usize = 4;
        let mut values = vec![];

        let mut logs = vec![];

        // cpu_time_ms
        labels[NAME_INDEX].1 = "cpu_time_ms".to_owned();
        labels[INSTANCE_INDEX].1 = self.config.instance.clone();
        labels[INSTANCE_TYPE_INDEX].1 = self.config.instance_type.clone();
        labels[SQL_DIGEST_INDEX].1 = hex::encode_upper(record.sql_digest);
        labels[PLAN_DIGEST_INDEX].1 = hex::encode_upper(record.plan_digest);
        values.extend(record.items.iter().map(|item| item.cpu_time_ms as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // stmt_exec_count
        labels[NAME_INDEX].1 = "stmt_exec_count".to_owned();
        values.clear();
        values.extend(record.items.iter().map(|item| item.stmt_exec_count as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // stmt_duration_sum_ns
        labels[NAME_INDEX].1 = "stmt_duration_sum_ns".to_owned();
        values.clear();
        values.extend(
            record
                .items
                .iter()
                .map(|item| item.stmt_duration_sum_ns as f64),
        );
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // stmt_duration_count
        labels[NAME_INDEX].1 = "stmt_duration_count".to_owned();
        values.clear();
        values.extend(
            record
                .items
                .iter()
                .map(|item| item.stmt_duration_count as f64),
        );
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // stmt_kv_exec_count
        let tikv_instances = record
            .items
            .iter()
            .flat_map(|item| item.stmt_kv_exec_count.keys())
            .collect::<BTreeSet<_>>();
        labels[NAME_INDEX].1 = "stmt_kv_exec_count".to_owned();
        labels[INSTANCE_TYPE_INDEX].1 = "tikv".to_owned();
        for tikv_instance in tikv_instances {
            values.clear();
            labels[INSTANCE_INDEX].1 = tikv_instance.clone();
            values.extend(record.items.iter().map(|item| {
                item.stmt_kv_exec_count
                    .get(tikv_instance)
                    .cloned()
                    .unwrap_or_default() as f64
            }));
            logs.push(make_metric_like_log_event(&labels, &timestamps, &values));
        }

        logs
    }

    fn parse_tidb_sql_meta(&self, sql_meta: SqlMeta) -> Vec<LogEvent> {
        vec![make_metric_like_log_event(
            &[
                ("__name__", "sql_meta".to_owned()),
                ("sql_digest", hex::encode_upper(sql_meta.sql_digest)),
                ("normalized_sql", sql_meta.normalized_sql),
                ("is_internal_sql", sql_meta.is_internal_sql.to_string()),
            ],
            &[Utc::now()],
            &[1.0],
        )]
    }

    fn parse_tidb_plan_meta(&self, plan_meta: PlanMeta) -> Vec<LogEvent> {
        vec![make_metric_like_log_event(
            &[
                ("__name__", "plan_meta".to_owned()),
                ("plan_digest", hex::encode_upper(plan_meta.plan_digest)),
                ("normalized_plan", plan_meta.normalized_plan),
            ],
            &[Utc::now()],
            &[1.0],
        )]
    }

    fn parse_tikv_record(&self, record: GroupTagRecord) -> Vec<LogEvent> {
        let (sql_digest, plan_digest, tag_label) =
            match ResourceGroupTag::decode(record.resource_group_tag.as_slice()) {
                Ok(resource_tag) => {
                    if resource_tag.sql_digest.is_none() {
                        return vec![];
                    }
                    (
                        hex::encode_upper(resource_tag.sql_digest.unwrap_or(vec![])),
                        hex::encode_upper(resource_tag.plan_digest.unwrap_or(vec![])),
                        match resource_tag.label {
                            Some(1) => "row".to_owned(),
                            Some(2) => "index".to_owned(),
                            _ => "unknown".to_owned(),
                        },
                    )
                }
                Err(_) => return vec![],
            };
        let timestamps = record
            .items
            .iter()
            .map(|item| {
                DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp(item.timestamp_sec as i64, 0),
                    Utc,
                )
            })
            .collect::<Vec<_>>();
        let mut labels = vec![
            ("__name__", String::new()),
            ("instance", self.config.instance.clone()),
            ("instance_type", self.config.instance_type.clone()),
            ("sql_digest", sql_digest),
            ("plan_digest", plan_digest),
            ("tag_label", tag_label),
        ];
        let mut values = vec![];

        let mut logs = vec![];

        // cpu_time_ms
        labels[0].1 = "cpu_time_ms".to_owned();
        values.extend(record.items.iter().map(|item| item.cpu_time_ms as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // read_keys
        labels[0].1 = "read_keys".to_owned();
        values.clear();
        values.extend(record.items.iter().map(|item| item.read_keys as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // write_keys
        labels[0].1 = "write_keys".to_owned();
        values.clear();
        values.extend(record.items.iter().map(|item| item.write_keys as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        logs
    }
}

fn make_metric_like_log_event(
    labels: &[(&'static str, String)],
    timestamps: &[DateTime<Utc>],
    values: &[f64],
) -> LogEvent {
    let mut labels_map = BTreeMap::new();
    for (k, v) in labels {
        labels_map.insert(k.to_string(), Value::Bytes(Bytes::from(v.clone())));
    }

    let timestamps_vec = timestamps
        .iter()
        .map(|t| Value::Timestamp(*t))
        .collect::<Vec<_>>();
    let values_vec = values
        .iter()
        .map(|v| Value::Float(NotNan::new(*v).unwrap()))
        .collect::<Vec<_>>();

    let mut log = BTreeMap::new();
    log.insert("labels".to_owned(), Value::Object(labels_map));
    log.insert("timestamps".to_owned(), Value::Array(timestamps_vec));
    log.insert("values".to_owned(), Value::Array(values_vec));
    LogEvent::from_map(log, EventMetadata::default())
}

fn translate_error(error: tonic::Status) -> State {
    if is_reset(&error) {
        State::RetryNow
    } else {
        emit!(TopSQLPubSubReceiveError { error });
        State::RetryDelay
    }
}

fn is_reset(error: &tonic::Status) -> bool {
    error
        .source()
        .and_then(|source| source.downcast_ref::<hyper::Error>())
        .and_then(|error| error.source())
        .and_then(|source| source.downcast_ref::<h2::Error>())
        .map_or(false, |error| error.is_remote() && error.is_reset())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::sources::tidb_topsql::proto::resource_metering_pubsub::{
        GroupTagRecord, GroupTagRecordItem,
    };
    use crate::test_util::components::{run_and_assert_source_compliance, SOURCE_TAGS};
    use crate::test_util::next_addr;
    use futures::Stream;
    use proto::resource_metering_pubsub::resource_metering_pub_sub_server::{
        ResourceMeteringPubSub, ResourceMeteringPubSubServer,
    };
    use proto::resource_metering_pubsub::ResourceUsageRecord;
    use proto::topsql_pubsub::top_sql_pub_sub_server::{TopSqlPubSub, TopSqlPubSubServer};
    use proto::topsql_pubsub::{
        top_sql_sub_response::RespOneof, PlanMeta, SqlMeta, TopSqlRecord, TopSqlRecordItem,
        TopSqlSubResponse,
    };
    use std::pin::Pin;
    use tonic::{Request, Response, Status};

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<TopSQLPubSubConfig>();
    }

    struct MockTopSqlPubSubServer {}

    #[tonic::async_trait]
    impl TopSqlPubSub for MockTopSqlPubSubServer {
        type SubscribeStream =
            Pin<Box<dyn Stream<Item = Result<TopSqlSubResponse, Status>> + Send + 'static>>;

        async fn subscribe(
            &self,
            _: Request<TopSqlSubRequest>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            Ok(Response::new(Box::pin(stream::iter(vec![
                Ok(TopSqlSubResponse {
                    resp_oneof: Some(RespOneof::Record(TopSqlRecord {
                        sql_digest: b"sql_digest".to_vec(),
                        plan_digest: b"plan_digest".to_vec(),
                        items: vec![TopSqlRecordItem {
                            timestamp_sec: 1655363650,
                            cpu_time_ms: 10,
                            stmt_exec_count: 20,
                            stmt_kv_exec_count: (vec![("127.0.0.1:20180".to_owned(), 10)])
                                .into_iter()
                                .collect(),
                            stmt_duration_sum_ns: 30,
                            stmt_duration_count: 20,
                        }],
                    })),
                }),
                Ok(TopSqlSubResponse {
                    resp_oneof: Some(RespOneof::SqlMeta(SqlMeta {
                        sql_digest: b"sql_digest".to_vec(),
                        normalized_sql: "sql_text".to_owned(),
                        is_internal_sql: false,
                    })),
                }),
                Ok(TopSqlSubResponse {
                    resp_oneof: Some(RespOneof::PlanMeta(PlanMeta {
                        plan_digest: b"plan_digest".to_vec(),
                        normalized_plan: "plan_text".to_owned(),
                    })),
                }),
            ])) as Self::SubscribeStream))
        }
    }

    #[tokio::test]
    async fn test_topsql_scrape_tidb() {
        let address = next_addr();

        tokio::spawn(async move {
            let svc = TopSqlPubSubServer::new(MockTopSqlPubSubServer {});
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve(address)
                .await
                .unwrap();
        });

        // wait for server to start
        tokio::time::sleep(Duration::from_secs(1)).await;

        let config = TopSQLPubSubConfig {
            instance: address.to_string(),
            instance_type: "tidb".to_owned(),
            tls: None,
        };

        let events =
            run_and_assert_source_compliance(config, Duration::from_secs(5), &SOURCE_TAGS).await;
        assert!(!events.is_empty());
    }

    struct MockResourceMeteringPubSubServer {}

    #[tonic::async_trait]
    impl ResourceMeteringPubSub for MockResourceMeteringPubSubServer {
        type SubscribeStream =
            Pin<Box<dyn Stream<Item = Result<ResourceUsageRecord, Status>> + Send + 'static>>;

        async fn subscribe(
            &self,
            _: Request<ResourceMeteringRequest>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            Ok(Response::new(
                Box::pin(stream::iter(vec![Ok(ResourceUsageRecord {
                    record_oneof: Some(RecordOneof::Record(GroupTagRecord {
                        resource_group_tag: ResourceGroupTag {
                            sql_digest: Some(b"sql_digest".to_vec()),
                            plan_digest: Some(b"plan_digest".to_vec()),
                            label: Some(1),
                        }
                        .encode_to_vec(),
                        items: vec![GroupTagRecordItem {
                            timestamp_sec: 1655363650,
                            cpu_time_ms: 10,
                            read_keys: 20,
                            write_keys: 30,
                        }],
                    })),
                })])) as Self::SubscribeStream,
            ))
        }
    }

    #[tokio::test]
    async fn test_topsql_scrape_tikv() {
        let address = next_addr();

        tokio::spawn(async move {
            let svc = ResourceMeteringPubSubServer::new(MockResourceMeteringPubSubServer {});
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve(address)
                .await
                .unwrap();
        });

        // wait for server to start
        tokio::time::sleep(Duration::from_secs(1)).await;

        let config = TopSQLPubSubConfig {
            instance: address.to_string(),
            instance_type: "tikv".to_owned(),
            tls: None,
        };

        let events =
            run_and_assert_source_compliance(config, Duration::from_secs(5), &SOURCE_TAGS).await;
        assert!(!events.is_empty());
    }
}
