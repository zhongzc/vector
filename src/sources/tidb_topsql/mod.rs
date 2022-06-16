use std::collections::{BTreeMap, BTreeSet};
use std::future::ready;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{
    future::Either,
    stream::{self, select},
    FutureExt, StreamExt,
};
use ordered_float::NotNan;
use prost::Message;
use proto::{
    resource_metering_pubsub::{
        resource_metering_pub_sub_client::ResourceMeteringPubSubClient,
        resource_usage_record::RecordOneof, ResourceMeteringRequest,
    },
    topsql_pubsub::{
        top_sql_pub_sub_client::TopSqlPubSubClient, top_sql_sub_response::RespOneof,
        ResourceGroupTag, TopSqlSubRequest,
    },
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tokio_stream::wrappers::IntervalStream;
use tonic::transport::Channel;
use vector_common::byte_size_of::ByteSizeOf;
use vector_common::internal_event::EventsReceived;

use crate::internal_events::BytesReceived;
use crate::{
    config::{self, GenerateConfig, Output, SourceConfig, SourceContext, SourceDescription},
    event::{EventMetadata, LogEvent, Value},
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
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct TopSQLPubSubConfig {
    instance: String,
    instance_type: String,
    tls: Option<TlsConfig>,
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
        let uri = self
            .instance
            .parse::<http::Uri>()
            .context(sources::UriParseSnafu)?;
        // TODO: support TLS
        let channel = Channel::from_shared(format!("http://{}", uri))?
            .keep_alive_timeout(Duration::from_secs(3))
            .http2_keep_alive_interval(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .connect_lazy();
        Ok(
            TopSQLSource::new(self.clone(), channel, cx.shutdown, cx.out)
                .run()
                .boxed(),
        )
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
    channel: Channel,
    shutdown: ShutdownSignal,
    out: SourceSender,
}

impl TopSQLSource {
    fn new(
        config: TopSQLPubSubConfig,
        channel: Channel,
        shutdown: ShutdownSignal,
        out: SourceSender,
    ) -> Self {
        Self {
            config,
            channel,
            shutdown,
            out,
        }
    }

    async fn run(self) -> Result<(), ()> {
        match self.config.instance_type.as_str() {
            "tidb" => self.scrape_tidb().await,
            "tikv" => self.scrape_tikv().await,
            _ => unreachable!(),
        }
    }

    async fn scrape_tidb(mut self) -> Result<(), ()> {
        let mut client = TopSqlPubSubClient::new(self.channel);
        let record_stream = match client.subscribe(TopSqlSubRequest {}).await {
            Ok(stream) => stream.into_inner().map(|r| Either::Left(r)),
            Err(_) => {
                // TODO: internal error
                return Err(());
            }
        };

        let instance_stream = IntervalStream::new(tokio::time::interval(Duration::from_secs(30)))
            .map(|_| {
                Either::Right((
                    self.config.instance.clone(),
                    self.config.instance_type.clone(),
                ))
            });

        let instance = self.config.instance.clone();
        let instance_type = self.config.instance_type.clone();
        let mut stream = select(record_stream, instance_stream)
            .take_until(self.shutdown)
            .take_while(|item| {
                ready(match item {
                    Either::Left(record) => record.is_ok(),
                    Either::Right(_) => true,
                })
            })
            .filter_map(|item| {
                ready({
                    match item {
                        Either::Left(record) => {
                            let record = record.unwrap();
                            emit!(BytesReceived {
                                byte_size: record.size_of(),
                                protocol: "http", // TODO: or https
                            });
                            record.resp_oneof.map(|r| match r {
                                RespOneof::Record(record) => {
                                    let timestamps = record
                                        .items
                                        .iter()
                                        .map(|item| {
                                            DateTime::<Utc>::from_utc(
                                                NaiveDateTime::from_timestamp(
                                                    item.timestamp_sec as i64,
                                                    0,
                                                ),
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
                                    labels[INSTANCE_INDEX].1 = instance.clone();
                                    labels[INSTANCE_TYPE_INDEX].1 = instance_type.clone();
                                    labels[SQL_DIGEST_INDEX].1 =
                                        hex::encode_upper(record.sql_digest);
                                    labels[PLAN_DIGEST_INDEX].1 =
                                        hex::encode_upper(record.plan_digest);
                                    values.extend(
                                        record.items.iter().map(|item| item.cpu_time_ms as f64),
                                    );
                                    logs.push(make_metric_like_log_event(
                                        &labels,
                                        &timestamps,
                                        &values,
                                    ));

                                    // stmt_exec_count
                                    labels[NAME_INDEX].1 = "stmt_exec_count".to_owned();
                                    values.clear();
                                    values.extend(
                                        record.items.iter().map(|item| item.stmt_exec_count as f64),
                                    );
                                    logs.push(make_metric_like_log_event(
                                        &labels,
                                        &timestamps,
                                        &values,
                                    ));

                                    // stmt_duration_sum_ns
                                    labels[NAME_INDEX].1 = "stmt_duration_sum_ns".to_owned();
                                    values.clear();
                                    values.extend(
                                        record
                                            .items
                                            .iter()
                                            .map(|item| item.stmt_duration_sum_ns as f64),
                                    );
                                    logs.push(make_metric_like_log_event(
                                        &labels,
                                        &timestamps,
                                        &values,
                                    ));

                                    // stmt_duration_count
                                    labels[NAME_INDEX].1 = "stmt_duration_count".to_owned();
                                    values.clear();
                                    values.extend(
                                        record
                                            .items
                                            .iter()
                                            .map(|item| item.stmt_duration_count as f64),
                                    );
                                    logs.push(make_metric_like_log_event(
                                        &labels,
                                        &timestamps,
                                        &values,
                                    ));

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
                                                .unwrap_or_default()
                                                as f64
                                        }));
                                        logs.push(make_metric_like_log_event(
                                            &labels,
                                            &timestamps,
                                            &values,
                                        ));
                                    }

                                    emit!(EventsReceived {
                                        count: logs.len(),
                                        byte_size: logs.size_of(),
                                    });
                                    stream::iter(logs)
                                }
                                RespOneof::SqlMeta(sql_meta) => {
                                    let logs = vec![make_metric_like_log_event(
                                        &[
                                            ("__name__", "sql_meta".to_owned()),
                                            ("sql_digest", hex::encode_upper(sql_meta.sql_digest)),
                                            ("normalized_sql", sql_meta.normalized_sql),
                                            (
                                                "is_internal_sql",
                                                sql_meta.is_internal_sql.to_string(),
                                            ),
                                        ],
                                        &[Utc::now()],
                                        &[1.0],
                                    )];

                                    emit!(EventsReceived {
                                        count: logs.len(),
                                        byte_size: logs.size_of(),
                                    });
                                    stream::iter(logs)
                                }
                                RespOneof::PlanMeta(plan_meta) => {
                                    let logs = vec![make_metric_like_log_event(
                                        &[
                                            ("__name__", "plan_meta".to_owned()),
                                            (
                                                "plan_digest",
                                                hex::encode_upper(plan_meta.plan_digest),
                                            ),
                                            ("normalized_plan", plan_meta.normalized_plan),
                                        ],
                                        &[Utc::now()],
                                        &[1.0],
                                    )];

                                    emit!(EventsReceived {
                                        count: logs.len(),
                                        byte_size: logs.size_of(),
                                    });
                                    stream::iter(logs)
                                }
                            })
                        }
                        Either::Right((instance, instance_type)) => {
                            Some(stream::iter(vec![make_metric_like_log_event(
                                &[
                                    ("__name__", "instance".to_owned()),
                                    ("instance", instance),
                                    ("instance_type", instance_type),
                                ],
                                &[Utc::now()],
                                &[1.0],
                            )]))
                        }
                    }
                })
            })
            .flatten();

        match self.out.send_event_stream(&mut stream).await {
            Ok(()) => {
                info!("Finished sending.");
                Ok(())
            }
            Err(_) => Err(()),
        }
    }

    async fn scrape_tikv(mut self) -> Result<(), ()> {
        let mut client = ResourceMeteringPubSubClient::new(self.channel);
        let record_stream = match client.subscribe(ResourceMeteringRequest {}).await {
            Ok(stream) => stream.into_inner().map(|r| Either::Left(r)),
            Err(_) => {
                // TODO: internal error
                return Err(());
            }
        };

        let instance_stream = IntervalStream::new(tokio::time::interval(Duration::from_secs(30)))
            .map(|_| {
                Either::Right((
                    self.config.instance.clone(),
                    self.config.instance_type.clone(),
                ))
            });

        let instance = self.config.instance.clone();
        let instance_type = self.config.instance_type.clone();
        let mut stream = select(record_stream, instance_stream)
            .take_until(self.shutdown)
            .take_while(|item| {
                ready(match item {
                    Either::Left(record) => record.is_ok(),
                    Either::Right(_) => true,
                })
            })
            .filter_map(|item| {
                ready({
                    match item {
                        Either::Left(record) => {
                            let record = record.unwrap();
                            emit!(BytesReceived {
                                byte_size: record.size_of(),
                                protocol: "http", // TODO: or https
                            });
                            record.record_oneof.map(|r| match r {
                                RecordOneof::Record(record) => {
                                    let (sql_digest, plan_digest, tag_label) =
                                        match ResourceGroupTag::decode(
                                            record.resource_group_tag.as_slice(),
                                        ) {
                                            Ok(resource_tag) => {
                                                if resource_tag.sql_digest.is_none() {
                                                    return stream::iter(vec![]);
                                                }
                                                (
                                                    hex::encode_upper(
                                                        resource_tag.sql_digest.unwrap_or(vec![]),
                                                    ),
                                                    hex::encode_upper(
                                                        resource_tag.plan_digest.unwrap_or(vec![]),
                                                    ),
                                                    match resource_tag.label {
                                                        Some(1) => "row".to_owned(),
                                                        Some(2) => "index".to_owned(),
                                                        _ => "unknown".to_owned(),
                                                    },
                                                )
                                            }
                                            Err(_) => return stream::iter(vec![]),
                                        };
                                    let timestamps = record
                                        .items
                                        .iter()
                                        .map(|item| {
                                            DateTime::<Utc>::from_utc(
                                                NaiveDateTime::from_timestamp(
                                                    item.timestamp_sec as i64,
                                                    0,
                                                ),
                                                Utc,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    let mut labels = vec![
                                        ("__name__", String::new()),
                                        ("instance", instance.clone()),
                                        ("instance_type", instance_type.clone()),
                                        ("sql_digest", sql_digest),
                                        ("plan_digest", plan_digest),
                                        ("tag_label", tag_label),
                                    ];
                                    let mut values = vec![];

                                    let mut logs = vec![];

                                    // cpu_time_ms
                                    labels[0].1 = "cpu_time_ms".to_owned();
                                    values.extend(
                                        record.items.iter().map(|item| item.cpu_time_ms as f64),
                                    );
                                    logs.push(make_metric_like_log_event(
                                        &labels,
                                        &timestamps,
                                        &values,
                                    ));

                                    // read_keys
                                    labels[0].1 = "read_keys".to_owned();
                                    values.clear();
                                    values.extend(
                                        record.items.iter().map(|item| item.read_keys as f64),
                                    );
                                    logs.push(make_metric_like_log_event(
                                        &labels,
                                        &timestamps,
                                        &values,
                                    ));

                                    // write_keys
                                    labels[0].1 = "write_keys".to_owned();
                                    values.clear();
                                    values.extend(
                                        record.items.iter().map(|item| item.write_keys as f64),
                                    );
                                    logs.push(make_metric_like_log_event(
                                        &labels,
                                        &timestamps,
                                        &values,
                                    ));

                                    emit!(EventsReceived {
                                        count: logs.len(),
                                        byte_size: logs.size_of(),
                                    });
                                    stream::iter(logs)
                                }
                            })
                        }
                        Either::Right((instance, instance_type)) => {
                            Some(stream::iter(vec![make_metric_like_log_event(
                                &[
                                    ("__name__", "instance".to_owned()),
                                    ("instance", instance),
                                    ("instance_type", instance_type),
                                ],
                                &[Utc::now()],
                                &[1.0],
                            )]))
                        }
                    }
                })
            })
            .flatten();

        match self.out.send_event_stream(&mut stream).await {
            Ok(()) => {
                info!("Finished sending.");
                Ok(())
            }
            Err(_) => Err(()),
        }
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
