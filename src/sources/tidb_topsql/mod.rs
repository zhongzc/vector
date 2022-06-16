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
use proto::topsql_pubsub::{
    top_sql_pub_sub_client::TopSqlPubSubClient, top_sql_sub_response::RespOneof, TopSqlSubRequest,
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tokio_stream::wrappers::IntervalStream;
use tonic::transport::Channel;

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
    }
    pub mod resource_metering_pubsub {
        include!(concat!(env!("OUT_DIR"), "/resource_usage_agent.rs"));
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
        let channel = Channel::from_shared(uri.to_string())?
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

                                    stream::iter(logs)
                                }
                                RespOneof::SqlMeta(sql_meta) => {
                                    stream::iter(vec![make_metric_like_log_event(
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
                                    )])
                                }
                                RespOneof::PlanMeta(plan_meta) => {
                                    stream::iter(vec![make_metric_like_log_event(
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
                                    )])
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

    async fn scrape_tikv(self) -> Result<(), ()> {
        Ok(())
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

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<TopSQLPubSubConfig>();
    }
}
