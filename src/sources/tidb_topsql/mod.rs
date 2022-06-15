use std::collections::BTreeMap;
use std::future::ready;
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{
    future::Either,
    stream::{self, select},
    FutureExt, StreamExt, TryFutureExt,
};
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tokio_stream::wrappers::IntervalStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::{
    config::{
        self, GenerateConfig, Output, ProxyConfig, SourceConfig, SourceContext, SourceDescription,
    },
    event::{LogEvent, Value},
    http::{Auth, HttpClient},
    internal_events::{EndpointBytesReceived, RequestCompleted, StreamClosedError},
    shutdown::ShutdownSignal,
    sources,
    tls::{TlsConfig, TlsSettings},
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

use crate::event::EventMetadata;
use crate::sources::tidb_topsql::proto::topsql_pubsub::top_sql_sub_response::RespOneof;
use crate::sources::tidb_topsql::proto::topsql_pubsub::TopSqlSubResponse;
use proto::resource_metering_pubsub::resource_metering_pub_sub_client;
use proto::topsql_pubsub::{top_sql_pub_sub_client::TopSqlPubSubClient, TopSqlSubRequest};
use tonic::{Response, Status, Streaming};

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

    async fn run(mut self) -> Result<(), ()> {
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
                                RespOneof::Record(record) => {}
                                RespOneof::SqlMeta(sql_meta) => {}
                                RespOneof::PlanMeta(plan_meta) => {}
                            });
                            let log = BTreeMap::new();
                            Some(LogEvent::from_map(log, EventMetadata::default()))
                        }
                        Either::Right((instance, instance_type)) => {
                            Some(make_metric_like_log_event(
                                &[
                                    ("__name__", "instance".to_owned()),
                                    ("instance", instance),
                                    ("instance_type", instance_type),
                                ],
                                &[Utc::now()],
                                &[1.0],
                            ))
                        }
                    }
                })
            });

        match self.out.send_event_stream(&mut stream).await {
            Ok(()) => {
                info!("Finished sending.");
                Ok(())
            }
            Err(_) => Err(()),
        }
    }

    async fn scrape_tikv(mut self) -> Result<(), ()> {
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
