mod parser;
mod proto;

use std::future::Future;
use std::time::Duration;

use futures::{StreamExt, TryFutureExt};
use http::uri::InvalidUri;
use serde::{Deserialize, Serialize};
use snafu::{Error, ResultExt, Snafu};
use tokio_stream::wrappers::IntervalStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use vector_common::byte_size_of::ByteSizeOf;
use vector_core::event::LogEvent;

use self::{
    parser::Parser,
    proto::{
        resource_metering_pubsub::{
            resource_metering_pub_sub_client::ResourceMeteringPubSubClient, ResourceMeteringRequest,
        },
        topsql_pubsub::{top_sql_pub_sub_client::TopSqlPubSubClient, TopSqlSubRequest},
    },
};
use crate::{
    config::{self, GenerateConfig, Output, SourceConfig, SourceContext, SourceDescription},
    internal_events::{
        BytesReceived, EventsReceived, StreamClosedError, TopSQLPubSubConnectError,
        TopSQLPubSubReceiveError, TopSQLPubSubSubscribeError,
    },
    shutdown::ShutdownSignal,
    sources,
    tls::{TlsConfig, TlsSettings},
    SourceSender,
};

#[derive(Debug, Snafu)]
enum ConfigError {
    #[snafu(display("Unsupported instance type {:?}", instance_type))]
    UnsupportedInstanceType { instance_type: String },
    #[snafu(display("Could not create endpoint: {}", source))]
    Endpoint { source: InvalidUri },
    #[snafu(display("Could not set up endpoint TLS settings: {}", source))]
    EndpointTls { source: tonic::transport::Error },
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
    SourceDescription::new::<TopSQLPubSubConfig>("topsql_pubsub")
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

        let (instance, endpoint, uri) = self.parse_instance()?;
        let source = TopSQLSource::new(
            self.instance_type.clone(),
            endpoint,
            uri,
            TlsSettings::from_options(&self.tls)?,
            cx.shutdown,
            cx.out,
            Parser::new(instance, self.instance_type.clone()),
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

impl TopSQLPubSubConfig {
    // return instance, endpoint, and uri
    fn parse_instance(&self) -> crate::Result<(String, String, http::Uri)> {
        let (instance, endpoint) = match (
            self.instance.starts_with("http://"),
            self.instance.starts_with("https://"),
            self.tls.is_some(),
        ) {
            (true, _, _) => {
                let instance = self.instance.strip_prefix("http://").unwrap().to_owned();
                let endpoint = self.instance.clone();
                (instance, endpoint)
            }
            (false, true, _) => {
                let instance = self.instance.strip_prefix("https://").unwrap().to_owned();
                let endpoint = self.instance.clone();
                (instance, endpoint)
            }
            (false, false, true) => {
                let instance = self.instance.clone();
                let endpoint = format!("https://{}", self.instance);
                (instance, endpoint)
            }
            (false, false, false) => {
                let instance = self.instance.clone();
                let endpoint = format!("http://{}", self.instance);
                (instance, endpoint)
            }
        };
        let uri = endpoint
            .parse::<http::Uri>()
            .context(sources::UriParseSnafu)?;
        Ok((instance, endpoint, uri))
    }
}

struct TopSQLSource {
    instance_type: String,
    endpoint: String,
    uri: http::Uri,
    tls: TlsSettings,
    shutdown: ShutdownSignal,
    out: SourceSender,
    parser: Parser,
    retry_delay: Duration,
}

enum State {
    RetryNow,
    RetryDelay,
    Shutdown,
}

impl TopSQLSource {
    #[allow(clippy::too_many_arguments)]
    const fn new(
        instance_type: String,
        endpoint: String,
        uri: http::Uri,
        tls: TlsSettings,
        shutdown: ShutdownSignal,
        out: SourceSender,
        parser: Parser,
        retry_delay: Duration,
    ) -> Self {
        Self {
            instance_type,
            endpoint,
            uri,
            tls,
            shutdown,
            out,
            parser,
            retry_delay,
        }
    }

    async fn run(mut self) -> crate::Result<()> {
        let mut endpoint = Channel::from_shared(self.endpoint.clone()).context(EndpointSnafu)?;
        if self.uri.scheme() != Some(&http::uri::Scheme::HTTP) {
            endpoint = endpoint
                .tls_config(self.make_tls_config())
                .context(EndpointTlsSnafu)?;
        }

        loop {
            let state = match self.instance_type.as_str() {
                "tidb" => self.run_once_tidb(&endpoint).await,
                "tikv" => self.run_once_tikv(&endpoint).await,
                _ => unreachable!(),
            };

            match state {
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
        let parser = self.parser.clone();
        self.run_once(
            endpoint,
            TopSqlPubSubClient::new,
            |mut client| async move { client.subscribe(TopSqlSubRequest {}).await },
            |response| parser.parse_tidb_response(response),
        )
        .await
    }

    async fn run_once_tikv(&mut self, endpoint: &Endpoint) -> State {
        let parser = self.parser.clone();
        self.run_once(
            endpoint,
            ResourceMeteringPubSubClient::new,
            |mut client| async move { client.subscribe(ResourceMeteringRequest {}).await },
            |response| parser.parse_tikv_response(response),
        )
        .await
    }

    async fn run_once<CB, SB, C, FS, R, RC>(
        &mut self,
        endpoint: &Endpoint,
        client_builder: CB,
        stream_builder: SB,
        response_converter: RC,
    ) -> State
    where
        CB: FnOnce(Channel) -> C,
        SB: FnOnce(C) -> FS,
        FS: Future<Output = Result<tonic::Response<tonic::codec::Streaming<R>>, tonic::Status>>,
        RC: Fn(R) -> Vec<LogEvent>,
        R: ByteSizeOf,
    {
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
        let client = client_builder(connection);

        let stream = tokio::select! {
            _ = &mut self.shutdown => return State::Shutdown,
            result = stream_builder(client) => match result {
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
                    Some(Ok(response)) => self.handle_response(response, &response_converter).await,
                    Some(Err(error)) => break translate_error(error),
                    None => break State::RetryNow,
                }
            }
        }
    }

    async fn handle_response<R, RC>(&mut self, response: R, response_converter: &RC)
    where
        RC: Fn(R) -> Vec<LogEvent>,
        R: ByteSizeOf,
    {
        emit!(BytesReceived {
            byte_size: response.size_of(),
            protocol: self.uri.scheme().unwrap().to_string().as_str(),
        });

        let events = response_converter(response);
        let count = events.len();
        emit!(EventsReceived {
            byte_size: events.size_of(),
            count,
        });
        if let Err(error) = self.out.send_batch(events).await {
            emit!(StreamClosedError { error, count })
        }
    }

    async fn handle_instance(&mut self) {
        let event = self.parser.parse_instance();
        if let Err(error) = self.out.send_event(event).await {
            emit!(StreamClosedError { error, count: 1 })
        }
    }

    fn make_tls_config(&self) -> ClientTlsConfig {
        let host = self.uri.host().unwrap_or_default();
        let mut config = ClientTlsConfig::new().domain_name(host);
        if let Some((cert, key)) = self.tls.identity_pem() {
            config = config.identity(Identity::from_pem(cert, key));
        }
        for authority in self.tls.authorities_pem() {
            config = config.ca_certificate(Certificate::from_pem(authority));
        }
        config
    }
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

    use std::io::{self, Write};
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::pin::Pin;

    use futures::Stream;
    use futures_util::stream;
    use prost::Message;
    use rcgen::{BasicConstraints, Certificate, CertificateParams, DnType, IsCa, SanType};
    use tempfile::NamedTempFile;
    use tonic::transport::ServerTlsConfig;
    use tonic::{Request, Response, Status};

    use self::proto::resource_metering_pubsub::{
        resource_metering_pub_sub_server::{ResourceMeteringPubSub, ResourceMeteringPubSubServer},
        resource_usage_record::RecordOneof,
        GroupTagRecord, GroupTagRecordItem, ResourceUsageRecord,
    };
    use self::proto::topsql_pubsub::{
        top_sql_pub_sub_server::{TopSqlPubSub, TopSqlPubSubServer},
        top_sql_sub_response::RespOneof,
        PlanMeta, ResourceGroupTag, SqlMeta, TopSqlRecord, TopSqlRecordItem, TopSqlSubResponse,
    };
    use crate::test_util::components::{run_and_assert_source_compliance, SOURCE_TAGS};
    use crate::test_util::next_addr;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<TopSQLPubSubConfig>();
    }

    #[tokio::test]
    async fn test_topsql_scrape_tidb() {
        let address = next_addr();
        let config = TopSQLPubSubConfig {
            instance: address.to_string(),
            instance_type: "tidb".to_owned(),
            tls: None,
            retry_delay_seconds: default_retry_delay(),
        };

        check_topsql_scrape_tidb(address, config, None).await;
    }

    #[tokio::test]
    async fn test_topsql_scrape_tidb_tls() {
        let address = next_addr();
        let (ca_file, server_crt, server_key) = generate_tls();
        let config = TopSQLPubSubConfig {
            instance: format!("localhost:{}", address.port()),
            instance_type: "tidb".to_owned(),
            tls: Some(TlsConfig {
                ca_file: Some(ca_file.path().to_path_buf()),
                ..Default::default()
            }),
            retry_delay_seconds: default_retry_delay(),
        };

        check_topsql_scrape_tidb(
            address,
            config,
            Some(ServerTlsConfig::new().identity(Identity::from_pem(server_crt, server_key))),
        )
        .await;
    }

    #[tokio::test]
    async fn test_topsql_scrape_tikv() {
        let address = next_addr();
        let config = TopSQLPubSubConfig {
            instance: address.to_string(),
            instance_type: "tikv".to_owned(),
            tls: None,
            retry_delay_seconds: default_retry_delay(),
        };

        check_topsql_scrape_tikv(address, config, None).await;
    }

    #[tokio::test]
    async fn test_topsql_scrape_tikv_tls() {
        let address = next_addr();
        let (ca_file, server_crt, server_key) = generate_tls();
        let config = TopSQLPubSubConfig {
            instance: format!("localhost:{}", address.port()),
            instance_type: "tikv".to_owned(),
            tls: Some(TlsConfig {
                ca_file: Some(ca_file.path().to_path_buf()),
                ..Default::default()
            }),
            retry_delay_seconds: default_retry_delay(),
        };

        check_topsql_scrape_tikv(
            address,
            config,
            Some(ServerTlsConfig::new().identity(Identity::from_pem(server_crt, server_key))),
        )
        .await;
    }

    async fn check_topsql_scrape_tidb(
        address: SocketAddr,
        config: TopSQLPubSubConfig,
        tls_config: Option<ServerTlsConfig>,
    ) {
        tokio::spawn(async move {
            let svc = TopSqlPubSubServer::new(MockTopSqlPubSubServer {});
            let mut sb = tonic::transport::Server::builder();
            if tls_config.is_some() {
                sb = sb.tls_config(tls_config.unwrap()).unwrap();
            }
            sb.add_service(svc).serve(address).await.unwrap();
        });

        // wait for server to start
        tokio::time::sleep(Duration::from_secs(1)).await;

        let events =
            run_and_assert_source_compliance(config, Duration::from_secs(2), &SOURCE_TAGS).await;
        assert!(!events.is_empty());
    }

    async fn check_topsql_scrape_tikv(
        address: SocketAddr,
        config: TopSQLPubSubConfig,
        tls_config: Option<ServerTlsConfig>,
    ) {
        tokio::spawn(async move {
            let svc = ResourceMeteringPubSubServer::new(MockResourceMeteringPubSubServer {});
            let mut sb = tonic::transport::Server::builder();
            if tls_config.is_some() {
                sb = sb.tls_config(tls_config.unwrap()).unwrap();
            }
            sb.add_service(svc).serve(address).await.unwrap();
        });

        // wait for server to start
        tokio::time::sleep(Duration::from_secs(1)).await;

        let events =
            run_and_assert_source_compliance(config, Duration::from_secs(2), &SOURCE_TAGS).await;
        assert!(!events.is_empty());
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
            let dump_record = TopSqlRecord {
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
            };

            let dump_sql_meta = SqlMeta {
                sql_digest: b"sql_digest".to_vec(),
                normalized_sql: "sql_text".to_owned(),
                is_internal_sql: false,
            };

            let dump_plan_meta = PlanMeta {
                plan_digest: b"plan_digest".to_vec(),
                normalized_plan: "plan_text".to_owned(),
            };

            Ok(Response::new(Box::pin(stream::iter(vec![
                Ok(TopSqlSubResponse {
                    resp_oneof: Some(RespOneof::Record(dump_record)),
                }),
                Ok(TopSqlSubResponse {
                    resp_oneof: Some(RespOneof::SqlMeta(dump_sql_meta)),
                }),
                Ok(TopSqlSubResponse {
                    resp_oneof: Some(RespOneof::PlanMeta(dump_plan_meta)),
                }),
            ])) as Self::SubscribeStream))
        }
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

    // generate_tls returns
    // - file `ca_file`
    // - signed-by-ca cert in String `server_crt` and `server_key`
    fn generate_tls() -> (NamedTempFile, String, String) {
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = Certificate::from_params(ca_params).unwrap();

        let mut server_params = CertificateParams::new(vec!["localhost".to_owned()]);
        let server_cert = Certificate::from_params(server_params).unwrap();
        let server_crt = server_cert.serialize_pem_with_signer(&ca_cert).unwrap();
        let server_key = server_cert.serialize_private_key_pem();

        let mut ca_file = NamedTempFile::new().unwrap();
        write!(ca_file, "{}", ca_cert.serialize_pem().unwrap()).unwrap();
        (ca_file, server_crt, server_key)
    }
}
