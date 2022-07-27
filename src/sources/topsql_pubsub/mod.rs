mod config;
mod consts;
mod upstream;
mod utils;

use std::{marker::PhantomData, time::Duration};

use futures::StreamExt;
use http::uri::InvalidUri;
use snafu::{Error, ResultExt, Snafu};
use tokio_stream::wrappers::IntervalStream;
use vector_core::ByteSizeOf;
use protobuf::Message;

use self::{
    upstream::{parser::UpstreamEventParser, Upstream, Upstream2},
    utils::instance_event,
};
use crate::{
    internal_events::{
        BytesReceived, EventsReceived, StreamClosedError,
        TopSQLPubSubReceiveError, TopSQLPubSubSubscribeError, TopSQLPubSubInitTLSError,
    },
    shutdown::ShutdownSignal,
    SourceSender,
};
use grpcio::{ChannelBuilder, Environment, ChannelCredentials, Channel, ChannelCredentialsBuilder};
use std::sync::Arc;
use crate::tls::TlsConfig;
use std::fs::read;

#[derive(Debug, Snafu)]
pub enum EndpointError {
    #[snafu(display("Could not create endpoint: {}", source))]
    Endpoint { source: InvalidUri },
    #[snafu(display("Could not set up endpoint TLS settings: {}", source))]
    EndpointTls { source: tonic::transport::Error },
}

pub struct TopSQLSource<U: Upstream2> {
    instance: String,
    instance_type: String,
    endpoint: String,
    uri: http::Uri,
    tls: TlsConfig,
    env: Arc<Environment>,
    shutdown: ShutdownSignal,
    out: SourceSender,
    retry_delay: Duration,
    _p: PhantomData<U>,
}

enum State {
    RetryNow,
    RetryDelay,
    Shutdown,
}

impl<U: Upstream2> TopSQLSource<U> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instance: String,
        instance_type: String,
        endpoint: String,
        uri: http::Uri,
        tls: TlsConfig,
        shutdown: ShutdownSignal,
        out: SourceSender,
        retry_delay: Duration,
    ) -> Self {
        Self {
            instance,
            instance_type,
            endpoint,
            uri,
            tls,
            shutdown,
            out,
            retry_delay,
            env: Arc::new(Environment::new(2)),
            _p: PhantomData::default(),
        }
    }

    async fn run(mut self) -> crate::Result<()> {
        loop {
            let state = self.run_once().await;
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

    async fn run_once(&mut self) -> State {
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone());
            cb.connect(&self.instance)
            // if self.uri.scheme() == Some(&http::uri::Scheme::HTTPS) {
            //     println!("https {:?}", &self.instance);
            //     let credentials = match self.make_credentials(&self.tls) {
            //         Ok(credentials) => credentials,
            //         Err(error) => {
            //             emit!(TopSQLPubSubInitTLSError { error });
            //             return State::Shutdown;
            //         }
            //     };
            //     cb.secure_connect(&self.instance, credentials)
            // } else {
            //     cb.connect(&self.instance)
            // }
        };

        println!("456");
        let client = U::build_client(channel);
        let mut response_stream = match U::build_stream(&client)  {
            Ok(stream) => stream,
            Err(error) => {
                emit!(TopSQLPubSubSubscribeError { error });
                return State::RetryDelay;
            }
        };

        let mut instance_stream =
            IntervalStream::new(tokio::time::interval(Duration::from_secs(30)));

        println!("789");
        loop {
            tokio::select! {
                _ = &mut self.shutdown => break State::Shutdown,
                _ = instance_stream.next() => self.handle_instance().await,
                response = response_stream.next() => {
                    println!("abc");
                    match response {
                        Some(Ok(response)) => self.handle_response(response).await,
                        Some(Err(error)) => {
                            emit!(TopSQLPubSubReceiveError { error });
                            break State::RetryDelay;
                        },
                        None => break State::RetryNow,
                    }
                }
            }
        }
    }

    async fn handle_response(&mut self, response: U::UpstreamEvent) {
        emit!(BytesReceived {
            byte_size: response.compute_size() as usize,
            protocol: self.uri.scheme().unwrap().to_string().as_str(),
        });

        let events = U::UpstreamEventParser::parse(response, self.instance.clone());
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
        let event = instance_event(self.instance.clone(), self.instance_type.clone());
        if let Err(error) = self.out.send_event(event).await {
            emit!(StreamClosedError { error, count: 1 })
        }
    }

    fn make_credentials(&self, tls_config: &TlsConfig) -> std::io::Result<ChannelCredentials> {
        let mut config = ChannelCredentialsBuilder::new();
        if let Some(ca_file) = tls_config.ca_file.as_ref() {
            let ca_content = read(ca_file)?;
            config = config.root_cert(ca_content);
        }
        if let (Some(crt_file), Some(key_file)) =
        (tls_config.crt_file.as_ref(), tls_config.key_file.as_ref())
        {
            let crt_content = read(crt_file)?;
            let key_content = read(key_file)?;
            config = config.cert(crt_content, key_content);
        }
        Ok(config.build())
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, net::SocketAddr};

    use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa};
    use tempfile::NamedTempFile;
    use tonic::transport::{Identity, ServerTlsConfig};

    use self::{
        config::{default_retry_delay, TopSQLPubSubConfig},
        consts::{INSTANCE_TYPE_TIDB, INSTANCE_TYPE_TIKV},
        upstream::{
            tidb::mock_upstream::MockTopSqlPubSubServer,
            tikv::mock_upstream::MockResourceMeteringPubSubServer,
        },
    };
    use super::*;
    use crate::{
        test_util::{
            components::{run_and_assert_source_compliance, SOURCE_TAGS},
            next_addr,
        },
        tls::TlsConfig,
    };

    #[tokio::test]
    async fn test_topsql_scrape_tidb() {
        let address = next_addr();
        let config = TopSQLPubSubConfig {
            instance: address.to_string(),
            instance_type: INSTANCE_TYPE_TIDB.to_owned(),
            tls: TlsConfig::default(),
            retry_delay_seconds: default_retry_delay(),
        };

        check_topsql_scrape_tidb(address, config, None).await;
    }

    #[tokio::test]
    async fn test_topsql_scrape_tidb_tls() {
        let address = next_addr();
        let (ca_file, crt_file, key_file, ca_crt,  server_crt, server_key) = generate_tls();
        let config = TopSQLPubSubConfig {
            instance: format!("localhost:{}", address.port()),
            instance_type: INSTANCE_TYPE_TIDB.to_owned(),
            tls: TlsConfig {
                ca_file: Some(ca_file.path().to_path_buf()),
                crt_file: Some(crt_file.path().to_path_buf()),
                key_file: Some(key_file.path().to_path_buf()),
                ..Default::default()
            },
            retry_delay_seconds: default_retry_delay(),
        };

        check_topsql_scrape_tidb(
            address,
            config,
            Some(grpcio::ServerCredentialsBuilder::new()
                // .root_cert(ca_crt.into_bytes(), grpcio::CertificateRequestType::DontRequestClientCertificate)
                .add_cert(server_crt.into_bytes(), server_key.into_bytes())
                .build())
            )
        .await;
    }

    #[tokio::test]
    async fn test_topsql_scrape_tikv() {
        let address = next_addr();
        let config = TopSQLPubSubConfig {
            instance: address.to_string(),
            instance_type: INSTANCE_TYPE_TIKV.to_owned(),
            tls: TlsConfig::default(),
            retry_delay_seconds: default_retry_delay(),
        };

        check_topsql_scrape_tikv(address, config, None).await;
    }

    #[tokio::test]
    async fn test_topsql_scrape_tikv_tls() {
        let address = next_addr();
        let (ca_file, crt_file, key_file, ca_crt,  server_crt, server_key) = generate_tls();
        let config = TopSQLPubSubConfig {
            instance: format!("localhost:{}", address.port()),
            instance_type: INSTANCE_TYPE_TIKV.to_owned(),
            tls: TlsConfig {
                ca_file: Some(ca_file.path().to_path_buf()),
                crt_file: Some(crt_file.path().to_path_buf()),
                key_file: Some(key_file.path().to_path_buf()),
                ..Default::default()
            },
            retry_delay_seconds: default_retry_delay(),
        };

        check_topsql_scrape_tikv(
            address,
            config,
            Some(ServerTlsConfig::new().identity(Identity::from_pem(server_crt, server_key)).client_ca_root(tonic::transport::Certificate::from_pem(ca_crt))),
        )
        .await;
    }

    async fn check_topsql_scrape_tidb(
        address: SocketAddr,
        config: TopSQLPubSubConfig,
        credentials: Option<grpcio::ServerCredentials>,
    ) {
        let mut server = MockTopSqlPubSubServer::start(address.port(), credentials);

        let events =
            run_and_assert_source_compliance(config, Duration::from_secs(2), &SOURCE_TAGS).await;
        assert!(!events.is_empty());
        server.shutdown().await.unwrap();
    }

    async fn check_topsql_scrape_tikv(
        address: SocketAddr,
        config: TopSQLPubSubConfig,
        tls_config: Option<ServerTlsConfig>,
    ) {
        tokio::spawn(MockResourceMeteringPubSubServer::run(address, tls_config));

        // wait for server to start
        tokio::time::sleep(Duration::from_secs(1)).await;

        let events =
            run_and_assert_source_compliance(config, Duration::from_secs(2), &SOURCE_TAGS).await;
        assert!(!events.is_empty());
    }

    // generate_tls returns
    // - file `ca_file`
    // - signed-by-ca cert in String `server_crt` and `server_key`
    fn generate_tls() -> (NamedTempFile, NamedTempFile, NamedTempFile, String, String, String) {
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = Certificate::from_params(ca_params).unwrap();

        let server_params = CertificateParams::new(vec!["localhost".to_owned()]);
        let server_cert = Certificate::from_params(server_params).unwrap();
        let server_crt = server_cert.serialize_pem_with_signer(&ca_cert).unwrap();
        let server_key = server_cert.serialize_private_key_pem();

        let ca_crt =  ca_cert.serialize_pem().unwrap();

        let mut ca_file = NamedTempFile::new().unwrap();
        write!(ca_file, "{}", ca_crt).unwrap();
        let mut crt_file = NamedTempFile::new().unwrap();
        write!(crt_file, "{}", server_crt).unwrap();
        let mut key_file = NamedTempFile::new().unwrap();
        write!(key_file, "{}", server_key).unwrap();
        (ca_file, crt_file, key_file, ca_crt, server_crt, server_key)
    }
}
