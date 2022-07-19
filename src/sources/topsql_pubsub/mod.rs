mod config;
mod consts;
mod upstream;
mod utils;

use std::{marker::PhantomData, time::Duration};

use futures::StreamExt;
use http::uri::InvalidUri;
use snafu::{Error, ResultExt, Snafu};
use tokio_stream::wrappers::IntervalStream;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use vector_core::ByteSizeOf;

use self::{
    upstream::{parser::UpstreamEventParser, Upstream},
    utils::instance_event,
};
use crate::{
    internal_events::{
        BytesReceived, EventsReceived, StreamClosedError, TopSQLPubSubConnectError,
        TopSQLPubSubReceiveError, TopSQLPubSubSubscribeError,
    },
    shutdown::ShutdownSignal,
    SourceSender,
};

#[derive(Debug, Snafu)]
pub enum EndpointError {
    #[snafu(display("Could not create endpoint: {}", source))]
    Endpoint { source: InvalidUri },
    #[snafu(display("Could not set up endpoint TLS settings: {}", source))]
    EndpointTls { source: tonic::transport::Error },
}

pub struct TopSQLSource<U: Upstream> {
    instance: String,
    instance_type: String,
    endpoint: String,
    uri: http::Uri,
    tls: ClientTlsConfig,
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

impl<U: Upstream> TopSQLSource<U> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instance: String,
        instance_type: String,
        endpoint: String,
        uri: http::Uri,
        tls: ClientTlsConfig,
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
            _p: PhantomData::default(),
        }
    }

    async fn run(mut self) -> crate::Result<()> {
        let mut endpoint = Channel::from_shared(self.endpoint.clone()).context(EndpointSnafu)?;
        if self.uri.scheme() != Some(&http::uri::Scheme::HTTP) {
            endpoint = endpoint
                .tls_config(self.tls.clone())
                .context(EndpointTlsSnafu)?;
        }

        loop {
            let state = self.run_once(&endpoint).await;
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

    async fn run_once(&mut self, endpoint: &Endpoint) -> State {
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
        let client = U::build_client(connection);

        let stream = tokio::select! {
            _ = &mut self.shutdown => return State::Shutdown,
            result = U::build_stream(client) => match result {
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
                    Some(Ok(response)) => self.handle_response(response).await,
                    Some(Err(error)) => break translate_error(error),
                    None => break State::RetryNow,
                }
            }
        }
    }

    async fn handle_response(&mut self, response: U::UpstreamEvent) {
        emit!(BytesReceived {
            byte_size: response.size_of(),
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
            instance_type: INSTANCE_TYPE_TIDB.to_owned(),
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
            instance_type: INSTANCE_TYPE_TIKV.to_owned(),
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
            instance_type: INSTANCE_TYPE_TIKV.to_owned(),
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
        tokio::spawn(MockTopSqlPubSubServer::run(address, tls_config));

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
    fn generate_tls() -> (NamedTempFile, String, String) {
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = Certificate::from_params(ca_params).unwrap();

        let server_params = CertificateParams::new(vec!["localhost".to_owned()]);
        let server_cert = Certificate::from_params(server_params).unwrap();
        let server_crt = server_cert.serialize_pem_with_signer(&ca_cert).unwrap();
        let server_key = server_cert.serialize_private_key_pem();

        let mut ca_file = NamedTempFile::new().unwrap();
        write!(ca_file, "{}", ca_cert.serialize_pem().unwrap()).unwrap();
        (ca_file, server_crt, server_key)
    }
}
