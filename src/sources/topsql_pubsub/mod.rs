mod config;
mod consts;
mod upstream;
mod utils;

use std::{marker::PhantomData, sync::Arc, time::Duration};

use futures::StreamExt;
use protobuf::Message;
use tokio_stream::wrappers::IntervalStream;
use vector_core::ByteSizeOf;

use self::{
    upstream::{parser::UpstreamEventParser, Upstream},
    utils::{instance_event, notify_pair},
};
use crate::{
    internal_events::{
        BytesReceived, EventsReceived, StreamClosedError, TopSQLPubSubConnectError,
        TopSQLPubSubReceiveError, TopSQLPubSubSubscribeError,
    },
    shutdown::ShutdownSignal,
    tls::TlsConfig,
    SourceSender,
};

pub struct TopSQLSource<U: Upstream> {
    instance: String,
    instance_type: String,
    uri: http::Uri,
    tls: TlsConfig,
    env: Arc<grpcio::Environment>,
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
        uri: http::Uri,
        tls: TlsConfig,
        shutdown: ShutdownSignal,
        out: SourceSender,
        retry_delay: Duration,
    ) -> Self {
        Self {
            instance,
            instance_type,
            uri,
            tls,
            shutdown,
            out,
            retry_delay,
            env: Arc::new(grpcio::Environment::new(2)),
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
        let (_notifier, notified) = notify_pair();
        let channel_fut =
            U::build_channel(&self.instance, Arc::clone(&self.env), &self.tls, notified);

        let channel = tokio::select! {
            channel = channel_fut => match channel {
                Ok(channel) => channel,
                Err(error) => {
                    emit!(TopSQLPubSubConnectError { error });
                    return State::RetryDelay;
                }
            },
            _ = &mut self.shutdown => return State::Shutdown,
        };

        let client = U::build_client(channel);
        let mut response_stream = match U::build_stream(&client) {
            Ok(stream) => stream,
            Err(error) => {
                emit!(TopSQLPubSubSubscribeError { error });
                return State::RetryDelay;
            }
        };

        let mut instance_stream =
            IntervalStream::new(tokio::time::interval(Duration::from_secs(30)));

        loop {
            tokio::select! {
                _ = &mut self.shutdown => break State::Shutdown,
                response = response_stream.next() => {
                    match response {
                        Some(Ok(response)) => self.handle_response(response).await,
                        Some(Err(error)) => {
                            emit!(TopSQLPubSubReceiveError { error });
                            break State::RetryDelay;
                        },
                        None => break State::RetryNow,
                    }
                }
                _ = instance_stream.next() => self.handle_instance().await,
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
}

#[cfg(test)]
mod tests {
    use std::{fs::read, net::SocketAddr};

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
        tls::{TlsConfig, TEST_PEM_CA_PATH, TEST_PEM_CRT_PATH, TEST_PEM_KEY_PATH},
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

    // TiDB uses a very tricky way to serve topsql pubsub which is hard
    // to mock in Rust.
    // #[tokio::test]
    // async fn test_topsql_scrape_tidb_tls() {
    //     let address = next_addr();
    //     let config = TopSQLPubSubConfig {
    //         instance: format!("localhost:{}", address.port()),
    //         instance_type: INSTANCE_TYPE_TIDB.to_owned(),
    //         tls: TlsConfig::test_config(),
    //         retry_delay_seconds: default_retry_delay(),
    //     };
    //
    //     let ca = read(TEST_PEM_CA_PATH).unwrap();
    //     let crt = read(TEST_PEM_CRT_PATH).unwrap();
    //     let key = read(TEST_PEM_KEY_PATH).unwrap();
    //
    //     check_topsql_scrape_tidb(
    //         address,
    //         config,
    //         Some(grpcio::ServerCredentialsBuilder::new()
    //             .root_cert(ca, grpcio::CertificateRequestType::RequestAndRequireClientCertificateButDontVerify)
    //             .add_cert(crt, key)
    //             .build())
    //         )
    //     .await;
    // }

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
        let config = TopSQLPubSubConfig {
            instance: format!("localhost:{}", address.port()),
            instance_type: INSTANCE_TYPE_TIKV.to_owned(),
            tls: TlsConfig::test_config(),
            retry_delay_seconds: default_retry_delay(),
        };

        let ca = read(TEST_PEM_CA_PATH).unwrap();
        let crt = read(TEST_PEM_CRT_PATH).unwrap();
        let key = read(TEST_PEM_KEY_PATH).unwrap();

        check_topsql_scrape_tikv(
            address,
            config,
            Some(grpcio::ServerCredentialsBuilder::new()
                .root_cert(ca, grpcio::CertificateRequestType::RequestAndRequireClientCertificateButDontVerify)
                .add_cert(crt, key)
                .build())
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
            run_and_assert_source_compliance(config, Duration::from_secs(3), &SOURCE_TAGS).await;
        assert!(!events.is_empty());
        server.shutdown().await.unwrap();
    }

    async fn check_topsql_scrape_tikv(
        address: SocketAddr,
        config: TopSQLPubSubConfig,
        credentials: Option<grpcio::ServerCredentials>,
    ) {
        let mut server = MockResourceMeteringPubSubServer::start(address.port(), credentials);

        let events =
            run_and_assert_source_compliance(config, Duration::from_secs(3), &SOURCE_TAGS).await;
        assert!(!events.is_empty());
        server.shutdown().await.unwrap();
    }
}
