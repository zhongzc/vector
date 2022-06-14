mod parser;

#[cfg(test)]
pub mod mock_upstream;

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tokio_openssl::SslStream;
use tracing_futures::Instrument;

use super::Upstream;
use crate::{
    internal_events::TopSQLPubSubConnectError,
    tls::{tls_connector_builder, MaybeTlsSettings, TlsConfig},
};

pub struct TiDBUpstream;

#[async_trait::async_trait]
impl Upstream for TiDBUpstream {
    type Client = tipb::TopSqlPubSubClient;
    type UpstreamEvent = tipb::TopSqlSubResponse;
    type UpstreamEventParser = parser::TopSqlSubResponseParser;

    async fn build_channel(
        address: &str,
        env: Arc<grpcio::Environment>,
        tls_config: &TlsConfig,
        shutdown_notify: impl Future<Output = ()> + Send + 'static,
    ) -> crate::Result<grpcio::Channel> {
        let tls_is_set = tls_config.ca_file.is_some()
            || tls_config.crt_file.is_some()
            || tls_config.key_file.is_some();

        let channel = if !tls_is_set {
            let cb = grpcio::ChannelBuilder::new(env);
            cb.connect(address)
        } else {
            // do proxy
            let port = proxy_tls(tls_config, address, shutdown_notify).await?;
            let cb = grpcio::ChannelBuilder::new(env);
            cb.connect(&format!("127.0.0.1:{}", port))
        };

        let ok = channel.wait_for_connected(Duration::from_secs(2)).await;
        if !ok {
            return Err(format!("failed to connect to {:?}", address).into());
        }

        Ok(channel)
    }

    fn build_client(channel: grpcio::Channel) -> Self::Client {
        Self::Client::new(channel)
    }

    fn build_stream(
        client: &Self::Client,
    ) -> Result<grpcio::ClientSStreamReceiver<Self::UpstreamEvent>, grpcio::Error> {
        client.subscribe(&tipb::TopSqlSubRequest::new())
    }
}

async fn proxy_tls(
    tls_config: &TlsConfig,
    address: &str,
    shutdown_notify: impl Future<Output = ()> + Send + 'static,
) -> crate::Result<u16> {
    let outbound = tls_connect(tls_config, address).await?;
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let local_address = listener.local_addr()?;

    tokio::spawn(
        async move {
            tokio::select! {
                _ = shutdown_notify => {},
                res = accept_and_proxy(listener, outbound) => if let Err(error) = res {
                    emit!(TopSQLPubSubConnectError { error });
                }
            }
        }
        .in_current_span(),
    );

    Ok(local_address.port())
}

async fn tls_connect(tls_config: &TlsConfig, address: &str) -> crate::Result<SslStream<TcpStream>> {
    let raw_stream = TcpStream::connect(address).await?;

    let tls_settings = MaybeTlsSettings::tls_client(&Some(tls_config.clone()))?;
    let config = tls_connector_builder(&tls_settings)?.build().configure()?;
    let uri = address.parse::<http::Uri>()?;
    let ssl = config.into_ssl(uri.host().unwrap_or_default())?;

    let mut stream = SslStream::new(ssl, raw_stream)?;
    Pin::new(&mut stream).connect().await?;

    Ok(stream)
}

async fn accept_and_proxy(
    listener: TcpListener,
    outbound: SslStream<TcpStream>,
) -> crate::Result<()> {
    let (inbound, _) = listener.accept().await?;
    drop(listener);
    transfer(inbound, outbound).await?;
    Ok(())
}

async fn transfer(
    mut inbound: tokio::net::TcpStream,
    outbound: SslStream<TcpStream>,
) -> crate::Result<()> {
    let (mut ri, mut wi) = inbound.split();
    let (mut ro, mut wo) = tokio::io::split(outbound);

    let client_to_server = async {
        tokio::io::copy(&mut ri, &mut wo).await?;
        wo.shutdown().await
    };

    let server_to_client = async {
        tokio::io::copy(&mut ro, &mut wi).await?;
        wi.shutdown().await
    };

    tokio::try_join!(client_to_server, server_to_client)?;

    Ok(())
}
