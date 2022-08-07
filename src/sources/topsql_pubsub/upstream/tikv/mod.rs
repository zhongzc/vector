#![allow(warnings)]

mod parser;
mod proto;

#[cfg(test)]
pub mod mock_upstream;

use std::{future::Future, pin::Pin};

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tokio_openssl::SslStream;
use tonic::{
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
    Status, Streaming,
};
use tracing::Instrument;

use super::Upstream;
use crate::{
    internal_events::TopSQLPubSubProxyConnectError,
    tls::{tls_connector_builder, MaybeTlsSettings, TlsConfig},
};

pub struct TiKVUpstream;

#[async_trait::async_trait]
impl Upstream for TiKVUpstream {
    type Client = proto::resource_metering_pub_sub_client::ResourceMeteringPubSubClient<Channel>;
    type UpstreamEvent = proto::ResourceUsageRecord;
    type UpstreamEventParser = parser::ResourceUsageRecordParser;

    async fn build_endpoint(
        address: String,
        tls_config: &TlsConfig,
        shutdown_notify: impl Future<Output = ()> + Send + 'static,
    ) -> crate::Result<Endpoint> {
        let tls_is_set = tls_config.ca_file.is_some()
            || tls_config.crt_file.is_some()
            || tls_config.key_file.is_some();

        let endpoint = if !tls_is_set {
            Channel::from_shared(address.clone())?
        } else {
            // do proxy
            let port = proxy_tls(tls_config, &address, shutdown_notify).await?;
            Channel::from_shared(format!("http://127.0.0.1:{}", port))?
        };

        Ok(endpoint)
    }

    fn build_client(channel: Channel) -> Self::Client {
        Self::Client::new(channel)
    }

    async fn build_stream(
        mut client: Self::Client,
    ) -> Result<Streaming<Self::UpstreamEvent>, Status> {
        client
            .subscribe(proto::ResourceMeteringRequest {})
            .await
            .map(|r| r.into_inner())
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
                    emit!(TopSQLPubSubProxyConnectError { error });
                }
            }
        }
        .in_current_span(),
    );

    Ok(local_address.port())
}

async fn tls_connect(tls_config: &TlsConfig, address: &str) -> crate::Result<SslStream<TcpStream>> {
    let uri = address.parse::<http::Uri>()?;
    let host = uri.host().unwrap_or_default();
    let port = uri.port().map(|p| p.as_u16()).unwrap_or(443);

    let raw_stream = TcpStream::connect(format!("{}:{:?}", &host, port)).await?;

    let tls_settings = MaybeTlsSettings::tls_client(&Some(tls_config.clone()))?;
    let mut config_builder = tls_connector_builder(&tls_settings)?;
    config_builder.set_alpn_protos(b"\x02h2")?;

    let config = config_builder.build().configure()?;
    let ssl = config.into_ssl(host)?;

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
