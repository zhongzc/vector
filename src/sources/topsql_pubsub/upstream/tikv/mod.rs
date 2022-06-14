mod parser;

#[cfg(test)]
pub mod mock_upstream;

use std::{fs::read, future::Future, sync::Arc, time::Duration};

use super::Upstream;
use crate::tls::TlsConfig;

pub struct TiKVUpstream;

#[async_trait::async_trait]
impl Upstream for TiKVUpstream {
    type Client = kvproto::resource_usage_agent_grpc::ResourceMeteringPubSubClient;
    type UpstreamEvent = kvproto::resource_usage_agent::ResourceUsageRecord;
    type UpstreamEventParser = parser::ResourceUsageRecordParser;

    async fn build_channel(
        address: &str,
        env: Arc<grpcio::Environment>,
        tls_config: &TlsConfig,
        _: impl Future<Output = ()> + Send + 'static,
    ) -> crate::Result<grpcio::Channel> {
        let channel = {
            let cb = grpcio::ChannelBuilder::new(env);
            if tls_config.ca_file.is_some()
                || tls_config.crt_file.is_some()
                || tls_config.key_file.is_some()
            {
                let credentials = make_credentials(tls_config)?;
                cb.secure_connect(address, credentials)
            } else {
                cb.connect(address)
            }
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
        client.subscribe(&kvproto::resource_usage_agent::ResourceMeteringRequest::new())
    }
}

fn make_credentials(tls_config: &TlsConfig) -> std::io::Result<grpcio::ChannelCredentials> {
    let mut config = grpcio::ChannelCredentialsBuilder::new();
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
