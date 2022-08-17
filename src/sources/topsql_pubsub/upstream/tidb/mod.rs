mod parser;
pub mod proto;

#[cfg(test)]
pub mod mock_upstream;

use std::future::Future;

use tonic::{
    transport::{Channel, Endpoint},
    Status, Streaming,
};

use super::{tls_proxy, Upstream};
use crate::tls::TlsConfig;

pub struct TiDBUpstream;

#[async_trait::async_trait]
impl Upstream for TiDBUpstream {
    type Client = proto::top_sql_pub_sub_client::TopSqlPubSubClient<Channel>;
    type UpstreamEvent = proto::TopSqlSubResponse;
    type UpstreamEventParser = parser::TopSqlSubResponseParser;

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
            let port = tls_proxy::tls_proxy(tls_config, &address, shutdown_notify).await?;
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
            .subscribe(proto::TopSqlSubRequest {})
            .await
            .map(|r| r.into_inner())
    }
}
