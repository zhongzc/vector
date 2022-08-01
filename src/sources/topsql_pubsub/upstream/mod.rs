use std::{future::Future, sync::Arc};

pub mod parser;
pub mod tidb;
pub mod tikv;

#[async_trait::async_trait]
pub trait Upstream: Send {
    type Client: Send;
    type UpstreamEvent: protobuf::Message;
    type UpstreamEventParser: parser::UpstreamEventParser<UpstreamEvent = Self::UpstreamEvent>;

    async fn build_channel(
        address: &str,
        env: Arc<grpcio::Environment>,
        tls_config: &crate::tls::TlsConfig,
        shutdown_notify: impl Future<Output = ()> + Send + 'static,
    ) -> crate::Result<grpcio::Channel>;

    fn build_client(channel: grpcio::Channel) -> Self::Client;

    fn build_stream(
        client: &Self::Client,
    ) -> Result<grpcio::ClientSStreamReceiver<Self::UpstreamEvent>, grpcio::Error>;
}
