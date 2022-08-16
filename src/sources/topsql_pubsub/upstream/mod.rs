pub mod parser;
pub mod tidb;
pub mod tikv;

mod tls_proxy;

use std::future::Future;

use tonic::transport::{Channel, Endpoint};
use vector_common::byte_size_of::ByteSizeOf;

#[async_trait::async_trait]
pub trait Upstream: Send {
    type Client: Send;
    type UpstreamEvent: ByteSizeOf + Send;
    type UpstreamEventParser: parser::UpstreamEventParser<UpstreamEvent = Self::UpstreamEvent>;

    async fn build_endpoint(
        address: String,
        tls_config: &crate::tls::TlsConfig,
        shutdown_notify: impl Future<Output = ()> + Send + 'static,
    ) -> crate::Result<Endpoint>;

    fn build_client(channel: Channel) -> Self::Client;

    async fn build_stream(
        client: Self::Client,
    ) -> Result<tonic::codec::Streaming<Self::UpstreamEvent>, tonic::Status>;
}
