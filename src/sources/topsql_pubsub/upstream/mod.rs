pub mod parser;
pub mod tidb;
pub mod tikv;

use tonic::transport::Channel;
use vector_common::byte_size_of::ByteSizeOf;
use grpcio::{Channel as Channel2, ChannelBuilder, ClientSStreamReceiver, Environment};

#[async_trait::async_trait]
pub trait Upstream: Send {
    type Client: Send;
    type UpstreamEvent: ByteSizeOf + Send;
    type UpstreamEventParser: parser::UpstreamEventParser<UpstreamEvent = Self::UpstreamEvent>;

    fn build_client(channel: Channel) -> Self::Client;

    async fn build_stream(
        client: Self::Client,
    ) -> Result<tonic::Response<tonic::codec::Streaming<Self::UpstreamEvent>>, tonic::Status>;
}

pub trait Upstream2: Send {
    type Client: Send;
    type UpstreamEvent: protobuf::Message;
    type UpstreamEventParser: parser::UpstreamEventParser<UpstreamEvent = Self::UpstreamEvent>;

    fn build_client(channel: Channel2) -> Self::Client;

    fn build_stream(
        client: &Self::Client,
    ) -> Result<ClientSStreamReceiver<Self::UpstreamEvent>, grpcio::Error>;
}
