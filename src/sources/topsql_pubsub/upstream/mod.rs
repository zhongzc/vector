pub mod parser;
pub mod tidb;
pub mod tikv;

pub trait Upstream: Send {
    type Client: Send;
    type UpstreamEvent: protobuf::Message;
    type UpstreamEventParser: parser::UpstreamEventParser<UpstreamEvent = Self::UpstreamEvent>;

    fn build_client(channel: grpcio::Channel) -> Self::Client;

    fn build_stream(
        client: &Self::Client,
    ) -> Result<grpcio::ClientSStreamReceiver<Self::UpstreamEvent>, grpcio::Error>;
}
