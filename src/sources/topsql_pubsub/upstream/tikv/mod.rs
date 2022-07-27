mod parser;

#[cfg(test)]
pub mod mock_upstream;

use super::Upstream;

pub struct TiKVUpstream;

impl Upstream for TiKVUpstream {
    type Client = kvproto::resource_usage_agent_grpc::ResourceMeteringPubSubClient;
    type UpstreamEvent = kvproto::resource_usage_agent::ResourceUsageRecord;
    type UpstreamEventParser = parser::ResourceUsageRecordParser;

    fn build_client(channel: grpcio::Channel) -> Self::Client {
        Self::Client::new(channel)
    }

    fn build_stream(
        client: &Self::Client,
    ) -> Result<grpcio::ClientSStreamReceiver<Self::UpstreamEvent>, grpcio::Error> {
        client.subscribe(&kvproto::resource_usage_agent::ResourceMeteringRequest::new())
    }
}
