mod parser;
mod proto;

#[cfg(test)]
pub mod mock_upstream;

use tonic::{transport::Channel, Response, Status, Streaming};

use super::Upstream;

pub struct TiKVUpstream;

#[async_trait::async_trait]
impl Upstream for TiKVUpstream {
    type Client = proto::resource_metering_pub_sub_client::ResourceMeteringPubSubClient<Channel>;
    type UpstreamEvent = proto::ResourceUsageRecord;
    type UpstreamEventParser = parser::ResourceUsageRecordParser;

    fn build_client(channel: Channel) -> Self::Client {
        Self::Client::new(channel)
    }

    async fn build_stream(
        mut client: Self::Client,
    ) -> Result<Response<Streaming<Self::UpstreamEvent>>, Status> {
        client.subscribe(proto::ResourceMeteringRequest {}).await
    }
}
