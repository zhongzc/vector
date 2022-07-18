mod parser;
mod proto;

use tonic::transport::Channel;
use tonic::{Response, Status, Streaming};

use super::{Source, INSTANCE_TYPE_TIKV};

pub struct TiKVSource;

#[async_trait::async_trait]
impl Source for TiKVSource {
    type Client = proto::resource_metering_pub_sub_client::ResourceMeteringPubSubClient<Channel>;
    type UpstreamEvent = proto::ResourceUsageRecord;
    type UpstreamEventParser = parser::ResourceUsageRecordParser;

    fn instance_type() -> &'static str {
        INSTANCE_TYPE_TIKV
    }

    fn build_client(channel: Channel) -> Self::Client {
        Self::Client::new(channel)
    }

    async fn build_stream(
        mut client: Self::Client,
    ) -> Result<Response<Streaming<Self::UpstreamEvent>>, Status> {
        async move { client.subscribe(proto::ResourceMeteringRequest {}).await }
    }

    fn build_parser() -> Self::UpstreamEventParser {
        parser::ResourceUsageRecordParser
    }
}

inventory::submit! {
    Box::new(TiKVSource) as Box<dyn Source>
}
