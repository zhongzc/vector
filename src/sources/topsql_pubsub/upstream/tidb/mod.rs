mod parser;
pub mod proto;

#[cfg(test)]
pub mod mock_upstream;

use tonic::{transport::Channel, Response, Status, Streaming};

use super::{Upstream, Upstream2};

pub struct TiDBUpstream;

#[async_trait::async_trait]
impl Upstream for TiDBUpstream {
    type Client = proto::top_sql_pub_sub_client::TopSqlPubSubClient<Channel>;
    type UpstreamEvent = proto::TopSqlSubResponse;
    type UpstreamEventParser = parser::TopSqlSubResponseParser;

    fn build_client(channel: Channel) -> Self::Client {
        Self::Client::new(channel)
    }

    async fn build_stream(
        mut client: Self::Client,
    ) -> Result<Response<Streaming<Self::UpstreamEvent>>, Status> {
        client.subscribe(proto::TopSqlSubRequest {}).await
    }
}

impl Upstream2 for TiDBUpstream {
    type Client = tipb::TopSqlPubSubClient;
    type UpstreamEvent = tipb::TopSqlSubResponse;
    type UpstreamEventParser = parser::TopSqlSubResponseParser2;

    fn build_client(channel: grpcio::Channel) -> Self::Client {
        Self::Client::new(channel)
    }

    fn build_stream(
        client: &Self::Client,
    ) -> Result<grpcio::ClientSStreamReceiver<Self::UpstreamEvent>, grpcio::Error> {
        client.subscribe(&tipb::TopSqlSubRequest::new() )
    }
}