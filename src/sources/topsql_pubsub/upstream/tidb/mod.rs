mod parser;

#[cfg(test)]
pub mod mock_upstream;

use super::Upstream;

pub struct TiDBUpstream;

impl Upstream for TiDBUpstream {
    type Client = tipb::TopSqlPubSubClient;
    type UpstreamEvent = tipb::TopSqlSubResponse;
    type UpstreamEventParser = parser::TopSqlSubResponseParser;

    fn build_client(channel: grpcio::Channel) -> Self::Client {
        Self::Client::new(channel)
    }

    fn build_stream(
        client: &Self::Client,
    ) -> Result<grpcio::ClientSStreamReceiver<Self::UpstreamEvent>, grpcio::Error> {
        client.subscribe(&tipb::TopSqlSubRequest::new())
    }
}
