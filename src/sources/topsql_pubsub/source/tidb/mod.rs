mod parser;
mod proto;

use crate::sources::topsql_pubsub::source::{Source, INSTANCE_TYPE_TIDB};
use tonic::transport::Channel;
use tonic::{Response, Status, Streaming};

pub struct TiDBSource;

#[async_trait::async_trait]
impl Source for TiDBSource {
    type Client = proto::top_sql_pub_sub_client::TopSqlPubSubClient<Channel>;
    type UpstreamEvent = proto::TopSqlSubResponse;
    type UpstreamEventParser = parser::TopSqlSubResponseParser;

    fn instance_type() -> &'static str {
        INSTANCE_TYPE_TIDB
    }

    fn build_client(channel: Channel) -> Self::Client {
        Self::Client::new(channel)
    }

    async fn build_stream(
        mut client: Self::Client,
    ) -> Result<Response<Streaming<Self::UpstreamEvent>>, Status> {
        async move { client.subscribe(proto::TopSqlSubRequest {}).await }
    }

    fn build_parser() -> Self::UpstreamEventParser {
        parser::TopSqlSubResponseParser
    }
}

inventory::submit! {
    Box::new(TiDBSource) as Box<dyn Source>
}
