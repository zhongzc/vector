mod parser;
mod tidb;
mod tikv;

use std::future::Future;

use tonic::transport::Channel;
use vector_common::byte_size_of::ByteSizeOf;

use self::parser::UpstreamEventParser;

pub const INSTANCE_TYPE_TIDB: &str = "tidb";
pub const INSTANCE_TYPE_TIKV: &str = "tikv";

pub const LABEL_NAME: &str = "__name__";
pub const LABEL_INSTANCE: &str = "instance";
pub const LABEL_INSTANCE_TYPE: &str = "instance_type";
pub const LABEL_SQL_DIGEST: &str = "sql_digest";
pub const LABEL_PLAN_DIGEST: &str = "plan_digest";
pub const LABEL_TAG_LABEL: &str = "tag_label";
pub const LABEL_NORMALIZED_SQL: &str = "normalized_sql";
pub const LABEL_IS_INTERNAL_SQL: &str = "is_internal_sql";
pub const LABEL_NORMALIZED_PLAN: &str = "normalized_plan";

pub const METRIC_NAME_CPU_TIME_MS: &str = "topsql_cpu_time_ms";
pub const METRIC_NAME_READ_KEYS: &str = "topsql_read_keys";
pub const METRIC_NAME_WRITE_KEYS: &str = "topsql_write_keys";
pub const METRIC_NAME_STMT_EXEC_COUNT: &str = "topsql_stmt_exec_count";
pub const METRIC_NAME_STMT_DURATION_SUM_NS: &str = "topsql_stmt_duration_sum_ns";
pub const METRIC_NAME_STMT_DURATION_COUNT: &str = "topsql_stmt_duration_count";
pub const METRIC_NAME_STMT_KV_EXEC_COUNT: &str = "topsql_stmt_kv_exec_count";
pub const METRIC_NAME_SQL_META: &str = "topsql_sql_meta";
pub const METRIC_NAME_PLAN_META: &str = "topsql_plan_meta";
pub const METRIC_NAME_INSTANCE: &str = "topsql_instance";

pub const KV_TAG_LABEL_ROW: &str = "row";
pub const KV_TAG_LABEL_INDEX: &str = "index";
pub const KV_TAG_LABEL_UNKNOWN: &str = "unknown";

inventory::collect!(Box<dyn Source>);

#[async_trait::async_trait]
pub trait Source {
    type Client;
    type UpstreamEvent: ByteSizeOf;
    type UpstreamEventParser: UpstreamEventParser<UpstreamEvent = Self::UpstreamEvent>;

    fn instance_type() -> &'static str;

    fn build_client(channel: Channel) -> Self::Client;

    async fn build_stream(
        client: Self::Client,
    ) -> Result<tonic::Response<tonic::codec::Streaming<Self::UpstreamEvent>>, tonic::Status>;

    fn build_parser() -> Self::UpstreamEventParser;
}
