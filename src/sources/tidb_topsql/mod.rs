use futures::{stream, FutureExt, StreamExt, TryFutureExt};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

use crate::{
    config::{
        self, GenerateConfig, Output, ProxyConfig, SourceConfig, SourceContext, SourceDescription,
    },
    http::{Auth, HttpClient},
    internal_events::{EndpointBytesReceived, RequestCompleted, StreamClosedError},
    shutdown::ShutdownSignal,
    sources,
    tls::{TlsConfig, TlsSettings},
    SourceSender,
};

#[allow(clippy::clone_on_ref_ptr)]
mod proto {
    mod resource_metering_pubsub {
        include!(concat!(env!("OUT_DIR"), "/resource_usage_agent.rs"));
    }
    mod topsql_pubsub {
        include!(concat!(env!("OUT_DIR"), "/tipb.rs"));
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct TopSQLPubSubConfig {
    instance: String,
    instance_type: String,
    tls: Option<TlsConfig>,
}

impl GenerateConfig for TopSQLPubSubConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            instance: "127.0.0.1:20180".to_owned(),
            instance_type: "tidb".to_owned(),
            tls: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "topsql_pubsub")]
impl SourceConfig for TopSQLPubSubConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        let url = self
            .instance
            .parse::<http::Uri>()
            .context(sources::UriParseSnafu)?;
        let tls = TlsSettings::from_options(&self.tls)?;
        Ok(topsql(
            self.clone(),
            url,
            tls,
            cx.proxy.clone(),
            cx.shutdown,
            cx.out,
        )
        .boxed())
    }

    fn outputs(&self) -> Vec<Output> {
        vec![Output::default(config::DataType::Log)]
    }

    fn source_type(&self) -> &'static str {
        "topsql_pubsub"
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

async fn topsql(
    config: TopSQLPubSubConfig,
    urls: http::Uri,
    tls: TlsSettings,
    proxy: ProxyConfig,
    shutdown: ShutdownSignal,
    mut out: SourceSender,
) -> Result<(), ()> {
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<TopSQLPubSubConfig>();
    }
}
