use std::time::Duration;

use futures::TryFutureExt;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

use super::{
    consts::{INSTANCE_TYPE_TIDB, INSTANCE_TYPE_TIKV},
    upstream::{tidb::TiDBUpstream, tikv::TiKVUpstream, Upstream},
    TopSQLSource,
};
use crate::{
    config::{self, GenerateConfig, Output, SourceConfig, SourceContext, SourceDescription},
    sources,
    tls::TlsConfig,
};

#[derive(Debug, Snafu)]
pub enum ConfigError {
    #[snafu(display("Unsupported instance type {:?}", instance_type))]
    UnsupportedInstanceType { instance_type: String },
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TopSQLPubSubConfig {
    pub instance: String,
    pub instance_type: String,
    pub tls: TlsConfig,

    /// The amount of time, in seconds, to wait between retry attempts after an error.
    #[serde(default = "default_retry_delay")]
    pub retry_delay_seconds: f64,
}

pub const fn default_retry_delay() -> f64 {
    1.0
}

inventory::submit! {
    SourceDescription::new::<TopSQLPubSubConfig>("topsql_pubsub")
}

impl GenerateConfig for TopSQLPubSubConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            instance: "127.0.0.1:10080".to_owned(),
            instance_type: "tidb".to_owned(),
            tls: TlsConfig::default(),
            retry_delay_seconds: default_retry_delay(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "topsql_pubsub")]
impl SourceConfig for TopSQLPubSubConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        match self.instance_type.as_str() {
            INSTANCE_TYPE_TIDB => self.run_source::<TiDBUpstream>(cx),
            INSTANCE_TYPE_TIKV => self.run_source::<TiKVUpstream>(cx),
            _ => {
                return Err(ConfigError::UnsupportedInstanceType {
                    instance_type: self.instance_type.clone(),
                }
                .into());
            }
        }
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

impl TopSQLPubSubConfig {
    fn run_source<U: Upstream + 'static>(
        &self,
        cx: SourceContext,
    ) -> crate::Result<sources::Source> {
        let (instance, uri) = self.parse_instance()?;
        let source = TopSQLSource::<U>::new(
            instance,
            self.instance_type.clone(),
            uri,
            self.tls.clone(),
            cx.shutdown,
            cx.out,
            Duration::from_secs_f64(self.retry_delay_seconds),
        )
        .run()
        .map_err(|error| error!(message = "Source failed.", %error));

        Ok(Box::pin(source))
    }

    // return instance and uri
    fn parse_instance(&self) -> crate::Result<(String, http::Uri)> {
        // expect no schema in instance
        let (instance, endpoint) = match (
            self.instance.starts_with("http://"),
            self.instance.starts_with("https://"),
            self.tls.ca_file.is_some()
                || self.tls.crt_file.is_some()
                || self.tls.key_file.is_some(),
        ) {
            (true, _, _) => {
                let instance = self.instance.strip_prefix("http://").unwrap().to_owned();
                let endpoint = self.instance.clone();
                (instance, endpoint)
            }
            (false, true, _) => {
                let instance = self.instance.strip_prefix("https://").unwrap().to_owned();
                let endpoint = self.instance.clone();
                (instance, endpoint)
            }
            (false, false, true) => {
                let instance = self.instance.clone();
                let endpoint = format!("https://{}", self.instance);
                (instance, endpoint)
            }
            (false, false, false) => {
                let instance = self.instance.clone();
                let endpoint = format!("http://{}", self.instance);
                (instance, endpoint)
            }
        };
        let uri = endpoint
            .parse::<http::Uri>()
            .context(sources::UriParseSnafu)?;
        Ok((instance, uri))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<TopSQLPubSubConfig>();
    }
}
