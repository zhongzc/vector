use serde::{Deserialize, Serialize};

use crate::{
    config::{
        self, GenerateConfig, Output, ProxyConfig, SourceConfig, SourceContext, SourceDescription,
    },
    http::{Auth, HttpClient},
    internal_events::{
        EndpointBytesReceived, PrometheusEventsReceived, PrometheusHttpError,
        PrometheusHttpResponseError, PrometheusParseError, RequestCompleted, StreamClosedError,
    },
    shutdown::ShutdownSignal,
    sources,
    tls::{TlsConfig, TlsSettings},
    SourceSender,
};

#[derive(Deserialize, Serialize, Clone, Debug)]
struct TopSQLScrapeConfig {
    instance: String,
    instance_type: String,
    tls: Option<TlsConfig>,
}

impl GenerateConfig for TopSQLScrapeConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            instance: "127.0.0.1:20180".to_owned(),
            instance_type: "tidb".to_owned(),
            tls: None,
        })
        .unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<TopSQLScrapeConfig>();
    }
}
