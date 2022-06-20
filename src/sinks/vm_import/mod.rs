use std::io::Write;

use bytes::{BufMut, Bytes, BytesMut};
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::{FutureExt, SinkExt};
use http::{Request, Uri};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use vector_core::config::{AcknowledgementsConfig, Input};
use vector_core::event::Event;

use crate::config::{self, GenerateConfig, SinkConfig, SinkDescription};
use crate::http::HttpClient;
use crate::sinks;
use crate::sinks::util::http::{BatchedHttpSink, HttpEventEncoder, HttpSink};
use crate::sinks::util::{
    BatchConfig, BoxedRawValue, JsonArrayBuffer, SinkBatchSettings,
    TowerRequestConfig,
};
use crate::tls::{TlsConfig, TlsSettings};

#[derive(Clone, Copy, Debug, Default)]
pub struct VMImportDefaultBatchSettings;

impl SinkBatchSettings for VMImportDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(1_000);
    const MAX_BYTES: Option<usize> = None;
    const TIMEOUT_SECS: f64 = 1.0;
}

#[derive(Debug, Deserialize, Serialize)]
pub struct VMImportConfig {
    pub endpoint: String,

    #[serde(default)]
    pub batch: BatchConfig<VMImportDefaultBatchSettings>,
    #[serde(default)]
    pub request: TowerRequestConfig,

    pub tls: Option<TlsConfig>,
}

inventory::submit! {
    SinkDescription::new::<VMImportConfig>("vm_import")
}

impl GenerateConfig for VMImportConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            endpoint: "127.0.0.1:8428/api/v1/import".to_owned(),
            batch: BatchConfig::default(),
            request: TowerRequestConfig::default(),
            tls: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "vm_import")]
impl SinkConfig for VMImportConfig {
    async fn build(
        &self,
        cx: config::SinkContext,
    ) -> crate::Result<(sinks::VectorSink, sinks::Healthcheck)> {
        let endpoint = self.endpoint.parse::<Uri>().context(sinks::UriParseSnafu)?;
        let tls_settings = TlsSettings::from_options(&self.tls)?;
        let batch_settings = self.batch.into_batch_settings()?;
        let request_settings = self.request.unwrap_with(&TowerRequestConfig::default());

        let client = HttpClient::new(tls_settings, cx.proxy())?;

        let healthcheck = healthcheck(endpoint.clone(), client.clone()).boxed();
        let sink = BatchedHttpSink::new(
            VMImportSink { endpoint },
            JsonArrayBuffer::new(batch_settings.size),
            request_settings,
            batch_settings.timeout,
            client,
            cx.acker(),
        )
        .sink_map_err(|e| error!(message = "VM import sink error.", %e));

        Ok((sinks::VectorSink::from_event_sink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn sink_type(&self) -> &'static str {
        "vm_import"
    }

    fn acknowledgements(&self) -> Option<&AcknowledgementsConfig> {
        None
    }
}

async fn healthcheck(endpoint: Uri, client: HttpClient) -> crate::Result<()> {
    let request = http::Request::get(endpoint)
        .body(hyper::Body::empty())
        .unwrap();

    let response = client.send(request).await?;

    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        Err(sinks::HealthcheckError::UnexpectedStatus { status }.into())
    }
}

struct VMImportSinkEventEncoder;

impl HttpEventEncoder<Value> for VMImportSinkEventEncoder {
    fn encode_event(&mut self, event: Event) -> Option<Value> {
        event
            .try_into_log()
            .map(|mut log| {
                let labels = log.remove("labels")?.into_object()?;
                let mut target_metric = serde_json::Map::with_capacity(labels.len());
                for (key, value) in labels {
                    let target_value = Value::String(
                        String::from_utf8_lossy(value.as_bytes()?.as_ref()).to_string(),
                    );
                    target_metric.insert(key, target_value);
                }

                let src_timestamps = log.remove("timestamps")?;
                let timestamps = src_timestamps.as_array()?;
                let mut target_timestamps = Vec::with_capacity(timestamps.len());
                for timestamp in timestamps {
                    let datetime = timestamp.as_timestamp()?;
                    target_timestamps.push(Value::Number(serde_json::Number::from(
                        datetime.timestamp_millis(),
                    )));
                }

                let src_values = log.remove("values")?;
                let values = src_values.as_array()?;
                let mut target_values = Vec::with_capacity(values.len());
                for value in values {
                    let number = value.as_float()?;
                    target_values.push(Value::Number(serde_json::Number::from_f64(
                        *number.as_ref(),
                    )?));
                }

                let mut target_map = serde_json::Map::with_capacity(3);
                target_map.insert(
                    "metric".to_owned(),
                    serde_json::Value::Object(target_metric),
                );
                target_map.insert(
                    "timestamps".to_owned(),
                    serde_json::Value::Array(target_timestamps),
                );
                target_map.insert("values".to_owned(), serde_json::Value::Array(target_values));

                Some(Value::Object(target_map))
            })
            .flatten()
    }
}

#[derive(Clone)]
struct VMImportSink {
    endpoint: Uri,
}

#[async_trait::async_trait]
impl HttpSink for VMImportSink {
    type Input = Value;
    type Output = Vec<BoxedRawValue>;
    type Encoder = VMImportSinkEventEncoder;

    fn build_encoder(&self) -> Self::Encoder {
        VMImportSinkEventEncoder {}
    }

    async fn build_request(&self, events: Self::Output) -> crate::Result<Request<Bytes>> {
        let buffer = BytesMut::new();
        let mut w = GzEncoder::new(buffer.writer(), Compression::default());

        for event in events {
            w.write(event.get().as_bytes())
                .expect("Writing to Vec can't fail");
            w.write(b"\n").expect("Writing to Vec can't fail");
        }
        let body = w
            .finish()
            .expect("Writing to Vec can't fail")
            .into_inner()
            .freeze();

        let builder = Request::post(self.endpoint.clone()).header("Content-Encoding", "gzip");
        let request = builder.body(body).unwrap();

        Ok(request)
    }
}
