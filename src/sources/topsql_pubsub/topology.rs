use crate::config::ProxyConfig;
use crate::http::{build_tls_connector, HttpClient, HttpError};
use crate::tls::TlsConfig;
use crate::tls::{MaybeTlsSettings, TlsSettings};
use hyper::Uri;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::collections::HashSet;
use std::fs::read;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

#[derive(Debug, Copy, Clone)]
pub enum InstanceType {
    Unknown,
    PD,
    TiDB,
    TiKV,
    TiFlash,
}

#[derive(Debug, Clone)]
pub struct Component {
    pub instance_type: InstanceType,
    pub host: String,
    pub primary_port: u16,
    pub secondary_port: u16,
}

impl Component {
    pub fn topsql_address(&self) -> Option<String> {
        match self.instance_type {
            InstanceType::TiDB => Some(format!("{}:{}", self.host, self.secondary_port)),
            InstanceType::TiKV => Some(format!("{}:{}", self.host, self.primary_port)),
            _ => None,
        }
    }
}

#[derive(Debug, Snafu)]
pub enum TopologyError {
    #[snafu(display("Failed to build TLS settings: {}", source))]
    BuildTlsSettings { source: crate::tls::TlsError },
    #[snafu(display("Failed to read ca file: {}", source))]
    ReadCaFile { source: std::io::Error },
    #[snafu(display("Failed to read crt file: {}", source))]
    ReadCrtFile { source: std::io::Error },
    #[snafu(display("Failed to read key file: {}", source))]
    ReadKeyFile { source: std::io::Error },

    #[snafu(display("Failed to build request: {}", source))]
    BuildRequest { source: http::Error },
    #[snafu(display("Failed to build HTTP client: {}", source))]
    BuildHttpClient { source: HttpError },
    #[snafu(display("Failed to build etcd client: {}", source))]
    BuildEtcdClient { source: etcd_client::Error },

    // PD
    #[snafu(display("Failed to get health: {}", source))]
    GetHealth { source: HttpError },
    #[snafu(display("Failed to get health text: {}", source))]
    GetHealthBytes { source: hyper::Error },
    #[snafu(display("Failed to parse health JSON text: {}", source))]
    HealthJsonFromStr { source: serde_json::Error },
    #[snafu(display("Failed to get members: {}", source))]
    GetMembers { source: HttpError },
    #[snafu(display("Failed to get members text: {}", source))]
    GetMembersBytes { source: hyper::Error },
    #[snafu(display("Failed to parse members JSON text: {}", source))]
    MembersJsonFromStr { source: serde_json::Error },

    // TiDB
    #[snafu(display("Failed to get topology: {}", source))]
    GetTopology { source: etcd_client::Error },
    #[snafu(display("Failed to read etcd key: {}", source))]
    ReadEtcdKey { source: etcd_client::Error },
    #[snafu(display("Failed to read etcd value: {}", source))]
    ReadEtcdValue { source: etcd_client::Error },
    #[snafu(display("Missing address in etcd key: {}", key))]
    MissingAddress { key: String },
    #[snafu(display("Missing type in etcd key: {}", key))]
    MissingType { key: String },
    #[snafu(display("Failed to parse ttl: {}", source))]
    ParseTTL { source: std::num::ParseIntError },
    #[snafu(display("Time drift occurred: {}", source))]
    TimeDrift { source: SystemTimeError },
    #[snafu(display("Failed to parse topology value Json text: {}", source))]
    TopologyValueJsonFromStr { source: serde_json::Error },

    // TiKV & TiFlash
    #[snafu(display("Failed to get stores: {}", source))]
    GetStores { source: HttpError },
    #[snafu(display("Failed to get stores text: {}", source))]
    GetStoresBytes { source: hyper::Error },
    #[snafu(display("Failed to parse stores JSON text: {}", source))]
    StoresJsonFromStr { source: serde_json::Error },

    #[snafu(display("Missing host in address: {}", address))]
    MissingHost { address: String },
    #[snafu(display("Missing port in address: {}", address))]
    MissingPort { address: String },
    #[snafu(display("Failed to parse address: {}", source))]
    ParseAddress { source: http::uri::InvalidUri },
}

pub async fn fetch_up_components(
    mut pd_address: String,
    tls_config: Option<TlsConfig>,
) -> Result<Vec<Component>, TopologyError> {
    let uri: Uri = pd_address.parse().context(ParseAddressSnafu)?;
    if uri.scheme().is_none() {
        if tls_config.is_some() {
            pd_address = format!("https://{}", pd_address);
        } else {
            pd_address = format!("http://{}", pd_address);
        }
    }

    if pd_address.chars().last() == Some('/') {
        pd_address.pop();
    }

    let proxy = ProxyConfig::from_env();
    let client = HttpClient::new(
        MaybeTlsSettings::tls_client(&tls_config).context(BuildTlsSettingsSnafu)?,
        &proxy,
    )
    .context(BuildHttpClientSnafu)?;

    let mut components = Vec::new();
    fetch_up_pds(&client, &pd_address, &mut components).await?;
    fetch_up_tidbs(&tls_config, &pd_address, &mut components).await?;
    fetch_up_stores(&client, &pd_address, &mut components).await?;

    Ok(components)
}

pub async fn fetch_up_pds(
    client: &HttpClient<hyper::Body>,
    pd_address: &str,
    components: &mut Vec<Component>,
) -> Result<(), TopologyError> {
    let req = http::Request::get(format!("{}/pd/api/v1/health", pd_address))
        .body(hyper::Body::empty())
        .context(BuildRequestSnafu)?;

    let res = client.send(req).await.context(GetHealthSnafu)?;

    let body = res.into_body();
    let bytes = hyper::body::to_bytes(body)
        .await
        .context(GetHealthBytesSnafu)?;

    let health =
        serde_json::from_slice::<HealthResponse>(&bytes).context(HealthJsonFromStrSnafu)?;
    let health_members = health
        .iter()
        .filter_map(|h| h.health.then(|| h.member_id))
        .collect::<HashSet<_>>();

    let req = http::Request::get(format!("{}/pd/api/v1/members", pd_address))
        .body(hyper::Body::empty())
        .context(BuildRequestSnafu)?;

    let res = client.send(req).await.context(GetMembersSnafu)?;

    let body = res.into_body();
    let bytes = hyper::body::to_bytes(body)
        .await
        .context(GetMembersBytesSnafu)?;

    let resp =
        serde_json::from_slice::<MembersResponse>(&bytes).context(MembersJsonFromStrSnafu)?;
    for member in resp.members {
        if health_members.contains(&member.member_id) {
            if let Some(url) = member.client_urls.iter().next() {
                let (host, port) = parse_host_port(url)?;
                components.push(Component {
                    instance_type: InstanceType::PD,
                    host,
                    primary_port: port,
                    secondary_port: port,
                });
            }
        }
    }

    Ok(())
}

pub async fn fetch_up_tidbs(
    tls_config: &Option<TlsConfig>,
    pd_address: &str,
    components: &mut Vec<Component>,
) -> Result<(), TopologyError> {
    let connect_opt = if let Some(tls_config) = tls_config.as_ref() {
        let mut tls_options = etcd_client::TlsOptions::new();

        if let Some(ca_file) = tls_config.ca_file.as_ref() {
            let cacert = read(ca_file).context(ReadCaFileSnafu)?;
            tls_options = tls_options.ca_certificate(etcd_client::Certificate::from_pem(cacert))
        }

        if let (Some(crt_file), Some(key_file)) =
            (tls_config.crt_file.as_ref(), tls_config.key_file.as_ref())
        {
            let cert = read(crt_file).context(ReadCrtFileSnafu)?;
            let key = read(key_file).context(ReadKeyFileSnafu)?;
            tls_options = tls_options.identity(etcd_client::Identity::from_pem(cert, key));
        }
        Some(etcd_client::ConnectOptions::new().with_tls(tls_options))
    } else {
        None
    };

    let mut client = etcd_client::Client::connect(&[pd_address], connect_opt)
        .await
        .context(BuildEtcdClientSnafu)?;

    let topo_prefix = "/topology/tidb/";
    let resp = client
        .get(
            topo_prefix,
            Some(etcd_client::GetOptions::new().with_prefix()),
        )
        .await
        .context(GetTopologySnafu)?;

    let mut up_tidbs = HashSet::new();
    let mut tidbs = Vec::new();
    for kv in resp.kvs() {
        let key = kv.key_str().context(ReadEtcdKeySnafu)?;
        let value = kv.value_str().context(ReadEtcdValueSnafu)?;

        let remaining_key = &key[topo_prefix.len()..];
        let mut key_labels = remaining_key.splitn(2, '/');
        let address = key_labels
            .next()
            .ok_or_else(|| TopologyError::MissingAddress {
                key: key.to_owned(),
            })?;
        let typ = key_labels
            .next()
            .ok_or_else(|| TopologyError::MissingType {
                key: key.to_owned(),
            })?;

        match typ {
            "info" => {
                let (host, port) = parse_host_port(address)?;
                let info = serde_json::from_str::<TopologyValue>(value)
                    .context(TopologyValueJsonFromStrSnafu)?;
                tidbs.push((
                    address.to_owned(),
                    Component {
                        instance_type: InstanceType::TiDB,
                        host,
                        primary_port: port,
                        secondary_port: info.status_port,
                    },
                ));
            }
            "ttl" => {
                let ttl = value.parse::<u128>().context(ParseTTLSnafu)?;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .context(TimeDriftSnafu)?
                    .as_nanos();
                if ttl + Duration::from_secs(45).as_nanos() >= now {
                    up_tidbs.insert(address.to_owned());
                }
            }
            _ => {}
        }
    }

    for (address, component) in tidbs {
        if up_tidbs.contains(&address) {
            components.push(component);
        }
    }

    Ok(())
}

pub async fn fetch_up_stores(
    client: &HttpClient<hyper::Body>,
    pd_address: &str,
    components: &mut Vec<Component>,
) -> Result<(), TopologyError> {
    let req = http::Request::get(format!("{}/pd/api/v1/stores", pd_address))
        .body(hyper::Body::empty())
        .context(BuildRequestSnafu)?;

    let res = client.send(req).await.context(GetStoresSnafu)?;

    let body = res.into_body();
    let bytes = hyper::body::to_bytes(body)
        .await
        .context(GetStoresBytesSnafu)?;

    let resp = serde_json::from_slice::<StoresResponse>(&bytes).context(StoresJsonFromStrSnafu)?;

    for StoreItem { store } in resp.stores {
        if store.state_name.to_lowercase().as_str() != "up" {
            continue;
        }

        let (host, primary_port) = parse_host_port(&store.address)?;
        let (_, secondary_port) = parse_host_port(&store.status_address)?;

        let instance_type = if store
            .labels
            .iter()
            .any(|LabelItem { key, value }| key == "engine" && value == "tiflash")
        {
            InstanceType::TiFlash
        } else {
            InstanceType::TiKV
        };

        components.push(Component {
            instance_type,
            host,
            primary_port,
            secondary_port,
        });
    }

    Ok(())
}

pub type HealthResponse = Vec<HealthItem>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HealthItem {
    pub member_id: u64,
    pub health: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MembersResponse {
    pub members: Vec<MemberItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemberItem {
    pub member_id: u64,
    pub client_urls: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopologyValue {
    pub status_port: u16,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoresResponse {
    pub stores: Vec<StoreItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreItem {
    pub store: StoreInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreInfo {
    pub address: String,
    pub status_address: String,
    pub state_name: String,
    #[serde(default)]
    pub labels: Vec<LabelItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LabelItem {
    pub key: String,
    pub value: String,
}

fn parse_host_port(address: &str) -> Result<(String, u16), TopologyError> {
    let uri: Uri = address.parse().context(ParseAddressSnafu)?;

    let host = uri.host().ok_or_else(|| TopologyError::MissingHost {
        address: address.to_owned(),
    })?;
    let port = uri.port().ok_or_else(|| TopologyError::MissingPort {
        address: address.to_owned(),
    })?;
    Ok((host.to_owned(), port.as_u16()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::TlsConfig;

    #[test]
    fn parse_address() {
        let (addr, port) = parse_host_port("localhost:2379").unwrap();
        assert_eq!(addr, "localhost");
        assert_eq!(port, 2379);

        let (addr, port) = parse_host_port("http://localhost:2379").unwrap();
        assert_eq!(addr, "localhost");
        assert_eq!(port, 2379);

        let (addr, port) = parse_host_port("https://localhost:2379").unwrap();
        assert_eq!(addr, "localhost");
        assert_eq!(port, 2379);
    }

    #[tokio::test]
    async fn t() {
        let tls_config = Some(TlsConfig {
            ca_file: Some("/home/zhongzc/.tiup/storage/cluster/clusters/tmp/tls/ca.crt".into()),
            crt_file: Some(
                "/home/zhongzc/.tiup/storage/cluster/clusters/tmp/tls/client.crt".into(),
            ),
            key_file: Some(
                "/home/zhongzc/.tiup/storage/cluster/clusters/tmp/tls/client.pem".into(),
            ),
            ..Default::default()
        });
        let componenets = fetch_up_components("https://localhost:2379".to_owned(), tls_config)
            .await
            .unwrap();
        println!("{:#?}", componenets);
    }
}
