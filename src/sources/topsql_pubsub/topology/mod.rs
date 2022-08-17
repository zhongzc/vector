mod fetch;

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
