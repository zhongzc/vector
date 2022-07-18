use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use ordered_float::NotNan;
use prost::Message;

use super::proto::{
    resource_metering_pubsub::{
        resource_usage_record::RecordOneof, GroupTagRecord, ResourceUsageRecord,
    },
    topsql_pubsub::{
        top_sql_sub_response::RespOneof, PlanMeta, ResourceGroupTag, SqlMeta, TopSqlRecord,
        TopSqlSubResponse,
    },
};
use crate::event::{EventMetadata, LogEvent, Value};

pub fn make_metric_like_log_event(
    labels: &[(&'static str, String)],
    timestamps: &[DateTime<Utc>],
    values: &[f64],
) -> LogEvent {
    let mut labels_map = BTreeMap::new();
    for (k, v) in labels {
        labels_map.insert(k.to_string(), Value::Bytes(Bytes::from(v.clone())));
    }

    let timestamps_vec = timestamps
        .iter()
        .map(|t| Value::Timestamp(*t))
        .collect::<Vec<_>>();
    let values_vec = values
        .iter()
        .map(|v| Value::Float(NotNan::new(*v).unwrap()))
        .collect::<Vec<_>>();

    let mut log = BTreeMap::new();
    log.insert("labels".to_owned(), Value::Object(labels_map));
    log.insert("timestamps".to_owned(), Value::Array(timestamps_vec));
    log.insert("values".to_owned(), Value::Array(values_vec));
    LogEvent::from_map(log, EventMetadata::default())
}
