use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use ordered_float::NotNan;
use prost::Message;

use super::proto::{resource_usage_record::RecordOneof, GroupTagRecord, ResourceUsageRecord};
use crate::event::{EventMetadata, LogEvent, Value};
use crate::sources::topsql_pubsub::{
    source::{
        parser::UpstreamEventParser, INSTANCE_TYPE_TIKV, KV_TAG_LABEL_INDEX, KV_TAG_LABEL_ROW,
        KV_TAG_LABEL_UNKNOWN, LABEL_INSTANCE, LABEL_INSTANCE_TYPE, LABEL_NAME, LABEL_PLAN_DIGEST,
        LABEL_SQL_DIGEST, LABEL_TAG_LABEL, METRIC_NAME_CPU_TIME_MS, METRIC_NAME_READ_KEYS,
        METRIC_NAME_WRITE_KEYS,
    },
    utils::make_metric_like_log_event,
};

pub struct ResourceUsageRecordParser;

impl UpstreamEventParser for ResourceUsageRecordParser {
    type UpstreamEvent = ResourceUsageRecord;

    fn parse(&self, response: Self::UpstreamEvent, instance: String) -> Vec<LogEvent> {
        match response.record_oneof {
            Some(RecordOneof::Record(record)) => self.parse_tikv_record(record, instance),
            None => vec![],
        }
    }
}

impl ResourceUsageRecordParser {
    fn parse_tikv_record(&self, record: GroupTagRecord, instance: String) -> Vec<LogEvent> {
        let (sql_digest, plan_digest, tag_label) =
            match ResourceGroupTag::decode(record.resource_group_tag.as_slice()) {
                Ok(resource_tag) => {
                    if resource_tag.sql_digest.is_none() {
                        return vec![];
                    }
                    (
                        hex::encode_upper(resource_tag.sql_digest.unwrap()),
                        hex::encode_upper(resource_tag.plan_digest.unwrap_or_default()),
                        match resource_tag.label {
                            Some(1) => KV_TAG_LABEL_ROW.to_owned(),
                            Some(2) => KV_TAG_LABEL_INDEX.to_owned(),
                            _ => KV_TAG_LABEL_UNKNOWN.to_owned(),
                        },
                    )
                }
                Err(_) => return vec![],
            };
        let timestamps = record
            .items
            .iter()
            .map(|item| {
                DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp(item.timestamp_sec as i64, 0),
                    Utc,
                )
            })
            .collect::<Vec<_>>();
        let mut labels = vec![
            (LABEL_NAME, String::new()),
            (LABEL_INSTANCE, instance),
            (LABEL_INSTANCE_TYPE, INSTANCE_TYPE_TIKV.to_owned()),
            (LABEL_SQL_DIGEST, sql_digest),
            (LABEL_PLAN_DIGEST, plan_digest),
            (LABEL_TAG_LABEL, tag_label),
        ];
        let mut values = vec![];

        let mut logs = vec![];

        // cpu_time_ms
        labels[0].1 = METRIC_NAME_CPU_TIME_MS.to_owned();
        values.extend(record.items.iter().map(|item| item.cpu_time_ms as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // read_keys
        labels[0].1 = METRIC_NAME_READ_KEYS.to_owned();
        values.clear();
        values.extend(record.items.iter().map(|item| item.read_keys as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // write_keys
        labels[0].1 = METRIC_NAME_WRITE_KEYS.to_owned();
        values.clear();
        values.extend(record.items.iter().map(|item| item.write_keys as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        logs
    }
}
