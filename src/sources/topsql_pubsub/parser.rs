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

#[derive(Clone)]
pub struct Parser {
    instance: String,
    instance_type: String,
}

impl Parser {
    pub const fn new(instance: String, instance_type: String) -> Self {
        Self {
            instance,
            instance_type,
        }
    }

    pub fn parse_instance(&self) -> LogEvent {
        make_metric_like_log_event(
            &[
                ("__name__", "instance".to_owned()),
                ("instance", self.instance.clone()),
                ("instance_type", self.instance_type.clone()),
            ],
            &[Utc::now()],
            &[1.0],
        )
    }

    pub fn parse_tidb_response(&self, response: TopSqlSubResponse) -> Vec<LogEvent> {
        match response.resp_oneof {
            Some(RespOneof::Record(record)) => self.parse_tidb_record(record),
            Some(RespOneof::SqlMeta(sql_meta)) => self.parse_tidb_sql_meta(sql_meta),
            Some(RespOneof::PlanMeta(plan_meta)) => self.parse_tidb_plan_meta(plan_meta),
            None => vec![],
        }
    }

    pub fn parse_tikv_response(&self, response: ResourceUsageRecord) -> Vec<LogEvent> {
        match response.record_oneof {
            Some(RecordOneof::Record(record)) => self.parse_tikv_record(record),
            None => vec![],
        }
    }

    fn parse_tidb_record(&self, record: TopSqlRecord) -> Vec<LogEvent> {
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
            ("__name__", String::new()),
            ("instance", String::new()),
            ("instance_type", String::new()),
            ("sql_digest", String::new()),
            ("plan_digest", String::new()),
        ];
        const NAME_INDEX: usize = 0;
        const INSTANCE_INDEX: usize = 1;
        const INSTANCE_TYPE_INDEX: usize = 2;
        const SQL_DIGEST_INDEX: usize = 3;
        const PLAN_DIGEST_INDEX: usize = 4;
        let mut values = vec![];

        let mut logs = vec![];

        // cpu_time_ms
        labels[NAME_INDEX].1 = "topsql_cpu_time_ms".to_owned();
        labels[INSTANCE_INDEX].1 = self.instance.clone();
        labels[INSTANCE_TYPE_INDEX].1 = self.instance_type.clone();
        labels[SQL_DIGEST_INDEX].1 = hex::encode_upper(record.sql_digest);
        labels[PLAN_DIGEST_INDEX].1 = hex::encode_upper(record.plan_digest);
        values.extend(record.items.iter().map(|item| item.cpu_time_ms as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // stmt_exec_count
        labels[NAME_INDEX].1 = "stmt_exec_count".to_owned();
        values.clear();
        values.extend(record.items.iter().map(|item| item.stmt_exec_count as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // stmt_duration_sum_ns
        labels[NAME_INDEX].1 = "topsql_stmt_duration_sum_ns".to_owned();
        values.clear();
        values.extend(
            record
                .items
                .iter()
                .map(|item| item.stmt_duration_sum_ns as f64),
        );
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // stmt_duration_count
        labels[NAME_INDEX].1 = "topsql_stmt_duration_count".to_owned();
        values.clear();
        values.extend(
            record
                .items
                .iter()
                .map(|item| item.stmt_duration_count as f64),
        );
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // stmt_kv_exec_count
        let tikv_instances = record
            .items
            .iter()
            .flat_map(|item| item.stmt_kv_exec_count.keys())
            .collect::<BTreeSet<_>>();
        labels[NAME_INDEX].1 = "topsql_stmt_kv_exec_count".to_owned();
        labels[INSTANCE_TYPE_INDEX].1 = "tikv".to_owned();
        for tikv_instance in tikv_instances {
            values.clear();
            labels[INSTANCE_INDEX].1 = tikv_instance.clone();
            values.extend(record.items.iter().map(|item| {
                item.stmt_kv_exec_count
                    .get(tikv_instance)
                    .cloned()
                    .unwrap_or_default() as f64
            }));
            logs.push(make_metric_like_log_event(&labels, &timestamps, &values));
        }

        logs
    }

    fn parse_tidb_sql_meta(&self, sql_meta: SqlMeta) -> Vec<LogEvent> {
        vec![make_metric_like_log_event(
            &[
                ("__name__", "topsql_sql_meta".to_owned()),
                ("sql_digest", hex::encode_upper(sql_meta.sql_digest)),
                ("normalized_sql", sql_meta.normalized_sql),
                ("is_internal_sql", sql_meta.is_internal_sql.to_string()),
            ],
            &[Utc::now()],
            &[1.0],
        )]
    }

    fn parse_tidb_plan_meta(&self, plan_meta: PlanMeta) -> Vec<LogEvent> {
        vec![make_metric_like_log_event(
            &[
                ("__name__", "topsql_plan_meta".to_owned()),
                ("plan_digest", hex::encode_upper(plan_meta.plan_digest)),
                ("normalized_plan", plan_meta.normalized_plan),
            ],
            &[Utc::now()],
            &[1.0],
        )]
    }

    fn parse_tikv_record(&self, record: GroupTagRecord) -> Vec<LogEvent> {
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
                            Some(1) => "row".to_owned(),
                            Some(2) => "index".to_owned(),
                            _ => "unknown".to_owned(),
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
            ("__name__", String::new()),
            ("instance", self.instance.clone()),
            ("instance_type", self.instance_type.clone()),
            ("sql_digest", sql_digest),
            ("plan_digest", plan_digest),
            ("tag_label", tag_label),
        ];
        let mut values = vec![];

        let mut logs = vec![];

        // cpu_time_ms
        labels[0].1 = "topsql_cpu_time_ms".to_owned();
        values.extend(record.items.iter().map(|item| item.cpu_time_ms as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // read_keys
        labels[0].1 = "topsql_read_keys".to_owned();
        values.clear();
        values.extend(record.items.iter().map(|item| item.read_keys as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // write_keys
        labels[0].1 = "topsql_write_keys".to_owned();
        values.clear();
        values.extend(record.items.iter().map(|item| item.write_keys as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        logs
    }
}

fn make_metric_like_log_event(
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
