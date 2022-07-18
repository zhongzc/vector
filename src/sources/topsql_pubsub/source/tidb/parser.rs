use std::collections::BTreeSet;

use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use ordered_float::NotNan;
use prost::Message;

use super::proto::{
    top_sql_sub_response::RespOneof, PlanMeta, ResourceGroupTag, SqlMeta, TopSqlRecord,
    TopSqlSubResponse,
};
use super::UpstreamEventParser;
use crate::{
    event::{EventMetadata, LogEvent, Value},
    sources::topsql_pubsub::{
        source::{
            parser::UpstreamEventParser, INSTANCE_TYPE_TIDB, INSTANCE_TYPE_TIKV, LABEL_INSTANCE,
            LABEL_INSTANCE_TYPE, LABEL_IS_INTERNAL_SQL, LABEL_NAME, LABEL_NORMALIZED_PLAN,
            LABEL_NORMALIZED_SQL, LABEL_PLAN_DIGEST, LABEL_SQL_DIGEST, METRIC_NAME_CPU_TIME_MS,
            METRIC_NAME_PLAN_META, METRIC_NAME_SQL_META, METRIC_NAME_STMT_DURATION_COUNT,
            METRIC_NAME_STMT_DURATION_SUM_NS, METRIC_NAME_STMT_EXEC_COUNT,
            METRIC_NAME_STMT_KV_EXEC_COUNT,
        },
        utils::make_metric_like_log_event,
    },
};

pub struct TopSqlSubResponseParser;

impl UpstreamEventParser for TopSqlSubResponseParser {
    type UpstreamEvent = TopSqlSubResponse;

    fn parse(&self, response: Self::UpstreamEvent, instance: String) -> Vec<LogEvent> {
        match response.resp_oneof {
            Some(RespOneof::Record(record)) => self.parse_tidb_record(record, instance),
            Some(RespOneof::SqlMeta(sql_meta)) => self.parse_tidb_sql_meta(sql_meta),
            Some(RespOneof::PlanMeta(plan_meta)) => self.parse_tidb_plan_meta(plan_meta),
            None => vec![],
        }
    }
}

impl TopSqlSubResponseParser {
    fn parse_tidb_record(&self, record: TopSqlRecord, instance: String) -> Vec<LogEvent> {
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
            (LABEL_INSTANCE, String::new()),
            (LABEL_INSTANCE_TYPE, String::new()),
            (LABEL_SQL_DIGEST, String::new()),
            (LABEL_PLAN_DIGEST, String::new()),
        ];
        const NAME_INDEX: usize = 0;
        const INSTANCE_INDEX: usize = 1;
        const INSTANCE_TYPE_INDEX: usize = 2;
        const SQL_DIGEST_INDEX: usize = 3;
        const PLAN_DIGEST_INDEX: usize = 4;
        let mut values = vec![];

        let mut logs = vec![];

        // cpu_time_ms
        labels[NAME_INDEX].1 = METRIC_NAME_CPU_TIME_MS.to_owned();
        labels[INSTANCE_INDEX].1 = instance;
        labels[INSTANCE_TYPE_INDEX].1 = INSTANCE_TYPE_TIDB.to_owned();
        labels[SQL_DIGEST_INDEX].1 = hex::encode_upper(record.sql_digest);
        labels[PLAN_DIGEST_INDEX].1 = hex::encode_upper(record.plan_digest);
        values.extend(record.items.iter().map(|item| item.cpu_time_ms as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // stmt_exec_count
        labels[NAME_INDEX].1 = METRIC_NAME_STMT_EXEC_COUNT.to_owned();
        values.clear();
        values.extend(record.items.iter().map(|item| item.stmt_exec_count as f64));
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // stmt_duration_sum_ns
        labels[NAME_INDEX].1 = METRIC_NAME_STMT_DURATION_SUM_NS.to_owned();
        values.clear();
        values.extend(
            record
                .items
                .iter()
                .map(|item| item.stmt_duration_sum_ns as f64),
        );
        logs.push(make_metric_like_log_event(&labels, &timestamps, &values));

        // stmt_duration_count
        labels[NAME_INDEX].1 = METRIC_NAME_STMT_DURATION_COUNT.to_owned();
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
        labels[NAME_INDEX].1 = METRIC_NAME_STMT_KV_EXEC_COUNT.to_owned();
        labels[INSTANCE_TYPE_INDEX].1 = INSTANCE_TYPE_TIKV.to_owned();
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
                (LABEL_NAME, METRIC_NAME_SQL_META.to_owned()),
                (LABEL_SQL_DIGEST, hex::encode_upper(sql_meta.sql_digest)),
                (LABEL_NORMALIZED_SQL, sql_meta.normalized_sql),
                (LABEL_IS_INTERNAL_SQL, sql_meta.is_internal_sql.to_string()),
            ],
            &[Utc::now()],
            &[1.0],
        )]
    }

    fn parse_tidb_plan_meta(&self, plan_meta: PlanMeta) -> Vec<LogEvent> {
        vec![make_metric_like_log_event(
            &[
                (LABEL_NAME, METRIC_NAME_PLAN_META.to_owned()),
                (LABEL_PLAN_DIGEST, hex::encode_upper(plan_meta.plan_digest)),
                (LABEL_NORMALIZED_PLAN, plan_meta.normalized_plan),
            ],
            &[Utc::now()],
            &[1.0],
        )]
    }
}
