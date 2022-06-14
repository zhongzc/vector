use crate::{
    event::LogEvent,
    sources::topsql_pubsub::{
        consts::{
            INSTANCE_TYPE_TIKV, KV_TAG_LABEL_INDEX, KV_TAG_LABEL_ROW, KV_TAG_LABEL_UNKNOWN,
            METRIC_NAME_CPU_TIME_MS, METRIC_NAME_READ_KEYS, METRIC_NAME_WRITE_KEYS,
        },
        upstream::parser::{Buf, UpstreamEventParser},
    },
};

pub struct ResourceUsageRecordParser;

impl UpstreamEventParser for ResourceUsageRecordParser {
    type UpstreamEvent = kvproto::resource_usage_agent::ResourceUsageRecord;

    fn parse(response: Self::UpstreamEvent, instance: String) -> Vec<LogEvent> {
        match response.record_oneof {
            Some(
                kvproto::resource_usage_agent::ResourceUsageRecord_oneof_record_oneof::Record(
                    record,
                ),
            ) => Self::parse_tikv_record(record, instance),
            None => vec![],
        }
    }
}

impl ResourceUsageRecordParser {
    fn parse_tikv_record(
        record: kvproto::resource_usage_agent::GroupTagRecord,
        instance: String,
    ) -> Vec<LogEvent> {
        let decoded = Self::decode_tag(record.resource_group_tag.as_slice());
        if decoded.is_none() {
            return vec![];
        }

        let mut logs = vec![];

        let (sql_digest, plan_digest, tag_label) = decoded.unwrap();
        let mut buf = Buf::new();
        buf.instance(instance)
            .instance_type(INSTANCE_TYPE_TIKV)
            .sql_digest(sql_digest)
            .plan_digest(plan_digest)
            .tag_label(tag_label);

        macro_rules! append {
            ($( ($label_name:expr, $item_name:tt), )* ) => {
                $(
                    buf.label_name($label_name)
                        .points(record.items.iter().filter_map(|item| {
                            if item.$item_name > 0 {
                                Some((item.timestamp_sec, item.$item_name as f64))
                            } else {
                                None
                            }
                        }));
                    if let Some(event) = buf.build_event() {
                        logs.push(event);
                    }
                )*
            };
        }
        append!(
            // cpu_time_ms
            (METRIC_NAME_CPU_TIME_MS, cpu_time_ms),
            // read_keys
            (METRIC_NAME_READ_KEYS, read_keys),
            // write_keys
            (METRIC_NAME_WRITE_KEYS, write_keys),
        );

        logs
    }

    fn decode_tag(tag: &[u8]) -> Option<(String, String, String)> {
        match protobuf::parse_from_bytes::<tipb::ResourceGroupTag>(tag) {
            Ok(resource_tag) => {
                if resource_tag.get_sql_digest().is_empty() {
                    warn!(message = "Unexpected resource tag without sql digest", tag = %hex::encode(tag));
                    None
                } else {
                    Some((
                        hex::encode_upper(resource_tag.get_sql_digest()),
                        hex::encode_upper(resource_tag.get_plan_digest()),
                        match resource_tag.get_label() {
                            tipb::ResourceGroupTagLabel::ResourceGroupTagLabelRow => {
                                KV_TAG_LABEL_ROW.to_owned()
                            }
                            tipb::ResourceGroupTagLabel::ResourceGroupTagLabelIndex => {
                                KV_TAG_LABEL_INDEX.to_owned()
                            }
                            _ => KV_TAG_LABEL_UNKNOWN.to_owned(),
                        },
                    ))
                }
            }
            Err(error) => {
                warn!(message = "Failed to decode resource tag", tag = %hex::encode(tag), %error);
                None
            }
        }
    }
}
