use chrono::Utc;
use vector_core::event::LogEvent;

use crate::sources::topsql_pubsub::{
    parser::{
        UpstreamEventParser, LABEL_INSTANCE, LABEL_INSTANCE_TYPE, LABEL_NAME, METRIC_NAME_INSTANCE,
    },
    utils::make_metric_like_log_event,
};

type InstanceType = String;

struct InstanceParser;

impl UpstreamEventParser for InstanceParser {
    type UpstreamEvent = InstanceType;

    fn parse(&self, instance_type: Self::UpstreamEvent, instance: String) -> Vec<LogEvent> {
        vec![make_metric_like_log_event(
            &[
                (LABEL_NAME, METRIC_NAME_INSTANCE.to_owned()),
                (LABEL_INSTANCE, instance),
                (LABEL_INSTANCE_TYPE, instance_type),
            ],
            &[Utc::now()],
            &[1.0],
        )]
    }
}
