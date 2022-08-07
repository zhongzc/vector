use metrics::counter;
use vector_core::internal_event::InternalEvent;

use crate::internal_events::prelude::{error_stage, error_type};

pub struct TopSQLPubSubBuildEndpointError {
    pub error: crate::Error,
}

impl InternalEvent for TopSQLPubSubBuildEndpointError {
    fn emit(self) {
        error!(
            message = "Failed to build endpoint.",
            error = %self.error,
            error_code = "failed_initializing",
            error_type = error_type::CONNECTION_FAILED,
            stage = error_stage::RECEIVING,
        );

        counter!(
            "component_errors_total", 1,
            "error_code" => "failed_initializing",
            "error_type" => error_type::CONNECTION_FAILED,
            "stage" => error_stage::RECEIVING,
        );
    }
}

pub struct TopSQLPubSubConnectError {
    pub error: crate::Error,
}

impl InternalEvent for TopSQLPubSubConnectError {
    fn emit(self) {
        error!(
            message = "Failed to connect to the server.",
            error = %self.error,
            error_code = "failed_connecting",
            error_type = error_type::CONNECTION_FAILED,
            stage = error_stage::RECEIVING,
        );

        counter!(
            "component_errors_total", 1,
            "error_code" => "failed_connecting",
            "error_type" => error_type::CONNECTION_FAILED,
            "stage" => error_stage::RECEIVING,
        );
    }
}

pub struct TopSQLPubSubProxyConnectError {
    pub error: crate::Error,
}

impl InternalEvent for TopSQLPubSubProxyConnectError {
    fn emit(self) {
        error!(
            message = "Proxy failed to connect to the server.",
            error = %self.error,
            error_code = "failed_connecting",
            error_type = error_type::CONNECTION_FAILED,
            stage = error_stage::RECEIVING,
        );

        counter!(
            "component_errors_total", 1,
            "error_code" => "failed_connecting",
            "error_type" => error_type::CONNECTION_FAILED,
            "stage" => error_stage::RECEIVING,
        );
    }
}

pub struct TopSQLPubSubSubscribeError {
    pub error: tonic::Status,
}

impl InternalEvent for TopSQLPubSubSubscribeError {
    fn emit(self) {
        error!(
            message = "Failed to set up subscription.",
            error = %self.error,
            error_code = "failed_subscription",
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::RECEIVING,
        );

        counter!(
            "component_errors_total", 1,
            "error_code" => "failed_subscription",
            "error_type" => error_type::REQUEST_FAILED,
            "stage" => error_stage::RECEIVING,
        );
    }
}

pub struct TopSQLPubSubReceiveError {
    pub error: tonic::Status,
}

impl InternalEvent for TopSQLPubSubReceiveError {
    fn emit(self) {
        error!(
            message = "Failed to fetch events.",
            error = %self.error,
            error_code = "failed_fetching_events",
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::RECEIVING,
        );

        counter!(
            "component_errors_total", 1,
            "error_code" => "failed_fetching_events",
            "error_type" => error_type::REQUEST_FAILED,
            "stage" => error_stage::RECEIVING,
        );
    }
}
