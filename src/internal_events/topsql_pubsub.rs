use metrics::counter;
use vector_core::internal_event::InternalEvent;

use crate::internal_events::prelude::{error_stage, error_type};

pub struct TopSQLPubSubInitTLSError {
    pub error: std::io::Error,
}

impl InternalEvent for TopSQLPubSubInitTLSError {
    fn emit(self) {
        error!(
            message = "Failed to init tls",
            error = %self.error,
            error_code = "failed_tls_initialization",
            error_type = error_type::CONFIGURATION_FAILED,
            stage = error_stage::RECEIVING,
        );

        counter!(
            "component_errors_total", 1,
            "error_code" => "failed_tls_initialization",
            "error_type" => error_type::CONFIGURATION_FAILED,
            "stage" => error_stage::RECEIVING,
        );
    }
}

pub struct TopSQLPubSubConnectError;

impl InternalEvent for TopSQLPubSubConnectError {
    fn emit(self) {
        error!(
            message = "Failed to connect to the server.",
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
    pub error: grpcio::Error,
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
    pub error: grpcio::Error,
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
