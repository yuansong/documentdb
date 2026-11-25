/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * documentdb_gateway_core/src/service/connection_loop/request_execution.rs
 *
 *-------------------------------------------------------------------------
 */

use either::Either::Left;
use tokio::{io::AsyncWrite, time::Instant};
use tracing::Instrument;

use crate::{
    context::{ConnectionContext, RequestContext},
    error::Result,
    postgres::PgDataClient,
    protocol::header::Header,
    requests::{
        request_tracker::RequestTracker, RequestIntervalKind, RequestObservation,
    },
    responses,
    service::connection_loop::routing::RequestRouter,
    telemetry::{self, client_info},
};

/// Requests slower than this are worth surfacing at info level even when the
/// debug-level per-request diagnostics stay disabled: on a healthy benchmark
/// workload every request is sub-millisecond, so a slow request is a real
/// regression signal rather than noise.
const DEFAULT_SLOW_REQUEST_LOG_THRESHOLD_MS: u64 = 250;

/// Per-interval elapsed time of one request, in microseconds.
///
/// The tracker accumulates nanoseconds; microseconds keep the log readable
/// (a 0.7 ms read shows up as `read_us=700`, not `0.0007`).
fn interval_us(tracker: &RequestTracker, interval: RequestIntervalKind) -> u64 {
    tracker.get_interval_elapsed_time(interval) / 1_000
}

/// Threshold above which a completed request is reported at info level.
///
/// The dynamic configuration key `slowQueryLogIntervalInMilliseconds`
/// overrides the built-in default when set to a positive value.
fn slow_request_threshold_ms(connection_context: &ConnectionContext) -> u64 {
    let configured = connection_context
        .dynamic_configuration()
        .slow_query_log_interval_ms();
    if configured > 0 {
        configured
    } else {
        DEFAULT_SLOW_REQUEST_LOG_THRESHOLD_MS
    }
}

/// Emits the request forwarding-path breakdown as one structured log line.
///
/// The intervals come from the request tracker that the whole pipeline records
/// into, so the line is the exact decomposition operators need to answer
/// "where did this request spend its time": socket read, wire parse
/// (`format`), backend connection acquisition (`open_backend`), the backend
/// round trip (`pg_exec`), the transaction bookkeeping round trips
/// (`begin_txn` / `set_timeout` / `commit_txn`), and the response write.
///
/// Levels, from quietest to loudest:
///
/// * `debug` — every completed request, only when the subscriber filter
///   enables debug (the `enabled!` guard keeps the cost to one atomic-free
///   check per request otherwise);
/// * `info` — failed requests, and requests slower than the configured
///   threshold, so regressions are visible without raising the log level.
///
/// `total` falls back to `HandleRequest` when `HandleMessage` was never
/// closed (error path), so a failed request still shows its overall span.
fn log_request_diagnostics(
    connection_context: &ConnectionContext,
    header: &Header,
    request_context: &RequestContext<'_>,
    response_bytes: usize,
    failed: bool,
) {
    let tracker = request_context.tracker;
    let total_us = interval_us(tracker, RequestIntervalKind::HandleMessage);
    let handle_request_us = interval_us(tracker, RequestIntervalKind::HandleRequest);
    let total_us = if total_us > 0 {
        total_us
    } else {
        handle_request_us
    };
    let total_ms = total_us / 1_000;

    let is_debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
    let slow = !failed && total_ms >= slow_request_threshold_ms(connection_context);

    if !failed && !slow && !is_debug_enabled {
        // Quiet, healthy request on an info-only filter: log nothing.
        return;
    }

    macro_rules! request_diagnostics {
        ($level:ident, $message:expr) => {
            tracing::$level!(
                activity_id = request_context.activity_id,
                op = %request_context.request().request_type(),
                request_bytes = header.message_length(),
                response_bytes,
                read_us = interval_us(tracker, RequestIntervalKind::ReadRequest),
                format_us = interval_us(tracker, RequestIntervalKind::FormatRequest),
                open_backend_us = interval_us(tracker, RequestIntervalKind::OpenBackendConnection),
                pg_exec_us = interval_us(tracker, RequestIntervalKind::ProcessRequest),
                begin_txn_us = interval_us(tracker, RequestIntervalKind::PostgresBeginTransaction),
                set_timeout_us =
                    interval_us(tracker, RequestIntervalKind::PostgresSetStatementTimeout),
                commit_txn_us =
                    interval_us(tracker, RequestIntervalKind::PostgresCommitTransaction),
                handle_request_us,
                write_response_us = interval_us(tracker, RequestIntervalKind::WriteResponse),
                total_us,
                total_ms,
                $message
            );
        };
    }

    if failed {
        request_diagnostics!(debug, "request FAILED (forwarding-path timings)");
    } else if slow {
        request_diagnostics!(info, "SLOW REQUEST (forwarding-path timings)");
    } else {
        request_diagnostics!(debug, "request completed (forwarding-path timings)");
    }
}

pub(super) async fn handle_request<T, R, W>(
    connection_context: &mut ConnectionContext,
    header: &Header,
    request_context: &RequestContext<'_>,
    request_router: &R,
    writer: &mut W,
    handle_message_start: Instant,
    interop_start: Instant,
) -> Result<()>
where
    T: PgDataClient,
    R: RequestRouter<T>,
    W: AsyncWrite + Unpin,
{
    let handle_request_start = Instant::now();
    let response_result = request_router
        .handle_request(request_context, connection_context)
        .await;
    request_context
        .tracker
        .record_duration(RequestIntervalKind::HandleRequest, handle_request_start);
    request_context
        .tracker
        .record_duration(RequestIntervalKind::Interop, interop_start);

    let response = match response_result {
        Ok(response) => response,
        Err(error) => {
            log_request_diagnostics(connection_context, header, request_context, 0, true);
            return Err(error);
        }
    };

    request_context
        .tracker
        .record_duration(RequestIntervalKind::HandleMessage, handle_message_start);

    if connection_context.requires_response {
        let handle_response_start = Instant::now();
        let raw_response = response.as_raw_document()?;
        let response_bytes = raw_response.as_bytes().len();
        let write_response_start = Instant::now();
        responses::writer::write_and_flush(header, raw_response, writer)
            .instrument(tracing::info_span!("gateway.write_response"))
            .await?;
        request_context
            .tracker
            .record_duration(RequestIntervalKind::WriteResponse, write_response_start);
        request_context
            .tracker
            .record_duration(RequestIntervalKind::HandleResponse, handle_response_start);

        log_request_diagnostics(connection_context, header, request_context, response_bytes, false);
    }

    if connection_context.request_metrics_enabled() {
        telemetry::record_gateway_metrics(
            header,
            Some(RequestObservation::Strict(request_context.request())),
            Left(&response),
            request_context.request().collection().unwrap_or(""),
            request_context.tracker,
        );
    }

    if let Some(telemetry) = connection_context.telemetry_provider.as_ref() {
        let collection = request_context.request().collection().unwrap_or("");

        telemetry.emit_request_event(
            connection_context,
            header,
            Some(RequestObservation::Strict(request_context.request())),
            Left(&response),
            collection,
            request_context.tracker,
            request_context.activity_id,
            &client_info::parse_client_info(connection_context.client_information.as_ref()),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::{
        postgres::DocumentDBDataClient,
        protocol::opcode::OpCode,
        requests::{request_tracker::RequestTracker, Request, RequestType, WireRequest},
        service::connection_loop::routing::DefaultRequestRouter,
        testing::{
            assert_header_matches, assert_success_response, build_op_msg_parts, build_raw_document,
            decode_op_msg_response, logout_document, test_connection_context,
            RecordingTelemetryProvider, TestDynamicConfiguration,
        },
    };

    async fn execute_handle_request<T>(
        connection_context: &mut ConnectionContext,
        header: &Header,
        request_context: &RequestContext<'_>,
        handle_message_start: Instant,
    ) -> (Result<()>, Vec<u8>)
    where
        T: PgDataClient,
    {
        let (mut response_writer, mut response_reader) = tokio::io::duplex(4096);
        let result = handle_request::<T, _, _>(
            connection_context,
            header,
            request_context,
            &DefaultRequestRouter {},
            &mut response_writer,
            handle_message_start,
            handle_message_start,
        )
        .await;
        drop(response_writer);

        let mut response_bytes = Vec::new();
        response_reader
            .read_to_end(&mut response_bytes)
            .await
            .expect("response reader should drain bytes");

        (result, response_bytes)
    }
    #[tokio::test]
    async fn handle_request_writes_response_and_emits_success_event() {
        let dynamic_configuration = Arc::new(TestDynamicConfiguration::default());
        let telemetry_provider = RecordingTelemetryProvider::default();
        let mut connection_context = test_connection_context(
            true,
            dynamic_configuration,
            Some(Box::new(telemetry_provider.clone())),
        )
        .await;
        let logout_document = logout_document();
        let request = Request::RawBuf(RequestType::Logout, build_raw_document(&logout_document));
        let request_info = request
            .extract_common()
            .expect("logout request should have valid common fields");
        let wire_request = WireRequest::from_request_and_info(&request, request_info);
        let request_tracker = RequestTracker::new();
        let request_context = RequestContext::new(
            "activity-handle-request-success",
            &wire_request,
            &request_tracker,
        );
        let (header, _) = build_op_msg_parts(&logout_document, 71);

        let (result, response_bytes) = execute_handle_request::<DocumentDBDataClient>(
            &mut connection_context,
            &header,
            &request_context,
            Instant::now(),
        )
        .await;

        assert!(result.is_ok(), "logout request should succeed");
        let (response_header, response_document) = decode_op_msg_response(&response_bytes);
        assert_header_matches(
            &response_header,
            response_header.message_length(),
            71,
            71,
            OpCode::Msg,
        );
        assert_success_response(&response_document);

        let events = telemetry_provider.events();
        assert_eq!(
            events.len(),
            1,
            "success path should emit one telemetry event"
        );
        assert_eq!(events[0].activity_id(), "activity-handle-request-success");
        assert_eq!(events[0].collection(), "");
        assert_eq!(events[0].request_type(), Some(RequestType::Logout));
        assert!(
            !events[0].is_error(),
            "success event should not be marked as an error"
        );
        assert_eq!(events[0].user_agent(), "");
        assert!(
            request_tracker.get_interval_elapsed_time(RequestIntervalKind::Interop) > 0,
            "successful request should record interop time"
        );
        assert!(
            request_tracker.get_interval_elapsed_time(RequestIntervalKind::HandleResponse) > 0,
            "successful response should record response handling time"
        );
    }

    #[tokio::test]
    async fn handle_request_skips_response_write_when_not_required() {
        let dynamic_configuration = Arc::new(TestDynamicConfiguration::default());
        let telemetry_provider = RecordingTelemetryProvider::default();
        let mut connection_context = test_connection_context(
            true,
            dynamic_configuration,
            Some(Box::new(telemetry_provider.clone())),
        )
        .await;
        connection_context.requires_response = false;

        let logout_document = logout_document();
        let request = Request::RawBuf(RequestType::Logout, build_raw_document(&logout_document));
        let request_info = request
            .extract_common()
            .expect("logout request should have valid common fields");
        let wire_request = WireRequest::from_request_and_info(&request, request_info);
        let request_tracker = RequestTracker::new();
        let request_context = RequestContext::new(
            "activity-handle-request-no-response",
            &wire_request,
            &request_tracker,
        );
        let (header, _) = build_op_msg_parts(&logout_document, 72);

        let (result, response_bytes) = execute_handle_request::<DocumentDBDataClient>(
            &mut connection_context,
            &header,
            &request_context,
            Instant::now(),
        )
        .await;

        assert!(
            result.is_ok(),
            "logout request should still succeed when no response is required"
        );
        assert!(
            response_bytes.is_empty(),
            "no wire response should be produced when requires_response is false"
        );

        let events = telemetry_provider.events();
        assert_eq!(events.len(), 1, "telemetry should still be emitted");
        assert_eq!(
            events[0].activity_id(),
            "activity-handle-request-no-response"
        );
        assert!(!events[0].is_error());
    }
}
