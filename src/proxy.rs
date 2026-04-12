use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Method, Uri};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http::StatusCode;
use reqwest::Client;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use crate::backend::BackendHandler;
use crate::config::Config;
use crate::error::ErrorKind;
use crate::events::*;
use crate::queue::EventQueue;
use crate::sampling;

#[derive(Clone)]
pub struct AppState {
    pub client: Client,
    pub config: Arc<Config>,
    pub event_queue: Arc<EventQueue>,
    pub backends: Arc<Vec<Box<dyn BackendHandler>>>,
}

pub async fn forward_handler(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path();

    // Try to match a backend
    let matched = state
        .backends
        .iter()
        .find_map(|b| b.match_request(&method, path));

    match matched {
        Some(request_match) => handle_instrumented(state, request_match, uri, headers, body).await,
        None => {
            // No backend matched — forward transparently without events
            forward_raw(
                &state.client,
                &state.config.upstream_base_url,
                &method,
                &uri,
                headers,
                body,
            )
            .await
        }
    }
}

async fn handle_instrumented(
    state: AppState,
    request_match: crate::backend::RequestMatch,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_id = Uuid::now_v7();
    let mut sampled = sampling::is_sampled(&request_id, state.config.sampling_rate);

    // Find the backend that matched (we need it for parsing)
    let backend = state
        .backends
        .iter()
        .find(|b| b.match_request(&http::Method::POST, uri.path()).is_some())
        .unwrap();

    // Parse request metadata (always, so we have it if we need to force-sample)
    let request_meta = backend.parse_request(&request_match, &body);
    let db_info = DbInfo::from(&request_match);
    let request_id_str = request_id.to_string();

    // Emit request event if sampled
    if sampled {
        let req_event = RequestEvent::new(
            request_id_str.clone(),
            true,
            request_meta.unsupported_shape,
            DbInfo {
                kind: db_info.kind.clone(),
                collection: db_info.collection.clone(),
            },
            QueryInfo {
                text: request_meta.query.text.clone(),
                hash: request_meta.query.hash.clone(),
                top_k: request_meta.query.top_k,
            },
        );
        state.event_queue.send(Event::Request(req_event));
    }

    // Forward request upstream
    let start = Instant::now();
    let upstream_url = format!("{}{}", state.config.upstream_base_url, uri);

    let result = state
        .client
        .request(http::Method::POST, &upstream_url)
        .headers(headers)
        .body(body)
        .send()
        .await;

    let latency_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(upstream_resp) => {
            let http_status = upstream_resp.status().as_u16();
            let resp_headers = upstream_resp.headers().clone();

            match upstream_resp.bytes().await {
                Ok(resp_body) => {
                    let is_5xx = http_status >= 500;

                    // Force-sample on 5xx
                    if is_5xx && !sampled {
                        sampled = true;
                        // Emit the request event we skipped
                        let req_event = RequestEvent::new(
                            request_id_str.clone(),
                            true,
                            request_meta.unsupported_shape,
                            DbInfo {
                                kind: db_info.kind.clone(),
                                collection: db_info.collection.clone(),
                            },
                            QueryInfo {
                                text: request_meta.query.text.clone(),
                                hash: request_meta.query.hash.clone(),
                                top_k: request_meta.query.top_k,
                            },
                        );
                        state.event_queue.send(Event::Request(req_event));
                    }

                    if sampled {
                        let response_meta =
                            backend.parse_response(&request_match, http_status, &resp_body);

                        let error_kind = if is_5xx {
                            Some(ErrorKind::Upstream5xx)
                        } else {
                            None
                        };

                        let resp_event = ResponseEvent::new(
                            request_id_str,
                            StatusInfo {
                                ok: http_status < 400,
                                http_status,
                                error_kind,
                            },
                            TimingInfo { latency_ms },
                            ResultInfo {
                                hits: response_meta.hits,
                            },
                        );
                        state.event_queue.send(Event::Response(resp_event));
                    }

                    // Build and return the response
                    let mut response = Response::builder().status(http_status);
                    for (name, value) in resp_headers.iter() {
                        response = response.header(name.as_str(), value.as_bytes());
                    }
                    response
                        .body(Body::from(resp_body))
                        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to read upstream response body");
                    emit_error_events(
                        &state,
                        &ErrorContext {
                            already_sampled: sampled,
                            request_id: &request_id_str,
                            request_meta: &request_meta,
                            db_info: &db_info,
                            latency_ms,
                            http_status: 502,
                            error_kind: ErrorKind::DecodeError,
                        },
                    );
                    StatusCode::BAD_GATEWAY.into_response()
                }
            }
        }
        Err(e) => {
            let (status, error_kind) = if e.is_timeout() {
                (StatusCode::GATEWAY_TIMEOUT, ErrorKind::UpstreamTimeout)
            } else if e.is_connect() {
                (StatusCode::BAD_GATEWAY, ErrorKind::UpstreamConnectError)
            } else {
                (StatusCode::BAD_GATEWAY, ErrorKind::InternalProxyError)
            };
            tracing::error!(error = %e, url = %upstream_url, "upstream request failed");
            emit_error_events(
                &state,
                &ErrorContext {
                    already_sampled: sampled,
                    request_id: &request_id_str,
                    request_meta: &request_meta,
                    db_info: &db_info,
                    latency_ms,
                    http_status: status.as_u16(),
                    error_kind,
                },
            );
            status.into_response()
        }
    }
}

struct ErrorContext<'a> {
    already_sampled: bool,
    request_id: &'a str,
    request_meta: &'a crate::backend::RequestMeta,
    db_info: &'a DbInfo,
    latency_ms: u64,
    http_status: u16,
    error_kind: ErrorKind,
}

/// Emit both request and response events for error cases, force-sampling if needed.
fn emit_error_events(state: &AppState, ctx: &ErrorContext<'_>) {
    // If not already sampled, emit the request event now (force-sample)
    if !ctx.already_sampled {
        let req_event = RequestEvent::new(
            ctx.request_id.to_string(),
            true,
            ctx.request_meta.unsupported_shape,
            DbInfo {
                kind: ctx.db_info.kind.clone(),
                collection: ctx.db_info.collection.clone(),
            },
            QueryInfo {
                text: ctx.request_meta.query.text.clone(),
                hash: ctx.request_meta.query.hash.clone(),
                top_k: ctx.request_meta.query.top_k,
            },
        );
        state.event_queue.send(Event::Request(req_event));
    }

    let resp_event = ResponseEvent::new(
        ctx.request_id.to_string(),
        StatusInfo {
            ok: false,
            http_status: ctx.http_status,
            error_kind: Some(ctx.error_kind.clone()),
        },
        TimingInfo {
            latency_ms: ctx.latency_ms,
        },
        ResultInfo { hits: vec![] },
    );
    state.event_queue.send(Event::Response(resp_event));
}

async fn forward_raw(
    client: &Client,
    upstream_base_url: &str,
    method: &Method,
    uri: &Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let upstream_url = format!("{}{}", upstream_base_url, uri);

    let req_builder = client
        .request(method.clone(), &upstream_url)
        .headers(headers)
        .body(body);

    match req_builder.send().await {
        Ok(upstream_resp) => {
            let status = upstream_resp.status();
            let resp_headers = upstream_resp.headers().clone();
            match upstream_resp.bytes().await {
                Ok(resp_body) => {
                    let mut response = Response::builder().status(status.as_u16());
                    for (name, value) in resp_headers.iter() {
                        response = response.header(name.as_str(), value.as_bytes());
                    }
                    response
                        .body(Body::from(resp_body))
                        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to read upstream response body");
                    StatusCode::BAD_GATEWAY.into_response()
                }
            }
        }
        Err(e) => {
            tracing::error!(error = %e, url = %upstream_url, "upstream request failed");
            if e.is_timeout() {
                StatusCode::GATEWAY_TIMEOUT.into_response()
            } else {
                StatusCode::BAD_GATEWAY.into_response()
            }
        }
    }
}
