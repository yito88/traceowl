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
    let request_id_str = request_id.to_string();
    let sampled = sampling::is_sampled(&request_id, state.config.sampling_rate);
    let start_timestamp_ms = now_ms();

    // Forward request upstream — no parsing yet
    let start = Instant::now();
    let upstream_url = format!("{}{}", state.config.upstream_base_url, uri);

    let result = state
        .client
        .request(http::Method::POST, &upstream_url)
        .headers(headers)
        .body(body.clone())
        .send()
        .await;

    let latency_ms = start.elapsed().as_millis() as u64;

    // Determine if we need to emit events (sampled, or force-sample on error)
    let (http_response, should_emit, http_status, error_kind, resp_body_for_parse) = match result {
        Ok(upstream_resp) => {
            let http_status = upstream_resp.status().as_u16();
            let resp_headers = upstream_resp.headers().clone();

            match upstream_resp.bytes().await {
                Ok(resp_body) => {
                    let is_5xx = http_status >= 500;
                    let should_emit = sampled || is_5xx;
                    let error_kind = if is_5xx {
                        Some(ErrorKind::Upstream5xx)
                    } else {
                        None
                    };

                    let mut response = Response::builder().status(http_status);
                    for (name, value) in resp_headers.iter() {
                        response = response.header(name.as_str(), value.as_bytes());
                    }
                    let http_response = response
                        .body(Body::from(resp_body.clone()))
                        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());

                    (
                        http_response,
                        should_emit,
                        http_status,
                        error_kind,
                        Some(resp_body),
                    )
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to read upstream response body");
                    (
                        StatusCode::BAD_GATEWAY.into_response(),
                        true, // always emit on error
                        502,
                        Some(ErrorKind::DecodeError),
                        None,
                    )
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
            (
                status.into_response(),
                true, // always emit on error
                status.as_u16(),
                Some(error_kind),
                None,
            )
        }
    };

    // Only parse and enqueue events if needed
    if should_emit {
        let backend = state
            .backends
            .iter()
            .find(|b| {
                b.match_request(&http::Method::POST, request_match.path())
                    .is_some()
            })
            .unwrap();

        let request_meta = backend.parse_request(&request_match, &body);
        let db_info = DbInfo::from(&request_match);

        let req_event = RequestEvent::new(
            request_id_str.clone(),
            start_timestamp_ms,
            true,
            request_meta.unsupported_shape,
            DbInfo {
                kind: db_info.kind.clone(),
                collection: db_info.collection.clone(),
            },
            QueryInfo {
                text: request_meta.query.text,
                hash: request_meta.query.hash,
                top_k: request_meta.query.top_k,
            },
        );
        state.event_queue.send(Event::Request(req_event));

        let hits = match resp_body_for_parse {
            Some(ref resp_body) => {
                backend
                    .parse_response(&request_match, http_status, resp_body)
                    .hits
            }
            None => vec![],
        };

        let resp_event = ResponseEvent::new(
            request_id_str,
            StatusInfo {
                ok: http_status < 400,
                http_status,
                error_kind,
            },
            TimingInfo { latency_ms },
            ResultInfo { hits },
        );
        state.event_queue.send(Event::Response(resp_event));
    }

    http_response
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
