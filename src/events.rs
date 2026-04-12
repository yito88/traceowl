use serde::Serialize;

use crate::error::ErrorKind;

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Event {
    Request(RequestEvent),
    Response(ResponseEvent),
}

#[derive(Debug, Serialize)]
pub struct RequestEvent {
    pub schema_version: u32,
    pub event_type: &'static str,
    pub request_id: String,
    pub timestamp_ms: u64,
    pub sampled: bool,
    pub unsupported_shape: bool,
    pub db: DbInfo,
    pub query: QueryInfo,
}

#[derive(Debug, Serialize)]
pub struct ResponseEvent {
    pub schema_version: u32,
    pub event_type: &'static str,
    pub request_id: String,
    pub timestamp_ms: u64,
    pub status: StatusInfo,
    pub timing: TimingInfo,
    pub result: ResultInfo,
}

#[derive(Debug, Serialize)]
pub struct DbInfo {
    pub kind: String,
    pub collection: String,
}

#[derive(Debug, Serialize)]
pub struct QueryInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    pub hash: String,
    pub top_k: u64,
}

#[derive(Debug, Serialize)]
pub struct StatusInfo {
    pub ok: bool,
    pub http_status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<ErrorKind>,
}

#[derive(Debug, Serialize)]
pub struct TimingInfo {
    pub latency_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct HitInfo {
    pub doc_id: String,
    pub rank: u32,
    pub score: f64,
}

#[derive(Debug, Serialize)]
pub struct ResultInfo {
    pub hits: Vec<HitInfo>,
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

impl RequestEvent {
    pub fn new(
        request_id: String,
        sampled: bool,
        unsupported_shape: bool,
        db: DbInfo,
        query: QueryInfo,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            event_type: "request",
            request_id,
            timestamp_ms: now_ms(),
            sampled,
            unsupported_shape,
            db,
            query,
        }
    }
}

impl ResponseEvent {
    pub fn new(
        request_id: String,
        status: StatusInfo,
        timing: TimingInfo,
        result: ResultInfo,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            event_type: "response",
            request_id,
            timestamp_ms: now_ms(),
            status,
            timing,
            result,
        }
    }
}
