use serde::{Deserialize, Serialize};
use std::fmt;

pub const EVENT_SCHEMA_VERSION_V1: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "event_type")]
pub enum EventV1 {
    #[serde(rename = "request")]
    Request(RequestEventV1),
    #[serde(rename = "response")]
    Response(ResponseEventV1),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequestEventV1 {
    pub schema_version: u32,
    pub request_id: String,
    pub timestamp_ms: u64,
    pub sampled: bool,
    pub unsupported_shape: bool,
    pub db: DbInfoV1,
    pub query: QueryInfoV1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResponseEventV1 {
    pub schema_version: u32,
    pub request_id: String,
    pub timestamp_ms: u64,
    pub status: StatusInfoV1,
    pub timing: TimingInfoV1,
    pub result: ResultInfoV1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DbInfoV1 {
    pub kind: String,
    pub collection: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryInfoV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub representation: Option<String>,
    pub hash: String,
    pub top_k: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatusInfoV1 {
    pub ok: bool,
    pub http_status: u16,
    pub error_kind: Option<ErrorKindV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimingInfoV1 {
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HitV1 {
    pub doc_id: String,
    pub rank: u32,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResultInfoV1 {
    pub hits: Vec<HitV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ErrorKindV1 {
    #[serde(rename = "upstream_timeout")]
    UpstreamTimeout,
    #[serde(rename = "upstream_connect_error")]
    UpstreamConnectError,
    #[serde(rename = "upstream_5xx")]
    Upstream5xx,
    #[serde(rename = "decode_error")]
    DecodeError,
    #[serde(rename = "internal_proxy_error")]
    InternalProxyError,
}

impl fmt::Display for ErrorKindV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKindV1::UpstreamTimeout => write!(f, "upstream_timeout"),
            ErrorKindV1::UpstreamConnectError => write!(f, "upstream_connect_error"),
            ErrorKindV1::Upstream5xx => write!(f, "upstream_5xx"),
            ErrorKindV1::DecodeError => write!(f, "decode_error"),
            ErrorKindV1::InternalProxyError => write!(f, "internal_proxy_error"),
        }
    }
}

impl RequestEventV1 {
    pub fn new(
        request_id: String,
        timestamp_ms: u64,
        sampled: bool,
        unsupported_shape: bool,
        db: DbInfoV1,
        query: QueryInfoV1,
    ) -> Self {
        Self {
            schema_version: EVENT_SCHEMA_VERSION_V1,
            request_id,
            timestamp_ms,
            sampled,
            unsupported_shape,
            db,
            query,
        }
    }
}

impl ResponseEventV1 {
    pub fn new(
        request_id: String,
        timestamp_ms: u64,
        status: StatusInfoV1,
        timing: TimingInfoV1,
        result: ResultInfoV1,
    ) -> Self {
        Self {
            schema_version: EVENT_SCHEMA_VERSION_V1,
            request_id,
            timestamp_ms,
            status,
            timing,
            result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> RequestEventV1 {
        RequestEventV1::new(
            "019d868b-09ae-7911-b3d8-bebfaa0bcbcb".to_string(),
            1776078752172,
            true,
            false,
            DbInfoV1 {
                kind: "qdrant".to_string(),
                collection: "stress_test".to_string(),
            },
            QueryInfoV1 {
                representation: None,
                hash: "70ca9986ff308aee97cc8b11e95cbc0b8b739d9f4f096fd54245133dd451742d"
                    .to_string(),
                top_k: 10,
            },
        )
    }

    fn sample_response() -> ResponseEventV1 {
        ResponseEventV1::new(
            "019d868a-9c2c-73f1-8dfa-e6bef409f52b".to_string(),
            1776078724140,
            StatusInfoV1 {
                ok: true,
                http_status: 200,
                error_kind: None,
            },
            TimingInfoV1 { latency_ms: 1 },
            ResultInfoV1 {
                hits: vec![HitV1 {
                    doc_id: "914".to_string(),
                    rank: 1,
                    score: 0.8315756,
                }],
            },
        )
    }

    #[test]
    fn test_request_serialization_shape() {
        let req = sample_request();
        let json = serde_json::to_value(EventV1::Request(req)).unwrap();
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["event_type"], "request");
        assert_eq!(json["request_id"], "019d868b-09ae-7911-b3d8-bebfaa0bcbcb");
        assert_eq!(json["sampled"], true);
        assert_eq!(json["unsupported_shape"], false);
        assert_eq!(json["db"]["kind"], "qdrant");
        assert_eq!(json["db"]["collection"], "stress_test");
        assert_eq!(json["query"]["top_k"], 10);
        assert!(!json["query"]["hash"].as_str().unwrap().is_empty());
        assert!(json.get("representation").is_none() || json["query"]["representation"].is_null());
    }

    #[test]
    fn test_response_serialization_shape() {
        let resp = sample_response();
        let json = serde_json::to_value(EventV1::Response(resp)).unwrap();
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["event_type"], "response");
        assert_eq!(json["status"]["ok"], true);
        assert_eq!(json["status"]["http_status"], 200);
        assert!(json["status"]["error_kind"].is_null());
        assert_eq!(json["timing"]["latency_ms"], 1);
        let hits = json["result"]["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["doc_id"], "914");
        assert_eq!(hits[0]["rank"], 1);
    }

    #[test]
    fn test_request_deserialization() {
        let json_str = r#"{
            "schema_version": 1,
            "event_type": "request",
            "request_id": "019d868b-09ae-7911-b3d8-bebfaa0bcbcb",
            "timestamp_ms": 1776078752172,
            "sampled": true,
            "unsupported_shape": false,
            "db": { "kind": "qdrant", "collection": "stress_test" },
            "query": { "hash": "70ca9986ff308aee97cc8b11e95cbc0b8b739d9f4f096fd54245133dd451742d", "top_k": 10 }
        }"#;
        let event: EventV1 = serde_json::from_str(json_str).unwrap();
        if let EventV1::Request(req) = event {
            assert_eq!(req.schema_version, 1);
            assert_eq!(req.db.kind, "qdrant");
            assert_eq!(req.query.top_k, 10);
        } else {
            panic!("expected request event");
        }
    }

    #[test]
    fn test_response_deserialization() {
        let json_str = r#"{
            "schema_version": 1,
            "event_type": "response",
            "request_id": "019d868a-9c2c-73f1-8dfa-e6bef409f52b",
            "timestamp_ms": 1776078724140,
            "status": { "ok": true, "http_status": 200, "error_kind": null },
            "timing": { "latency_ms": 1 },
            "result": { "hits": [{ "doc_id": "914", "rank": 1, "score": 0.8315756 }] }
        }"#;
        let event: EventV1 = serde_json::from_str(json_str).unwrap();
        if let EventV1::Response(resp) = event {
            assert_eq!(resp.status.http_status, 200);
            assert!(resp.status.error_kind.is_none());
            assert_eq!(resp.result.hits.len(), 1);
        } else {
            panic!("expected response event");
        }
    }

    #[test]
    fn test_error_kind_deserialization() {
        let json_str = r#"{
            "schema_version": 1,
            "event_type": "response",
            "request_id": "test-id",
            "timestamp_ms": 100,
            "status": { "ok": false, "http_status": 502, "error_kind": "upstream_connect_error" },
            "timing": { "latency_ms": 5 },
            "result": { "hits": [] }
        }"#;
        let event: EventV1 = serde_json::from_str(json_str).unwrap();
        if let EventV1::Response(resp) = event {
            assert_eq!(
                resp.status.error_kind,
                Some(ErrorKindV1::UpstreamConnectError)
            );
        } else {
            panic!("expected response event");
        }
    }

    #[test]
    fn test_roundtrip_request() {
        let req = sample_request();
        let event = EventV1::Request(req.clone());
        let json_str = serde_json::to_string(&event).unwrap();
        let deserialized: EventV1 = serde_json::from_str(&json_str).unwrap();
        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_roundtrip_response() {
        let resp = sample_response();
        let event = EventV1::Response(resp.clone());
        let json_str = serde_json::to_string(&event).unwrap();
        let deserialized: EventV1 = serde_json::from_str(&json_str).unwrap();
        assert_eq!(event, deserialized);
    }
}
