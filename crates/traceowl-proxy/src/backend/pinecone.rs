use http::Method;

use crate::events::{HitInfo, QueryInfo};

use super::{
    BackendHandler, RequestMatch, RequestMeta, ResponseMeta,
    hashing::{QueryRepresentation, compute_query_hash},
};

pub struct PineconeHandler;

impl BackendHandler for PineconeHandler {
    fn match_request(&self, method: &Method, path: &str) -> Option<RequestMatch> {
        if method != Method::POST {
            return None;
        }
        let path = path.trim_end_matches('/');
        if path == "/query" {
            Some(RequestMatch {
                db_kind: "pinecone".to_string(),
                // Collection (index) is host-based in Pinecone and not in the URL
                // path. We populate it from the namespace in parse_request via
                // collection_override.
                collection: String::new(),
                path: path.to_string(),
            })
        } else {
            None
        }
    }

    fn parse_request(&self, _matched: &RequestMatch, body: &[u8]) -> RequestMeta {
        let parsed: Result<serde_json::Value, _> = serde_json::from_slice(body);

        match parsed {
            Ok(body_json) => {
                let top_k = body_json["topK"].as_u64().unwrap_or(10);
                let namespace = body_json["namespace"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();

                match extract_dense_vector(&body_json) {
                    Some(vec) => {
                        let repr = QueryRepresentation::Vector(vec);
                        let hash = compute_query_hash(&namespace, None, top_k, &repr);
                        RequestMeta {
                            unsupported_shape: false,
                            collection_override: Some(namespace),
                            query: QueryInfo {
                                representation: None,
                                hash,
                                top_k,
                            },
                        }
                    }
                    None => RequestMeta {
                        unsupported_shape: true,
                        collection_override: Some(namespace),
                        query: QueryInfo {
                            representation: None,
                            hash: String::new(),
                            top_k,
                        },
                    },
                }
            }
            Err(_) => RequestMeta {
                unsupported_shape: true,
                collection_override: Some(String::new()),
                query: QueryInfo {
                    representation: None,
                    hash: String::new(),
                    top_k: 0,
                },
            },
        }
    }

    fn parse_response(&self, _matched: &RequestMatch, _status: u16, body: &[u8]) -> ResponseMeta {
        let hits = parse_pinecone_hits(body);
        ResponseMeta { hits }
    }
}

/// Extract a dense vector from a Pinecone request body.
/// Supports: `{ "vector": [0.1, 0.2, ...], ... }`
/// Returns `None` for id-based, text-based, or other unsupported shapes.
fn extract_dense_vector(body: &serde_json::Value) -> Option<Vec<f32>> {
    if let serde_json::Value::Array(arr) = &body["vector"]
        && !arr.is_empty()
        && arr.iter().all(|v| v.is_number())
    {
        let floats: Vec<f32> = arr
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect();
        if floats.len() == arr.len() {
            return Some(floats);
        }
    }
    None
}

fn parse_pinecone_hits(body: &[u8]) -> Vec<HitInfo> {
    let parsed: Result<serde_json::Value, _> = serde_json::from_slice(body);
    let mut hits = Vec::new();

    if let Ok(json) = parsed
        && let Some(matches) = json["matches"].as_array()
    {
        for (rank, m) in matches.iter().enumerate() {
            let doc_id = m["id"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let score = m["score"].as_f64().unwrap_or(0.0);
            hits.push(HitInfo {
                doc_id,
                rank: (rank + 1) as u32,
                score,
            });
        }
    }

    hits
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_match() -> RequestMatch {
        RequestMatch {
            db_kind: "pinecone".to_string(),
            collection: String::new(),
            path: "/query".to_string(),
        }
    }

    #[test]
    fn test_match_query_endpoint() {
        let handler = PineconeHandler;
        let m = handler.match_request(&Method::POST, "/query");
        assert!(m.is_some());
        let m = m.unwrap();
        assert_eq!(m.db_kind, "pinecone");
    }

    #[test]
    fn test_no_match_get() {
        let handler = PineconeHandler;
        assert!(handler.match_request(&Method::GET, "/query").is_none());
    }

    #[test]
    fn test_no_match_other_path() {
        let handler = PineconeHandler;
        assert!(handler.match_request(&Method::POST, "/vectors/upsert").is_none());
    }

    #[test]
    fn test_supported_shape_with_namespace() {
        let handler = PineconeHandler;
        let body = br#"{
            "vector": [0.1, 0.2, 0.3],
            "topK": 5,
            "namespace": "my-ns"
        }"#;
        let meta = handler.parse_request(&make_match(), body);
        assert!(!meta.unsupported_shape);
        assert_eq!(meta.query.top_k, 5);
        assert!(!meta.query.hash.is_empty());
        assert_eq!(meta.collection_override, Some("my-ns".to_string()));
    }

    #[test]
    fn test_supported_shape_without_namespace() {
        let handler = PineconeHandler;
        let body = br#"{"vector": [0.1, 0.2, 0.3], "topK": 10}"#;
        let meta = handler.parse_request(&make_match(), body);
        assert!(!meta.unsupported_shape);
        assert!(!meta.query.hash.is_empty());
        assert_eq!(meta.collection_override, Some(String::new()));
    }

    #[test]
    fn test_unsupported_id_based_query() {
        let handler = PineconeHandler;
        let body = br#"{"id": "vec1", "topK": 5, "namespace": "ns"}"#;
        let meta = handler.parse_request(&make_match(), body);
        assert!(meta.unsupported_shape);
        assert!(meta.query.hash.is_empty());
    }

    #[test]
    fn test_unsupported_missing_vector() {
        let handler = PineconeHandler;
        let body = br#"{"topK": 5}"#;
        let meta = handler.parse_request(&make_match(), body);
        assert!(meta.unsupported_shape);
    }

    #[test]
    fn test_hash_deterministic() {
        let handler = PineconeHandler;
        let body = br#"{"vector": [0.1, 0.2, 0.3], "topK": 5, "namespace": "ns"}"#;
        let h1 = handler.parse_request(&make_match(), body).query.hash;
        let h2 = handler.parse_request(&make_match(), body).query.hash;
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_response_extraction() {
        let body = br#"{
            "matches": [
                {"id": "vec1", "score": 0.95},
                {"id": "vec2", "score": 0.80}
            ],
            "namespace": "ns"
        }"#;
        let hits = parse_pinecone_hits(body);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].doc_id, "vec1");
        assert_eq!(hits[0].rank, 1);
        assert_eq!(hits[0].score, 0.95);
        assert_eq!(hits[1].doc_id, "vec2");
        assert_eq!(hits[1].rank, 2);
        assert_eq!(hits[1].score, 0.80);
    }

    #[test]
    fn test_response_empty_matches() {
        let body = br#"{"matches": [], "namespace": "ns"}"#;
        let hits = parse_pinecone_hits(body);
        assert!(hits.is_empty());
    }

    #[test]
    fn test_end_to_end_request_creates_event_fields() {
        // Verify that a complete Pinecone request produces all fields
        // needed to build a request event.
        let handler = PineconeHandler;
        let matched = handler
            .match_request(&Method::POST, "/query")
            .unwrap();
        assert_eq!(matched.db_kind, "pinecone");

        let body = br#"{
            "vector": [0.5, 0.6, 0.7],
            "topK": 3,
            "namespace": "prod",
            "includeValues": false,
            "includeMetadata": true
        }"#;
        let meta = handler.parse_request(&matched, body);
        assert!(!meta.unsupported_shape);
        assert_eq!(meta.query.top_k, 3);
        assert!(!meta.query.hash.is_empty());
        assert_eq!(meta.collection_override, Some("prod".to_string()));
    }
}
