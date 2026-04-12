use http::Method;
use sha2::{Digest, Sha256};

use crate::events::{HitInfo, QueryInfo};

use super::{BackendHandler, RequestMatch, RequestMeta, ResponseMeta};

pub struct QdrantHandler;

impl BackendHandler for QdrantHandler {
    fn match_request(&self, method: &Method, path: &str) -> Option<RequestMatch> {
        if method != Method::POST {
            return None;
        }
        // Match: /collections/{collection_name}/points/query
        let path = path.trim_end_matches('/');
        let parts: Vec<&str> = path.split('/').collect();
        // Expected: ["", "collections", "{name}", "points", "query"]
        if parts.len() == 5
            && parts[1] == "collections"
            && parts[3] == "points"
            && parts[4] == "query"
        {
            Some(RequestMatch {
                db_kind: "qdrant".to_string(),
                collection: parts[2].to_string(),
                path: path.to_string(),
            })
        } else {
            None
        }
    }

    fn parse_request(&self, matched: &RequestMatch, body: &[u8]) -> RequestMeta {
        let parsed: Result<serde_json::Value, _> = serde_json::from_slice(body);

        match parsed {
            Ok(body_json) => {
                let (unsupported_shape, query_text) = match &body_json["query"] {
                    serde_json::Value::Array(arr) => {
                        // Simple dense vector: array of numbers
                        let all_numbers = arr.iter().all(|v| v.is_number());
                        if all_numbers && !arr.is_empty() {
                            (false, serde_json::to_string(arr).unwrap_or_default())
                        } else {
                            (
                                true,
                                serde_json::to_string(&body_json["query"]).unwrap_or_default(),
                            )
                        }
                    }
                    _ => {
                        // Any other shape (object, string, null) is unsupported
                        (
                            true,
                            serde_json::to_string(&body_json["query"]).unwrap_or_default(),
                        )
                    }
                };

                let top_k = body_json["limit"]
                    .as_u64()
                    .or_else(|| body_json["top"].as_u64())
                    .unwrap_or(10);

                let normalized_text = normalize_query_text(&query_text);
                let hash = compute_query_hash(&matched.collection, &normalized_text, top_k);

                RequestMeta {
                    unsupported_shape,
                    query: QueryInfo {
                        text: Some(normalized_text),
                        hash,
                        top_k,
                    },
                }
            }
            Err(_) => {
                // Can't parse body — mark as unsupported but don't fail forwarding
                RequestMeta {
                    unsupported_shape: true,
                    query: QueryInfo {
                        text: None,
                        hash: String::new(),
                        top_k: 0,
                    },
                }
            }
        }
    }

    fn parse_response(&self, _matched: &RequestMatch, _status: u16, body: &[u8]) -> ResponseMeta {
        let hits = parse_qdrant_hits(body);
        ResponseMeta { hits }
    }
}

fn normalize_query_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<&str>>().join(" ")
}

fn compute_query_hash(collection: &str, normalized_text: &str, top_k: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(collection.as_bytes());
    hasher.update(normalized_text.as_bytes());
    hasher.update(top_k.to_le_bytes());
    format!("{:x}", hasher.finalize())
}

fn parse_qdrant_hits(body: &[u8]) -> Vec<HitInfo> {
    let parsed: Result<serde_json::Value, _> = serde_json::from_slice(body);
    let mut hits = Vec::new();

    if let Ok(json) = parsed {
        if let Some(points) = json["result"]
            .as_array()
            .or_else(|| json["points"].as_array())
        {
            for (rank, point) in points.iter().enumerate() {
                let doc_id = point["id"]
                    .as_str()
                    .map(|s| s.to_string())
                    .or_else(|| point["id"].as_u64().map(|n| n.to_string()))
                    .or_else(|| point["id"].as_f64().map(|n| n.to_string()))
                    .unwrap_or_default();

                let score = point["score"].as_f64().unwrap_or(0.0);

                hits.push(HitInfo {
                    doc_id,
                    rank: (rank + 1) as u32,
                    score,
                });
            }
        }
    }

    hits
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_query_endpoint() {
        let handler = QdrantHandler;
        let m = handler.match_request(&Method::POST, "/collections/my_col/points/query");
        assert!(m.is_some());
        let m = m.unwrap();
        assert_eq!(m.db_kind, "qdrant");
        assert_eq!(m.collection, "my_col");
    }

    #[test]
    fn test_no_match_get() {
        let handler = QdrantHandler;
        assert!(handler
            .match_request(&Method::GET, "/collections/my_col/points/query")
            .is_none());
    }

    #[test]
    fn test_no_match_other_path() {
        let handler = QdrantHandler;
        assert!(handler
            .match_request(&Method::POST, "/collections/my_col/points/search")
            .is_none());
    }

    #[test]
    fn test_parse_simple_dense_query() {
        let handler = QdrantHandler;
        let matched = RequestMatch {
            db_kind: "qdrant".to_string(),
            collection: "test".to_string(),
            path: "/collections/test/points/query".to_string(),
        };
        let body = br#"{"query": [0.1, 0.2, 0.3], "limit": 5}"#;
        let meta = handler.parse_request(&matched, body);
        assert!(!meta.unsupported_shape);
        assert_eq!(meta.query.top_k, 5);
        assert!(!meta.query.hash.is_empty());
    }

    #[test]
    fn test_parse_unsupported_shape() {
        let handler = QdrantHandler;
        let matched = RequestMatch {
            db_kind: "qdrant".to_string(),
            collection: "test".to_string(),
            path: "/collections/test/points/query".to_string(),
        };
        let body = br#"{"query": {"nearest": {"id": 123}}, "limit": 5}"#;
        let meta = handler.parse_request(&matched, body);
        assert!(meta.unsupported_shape);
    }

    #[test]
    fn test_normalize_query_text() {
        assert_eq!(normalize_query_text("  hello   world  "), "hello world");
    }

    #[test]
    fn test_query_hash_deterministic() {
        let h1 = compute_query_hash("col", "text", 10);
        let h2 = compute_query_hash("col", "text", 10);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_parse_response_hits() {
        let body = br#"{"result": [{"id": "abc", "score": 0.95}, {"id": 42, "score": 0.8}]}"#;
        let hits = parse_qdrant_hits(body);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].doc_id, "abc");
        assert_eq!(hits[0].rank, 1);
        assert_eq!(hits[0].score, 0.95);
        assert_eq!(hits[1].doc_id, "42");
        assert_eq!(hits[1].rank, 2);
    }
}
