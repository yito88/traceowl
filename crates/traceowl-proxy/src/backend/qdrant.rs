use http::Method;

use crate::events::{HitInfo, QueryInfo};

use super::{
    BackendHandler, RequestMatch, RequestMeta, ResponseMeta,
    hashing::{QueryRepresentation, compute_query_hash},
};

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
                let top_k = body_json["limit"]
                    .as_u64()
                    .or_else(|| body_json["top"].as_u64())
                    .unwrap_or(10);

                match extract_dense_vector(&body_json["query"]) {
                    Some(vec) => {
                        let repr = QueryRepresentation::Vector(vec);
                        let hash = compute_query_hash(&matched.collection, None, top_k, &repr);
                        RequestMeta {
                            unsupported_shape: false,
                            collection_override: None,
                            query: QueryInfo {
                                representation: None,
                                hash,
                                top_k,
                            },
                        }
                    }
                    None => RequestMeta {
                        unsupported_shape: true,
                        collection_override: None,
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
                collection_override: None,
                query: QueryInfo {
                    representation: None,
                    hash: String::new(),
                    top_k: 0,
                },
            },
        }
    }

    fn parse_response(&self, _matched: &RequestMatch, _status: u16, body: &[u8]) -> ResponseMeta {
        let hits = parse_qdrant_hits(body);
        ResponseMeta { hits }
    }
}

/// Try to extract a dense vector from the query field.
/// Supports:
///   - Plain array: [0.1, 0.2, ...]
///   - Wrapped object: {"nearest": [0.1, 0.2, ...]}
///
/// Returns `None` for unsupported shapes.
fn extract_dense_vector(query: &serde_json::Value) -> Option<Vec<f32>> {
    if let Some(vec) = as_f32_array(query) {
        return Some(vec);
    }
    if let Some(obj) = query.as_object()
        && let Some(nearest) = obj.get("nearest")
        && let Some(vec) = as_f32_array(nearest)
    {
        return Some(vec);
    }
    None
}

fn as_f32_array(value: &serde_json::Value) -> Option<Vec<f32>> {
    if let serde_json::Value::Array(arr) = value
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

fn parse_qdrant_hits(body: &[u8]) -> Vec<HitInfo> {
    let parsed: Result<serde_json::Value, _> = serde_json::from_slice(body);
    let mut hits = Vec::new();

    if let Ok(json) = parsed
        && let Some(points) = json["result"]["points"]
            .as_array()
            .or_else(|| json["result"].as_array())
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
        assert!(
            handler
                .match_request(&Method::GET, "/collections/my_col/points/query")
                .is_none()
        );
    }

    #[test]
    fn test_no_match_other_path() {
        let handler = QdrantHandler;
        assert!(
            handler
                .match_request(&Method::POST, "/collections/my_col/points/search")
                .is_none()
        );
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
        assert!(meta.collection_override.is_none());
    }

    #[test]
    fn test_parse_wrapped_nearest_vector() {
        let handler = QdrantHandler;
        let matched = RequestMatch {
            db_kind: "qdrant".to_string(),
            collection: "test".to_string(),
            path: "/collections/test/points/query".to_string(),
        };
        let body = br#"{"query": {"nearest": [0.1, 0.2, 0.3]}, "limit": 5}"#;
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
    fn test_hash_from_vector_semantics() {
        // Verify the hash is derived from vector values, not JSON serialization.
        // Two bodies with numerically identical vectors but different JSON formatting
        // must produce the same hash.
        let handler = QdrantHandler;
        let matched = RequestMatch {
            db_kind: "qdrant".to_string(),
            collection: "col".to_string(),
            path: "/collections/col/points/query".to_string(),
        };
        let body1 = br#"{"query": [0.100, 0.200, 0.300], "limit": 10}"#;
        let body2 = br#"{"query": [0.1, 0.2, 0.3], "limit": 10}"#;
        let meta1 = handler.parse_request(&matched, body1);
        let meta2 = handler.parse_request(&matched, body2);
        assert_eq!(meta1.query.hash, meta2.query.hash);
    }

    #[test]
    fn test_query_hash_deterministic() {
        let handler = QdrantHandler;
        let matched = RequestMatch {
            db_kind: "qdrant".to_string(),
            collection: "col".to_string(),
            path: "/collections/col/points/query".to_string(),
        };
        let body = br#"{"query": [0.1, 0.2, 0.3], "limit": 10}"#;
        let h1 = handler.parse_request(&matched, body).query.hash;
        let h2 = handler.parse_request(&matched, body).query.hash;
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
