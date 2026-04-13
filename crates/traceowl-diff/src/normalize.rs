use crate::join::JoinedRetrieval;

/// A normalized retrieval record for comparison.
#[derive(Debug, Clone)]
pub struct NormalizedRetrieval {
    pub request_id: String,
    pub query_hash: String,
    pub collection: String,
    pub top_k: u64,
    pub hits: Vec<NormalizedHit>,
}

#[derive(Debug, Clone)]
pub struct NormalizedHit {
    pub doc_id: String,
    pub rank: u32,
    pub score: f64,
}

/// Build normalized retrieval records from joined pairs.
/// Only keeps successful retrievals (status.ok == true).
pub fn build_retrievals(joined: Vec<JoinedRetrieval>) -> Vec<NormalizedRetrieval> {
    let total = joined.len();
    let mut retrievals = Vec::new();
    let mut filtered = 0u64;

    for pair in joined {
        if !pair.response.status.ok {
            filtered += 1;
            continue;
        }

        let hits = pair
            .response
            .result
            .hits
            .into_iter()
            .map(|h| NormalizedHit {
                doc_id: h.doc_id,
                rank: h.rank,
                score: h.score,
            })
            .collect();

        retrievals.push(NormalizedRetrieval {
            request_id: pair.request.request_id,
            query_hash: pair.request.query.hash,
            collection: pair.request.db.collection,
            top_k: pair.request.query.top_k,
            hits,
        });
    }

    tracing::info!(
        total,
        kept = retrievals.len(),
        filtered_non_ok = filtered,
        "normalized retrievals"
    );

    retrievals
}

#[cfg(test)]
mod tests {
    use super::*;
    use traceowl_schema::event_v1::*;

    fn make_joined(id: &str, ok: bool) -> JoinedRetrieval {
        JoinedRetrieval {
            request: RequestEventV1::new(
                id.to_string(),
                100,
                true,
                false,
                DbInfoV1 {
                    kind: "qdrant".to_string(),
                    collection: "test_col".to_string(),
                },
                QueryInfoV1 {
                    representation: None,
                    hash: format!("hash_{id}"),
                    top_k: 10,
                },
            ),
            response: ResponseEventV1::new(
                id.to_string(),
                200,
                StatusInfoV1 {
                    ok,
                    http_status: if ok { 200 } else { 502 },
                    error_kind: None,
                },
                TimingInfoV1 { latency_ms: 5 },
                ResultInfoV1 {
                    hits: vec![HitV1 {
                        doc_id: "doc1".to_string(),
                        rank: 1,
                        score: 0.9,
                    }],
                },
            ),
        }
    }

    #[test]
    fn test_build_retrievals_success() {
        let joined = vec![make_joined("req1", true)];
        let retrievals = build_retrievals(joined);
        assert_eq!(retrievals.len(), 1);
        assert_eq!(retrievals[0].request_id, "req1");
        assert_eq!(retrievals[0].query_hash, "hash_req1");
        assert_eq!(retrievals[0].collection, "test_col");
        assert_eq!(retrievals[0].top_k, 10);
        assert_eq!(retrievals[0].hits.len(), 1);
    }

    #[test]
    fn test_filter_non_ok() {
        let joined = vec![make_joined("req1", true), make_joined("req2", false)];
        let retrievals = build_retrievals(joined);
        assert_eq!(retrievals.len(), 1);
        assert_eq!(retrievals[0].request_id, "req1");
    }

    #[test]
    fn test_all_filtered() {
        let joined = vec![make_joined("req1", false)];
        let retrievals = build_retrievals(joined);
        assert_eq!(retrievals.len(), 0);
    }
}
