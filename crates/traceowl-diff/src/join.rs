use std::collections::HashMap;

use crate::events::{Event, RequestEvent, ResponseEvent};

/// A joined request+response pair for the same request_id.
pub struct JoinedRetrieval {
    pub request: RequestEvent,
    pub response: ResponseEvent,
}

/// Join request and response events by request_id.
/// Only returns pairs where both request and response exist.
pub fn join_events(events: Vec<Event>) -> Vec<JoinedRetrieval> {
    let mut requests: HashMap<String, RequestEvent> = HashMap::new();
    let mut responses: HashMap<String, ResponseEvent> = HashMap::new();

    for event in events {
        match event {
            Event::Request(req) => {
                requests.entry(req.request_id.clone()).or_insert(req);
            }
            Event::Response(resp) => {
                responses.entry(resp.request_id.clone()).or_insert(resp);
            }
        }
    }

    let total_requests = requests.len();
    let total_responses = responses.len();

    let mut joined = Vec::new();
    let mut orphan_requests = 0u64;

    for (request_id, request) in requests {
        if let Some(response) = responses.remove(&request_id) {
            joined.push(JoinedRetrieval { request, response });
        } else {
            orphan_requests += 1;
        }
    }

    let orphan_responses = responses.len() as u64;

    tracing::info!(
        total_requests,
        total_responses,
        joined = joined.len(),
        orphan_requests,
        orphan_responses,
        "join completed"
    );

    joined
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::*;

    fn make_request(id: &str) -> Event {
        Event::Request(RequestEvent::new(
            id.to_string(),
            100,
            true,
            false,
            DbInfo {
                kind: "qdrant".to_string(),
                collection: "test".to_string(),
            },
            QueryInfo {
                representation: None,
                hash: "hash1".to_string(),
                top_k: 10,
            },
        ))
    }

    fn make_response(id: &str, ok: bool) -> Event {
        Event::Response(ResponseEvent::new(
            id.to_string(),
            200,
            StatusInfo {
                ok,
                http_status: if ok { 200 } else { 502 },
                error_kind: None,
            },
            TimingInfo { latency_ms: 5 },
            ResultInfo {
                hits: vec![HitInfo {
                    doc_id: "doc1".to_string(),
                    rank: 1,
                    score: 0.9,
                }],
            },
        ))
    }

    #[test]
    fn test_join_matched_pair() {
        let events = vec![make_request("req1"), make_response("req1", true)];
        let joined = join_events(events);
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].request.request_id, "req1");
        assert_eq!(joined[0].response.request_id, "req1");
    }

    #[test]
    fn test_orphan_request_skipped() {
        let events = vec![make_request("req1")];
        let joined = join_events(events);
        assert_eq!(joined.len(), 0);
    }

    #[test]
    fn test_orphan_response_skipped() {
        let events = vec![make_response("req1", true)];
        let joined = join_events(events);
        assert_eq!(joined.len(), 0);
    }

    #[test]
    fn test_multiple_pairs() {
        let events = vec![
            make_request("req1"),
            make_request("req2"),
            make_response("req1", true),
            make_response("req2", true),
        ];
        let joined = join_events(events);
        assert_eq!(joined.len(), 2);
    }

    #[test]
    fn test_mixed_matched_and_orphans() {
        let events = vec![
            make_request("req1"),
            make_request("req2"),
            make_response("req1", true),
            make_response("req3", true),
        ];
        let joined = join_events(events);
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].request.request_id, "req1");
    }
}
