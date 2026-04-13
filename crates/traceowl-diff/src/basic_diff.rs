use std::collections::{BTreeSet, HashMap};

use crate::normalize::NormalizedRetrieval;
use crate::output::{
    DiffHit, DiffKey, DiffRecord, DiffSide, DiffSummary, RankChange, ScoreChange,
};

pub fn compute_diff(baseline: &NormalizedRetrieval, candidate: &NormalizedRetrieval) -> DiffRecord {
    let baseline_ids: BTreeSet<&str> = baseline.hits.iter().map(|h| h.doc_id.as_str()).collect();
    let candidate_ids: BTreeSet<&str> = candidate.hits.iter().map(|h| h.doc_id.as_str()).collect();

    let added_doc_ids: Vec<String> = candidate_ids
        .difference(&baseline_ids)
        .map(|s| s.to_string())
        .collect();
    let removed_doc_ids: Vec<String> = baseline_ids
        .difference(&candidate_ids)
        .map(|s| s.to_string())
        .collect();

    // Build lookup maps for docs present in both
    let baseline_map: HashMap<&str, &crate::normalize::NormalizedHit> =
        baseline.hits.iter().map(|h| (h.doc_id.as_str(), h)).collect();
    let candidate_map: HashMap<&str, &crate::normalize::NormalizedHit> =
        candidate.hits.iter().map(|h| (h.doc_id.as_str(), h)).collect();

    let common: BTreeSet<&str> = baseline_ids.intersection(&candidate_ids).copied().collect();

    let mut rank_changes = Vec::new();
    let mut score_changes = Vec::new();

    for doc_id in &common {
        let b = baseline_map[doc_id];
        let c = candidate_map[doc_id];

        if b.rank != c.rank {
            rank_changes.push(RankChange {
                doc_id: doc_id.to_string(),
                before_rank: b.rank,
                after_rank: c.rank,
            });
        }

        if (b.score - c.score).abs() > f64::EPSILON {
            score_changes.push(ScoreChange {
                doc_id: doc_id.to_string(),
                before_score: b.score,
                after_score: c.score,
            });
        }
    }

    let top_k = baseline.top_k;
    let top_k_overlap = if top_k > 0 {
        common.len() as f64 / top_k as f64
    } else {
        0.0
    };

    let baseline_top1 = baseline.hits.iter().find(|h| h.rank == 1).map(|h| &h.doc_id);
    let candidate_top1 = candidate
        .hits
        .iter()
        .find(|h| h.rank == 1)
        .map(|h| &h.doc_id);
    let top_1_changed = baseline_top1 != candidate_top1;

    DiffRecord {
        key: DiffKey {
            query_hash: baseline.query_hash.clone(),
            collection: baseline.collection.clone(),
            top_k,
        },
        baseline: DiffSide {
            request_id: baseline.request_id.clone(),
            hits: baseline
                .hits
                .iter()
                .map(|h| DiffHit {
                    doc_id: h.doc_id.clone(),
                    rank: h.rank,
                    score: h.score,
                })
                .collect(),
        },
        candidate: DiffSide {
            request_id: candidate.request_id.clone(),
            hits: candidate
                .hits
                .iter()
                .map(|h| DiffHit {
                    doc_id: h.doc_id.clone(),
                    rank: h.rank,
                    score: h.score,
                })
                .collect(),
        },
        summary: DiffSummary {
            added_doc_ids,
            removed_doc_ids,
            rank_changes,
            score_changes,
            top_k_overlap,
            top_1_changed,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::{NormalizedHit, NormalizedRetrieval};

    fn make_retrieval(
        request_id: &str,
        hits: Vec<(&str, u32, f64)>,
    ) -> NormalizedRetrieval {
        NormalizedRetrieval {
            request_id: request_id.to_string(),
            query_hash: "hash1".to_string(),
            collection: "test_col".to_string(),
            top_k: 3,
            hits: hits
                .into_iter()
                .map(|(doc_id, rank, score)| NormalizedHit {
                    doc_id: doc_id.to_string(),
                    rank,
                    score,
                })
                .collect(),
        }
    }

    #[test]
    fn test_identical_results() {
        let baseline = make_retrieval("b1", vec![("d1", 1, 0.9), ("d2", 2, 0.8), ("d3", 3, 0.7)]);
        let candidate =
            make_retrieval("c1", vec![("d1", 1, 0.9), ("d2", 2, 0.8), ("d3", 3, 0.7)]);
        let diff = compute_diff(&baseline, &candidate);

        assert!(diff.summary.added_doc_ids.is_empty());
        assert!(diff.summary.removed_doc_ids.is_empty());
        assert!(diff.summary.rank_changes.is_empty());
        assert!(diff.summary.score_changes.is_empty());
        assert!((diff.summary.top_k_overlap - 1.0).abs() < f64::EPSILON);
        assert!(!diff.summary.top_1_changed);
    }

    #[test]
    fn test_completely_different_results() {
        let baseline = make_retrieval("b1", vec![("d1", 1, 0.9)]);
        let candidate = make_retrieval("c1", vec![("d2", 1, 0.85)]);
        let diff = compute_diff(&baseline, &candidate);

        assert_eq!(diff.summary.added_doc_ids, vec!["d2"]);
        assert_eq!(diff.summary.removed_doc_ids, vec!["d1"]);
        assert!(diff.summary.rank_changes.is_empty());
        assert!(diff.summary.score_changes.is_empty());
        assert!((diff.summary.top_k_overlap - 0.0).abs() < f64::EPSILON);
        assert!(diff.summary.top_1_changed);
    }

    #[test]
    fn test_rank_and_score_changes() {
        let baseline = make_retrieval("b1", vec![("d1", 1, 0.9), ("d2", 2, 0.8)]);
        let candidate = make_retrieval("c1", vec![("d2", 1, 0.85), ("d1", 2, 0.75)]);
        let diff = compute_diff(&baseline, &candidate);

        assert!(diff.summary.added_doc_ids.is_empty());
        assert!(diff.summary.removed_doc_ids.is_empty());
        assert_eq!(diff.summary.rank_changes.len(), 2);
        assert_eq!(diff.summary.score_changes.len(), 2);
        assert!(diff.summary.top_1_changed);

        // top_k=3, 2 docs in common
        assert!((diff.summary.top_k_overlap - 2.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn test_partial_overlap() {
        let baseline = make_retrieval("b1", vec![("d1", 1, 0.9), ("d2", 2, 0.8), ("d3", 3, 0.7)]);
        let candidate =
            make_retrieval("c1", vec![("d1", 1, 0.9), ("d4", 2, 0.85), ("d5", 3, 0.7)]);
        let diff = compute_diff(&baseline, &candidate);

        assert_eq!(diff.summary.added_doc_ids.len(), 2); // d4, d5
        assert_eq!(diff.summary.removed_doc_ids.len(), 2); // d2, d3
        assert!(!diff.summary.top_1_changed);
        assert!((diff.summary.top_k_overlap - 1.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn test_empty_baseline() {
        let baseline = make_retrieval("b1", vec![]);
        let candidate = make_retrieval("c1", vec![("d1", 1, 0.9)]);
        let diff = compute_diff(&baseline, &candidate);

        assert_eq!(diff.summary.added_doc_ids, vec!["d1"]);
        assert!(diff.summary.removed_doc_ids.is_empty());
        assert!(diff.summary.top_1_changed);
        assert!((diff.summary.top_k_overlap - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_diff_key_fields() {
        let baseline = make_retrieval("b1", vec![("d1", 1, 0.9)]);
        let candidate = make_retrieval("c1", vec![("d1", 1, 0.9)]);
        let diff = compute_diff(&baseline, &candidate);

        assert_eq!(diff.key.query_hash, "hash1");
        assert_eq!(diff.key.collection, "test_col");
        assert_eq!(diff.key.top_k, 3);
        assert_eq!(diff.baseline.request_id, "b1");
        assert_eq!(diff.candidate.request_id, "c1");
    }
}
