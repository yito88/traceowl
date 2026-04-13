use std::path::PathBuf;

use traceowl_diff::basic_diff;
use traceowl_diff::input;
use traceowl_diff::join;
use traceowl_diff::normalize;
use traceowl_diff::output;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

#[test]
fn test_end_to_end_diff() {
    let baseline_events = input::read_events(&fixture_path("baseline.jsonl")).unwrap();
    let candidate_events = input::read_events(&fixture_path("candidate.jsonl")).unwrap();

    // baseline: 7 lines (4 req + 3 resp), but one resp is error, one req is orphan
    assert_eq!(baseline_events.len(), 7);
    // candidate: 6 lines (3 req + 3 resp)
    assert_eq!(candidate_events.len(), 6);

    let baseline_joined = join::join_events(baseline_events);
    let candidate_joined = join::join_events(candidate_events);

    // baseline: req-b1 + req-b2 joined (req-b3 has response but non-ok, req-b4 orphan)
    assert_eq!(baseline_joined.len(), 3); // b1, b2, b3 joined
    assert_eq!(candidate_joined.len(), 3); // c1, c2, c3 joined

    let baseline_retrievals = normalize::build_retrievals(baseline_joined);
    let candidate_retrievals = normalize::build_retrievals(candidate_joined);

    // baseline: b3 filtered (non-ok), so 2 remain
    assert_eq!(baseline_retrievals.len(), 2);
    assert_eq!(candidate_retrievals.len(), 3);

    // Group by hash
    let mut baseline_map = std::collections::HashMap::new();
    for r in &baseline_retrievals {
        baseline_map.entry(r.query_hash.clone()).or_insert(r);
    }
    let mut candidate_map = std::collections::HashMap::new();
    for r in &candidate_retrievals {
        candidate_map.entry(r.query_hash.clone()).or_insert(r);
    }

    // hash_aaa and hash_bbb should match
    assert!(baseline_map.contains_key("hash_aaa"));
    assert!(baseline_map.contains_key("hash_bbb"));
    assert!(candidate_map.contains_key("hash_aaa"));
    assert!(candidate_map.contains_key("hash_bbb"));
    assert!(candidate_map.contains_key("hash_eee"));

    // Diff hash_aaa: top-1 changed (914 -> 822), 100 and 200 shared
    let diff_aaa = basic_diff::compute_diff(baseline_map["hash_aaa"], candidate_map["hash_aaa"]);
    assert_eq!(diff_aaa.key.query_hash, "hash_aaa");
    assert_eq!(diff_aaa.key.collection, "stress_test");
    assert_eq!(diff_aaa.key.top_k, 3);
    assert!(diff_aaa.summary.top_1_changed);
    assert_eq!(diff_aaa.summary.added_doc_ids, vec!["822"]);
    assert_eq!(diff_aaa.summary.removed_doc_ids, vec!["914"]);
    // 2 docs in common (100, 200) out of top_k=3
    assert!((diff_aaa.summary.top_k_overlap - 2.0 / 3.0).abs() < 0.001);

    // Diff hash_bbb: same docs, only score changes
    let diff_bbb = basic_diff::compute_diff(baseline_map["hash_bbb"], candidate_map["hash_bbb"]);
    assert!(!diff_bbb.summary.top_1_changed);
    assert!(diff_bbb.summary.added_doc_ids.is_empty());
    assert!(diff_bbb.summary.removed_doc_ids.is_empty());
    assert!(diff_bbb.summary.rank_changes.is_empty());
    assert_eq!(diff_bbb.summary.score_changes.len(), 2);
    assert!((diff_bbb.summary.top_k_overlap - 1.0).abs() < f64::EPSILON);

    // Write output and verify it's valid JSONL
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("diff.jsonl");

    let diffs = vec![diff_aaa, diff_bbb];
    output::write_diffs(&output_path, &diffs).unwrap();

    // Read back and verify valid JSON
    let content = std::fs::read_to_string(&output_path).unwrap();
    let lines: Vec<&str> = content.trim().split('\n').collect();
    assert_eq!(lines.len(), 2);

    for line in &lines {
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(parsed["key"]["query_hash"].is_string());
        assert!(parsed["summary"]["top_k_overlap"].is_f64());
        assert!(parsed["summary"]["top_1_changed"].is_boolean());
    }

    // Verify first record is hash_aaa
    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["key"]["query_hash"], "hash_aaa");
    assert_eq!(first["summary"]["top_1_changed"], true);
    assert_eq!(first["summary"]["added_doc_ids"][0], "822");
}
