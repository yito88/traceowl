use std::collections::HashMap;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use traceowl_diff::basic_diff;
use traceowl_diff::cli::Args;
use traceowl_diff::input;
use traceowl_diff::join;
use traceowl_diff::normalize::{self, NormalizedRetrieval};
use traceowl_diff::output;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    // Read events from both files
    let baseline_events = input::read_events(&args.baseline)?;
    let candidate_events = input::read_events(&args.candidate)?;

    // Join request+response pairs
    let baseline_joined = join::join_events(baseline_events);
    let candidate_joined = join::join_events(candidate_events);

    // Build normalized retrievals (ok-only)
    let baseline_retrievals = normalize::build_retrievals(baseline_joined);
    let candidate_retrievals = normalize::build_retrievals(candidate_joined);

    // Group by query_hash
    let baseline_map = group_by_hash(baseline_retrievals);
    let candidate_map = group_by_hash(candidate_retrievals);

    // Compare matched hashes
    let mut diffs = Vec::new();
    let mut matched = 0u64;
    let mut baseline_only = 0u64;

    // Use sorted keys for deterministic output
    let mut keys: Vec<&String> = baseline_map.keys().collect();
    keys.sort();

    for hash in keys {
        let baseline = &baseline_map[hash];
        if let Some(candidate) = candidate_map.get(hash) {
            let diff = basic_diff::compute_diff(baseline, candidate);
            diffs.push(diff);
            matched += 1;
        } else {
            baseline_only += 1;
        }
    }

    let candidate_only = candidate_map
        .keys()
        .filter(|k| !baseline_map.contains_key(*k))
        .count() as u64;

    tracing::info!(
        matched,
        baseline_only,
        candidate_only,
        diffs = diffs.len(),
        "diff complete"
    );

    // Write output
    output::write_diffs(&args.output, &diffs)?;

    Ok(())
}

fn group_by_hash(retrievals: Vec<NormalizedRetrieval>) -> HashMap<String, NormalizedRetrieval> {
    let mut map = HashMap::new();
    for r in retrievals {
        let hash = r.query_hash.clone();
        if map.contains_key(&hash) {
            tracing::warn!(
                query_hash = %hash,
                "duplicate query hash encountered, keeping first"
            );
        } else {
            map.entry(hash).or_insert(r);
        }
    }
    map
}
