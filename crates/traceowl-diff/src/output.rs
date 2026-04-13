use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct DiffRecord {
    pub key: DiffKey,
    pub baseline: DiffSide,
    pub candidate: DiffSide,
    pub summary: DiffSummary,
}

#[derive(Debug, Serialize)]
pub struct DiffKey {
    pub query_hash: String,
    pub collection: String,
    pub top_k: u64,
}

#[derive(Debug, Serialize)]
pub struct DiffSide {
    pub request_id: String,
    pub hits: Vec<DiffHit>,
}

#[derive(Debug, Serialize)]
pub struct DiffHit {
    pub doc_id: String,
    pub rank: u32,
    pub score: f64,
}

#[derive(Debug, Serialize)]
pub struct DiffSummary {
    pub added_doc_ids: Vec<String>,
    pub removed_doc_ids: Vec<String>,
    pub rank_changes: Vec<RankChange>,
    pub score_changes: Vec<ScoreChange>,
    pub top_k_overlap: f64,
    pub top_1_changed: bool,
}

#[derive(Debug, Serialize)]
pub struct RankChange {
    pub doc_id: String,
    pub before_rank: u32,
    pub after_rank: u32,
}

#[derive(Debug, Serialize)]
pub struct ScoreChange {
    pub doc_id: String,
    pub before_score: f64,
    pub after_score: f64,
}

pub fn write_diffs(path: &Path, diffs: &[DiffRecord]) -> Result<()> {
    let file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);

    for diff in diffs {
        let line = serde_json::to_string(diff)?;
        writeln!(writer, "{line}")?;
    }

    writer.flush()?;

    tracing::info!(
        path = %path.display(),
        records = diffs.len(),
        "wrote diff output"
    );
    Ok(())
}
