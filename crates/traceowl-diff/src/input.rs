use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};

use crate::events::Event;

// NOTE: all events from all files are accumulated in memory before processing.
// For very large inputs this could OOM. A streaming pipeline would be needed
// to fix this properly, which is out of scope for v1.
pub fn read_events_from_files(paths: &[std::path::PathBuf]) -> Result<Vec<Event>> {
    let mut all_events = Vec::new();
    for path in paths {
        let events = read_events(path)?;
        all_events.extend(events);
    }
    Ok(all_events)
}

pub fn read_events(path: &Path) -> Result<Vec<Event>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    let mut line_num = 0u64;
    let mut skipped = 0u64;

    for line in reader.lines() {
        line_num += 1;
        let line = line.with_context(|| format!("failed to read line {line_num}"))?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<Event>(line) {
            Ok(event) => events.push(event),
            Err(e) => {
                tracing::warn!(line = line_num, error = %e, "skipping malformed event line");
                skipped += 1;
            }
        }
    }

    tracing::info!(
        path = %path.display(),
        total_lines = line_num,
        parsed = events.len(),
        skipped = skipped,
        "read events"
    );
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn request_line(request_id: &str) -> String {
        format!(
            r#"{{"event_type":"request","schema_version":1,"request_id":"{request_id}","timestamp_ms":100,"sampled":true,"unsupported_shape":false,"db":{{"kind":"qdrant","collection":"test"}},"query":{{"hash":"abc123","top_k":10}}}}"#
        )
    }

    fn response_line(request_id: &str) -> String {
        format!(
            r#"{{"event_type":"response","schema_version":1,"request_id":"{request_id}","timestamp_ms":200,"status":{{"ok":true,"http_status":200,"error_kind":null}},"timing":{{"latency_ms":5}},"result":{{"hits":[{{"doc_id":"1","rank":1,"score":0.9}}]}}}}"#
        )
    }

    #[test]
    fn test_read_valid_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let mut f = File::create(&path).unwrap();
        writeln!(f, "{}", request_line("req1")).unwrap();
        writeln!(f, "{}", response_line("req1")).unwrap();

        let events = read_events(&path).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_skip_malformed_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let mut f = File::create(&path).unwrap();
        writeln!(f, "{}", request_line("req1")).unwrap();
        writeln!(f, "not valid json").unwrap();
        writeln!(f, "{}", response_line("req1")).unwrap();

        let events = read_events(&path).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_read_events_from_multiple_files() {
        let dir = tempfile::tempdir().unwrap();
        let path1 = dir.path().join("events1.jsonl");
        let path2 = dir.path().join("events2.jsonl");

        let mut f1 = File::create(&path1).unwrap();
        writeln!(f1, "{}", request_line("req1")).unwrap();
        writeln!(f1, "{}", response_line("req1")).unwrap();

        let mut f2 = File::create(&path2).unwrap();
        writeln!(f2, "{}", request_line("req2")).unwrap();
        writeln!(f2, "{}", response_line("req2")).unwrap();

        let events = read_events_from_files(&[path1, path2]).unwrap();
        assert_eq!(events.len(), 4);
    }

    #[test]
    fn test_skip_empty_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let mut f = File::create(&path).unwrap();
        writeln!(f, "{}", request_line("req1")).unwrap();
        writeln!(f).unwrap();
        writeln!(f, "{}", response_line("req1")).unwrap();

        let events = read_events(&path).unwrap();
        assert_eq!(events.len(), 2);
    }
}
