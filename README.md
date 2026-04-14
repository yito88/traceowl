# TraceOwl

**Understand how your retrieval changes.**

TraceOwl captures, compares, and explains differences in VectorDB search results so you can quickly understand what changed and where to focus your review.

---

## Example Report

### Summary & Guidance
![summary](./docs/report_summary.png)

### Top Changed Queries
![table](./docs/report_table.png)

### Example Diff
![example](./docs/report_example.png)

---

## What it does

- Capture real retrieval queries via a proxy
- Compare before/after search results
- Show what changed (documents, ranking, scores)
- Highlight queries you should review first

---

## Why

Retrieval systems change frequently; embeddings, ranking logic, filters, and indexing strategies evolve.

But understanding *how* those changes affect results is difficult.

TraceOwl helps you:

- Detect unintended retrieval changes
- Understand ranking differences
- Identify high-impact queries quickly
- Review changes efficiently

---

## How it works

```
Client → Proxy → VectorDB
               ↓
            JSONL logs
               ↓
         traceowl-diff
               ↓
           analyzer
               ↓
           HTML report
```

---

## Components

- **traceowl-proxy**  
  Transparent proxy that captures queries and responses

- **traceowl-diff**  
  Computes differences between retrieval results

- **analyzer (private)**  
  Generates human-readable HTML reports

---

## Quickstart (conceptual)

1. Run `traceowl-proxy`
2. Send queries through the proxy
3. Collect event logs
4. Run `traceowl-diff`
5. Generate HTML report with analyzer

---

## Design Principles

- **Diff first, judgment later**  
  TraceOwl shows what changed - not whether it's good or bad

- **Human-in-the-loop**  
  Focus on helping engineers review changes efficiently

- **Deterministic and explainable**
  No black-box scoring — all differences are traceable

---

## Status

Early-stage project. Core pipeline is working:

- Proxy → Diff → Analyzer → Report

Actively evolving.
