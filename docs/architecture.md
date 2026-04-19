# Architecture

## How it works

```
Client → TraceOwl Proxy → VectorDB
                 ↓
            JSONL logs (local or S3-compatible)
                 ↓
           TraceOwl Analyzer
                 ↓
             HTML report
```

## Components

- **traceowl-proxy**  
  Transparent proxy that captures queries and responses

- **traceowl-diff**  
  Computes differences between retrieval results

- **traceowl-analyzer (commercial)**  
  Advanced analysis and generates human-readable HTML reports
