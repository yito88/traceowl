# Operation

## TraceOwl Proxy

**1. Create a config file**

Copy the example and edit to match your setup:

```bash
cp config.example.toml config.toml
```

Key fields:

```toml
backend = "qdrant"                        # "qdrant" or "pinecone"
listen_addr = "0.0.0.0:6333"             # port your clients connect to
upstream_base_url = "http://localhost:6334"  # your real VectorDB
sampling_rate = 0.1                       # fraction of requests to record

[sink]
mode = "local_only"                       # "local_only" or "local_plus_s3"
local_output_root = "./data"              # where JSONL files are written
```

**2a. Run the binary**

```bash
./target/release/traceowl-proxy config.toml
```

**2b. Run with Docker**

```bash
docker run -d \
  -p 6333:6333 \
  -v $(pwd)/config.toml:/config.toml \
  -v $(pwd)/data:/data \
  traceowl-proxy:latest /config.toml
```

**3. Start a tracing session**

Tracing is off by default. Start it when you want to capture events:

```bash
curl -s -X POST http://localhost:6333/control/tracing/start \
  -H 'Content-Type: application/json' \
  -d '{"sampling_rate": 0.1}' | jq .
```

Point your application at the proxy instead of the VectorDB directly and run
your queries as normal. The proxy forwards all traffic transparently.

**4. Stop tracing and flush events**

```bash
curl -s -X POST http://localhost:6333/control/tracing/stop | jq .
```

This flushes any buffered events to disk before returning. Event files appear
under `local_output_root` as `events/<session-id>/<seq>.jsonl`.

**Check status at any time**

```bash
curl -s http://localhost:6333/control/status | jq .
```

---

## TraceOwl Diff

Compare two sets of event files (e.g. before and after an embedding change):

```bash
traceowl-diff \
  --baseline events/baseline/events-*.jsonl \
  --candidate events/candidate/events-*.jsonl \
  --output diff.jsonl
```

The output `diff.jsonl` contains one record per matched query hash showing what
changed between the two retrieval runs. Feed it into traceowl-analyzer to
generate an HTML report.

---

## TraceOwl Analyzer (Commercial)

TraceOwl Analyzer can work as a CLI tool(Analyze mode) or as a web server(Server mode).

### Configuration

If you use the analyzer with local JSONL files, no config is needed. Just point it at the files.

For S3 inputs, create a config file:

```toml
[server]
port = 8080
runs_dir = "/runs"               # where to analysis runs

[s3]
endpoint = "http://minio:9000"   # omit for AWS S3
region = "us-east-1"
access_key = "..."
secret_key = "..."
force_path_style = true          # required for MinIO / Ceph
bucket = "traceowl-events"
```

The `[s3]` section is only required when using S3 prefix inputs.

### Analyze mode

#### Local directories

```sh
docker run --rm \
  -v $(pwd)/data:/data \
  traceowl-analyzer \  # no config needed for local files
  analyze \
  --baseline-dir /data/inputs/baseline_events \
  --candidate-dir /data/inputs/candidate_events \
  --output-html /data/report.html \
  --summary-json /data/summary.json
```

#### S3 prefix inputs

```sh
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/config.toml:/config.toml \
  traceowl-analyzer \
  analyze \
  --config config.toml \
  --baseline-s3-prefix events/session-baseline/ \
  --candidate-s3-prefix events/session-candidate/ \
  --output-html /data/report.html \
  --summary-json /data/summary.json
```

#### Mixed (one local, one S3)

```sh
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/config.toml:/config.toml \
  traceowl-analyzer \
  analyze \
  --config config.toml \
  --baseline-dir /data/inputs/baseline_events \
  --candidate-s3-prefix events/session-candidate/ \
  --output-html /data/report.html \
  --summary-json /data/summary.json
```

### Server mode

```sh
docker run --rm \
  -v $(pwd)/runs:/runs \
  -v $(pwd)/config.toml:/config.toml \
  -p 8080:8080 \
  traceowl-analyzer \
  server --config /config.toml
```

The server UI supports three input modes per side: **Upload**, **Local Dir**, and **S3 Prefix**.
