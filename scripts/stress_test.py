#!/usr/bin/env python3
"""
Stress test for traceowl-proxy against a real Qdrant instance.

Phases:
  1. Ramp-up: increasing concurrency levels, measure throughput + latency percentiles
  2. Soak: sustained moderate load, check memory stability + event correctness

Prerequisites:
  - Docker (for Qdrant container)
  - Pre-built proxy binary (cargo build --release)
  - pip install qdrant-client

Usage:
  python scripts/stress_test.py                        # full run
  python scripts/stress_test.py --ramp-only            # ramp-up only
  python scripts/stress_test.py --soak-only --soak-duration 60  # 1-min soak
  python scripts/stress_test.py --skip-docker          # use existing Qdrant
"""

import argparse
import asyncio
import glob
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from qdrant_client import AsyncQdrantClient, models

COLLECTION_NAME = "stress_test"
VECTOR_DIM = 128
NUM_SEED_POINTS = 1000
RAMP_CONCURRENCY_LEVELS = [1, 5, 10, 25, 50, 100]
RAMP_REQUESTS_PER_LEVEL = 500
SOAK_CONCURRENCY = 25
SOAK_WINDOW_SECONDS = 10


@dataclass
class LatencyStats:
    latencies: list = field(default_factory=list)
    errors: int = 0

    def record(self, latency_ms: float):
        self.latencies.append(latency_ms)

    def record_error(self):
        self.errors += 1

    def percentile(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        sorted_lat = sorted(self.latencies)
        idx = int(len(sorted_lat) * p / 100.0)
        idx = min(idx, len(sorted_lat) - 1)
        return sorted_lat[idx]

    @property
    def count(self) -> int:
        return len(self.latencies) + self.errors

    @property
    def success_count(self) -> int:
        return len(self.latencies)


def random_vector() -> list:
    return [random.random() for _ in range(VECTOR_DIM)]


def generate_config_toml(
    qdrant_port: int, proxy_port: int, output_dir: str, queue_capacity: int = 16384
) -> str:
    return f"""\
upstream_base_url = "http://localhost:{qdrant_port}"
listen_addr = "127.0.0.1:{proxy_port}"
sampling_rate = 1.0
queue_capacity = {queue_capacity}
output_dir = "{output_dir}"
rotation_max_bytes = 10485760
flush_interval_ms = 500
flush_max_events = 1000
upstream_request_timeout_ms = 30000
"""


# ---------------------------------------------------------------------------
# Lifecycle helpers
# ---------------------------------------------------------------------------

def start_qdrant_container(port: int) -> str:
    container_name = "traceowl-stress-qdrant"
    # Remove stale container if any
    subprocess.run(
        ["docker", "rm", "-f", container_name],
        capture_output=True,
    )
    result = subprocess.run(
        [
            "docker", "run", "-d",
            "--name", container_name,
            "-p", f"{port}:6333",
            "qdrant/qdrant:latest",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Failed to start Qdrant container: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    print(f"Started Qdrant container on port {port}")
    return container_name


def stop_qdrant_container(container_name: str):
    subprocess.run(["docker", "rm", "-f", container_name], capture_output=True)
    print(f"Stopped Qdrant container: {container_name}")


async def wait_for_qdrant(url: str, timeout: float = 30.0):
    """Poll Qdrant readiness endpoint."""
    import httpx

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{url}/readyz", timeout=2.0)
                if resp.status_code == 200:
                    print("Qdrant is ready")
                    return
        except Exception:
            pass
        await asyncio.sleep(0.5)
    print("Qdrant did not become ready in time", file=sys.stderr)
    sys.exit(1)


def start_proxy(proxy_bin: str, config_path: str) -> subprocess.Popen:
    proc = subprocess.Popen(
        [proxy_bin, config_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Give the proxy a moment to bind
    time.sleep(0.5)
    if proc.poll() is not None:
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        print(f"Proxy failed to start: {stderr}", file=sys.stderr)
        sys.exit(1)
    print(f"Started proxy (pid={proc.pid})")
    return proc


def stop_proxy(proc: subprocess.Popen):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    print("Proxy stopped")


async def setup_qdrant(qdrant_url: str):
    """Create collection and upsert seed points directly to Qdrant."""
    client = AsyncQdrantClient(url=qdrant_url, prefer_grpc=False)
    try:
        if await client.collection_exists(COLLECTION_NAME):
            await client.delete_collection(COLLECTION_NAME)
        await client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=models.VectorParams(
                size=VECTOR_DIM,
                distance=models.Distance.COSINE,
            ),
        )
        print(f"Created collection '{COLLECTION_NAME}' (dim={VECTOR_DIM})")

        # Upsert in batches of 100
        batch_size = 100
        for start in range(0, NUM_SEED_POINTS, batch_size):
            end = min(start + batch_size, NUM_SEED_POINTS)
            points = [
                models.PointStruct(
                    id=i,
                    vector=random_vector(),
                    payload={"idx": i},
                )
                for i in range(start, end)
            ]
            await client.upsert(collection_name=COLLECTION_NAME, points=points, wait=True)
        print(f"Upserted {NUM_SEED_POINTS} points")
    finally:
        await client.close()


async def teardown_qdrant(qdrant_url: str):
    client = AsyncQdrantClient(url=qdrant_url, prefer_grpc=False)
    try:
        await client.delete_collection(collection_name=COLLECTION_NAME)
    except Exception:
        pass
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Phase 1: Ramp-up
# ---------------------------------------------------------------------------

async def run_ramp(proxy_url: str):
    print("\n" + "=" * 60)
    print("PHASE 1: Throughput / Latency Ramp-Up")
    print("=" * 60)

    for concurrency in RAMP_CONCURRENCY_LEVELS:
        stats = LatencyStats()
        semaphore = asyncio.Semaphore(concurrency)
        client = AsyncQdrantClient(url=proxy_url, prefer_grpc=False, timeout=30)

        first_error_logged = False

        async def do_query():
            nonlocal first_error_logged
            async with semaphore:
                t0 = time.monotonic()
                try:
                    await client.query_points(
                        collection_name=COLLECTION_NAME,
                        query=random_vector(),
                        limit=10,
                    )
                    latency_ms = (time.monotonic() - t0) * 1000
                    stats.record(latency_ms)
                except Exception as e:
                    stats.record_error()
                    if not first_error_logged:
                        first_error_logged = True
                        print(f"  First error: {type(e).__name__}: {e}")

        wall_start = time.monotonic()
        tasks = [asyncio.create_task(do_query()) for _ in range(RAMP_REQUESTS_PER_LEVEL)]
        await asyncio.gather(*tasks)
        wall_duration = time.monotonic() - wall_start

        await client.close()

        throughput = stats.success_count / wall_duration if wall_duration > 0 else 0
        error_pct = (stats.errors / stats.count * 100) if stats.count > 0 else 0

        print(f"\nConcurrency: {concurrency:>3} | "
              f"Requests: {stats.count} | "
              f"Duration: {wall_duration:.1f}s")
        print(f"  Throughput: {throughput:.1f} req/s")
        print(f"  Latency  p50: {stats.percentile(50):.1f}ms | "
              f"p95: {stats.percentile(95):.1f}ms | "
              f"p99: {stats.percentile(99):.1f}ms")
        print(f"  Errors: {stats.errors} ({error_pct:.1f}%)")


# ---------------------------------------------------------------------------
# Phase 2: Soak test
# ---------------------------------------------------------------------------

def get_rss_kb(pid: int) -> int | None:
    """Read VmRSS from /proc/{pid}/status (Linux only)."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (FileNotFoundError, PermissionError, ValueError):
        return None
    return None


async def run_soak(proxy_url: str, duration: int, proxy_pid: int):
    print("\n" + "=" * 60)
    print(f"PHASE 2: Soak Test ({duration}s, concurrency={SOAK_CONCURRENCY})")
    print("=" * 60)

    rss_start = get_rss_kb(proxy_pid)
    if rss_start is not None:
        print(f"  Proxy RSS at start: {rss_start} KB")

    semaphore = asyncio.Semaphore(SOAK_CONCURRENCY)
    client = AsyncQdrantClient(url=proxy_url, prefer_grpc=False, timeout=30)
    running = True

    window_stats = LatencyStats()
    overall_stats = LatencyStats()
    window_start = time.monotonic()
    soak_start = time.monotonic()

    async def worker():
        nonlocal window_stats, window_start
        while running:
            async with semaphore:
                if not running:
                    break
                t0 = time.monotonic()
                try:
                    await client.query_points(
                        collection_name=COLLECTION_NAME,
                        query=random_vector(),
                        limit=10,
                    )
                    latency_ms = (time.monotonic() - t0) * 1000
                    window_stats.record(latency_ms)
                    overall_stats.record(latency_ms)
                except Exception:
                    window_stats.record_error()
                    overall_stats.record_error()

    async def reporter():
        nonlocal window_stats, window_start
        while running:
            await asyncio.sleep(SOAK_WINDOW_SECONDS)
            if not running:
                break
            elapsed = time.monotonic() - window_start
            ws = window_stats
            window_stats = LatencyStats()
            window_start = time.monotonic()

            if ws.count == 0:
                continue
            tp = ws.success_count / elapsed if elapsed > 0 else 0
            ep = (ws.errors / ws.count * 100) if ws.count > 0 else 0
            total_elapsed = time.monotonic() - soak_start
            print(f"  [{total_elapsed:6.0f}s] "
                  f"req={ws.count:>5}  "
                  f"tp={tp:>7.1f} req/s  "
                  f"p50={ws.percentile(50):>6.1f}ms  "
                  f"p95={ws.percentile(95):>6.1f}ms  "
                  f"err={ws.errors}({ep:.1f}%)")

    # Launch workers and reporter
    workers = [asyncio.create_task(worker()) for _ in range(SOAK_CONCURRENCY)]
    reporter_task = asyncio.create_task(reporter())

    await asyncio.sleep(duration)
    running = False

    for w in workers:
        w.cancel()
    reporter_task.cancel()
    await asyncio.gather(*workers, reporter_task, return_exceptions=True)
    await client.close()

    # Summary
    total_elapsed = time.monotonic() - soak_start
    tp = overall_stats.success_count / total_elapsed if total_elapsed > 0 else 0
    ep = (overall_stats.errors / overall_stats.count * 100) if overall_stats.count > 0 else 0
    print(f"\n  Soak summary:")
    print(f"    Total requests : {overall_stats.count}")
    print(f"    Throughput     : {tp:.1f} req/s")
    print(f"    Latency p50    : {overall_stats.percentile(50):.1f}ms")
    print(f"    Latency p95    : {overall_stats.percentile(95):.1f}ms")
    print(f"    Latency p99    : {overall_stats.percentile(99):.1f}ms")
    print(f"    Errors         : {overall_stats.errors} ({ep:.1f}%)")

    rss_end = get_rss_kb(proxy_pid)
    if rss_start is not None and rss_end is not None:
        delta = rss_end - rss_start
        print(f"    RSS start      : {rss_start} KB")
        print(f"    RSS end        : {rss_end} KB")
        print(f"    RSS delta      : {delta:+d} KB")


# ---------------------------------------------------------------------------
# JSONL validation
# ---------------------------------------------------------------------------

def validate_events(output_dir: str, sample_size: int = 100):
    print("\n" + "-" * 40)
    print("JSONL Event Validation")
    print("-" * 40)

    files = sorted(glob.glob(os.path.join(output_dir, "events-*.jsonl")))
    if not files:
        print("  No JSONL files found!")
        return

    total_size = sum(os.path.getsize(f) for f in files)
    print(f"  Files: {len(files)}, Total size: {total_size / 1024:.1f} KB")

    # Collect all lines
    all_lines = []
    for fp in files:
        with open(fp) as f:
            for line in f:
                line = line.strip()
                if line:
                    all_lines.append(line)

    print(f"  Total events: {len(all_lines)}")

    if not all_lines:
        print("  No events to validate!")
        return

    # Sample
    sample = random.sample(all_lines, min(sample_size, len(all_lines)))
    passed = 0
    failed = 0

    for line in sample:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            failed += 1
            continue

        ok = True
        # Common fields
        for key in ("schema_version", "event_type", "request_id", "timestamp_ms"):
            if key not in event:
                ok = False
                break

        if ok and event.get("event_type") == "response":
            if "status" not in event or "http_status" not in event.get("status", {}):
                ok = False
            if "timing" not in event or "latency_ms" not in event.get("timing", {}):
                ok = False

        if ok and event.get("event_type") == "request":
            if "db" not in event or "kind" not in event.get("db", {}):
                ok = False
            if "query" not in event:
                ok = False

        if ok:
            passed += 1
        else:
            failed += 1

    print(f"  Validated sample: {len(sample)} events")
    print(f"    Passed: {passed}")
    print(f"    Failed: {failed}")

    # Count request/response balance
    req_count = sum(1 for l in all_lines if '"event_type":"request"' in l or '"event_type": "request"' in l)
    resp_count = sum(1 for l in all_lines if '"event_type":"response"' in l or '"event_type": "response"' in l)
    print(f"  Request events : {req_count}")
    print(f"  Response events: {resp_count}")
    if req_count != resp_count:
        print(f"  WARNING: request/response count mismatch (delta={abs(req_count - resp_count)})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description="traceowl-proxy stress test")
    parser.add_argument("--qdrant-port", type=int, default=6333)
    parser.add_argument("--proxy-port", type=int, default=9090)
    parser.add_argument("--proxy-bin", default="target/release/traceowl-proxy")
    parser.add_argument("--skip-docker", action="store_true",
                        help="Assume Qdrant is already running")
    parser.add_argument("--ramp-only", action="store_true")
    parser.add_argument("--soak-only", action="store_true")
    parser.add_argument("--soak-duration", type=int, default=300,
                        help="Soak test duration in seconds (default: 300)")
    parser.add_argument("--output-dir", default="/tmp/traceowl-stress")
    parser.add_argument("--queue-capacity", type=int, default=16384,
                        help="Event queue capacity (default: 16384)")
    args = parser.parse_args()

    qdrant_url = f"http://localhost:{args.qdrant_port}"
    proxy_url = f"http://localhost:{args.proxy_port}"
    output_dir = args.output_dir
    container_name = None
    proxy_proc = None

    # Ensure clean output dir
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    try:
        # 1. Start Qdrant
        if not args.skip_docker:
            container_name = start_qdrant_container(args.qdrant_port)

        await wait_for_qdrant(qdrant_url)

        # 2. Generate config and start proxy
        config_path = os.path.join(output_dir, "config.toml")
        with open(config_path, "w") as f:
            f.write(generate_config_toml(args.qdrant_port, args.proxy_port, output_dir, args.queue_capacity))

        proxy_proc = start_proxy(args.proxy_bin, config_path)

        # 3. Setup Qdrant data
        await setup_qdrant(qdrant_url)

        # Give proxy a moment to be fully ready
        await asyncio.sleep(0.5)

        # 4. Run phases
        run_ramp_phase = not args.soak_only
        run_soak_phase = not args.ramp_only

        if run_ramp_phase:
            await run_ramp(proxy_url)

        if run_soak_phase:
            await run_soak(proxy_url, args.soak_duration, proxy_proc.pid)

        # 5. Validate events
        # Wait for flush
        await asyncio.sleep(2)
        validate_events(output_dir)

        # Teardown collection
        await teardown_qdrant(qdrant_url)

        print("\nStress test complete.")

    finally:
        if proxy_proc is not None:
            stop_proxy(proxy_proc)
            # Print proxy stderr for diagnostics
            if proxy_proc.stderr:
                stderr = proxy_proc.stderr.read().decode()
                if stderr:
                    dropped_lines = [l for l in stderr.splitlines() if "drop" in l.lower()]
                    if dropped_lines:
                        print(f"\n  Queue drop messages from proxy:")
                        for line in dropped_lines[:10]:
                            print(f"    {line}")

        if container_name is not None:
            stop_qdrant_container(container_name)


if __name__ == "__main__":
    asyncio.run(main())
