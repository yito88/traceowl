#!/usr/bin/env python3
"""
TraceOwl end-to-end demo.

Runs the full pipeline:
  1. Start Qdrant (Docker)
  2. Build baseline collection (baseline embedding profile)
  3. Run queries through proxy -> baseline events
  4. Rebuild the same collection with candidate embedding profile
  5. Run the same queries through proxy -> candidate events
  6. Run traceowl-diff on both event sets -> diff.jsonl

Both query runs use the SAME query vectors (baseline profile) so that the
proxy produces matching query hashes, allowing traceowl-diff to compare them.

Usage:
  python scripts/demo.py [options]

Prerequisites:
  pip install qdrant-client numpy
  cargo build --release  (from the traceowl workspace root)
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Dict, List

# ---------------------------------------------------------------------------
# Import shared embedding logic from the same directory
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.parent  # scripts/demo/ -> scripts/ -> repo root

sys.path.insert(0, str(SCRIPT_DIR))
from build_demo_collections import (  # noqa: E402
    embed_text,
    recreate_collection,
    upsert_docs,
)

from qdrant_client import QdrantClient  # noqa: E402

# ---------------------------------------------------------------------------
# Synthetic corpus and queries
# ---------------------------------------------------------------------------

DOCS: List[Dict] = [
    # auth
    {"doc_id": "auth-001", "category": "auth", "title": "How to reset your password",
     "content": "If you forgot your password click the login page and select account recovery. Enter your email to receive a reset link."},
    {"doc_id": "auth-002", "category": "auth", "title": "Account locked after failed login attempts",
     "content": "Your account is locked after five failed login attempts. Contact support or wait 30 minutes before retrying your password."},
    {"doc_id": "auth-003", "category": "auth", "title": "Enable two-factor authentication",
     "content": "Secure your account by enabling 2fa. Go to account settings and link your email or authenticator app."},
    {"doc_id": "auth-004", "category": "auth", "title": "Change your account email address",
     "content": "To change your login email go to account profile settings. A confirmation link will be sent to both your old and new email."},
    {"doc_id": "auth-005", "category": "auth", "title": "Single sign-on configuration",
     "content": "Admins can configure SSO so users log in with their company email. Contact your IT team to set up the account integration."},
    # billing
    {"doc_id": "bill-001", "category": "billing", "title": "How to request a refund",
     "content": "To request a refund for a recent payment submit a ticket with your invoice number. Refunds are processed within 5 business days."},
    {"doc_id": "bill-002", "category": "billing", "title": "Understanding your invoice",
     "content": "Your monthly invoice lists all subscription charges, usage overages, and applicable taxes. Download PDF invoices from the billing portal."},
    {"doc_id": "bill-003", "category": "billing", "title": "Cancel or downgrade your subscription",
     "content": "You can cancel your subscription at any time from the billing page. Downgrading takes effect at the end of the current billing cycle."},
    {"doc_id": "bill-004", "category": "billing", "title": "Update payment method",
     "content": "Replace your credit card or bank details from the billing section. All payment information is encrypted and stored securely."},
    {"doc_id": "bill-005", "category": "billing", "title": "Pricing plans overview",
     "content": "Compare pricing tiers and features on the pricing page. Annual subscription plans offer a 20 percent discount over monthly billing."},
    # usage
    {"doc_id": "use-001", "category": "usage", "title": "Create a new project",
     "content": "Click the New Project button on the dashboard to create a project. Add team members and set permissions before you upload files."},
    {"doc_id": "use-002", "category": "usage", "title": "Upload files to your project",
     "content": "Drag and drop files into the project workspace or use the upload button. Large files are chunked automatically during upload."},
    {"doc_id": "use-003", "category": "usage", "title": "Share a project with collaborators",
     "content": "Use the Share button to invite collaborators by email. You can set view-only or edit permissions per user on the project."},
    {"doc_id": "use-004", "category": "usage", "title": "Export your data",
     "content": "Export project data as CSV or JSON from the project settings. Scheduled exports can be created to run automatically every day."},
    {"doc_id": "use-005", "category": "usage", "title": "Delete a project",
     "content": "Deleting a project is permanent and cannot be undone. Type the project name to confirm deletion from the settings page."},
    # errors
    {"doc_id": "err-001", "category": "errors", "title": "API connection refused error",
     "content": "A connection refused error means the API server is not reachable. Check your network, firewall rules, and that the API endpoint is correct."},
    {"doc_id": "err-002", "category": "errors", "title": "Request timeout troubleshooting",
     "content": "Timeout errors occur when the API does not respond within the allowed time. Retry with exponential backoff and check for service status."},
    {"doc_id": "err-003", "category": "errors", "title": "Authentication failed API error",
     "content": "A 401 error from the API means your token is missing or expired. Refresh your API token and ensure it is included in the request header."},
    {"doc_id": "err-004", "category": "errors", "title": "Rate limit exceeded",
     "content": "HTTP 429 means you have exceeded the API rate limit. Slow down your request rate and implement retry logic with backoff in your client."},
    {"doc_id": "err-005", "category": "errors", "title": "Internal server error 500",
     "content": "A 500 response indicates a server-side error. Check the status page for incidents and contact support if the error persists after retrying."},
    # misc
    {"doc_id": "misc-001", "category": "misc", "title": "About our company",
     "content": "We are a cloud software company focused on data privacy and reliability. Read about our mission and team on the company about page."},
    {"doc_id": "misc-002", "category": "misc", "title": "Privacy policy summary",
     "content": "Our privacy policy explains how we collect store and use your data. We do not sell personal data to third parties. Review the full terms online."},
    {"doc_id": "misc-003", "category": "misc", "title": "Service status page",
     "content": "Check real-time status of all services including API uptime at our status page. Subscribe to incident notifications via email or webhook."},
    {"doc_id": "misc-004", "category": "misc", "title": "Contact support",
     "content": "Reach our support team by submitting a ticket or starting a live chat. Priority support is available for enterprise subscription customers."},
    {"doc_id": "misc-005", "category": "misc", "title": "Terms of service",
     "content": "By using our platform you agree to the terms of service. Review acceptable use policies and data retention terms before signing up."},
]

QUERIES: List[Dict] = [
    {"query_id": "q01", "query": "forgot password login reset"},
    {"query_id": "q02", "query": "account locked failed login attempts"},
    {"query_id": "q03", "query": "two factor authentication email setup"},
    {"query_id": "q04", "query": "refund payment request invoice"},
    {"query_id": "q05", "query": "subscription billing cancel downgrade"},
    {"query_id": "q06", "query": "invoice pricing plan upgrade"},
    {"query_id": "q07", "query": "upload files to project create"},
    {"query_id": "q08", "query": "share project collaborators export"},
    {"query_id": "q09", "query": "delete project data"},
    {"query_id": "q10", "query": "api connection error refused"},
    {"query_id": "q11", "query": "timeout error api request failed"},
    {"query_id": "q12", "query": "rate limit exceeded api retry"},
    {"query_id": "q13", "query": "company privacy policy terms"},
    {"query_id": "q14", "query": "status page contact support"},
    {"query_id": "q15", "query": "internal server error 500"},
]

COLLECTION_NAME = "demo_collection"


# ---------------------------------------------------------------------------
# Infrastructure helpers
# ---------------------------------------------------------------------------

def wait_for_qdrant(url: str, timeout: int = 30) -> None:
    print(f"  Waiting for Qdrant at {url} ...")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f"{url}/healthz", timeout=2)
            print("  Qdrant ready.")
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"Qdrant not ready at {url} after {timeout}s")


def wait_for_port(host: str, port: int, timeout: int = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"Port {host}:{port} not available after {timeout}s")


def write_proxy_config(path: Path, listen_addr: str, upstream_url: str, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    config = f"""\
listen_addr = "{listen_addr}"
upstream_base_url = "{upstream_url}"
sampling_rate = 1.0
queue_capacity = 8192
output_dir = "{output_dir}"
flush_interval_ms = 500
flush_max_events = 500
upstream_request_timeout_ms = 10000
include_query_representation = false
"""
    path.write_text(config, encoding="utf-8")


def start_proxy(proxy_bin: Path, config_path: Path) -> subprocess.Popen:
    return subprocess.Popen(
        [str(proxy_bin), "--config", str(config_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def stop_proxy(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def send_queries(proxy_url: str, queries: List[Dict], profile: Dict,
                 vector_size: int, top_k: int) -> None:
    client = QdrantClient(url=proxy_url)
    for q in queries:
        vec = embed_text(q["query"], profile, vector_size).tolist()
        client.query_points(
            collection_name=COLLECTION_NAME,
            query=vec,
            limit=top_k,
            with_payload=False,
            with_vectors=False,
        )
    print(f"  Sent {len(queries)} queries through proxy.")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="TraceOwl end-to-end demo")
    ap.add_argument("--qdrant-port", type=int, default=6333)
    ap.add_argument("--proxy-port", type=int, default=9090)
    ap.add_argument("--proxy-bin", type=Path,
                    default=REPO_ROOT / "target" / "release" / "traceowl-proxy")
    ap.add_argument("--diff-bin", type=Path,
                    default=REPO_ROOT / "target" / "release" / "traceowl-diff")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/traceowl_demo_out"))
    ap.add_argument("--skip-docker", action="store_true",
                    help="Assume Qdrant is already running, skip Docker lifecycle")
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--vector-size", type=int, default=128)
    args = ap.parse_args()

    proxy_bin = args.proxy_bin.resolve()
    diff_bin = args.diff_bin.resolve()

    if not proxy_bin.exists():
        sys.exit(f"Proxy binary not found: {proxy_bin}\nRun: cargo build --release")
    if not diff_bin.exists():
        sys.exit(f"Diff binary not found: {diff_bin}\nRun: cargo build --release")

    qdrant_url = f"http://localhost:{args.qdrant_port}"
    proxy_url = f"http://localhost:{args.proxy_port}"
    proxy_addr = f"127.0.0.1:{args.proxy_port}"
    output_dir = args.output_dir.resolve()
    baseline_events_dir = output_dir / "baseline_events"
    candidate_events_dir = output_dir / "candidate_events"
    diff_output = output_dir / "diff.jsonl"

    baseline_profile = json.loads((SCRIPT_DIR / "baseline_profile.json").read_text())
    candidate_profile = json.loads((SCRIPT_DIR / "candidate_profile.json").read_text())

    docker_container = "traceowl-demo-qdrant"
    proxy_proc: subprocess.Popen | None = None

    try:
        # ------------------------------------------------------------------
        # Step 1: Start Qdrant
        # ------------------------------------------------------------------
        if not args.skip_docker:
            print("[1/6] Starting Qdrant via Docker ...")
            subprocess.run(["docker", "rm", "-f", docker_container], capture_output=True)
            subprocess.run(
                [
                    "docker", "run", "-d",
                    "--name", docker_container,
                    "-p", f"{args.qdrant_port}:6333",
                    "qdrant/qdrant:latest",
                ],
                check=True,
            )
        else:
            print("[1/6] Skipping Docker (--skip-docker set).")

        wait_for_qdrant(qdrant_url)
        qdrant_client = QdrantClient(url=qdrant_url)

        # ------------------------------------------------------------------
        # Step 2: Build baseline collection
        # ------------------------------------------------------------------
        print(f"[2/6] Building baseline collection '{COLLECTION_NAME}' ...")
        recreate_collection(qdrant_client, COLLECTION_NAME, args.vector_size)
        upsert_docs(qdrant_client, COLLECTION_NAME, DOCS, baseline_profile, args.vector_size)
        print(f"  Upserted {len(DOCS)} docs with baseline profile.")

        # ------------------------------------------------------------------
        # Step 3: Run baseline queries through proxy
        # ------------------------------------------------------------------
        print("[3/6] Running baseline queries through proxy ...")
        with tempfile.NamedTemporaryFile(suffix=".toml", delete=False) as tmp:
            cfg_path = Path(tmp.name)
        write_proxy_config(cfg_path, proxy_addr, qdrant_url, baseline_events_dir)

        proxy_proc = start_proxy(proxy_bin, cfg_path)
        wait_for_port("127.0.0.1", args.proxy_port)
        send_queries(proxy_url, QUERIES, baseline_profile, args.vector_size, args.top_k)
        stop_proxy(proxy_proc)
        proxy_proc = None
        time.sleep(1)  # allow final flush to complete
        print(f"  Baseline events written to: {baseline_events_dir}")

        # ------------------------------------------------------------------
        # Step 4: Rebuild collection with candidate embedding
        # ------------------------------------------------------------------
        print(f"[4/6] Rebuilding '{COLLECTION_NAME}' with candidate profile ...")
        recreate_collection(qdrant_client, COLLECTION_NAME, args.vector_size)
        upsert_docs(qdrant_client, COLLECTION_NAME, DOCS, candidate_profile, args.vector_size)
        print(f"  Upserted {len(DOCS)} docs with candidate profile.")

        # ------------------------------------------------------------------
        # Step 5: Run candidate queries through proxy
        # ------------------------------------------------------------------
        print("[5/6] Running candidate queries through proxy ...")
        write_proxy_config(cfg_path, proxy_addr, qdrant_url, candidate_events_dir)

        proxy_proc = start_proxy(proxy_bin, cfg_path)
        wait_for_port("127.0.0.1", args.proxy_port)
        # Use baseline profile for query embedding so vectors are identical
        # -> proxy produces the same hashes -> traceowl-diff can match them
        send_queries(proxy_url, QUERIES, baseline_profile, args.vector_size, args.top_k)
        stop_proxy(proxy_proc)
        proxy_proc = None
        time.sleep(1)  # allow final flush to complete
        print(f"  Candidate events written to: {candidate_events_dir}")

        # ------------------------------------------------------------------
        # Step 6: Run traceowl-diff
        # ------------------------------------------------------------------
        print("[6/6] Running traceowl-diff ...")
        baseline_files = sorted(glob.glob(str(baseline_events_dir / "*.jsonl")))
        candidate_files = sorted(glob.glob(str(candidate_events_dir / "*.jsonl")))

        if not baseline_files:
            sys.exit("No baseline event files found — did the proxy start correctly?")
        if not candidate_files:
            sys.exit("No candidate event files found — did the proxy start correctly?")

        diff_cmd = (
            [str(diff_bin)]
            + ["--baseline"] + baseline_files
            + ["--candidate"] + candidate_files
            + ["--output", str(diff_output)]
        )
        subprocess.run(diff_cmd, check=True, env={**os.environ, "RUST_LOG": "info"})

        print()
        print("=== Demo complete ===")
        print(f"  Baseline events : {baseline_events_dir}")
        print(f"  Candidate events: {candidate_events_dir}")
        print(f"  Diff output     : {diff_output}")

    finally:
        if proxy_proc is not None:
            stop_proxy(proxy_proc)
        if not args.skip_docker:
            subprocess.run(["docker", "rm", "-f", docker_container], capture_output=True)


if __name__ == "__main__":
    main()
