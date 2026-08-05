#!/usr/bin/env python3
"""Same-origin local service for the SPARQL correction workbench."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import threading
import time
import uuid
import ipaddress
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, Mapping
from urllib.parse import urlsplit, urlunsplit

from holdout_selectors import holdout_input_policy, validate_selectors_current
from run_llm_generation import OpenAI, extract_first_json_object, sha256_json, sha256_text, validate_output
from run_queries import execute_sparql_observation, load_datasets, load_endpoints
from sparql_corrections import (
    candidate_digest,
    load_jsonl,
    safe_execution_projection,
    sparql_provenance,
    validate_candidate,
)
from sparql_versions import resolve_sparql_version, sparql_hash

ROOT = Path(__file__).resolve().parent
MAX_BODY = 512_000
MAX_ERROR = 1000
PUBLIC_ASSETS = {"/corrections.html", "/correction_app.js", "/styles.css", "/sparql_correction_data.js"}


def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def canonical_digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def read_bundle(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    match = re.search(r"window\.SPARQL_CORRECTION_DATA\s*=\s*(\{.*\})\s*;\s*$", text, re.S)
    if not match:
        raise ValueError(f"Unsupported correction bundle: {path}")
    payload = json.loads(match.group(1))
    if payload.get("mode") != "sparql_correction" or not isinstance(payload.get("records"), list):
        raise ValueError("Invalid correction bundle")
    claimed = payload.get("bundle_digest")
    unsigned = dict(payload)
    unsigned.pop("bundle_digest", None)
    if claimed != canonical_digest(unsigned):
        raise ValueError("Correction bundle digest does not match its contents")
    return payload


def append_jsonl(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def safe_endpoint(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    parts = urlsplit(value)
    host = parts.hostname or ""
    if parts.port:
        host += f":{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def safe_error(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).replace(str(ROOT), "<repo>")
    text = re.sub(r"(?i)bearer\s+[a-z0-9._~+/=-]+", "Bearer <redacted>", text)
    text = re.sub(r"https?://[^\s/@]+:[^\s/@]+@", lambda match: match.group(0).split("://", 1)[0] + "://<redacted>@", text)
    text = re.sub(r"([?&](?:api[_-]?key|token|password|secret)=)[^\s&]+", r"\1<redacted>", text, flags=re.I)
    text = re.sub(r"(?i)(api[_-]?key|token|password|secret)=([^\s&]+)", r"\1=<redacted>", text)
    text = re.sub(r"https?://[^\s<>\"']+", lambda match: safe_endpoint(match.group(0)) or "<redacted-url>", text, flags=re.I)
    return text[:MAX_ERROR]


def allowed_static_path(raw_path: str) -> str | None:
    path = urlsplit(raw_path).path
    return path if path in PUBLIC_ASSETS else None


class Workbench:
    def __init__(
        self,
        *,
        bundle: Mapping[str, Any],
        queries: list[Dict[str, Any]],
        holdout_pairs: set[tuple[str, str]],
        seeds_path: Path,
        attempts_path: Path,
        suggestions_path: Path,
        model: str,
        suggestion_provider: Callable[[Dict[str, Any]], tuple[Dict[str, Any], Dict[str, Any]]] | None = None,
    ) -> None:
        self.holdout_pairs = holdout_pairs
        self.bundle_digest = str(bundle.get("bundle_digest") or canonical_digest(bundle))
        self.queries = {(str(q.get("kg_id") or ""), str(q.get("query_id") or "")): q for q in queries}
        self.candidates: Dict[str, Dict[str, Any]] = {}
        # Identity filtering deliberately precedes validation or evidence access.
        for raw in bundle.get("records") or []:
            key = (str(raw.get("kg_id") or ""), str(raw.get("query_id") or ""))
            if key in holdout_pairs:
                continue
            validate_candidate(raw)
            self.candidates[str(raw["candidate_id"])] = dict(raw)
        self.endpoints = load_endpoints(seeds_path)
        self.datasets = load_datasets(seeds_path)
        self.attempts_path = attempts_path
        self.suggestions_path = suggestions_path
        self.model = model
        self.suggestion_provider = suggestion_provider
        self._write_lock = threading.Lock()
        self._execution_locks: Dict[str, threading.Lock] = {}
        self.prompt = (ROOT / "prompts/sparql_correction.prompt.txt").read_text(encoding="utf-8")
        self.schema = json.loads((ROOT / "schemas/sparql_correction_suggestion.schema.json").read_text(encoding="utf-8"))
        self.prompt_hash = "sha256:" + sha256_text(self.prompt)
        self.schema_hash = "sha256:" + sha256_json(self.schema)

    def candidate(self, request: Mapping[str, Any]) -> Dict[str, Any]:
        identifier = str(request.get("candidate_id") or "")
        candidate = self.candidates.get(identifier)
        if candidate is None:
            raise PermissionError("Unknown or holdout-excluded candidate")
        key = (str(candidate["kg_id"]), str(candidate["query_id"]))
        if key in self.holdout_pairs:
            raise PermissionError("Holdout identities cannot use workbench APIs")
        if request.get("candidate_digest") != candidate_digest(candidate):
            # Bundles carry candidate_digest as an additive field; authoritative digest excludes it.
            authoritative = dict(candidate)
            authoritative.pop("candidate_digest", None)
            if request.get("candidate_digest") != candidate_digest(authoritative):
                raise ValueError("Stale candidate digest")
        record = self.queries.get(key)
        if record is None:
            raise ValueError("Candidate is absent from canonical queries")
        retained_base = resolve_sparql_version(record, int(candidate["base_sparql_version"]))
        expected_provenance = sparql_provenance(record, retained_base)
        if candidate.get("sparql_provenance") != expected_provenance:
            raise ValueError("Candidate SPARQL provenance is stale")
        return candidate

    def execute(self, request: Mapping[str, Any]) -> Dict[str, Any]:
        candidate = self.candidate(request)
        target = str(request.get("target") or "")
        if target not in {"base", "proposal", "latest_approved"}:
            raise ValueError("Unsupported execution target")
        key = (str(candidate["kg_id"]), str(candidate["query_id"]))
        record = self.queries.get(key)
        if record is None:
            raise ValueError("Candidate is absent from canonical queries")
        retained_base = resolve_sparql_version(record, int(candidate["base_sparql_version"]))
        if retained_base["sparql_hash"] != candidate["base_sparql_hash"] or retained_base["sparql"] != candidate["base_sparql"]:
            raise ValueError("Candidate base version no longer resolves in canonical queries")
        version: int | None
        if target == "base":
            text = str(candidate["base_sparql"])
            digest = str(candidate["base_sparql_hash"])
            version = int(candidate["base_sparql_version"])
        elif target == "latest_approved":
            resolved = resolve_sparql_version(record, "latest")
            text, digest, version = resolved["sparql"], resolved["sparql_hash"], resolved["sparql_version"]
        else:
            text = str(request.get("sparql") or "").strip()
            if not text:
                raise ValueError("Proposal execution requires SPARQL")
            digest, version = sparql_hash(text), None
        if request.get("sparql_hash") != digest:
            raise ValueError("Stale or mismatched SPARQL hash")
        execution_lock = self._execution_locks.setdefault(key[0], threading.Lock())
        with execution_lock:
            observation = execute_sparql_observation(
                kg_id=key[0], retained_sparql=text, retained_hash=digest, sparql_version=version,
                endpoints=self.endpoints, datasets=self.datasets,
            )
        attempt = {
            "schema": "musparql.sparql-correction-ui-execution.v1",
            "attempt_id": "exec-" + uuid.uuid4().hex,
            "candidate_id": candidate["candidate_id"], "candidate_digest": request["candidate_digest"],
            "bundle_digest": self.bundle_digest, "kg_id": key[0], "query_id": key[1], "target": target,
            **observation,
        }
        attempt["endpoint"] = safe_endpoint(attempt.get("endpoint"))
        attempt["graph"] = safe_endpoint(attempt.get("graph"))
        attempt["error"] = safe_error(attempt.get("error"))
        attempt["attempt_digest"] = canonical_digest(attempt)
        with self._write_lock:
            append_jsonl(self.attempts_path, attempt)
        return attempt

    def suggestion_input(self, candidate: Mapping[str, Any], record: Mapping[str, Any]) -> Dict[str, Any]:
        latest = resolve_sparql_version(record, "latest")
        retained_base = resolve_sparql_version(record, int(candidate["base_sparql_version"]))
        if retained_base["sparql_hash"] != candidate["base_sparql_hash"] or retained_base["sparql"] != candidate["base_sparql"]:
            raise ValueError("Candidate base version no longer resolves in canonical queries")
        prior_edits = [
            {
                "version": item.get("version"), "sparql_hash": sparql_hash(str(item.get("sparql"))),
                "edit_type": item.get("edit_type"), "note": item.get("note"),
                "evidence_ids": list(item.get("evidence_ids") or []),
            }
            for item in (candidate.get("existing_edits") or []) if isinstance(item, Mapping) and item.get("sparql")
        ]
        correction_history = [
            {
                key: item.get(key) for key in (
                    "decision", "edit_type", "rationale", "proposal_origin", "proposal_model",
                    "reviewed_at", "base_sparql_version", "base_sparql_hash",
                ) if item.get(key) is not None
            }
            for item in (candidate.get("correction_history") or []) if isinstance(item, Mapping)
        ]
        return {
            "kg_id": candidate["kg_id"], "query_id": candidate["query_id"],
            "query_label": candidate.get("query_label"),
            "base_sparql": candidate["base_sparql"], "base_sparql_hash": candidate["base_sparql_hash"],
            "latest_approved": {"sparql": latest["sparql"], "sparql_version": latest["sparql_version"], "sparql_hash": latest["sparql_hash"]},
            "failure_classification": candidate.get("triage"),
            "execution_observation": safe_execution_projection(candidate.get("execution") or {}),
            "evidence": candidate.get("evidence") or [],
            "endpoint_context": {
                "configured": candidate["kg_id"] in self.endpoints,
                "graph": safe_endpoint(self.endpoints.get(candidate["kg_id"]).primary.graph) if candidate["kg_id"] in self.endpoints else None,
                "local_runtime": candidate["kg_id"] in self.datasets,
            },
            "prior_edits": prior_edits,
            "correction_history": correction_history,
        }

    def suggest(self, request: Mapping[str, Any]) -> Dict[str, Any]:
        candidate = self.candidate(request)
        key = (str(candidate["kg_id"]), str(candidate["query_id"]))
        record = self.queries.get(key)
        if record is None:
            raise ValueError("Candidate is absent from canonical queries")
        payload = self.suggestion_input(candidate, record)
        started = time.time()
        if self.suggestion_provider:
            output, response_meta = self.suggestion_provider(payload)
        else:
            if OpenAI is None:
                raise RuntimeError("The openai package is not installed")
            client_args: Dict[str, Any] = {"timeout": 180}
            if os.getenv("GRAPHIA_API_KEY"):
                client_args["api_key"] = os.getenv("GRAPHIA_API_KEY")
            if os.getenv("GRAPHIA_BASE_URL"):
                client_args["base_url"] = os.getenv("GRAPHIA_BASE_URL")
            client = OpenAI(**client_args)
            system = self.prompt + "\nOutput schema (JSON):\n" + json.dumps(self.schema, ensure_ascii=False)
            response = client.responses.create(
                model=self.model,
                input=[{"role": "system", "content": system}, {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
            )
            output = extract_first_json_object((response.output_text or "").strip())
            if output is None:
                raise ValueError("Agent returned no JSON object")
            response_meta = {"id": getattr(response, "id", None), "model": getattr(response, "model", None)}
        valid, error = validate_output(output, self.schema)
        if not valid:
            raise ValueError(f"Agent suggestion schema validation failed: {error}")
        valid_evidence = {str(item.get("evidence_id")) for item in candidate.get("evidence") or [] if isinstance(item, Mapping)}
        unknown = sorted(set(output.get("evidence_ids") or []) - valid_evidence)
        if unknown:
            raise ValueError(f"Agent suggestion cites unknown evidence IDs: {unknown}")
        if output.get("recommendation") == "edit":
            proposed_hash = sparql_hash(str(output["proposed_sparql"]))
        else:
            proposed_hash = None
        suggestion = {
            "schema": "musparql.sparql-correction-agent-suggestion.v1",
            "suggestion_id": "suggest-" + uuid.uuid4().hex,
            "candidate_id": candidate["candidate_id"], "candidate_digest": request["candidate_digest"],
            "bundle_digest": self.bundle_digest, "kg_id": key[0], "query_id": key[1],
            "output": output, "proposed_sparql_hash": proposed_hash,
            "model": response_meta.get("model") or self.model, "request_id": response_meta.get("id"),
            "prompt_hash": self.prompt_hash, "schema_hash": self.schema_hash,
            "input_hash": canonical_digest(payload), "generated_at": now(),
            "duration_ms": int((time.time() - started) * 1000),
        }
        suggestion["suggestion_digest"] = canonical_digest(suggestion)
        with self._write_lock:
            append_jsonl(self.suggestions_path, suggestion)
        return suggestion


class Handler(SimpleHTTPRequestHandler):
    workbench: Workbench

    def __init__(self, *args: Any, directory: str | None = None, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(ROOT / "review"), **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        # Deliberately log only method/path/status; never request bodies, queries or credentials.
        super().log_message(format, *args)

    def json_response(self, status: int, payload: Mapping[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/api/health":
            self.json_response(200, {"ok": True, "candidate_count": len(self.workbench.candidates)})
            return
        if self.path == "/":
            self.send_response(302); self.send_header("Location", "/corrections.html"); self.end_headers(); return
        allowed = allowed_static_path(self.path)
        if allowed is None:
            self.json_response(404, {"error": "Not found"})
            return
        original_path = self.path
        self.path = allowed
        try:
            super().do_GET()
        finally:
            self.path = original_path

    def do_POST(self) -> None:
        if self.path not in {"/api/execute", "/api/suggest"}:
            self.json_response(404, {"error": "Not found"}); return
        try:
            origin = self.headers.get("Origin")
            port = self.server.server_address[1]
            bound_host = str(self.server.server_address[0])
            url_host = f"[{bound_host}]" if ":" in bound_host else bound_host
            expected_origins = {f"http://{url_host}:{port}", f"http://localhost:{port}"}
            if origin and origin not in expected_origins:
                raise PermissionError("Cross-origin workbench requests are forbidden")
            host = (self.headers.get("Host") or "").lower()
            if host not in {f"{url_host}:{port}".lower(), f"localhost:{port}"}:
                raise PermissionError("Invalid workbench Host header")
            if self.headers.get_content_type() != "application/json":
                raise ValueError("Content-Type must be application/json")
            length = int(self.headers.get("Content-Length") or "0")
            if length <= 0 or length > MAX_BODY:
                raise ValueError("Invalid request size")
            request = json.loads(self.rfile.read(length))
            if not isinstance(request, dict):
                raise ValueError("JSON object required")
            result = self.workbench.execute(request) if self.path.endswith("execute") else self.workbench.suggest(request)
            self.json_response(200, result)
        except PermissionError as exc:
            self.json_response(403, {"error": str(exc)[:MAX_ERROR]})
        except (ValueError, KeyError) as exc:
            self.json_response(400, {"error": safe_error(exc)})
        except Exception as exc:
            self.json_response(503, {"error": safe_error(f"{exc.__class__.__name__}: {exc}")})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", default="review/sparql_correction_data.js")
    parser.add_argument("--queries", default="kg_queries.jsonl")
    parser.add_argument("--seeds", default="seeds.yaml")
    parser.add_argument("--holdout-selectors", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--model", default="gpt-5")
    parser.add_argument("--attempt-log", default="review/local_workbench_execution_attempts.jsonl")
    parser.add_argument("--suggestion-log", default="review/local_workbench_suggestions.jsonl")
    parser.add_argument("--synthetic-suggestion", default="", help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        if not ipaddress.ip_address(args.host).is_loopback:
            raise ValueError("Correction service must bind to a loopback IP address")
    except ValueError as exc:
        raise ValueError("--host must be a loopback IP address such as 127.0.0.1 or ::1") from exc
    queries = load_jsonl(Path(args.queries))
    selector_rows = load_jsonl(Path(args.holdout_selectors))
    pairs = validate_selectors_current(selector_rows, queries)
    provider = None
    if args.synthetic_suggestion:
        synthetic = json.loads(Path(args.synthetic_suggestion).read_text(encoding="utf-8"))
        provider = lambda _payload: (synthetic, {"id": "synthetic-request", "model": "synthetic-agent"})
    workbench = Workbench(
        bundle=read_bundle(Path(args.bundle)), queries=queries, holdout_pairs=pairs,
        seeds_path=Path(args.seeds), attempts_path=Path(args.attempt_log), suggestions_path=Path(args.suggestion_log),
        model=args.model, suggestion_provider=provider,
    )
    Handler.workbench = workbench
    server_class = ThreadingHTTPServer
    if ipaddress.ip_address(args.host).version == 6:
        class IPv6ThreadingHTTPServer(ThreadingHTTPServer):
            address_family = socket.AF_INET6
        server_class = IPv6ThreadingHTTPServer
    server = server_class((args.host, args.port), Handler)
    display_host = f"[{args.host}]" if ":" in args.host else args.host
    print(f"Correction workbench: http://{display_host}:{args.port}/corrections.html")
    print(f"Loaded {len(workbench.candidates)} non-holdout candidates; selector policy={holdout_input_policy(args)}")
    server.serve_forever()


if __name__ == "__main__":
    main()
