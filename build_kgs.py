#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
import yaml


@dataclass
class SparqlConfig:
    endpoint: str
    auth: str = "none"
    expected_namespaces: Optional[List[str]] = None


@dataclass
class KGSeed:
    kg_id: str
    name: str
    project: Optional[str] = None
    description_hint: Optional[str] = None
    sparql: Optional[SparqlConfig] = None
    repos: List[str] = None
    docs: List[str] = None
    priority: Optional[str] = None
    notes: Optional[str] = None
    dataset: Optional["KGDataset"] = None


@dataclass
class KGDataset:
    dump_url: Optional[str] = None
    local_path: Optional[str] = None
    format: Optional[str] = None


def slugify_filename(text: str, max_len: int = 80) -> str:
    """Make a filesystem-friendly slug from a URL or label."""
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    return text[:max_len] if len(text) > max_len else text


def fetch_url_text(url: str, timeout_s: int = 20) -> Dict[str, Any]:
    """Fetch a URL as text. Returns dict with url, text, and optional error."""
    try:
        r = requests.get(url, timeout=timeout_s, headers={"User-Agent": "kg-pipeline/0.1"})
        if r.status_code != 200:
            return {"url": url, "text": "", "error": f"http_{r.status_code}"}
        return {"url": url, "text": r.text}
    except requests.RequestException as e:
        return {"url": url, "text": "", "error": f"request_error:{e.__class__.__name__}"}


def load_local_text_source(path_str: str) -> Dict[str, Any]:
    path = Path(path_str)
    if path.suffix.lower() == ".pdf":
        return {
            "url": path_str,
            "resolved_url": path_str,
            "text": "",
            "source_path": str(path),
            "is_local_file": True,
        }
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError as e:
        return {"url": path_str, "text": "", "error": f"local_read_error:{e.__class__.__name__}"}
    return {
        "url": path_str,
        "resolved_url": path_str,
        "text": text,
        "source_path": str(path),
        "is_local_file": True,
    }


def github_headers(accept: str = "application/vnd.github+json") -> Dict[str, str]:
    headers = {
        "Accept": accept,
        "User-Agent": "kg-pipeline/0.1",
    }
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def resolve_github_ref(owner: str, repo: str, ref: str, timeout_s: int = 20) -> Optional[str]:
    commit_api_url = f"https://api.github.com/repos/{owner}/{repo}/commits/{ref}"
    try:
        resp = requests.get(commit_api_url, timeout=timeout_s, headers=github_headers())
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    payload = resp.json()
    sha = payload.get("sha")
    return sha.strip() if isinstance(sha, str) and sha.strip() else None


def fetch_github_readme(repo_url: str, timeout_s: int = 20) -> Dict[str, Any]:
    """Fetch a GitHub repo README pinned to a resolved commit when possible."""
    parts = repo_url.rstrip("/").split("/")
    if len(parts) < 2:
        return {"url": repo_url, "text": "", "error": "bad_repo_url"}
    owner, repo = parts[-2], parts[-1]

    repo_api_url = f"https://api.github.com/repos/{owner}/{repo}"
    api_url = f"https://api.github.com/repos/{owner}/{repo}/readme"
    try:
        repo_resp = requests.get(repo_api_url, timeout=timeout_s, headers=github_headers())
        if repo_resp.status_code != 200:
            api_error = {"url": api_url, "text": "", "error": f"http_{repo_resp.status_code}"}
        else:
            repo_meta = repo_resp.json()
            default_branch = repo_meta.get("default_branch")
            if not isinstance(default_branch, str) or not default_branch.strip():
                api_error = {"url": api_url, "text": "", "error": "missing_default_branch"}
            else:
                branch_api_url = f"https://api.github.com/repos/{owner}/{repo}/branches/{default_branch}"
                branch_resp = requests.get(branch_api_url, timeout=timeout_s, headers=github_headers())
                if branch_resp.status_code != 200:
                    api_error = {"url": api_url, "text": "", "error": f"http_{branch_resp.status_code}"}
                else:
                    branch_meta = branch_resp.json()
                    commit_obj = branch_meta.get("commit")
                    commit_sha = commit_obj.get("sha") if isinstance(commit_obj, dict) else None
                    if not isinstance(commit_sha, str) or not commit_sha.strip():
                        api_error = {"url": api_url, "text": "", "error": "missing_commit_sha"}
                    else:
                        readme_resp = requests.get(
                            api_url,
                            timeout=timeout_s,
                            headers=github_headers(),
                            params={"ref": commit_sha},
                        )
                        if readme_resp.status_code != 200:
                            api_error = {"url": api_url, "text": "", "error": f"http_{readme_resp.status_code}"}
                        else:
                            readme_meta = readme_resp.json()
                            readme_path = readme_meta.get("path")
                            if not isinstance(readme_path, str) or not readme_path.strip():
                                api_error = {"url": api_url, "text": "", "error": "missing_readme_path"}
                            else:
                                raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{commit_sha}/{readme_path}"
                                raw_resp = requests.get(
                                    raw_url,
                                    timeout=timeout_s,
                                    headers={"User-Agent": "kg-pipeline/0.1"},
                                )
                                if raw_resp.status_code != 200:
                                    api_error = {"url": api_url, "text": "", "error": f"http_{raw_resp.status_code}"}
                                else:
                                    return {
                                        "url": repo_url,
                                        "resolved_url": raw_url,
                                        "text": raw_resp.text,
                                        "repo_commit": commit_sha,
                                        "source_path": readme_path,
                                    }
    except requests.RequestException as e:
        api_error = {"url": api_url, "text": "", "error": f"request_error:{e.__class__.__name__}"}

    # Fallback: try raw GitHub URLs without commit pinning.
    raw_base = f"https://raw.githubusercontent.com/{owner}/{repo}/HEAD"
    candidates = [
        "README.md",
        "README.rst",
        "README.txt",
        "README",
        "readme.md",
        "readme.rst",
        "readme.txt",
        "readme",
    ]
    for name in candidates:
        raw_url = f"{raw_base}/{name}"
        try:
            r = requests.get(
                raw_url,
                timeout=timeout_s,
                headers={"User-Agent": "kg-pipeline/0.1"},
            )
            if r.status_code == 200:
                return {"url": repo_url, "resolved_url": raw_url, "text": r.text, "source_path": name}
        except requests.RequestException:
            continue

    return api_error


def fetch_github_blob_text(url: str, timeout_s: int = 20) -> Dict[str, Any]:
    parts = url.split("github.com/", 1)
    if len(parts) != 2:
        return fetch_url_text(url, timeout_s=timeout_s)
    path = parts[1]
    blob_marker = "/blob/"
    if blob_marker not in path:
        return fetch_url_text(url, timeout_s=timeout_s)

    repo_part, blob_part = path.split(blob_marker, 1)
    repo_bits = repo_part.strip("/").split("/")
    if len(repo_bits) < 2:
        return fetch_url_text(url, timeout_s=timeout_s)
    owner, repo = repo_bits[0], repo_bits[1]
    blob_bits = blob_part.split("/", 1)
    if len(blob_bits) != 2:
        return fetch_url_text(url, timeout_s=timeout_s)
    ref, source_path = blob_bits
    commit_sha = resolve_github_ref(owner, repo, ref, timeout_s=timeout_s)
    resolved_ref = commit_sha or ref
    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{resolved_ref}/{source_path}"
    fetched = fetch_url_text(raw_url, timeout_s=timeout_s)
    fetched["url"] = url
    fetched["resolved_url"] = raw_url
    fetched["repo_commit"] = commit_sha
    fetched["source_path"] = source_path
    return fetched


def source_cache_path(kg_id: str, idx: int, source: Dict[str, Any], out_dir: Path) -> Path:
    url = source.get("resolved_url") or source.get("url") or source.get("source_url") or ""
    netloc = urlparse(url).netloc
    base = slugify_filename(netloc) if netloc else "source"
    fname = f"{kg_id}__{idx:02d}__{base}.txt"
    return out_dir / fname


def load_cached_source(path: Path) -> Optional[Dict[str, Any]]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    if not raw.strip():
        return None
    header, sep, body = raw.partition("\n\n")
    if not sep:
        return None
    source_url = ""
    if header.startswith("SOURCE: "):
        source_url = header[len("SOURCE: ") :].strip()
    return {
        "resolved_url": source_url,
        "text": body,
        "cache_path": str(path),
    }


def apply_cached_fallbacks(kg_id: str, sources: List[Dict[str, Any]], out_dir: Path) -> List[Dict[str, Any]]:
    resolved: List[Dict[str, Any]] = []
    for idx, src in enumerate(sources, start=1):
        current = dict(src)
        cache_path = source_cache_path(kg_id, idx, current, out_dir)
        current["cache_path"] = str(cache_path)
        if current.get("text"):
            resolved.append(current)
            continue
        cached = load_cached_source(cache_path)
        if cached is None:
            resolved.append(current)
            continue
        current["text"] = cached["text"]
        if not current.get("resolved_url") and cached.get("resolved_url"):
            current["resolved_url"] = cached["resolved_url"]
        current["used_cached_copy"] = True
        resolved.append(current)
    return resolved


def save_sources(kg_id: str, sources: List[Dict[str, Any]], out_dir: Path) -> List[str]:
    """Save each source text to a file. Returns list of file paths (as strings)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: List[str] = []

    for idx, src in enumerate(sources, start=1):
        if src.get("is_local_file"):
            saved.append(str(src.get("source_path") or src.get("url") or src.get("resolved_url")))
            continue

        text = (src.get("text") or "").strip()
        if not text:
            continue

        path = source_cache_path(kg_id, idx, src, out_dir)
        url = src.get("resolved_url") or src.get("url") or src.get("source_url") or ""

        path.write_text(f"SOURCE: {url}\n\n{text}", encoding="utf-8")
        saved.append(str(path))

    return saved


def load_seeds(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing seeds file: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("seeds.yaml must contain a top-level mapping (dictionary).")

    kgs = data.get("kgs")
    if not isinstance(kgs, list):
        raise ValueError("seeds.yaml must have a top-level key 'kgs' containing a list.")

    for i, item in enumerate(kgs):
        if not isinstance(item, dict):
            raise ValueError(f"seeds.yaml: kgs[{i}] must be a mapping (dict).")

    return kgs


def parse_kg_seed(raw: Dict[str, Any]) -> KGSeed:
    kg_id = raw.get("kg_id")
    name = raw.get("name")
    if not isinstance(kg_id, str) or not kg_id.strip():
        raise ValueError("Each KG must have a non-empty string 'kg_id'.")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"KG '{kg_id}': must have a non-empty string 'name'.")

    repos = raw.get("repos") or []
    docs = raw.get("docs") or []
    if not isinstance(repos, list) or not all(isinstance(x, str) for x in repos):
        raise ValueError(f"KG '{kg_id}': 'repos' must be a list of strings.")
    if not isinstance(docs, list) or not all(isinstance(x, str) for x in docs):
        raise ValueError(f"KG '{kg_id}': 'docs' must be a list of strings.")

    sparql_cfg = None
    sparql = raw.get("sparql")
    if sparql is not None:
        if not isinstance(sparql, dict):
            raise ValueError(f"KG '{kg_id}': 'sparql' must be a mapping (dict).")
        endpoint = sparql.get("endpoint")
        auth = sparql.get("auth", "none")
        expected_namespaces = sparql.get("expected_namespaces")
        if not isinstance(endpoint, str) or not endpoint.strip():
            raise ValueError(f"KG '{kg_id}': sparql.endpoint must be a non-empty string.")
        if not isinstance(auth, str):
            raise ValueError(f"KG '{kg_id}': sparql.auth must be a string.")
        if expected_namespaces is not None:
            if not isinstance(expected_namespaces, list) or not all(
                isinstance(x, str) for x in expected_namespaces
            ):
                raise ValueError(
                    f"KG '{kg_id}': sparql.expected_namespaces must be a list of strings."
                )
        sparql_cfg = SparqlConfig(
            endpoint=endpoint.strip(),
            auth=auth.strip(),
            expected_namespaces=expected_namespaces,
        )

    dataset_cfg = None
    dataset = raw.get("dataset")
    if dataset is not None:
        if not isinstance(dataset, dict):
            raise ValueError(f"KG '{kg_id}': dataset must be a mapping (dict).")
        dump_url = dataset.get("dump_url")
        local_path = dataset.get("local_path")
        fmt = dataset.get("format")
        if dump_url is not None and not isinstance(dump_url, str):
            raise ValueError(f"KG '{kg_id}': dataset.dump_url must be a string.")
        if local_path is not None and not isinstance(local_path, str):
            raise ValueError(f"KG '{kg_id}': dataset.local_path must be a string.")
        if fmt is not None and not isinstance(fmt, str):
            raise ValueError(f"KG '{kg_id}': dataset.format must be a string.")
        dataset_cfg = KGDataset(
            dump_url=dump_url.strip() if isinstance(dump_url, str) else None,
            local_path=local_path.strip() if isinstance(local_path, str) else None,
            format=fmt.strip() if isinstance(fmt, str) else None,
        )

    return KGSeed(
        kg_id=kg_id.strip(),
        name=name.strip(),
        project=raw.get("project"),
        description_hint=raw.get("description_hint"),
        sparql=sparql_cfg,
        repos=repos,
        docs=docs,
        priority=raw.get("priority"),
        notes=raw.get("notes"),
        dataset=dataset_cfg,
    )


def kgseed_to_record(kg: KGSeed) -> Dict[str, Any]:
    today = date.today().isoformat()
    sparql_obj = None
    if kg.sparql:
        sparql_obj = {
            "endpoint": kg.sparql.endpoint,
            "auth": kg.sparql.auth,
            "graph": None,
        }
        if kg.sparql.expected_namespaces:
            sparql_obj["expected_namespaces"] = list(kg.sparql.expected_namespaces)
    dataset_obj = {"dump_url": None, "local_path": None, "format": None}
    if kg.dataset:
        dataset_obj = {
            "dump_url": kg.dataset.dump_url,
            "local_path": kg.dataset.local_path,
            "format": kg.dataset.format,
        }
    return {
        "kg_id": kg.kg_id,
        "name": kg.name,
        "project": kg.project,
        "description": None,
        "sparql": sparql_obj,
        "dataset": dataset_obj,
        "repos": list(kg.repos or []),
        "docs": list(kg.docs or []),
        "notes": kg.notes,
        "created_at": today,
        "updated_at": today,
        "description_hint": kg.description_hint,
        "priority": kg.priority,
    }


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def fetch_sources_for_kg(kg: KGSeed) -> List[Dict[str, Any]]:
    """Fetch README + docs sources for a KG (deterministic, no OpenAI)."""
    sources: List[Dict[str, Any]] = []

    for repo_url in (kg.repos or []):
        if "github.com" in repo_url:
            sources.append(fetch_github_readme(repo_url))
        else:
            sources.append(fetch_url_text(repo_url))

    for doc_url in (kg.docs or []):
        doc_path = Path(doc_url)
        if doc_path.exists():
            sources.append(load_local_text_source(doc_url))
            continue
        if "github.com/" in doc_url and "/blob/" in doc_url:
            sources.append(fetch_github_blob_text(doc_url))
        else:
            sources.append(fetch_url_text(doc_url))

    return sources


def print_source_status(kg: KGSeed, sources: List[Dict[str, Any]], saved_files: List[str]) -> None:
    endpoint = kg.sparql.endpoint if kg.sparql else "(no endpoint)"
    errors = [s for s in sources if s.get("error")]
    cached = [s for s in sources if s.get("used_cached_copy")]
    local_files = [s for s in sources if s.get("is_local_file")]

    print(f"- {kg.kg_id}: {kg.name}")
    print(f"  endpoint: {endpoint}")
    print(f"  repos:    {len(kg.repos or [])}")
    print(f"  docs:     {len(kg.docs or [])}")
    print(f"  sources:  {len(saved_files)} saved")
    if local_files:
        print(f"  local:    {len(local_files)} curated/local docs")
    if errors:
        print(f"  WARNING:  {len(errors)} source fetch failures")
        for src in errors:
            source_url = src.get("url") or src.get("resolved_url") or "(unknown source)"
            extra = ""
            if src.get("used_cached_copy"):
                extra = f" -> reused cached copy {src.get('cache_path')}"
            print(f"    - {source_url}: {src.get('error')}{extra}")
    elif cached:
        print(f"  NOTE:     reused {len(cached)} cached source copies")
    print()


def main() -> None:
    seeds_path = Path("seeds.yaml")
    out_path = Path("kgs.jsonl")
    sources_dir = Path("kg_sources")

    raw_kgs = load_seeds(seeds_path)
    kgs = [parse_kg_seed(r) for r in raw_kgs]

    print(f"\nStep 1: loaded {len(kgs)} KGs from {seeds_path.resolve()}\n")

    print("Step 2: fetching README/doc sources (deterministic, no OpenAI yet)\n")
    sources_by_kg: Dict[str, List[Dict[str, Any]]] = {}
    saved_by_kg: Dict[str, List[str]] = {}
    for kg in kgs:
        sources = apply_cached_fallbacks(kg.kg_id, fetch_sources_for_kg(kg), sources_dir)
        saved_files = save_sources(kg.kg_id, sources, sources_dir)
        sources_by_kg[kg.kg_id] = sources
        saved_by_kg[kg.kg_id] = saved_files
        print_source_status(kg, sources, saved_files)

    print("Step 3: writing kgs.jsonl (metadata + provenance, no generated descriptions yet)\n")
    records: List[Dict[str, Any]] = []
    for kg in kgs:
        rec = kgseed_to_record(kg)
        sources = sources_by_kg.get(kg.kg_id, [])
        saved_files = saved_by_kg.get(kg.kg_id, [])

        rec["source_urls"] = [
            s.get("resolved_url") or s.get("url")
            for s in sources
            if s.get("resolved_url") or s.get("url")
        ]
        rec["source_details"] = [
            {
                "source_url": s.get("url"),
                "resolved_url": s.get("resolved_url") or s.get("url"),
                "repo_commit": s.get("repo_commit"),
                "source_path": s.get("source_path"),
                "error": s.get("error"),
                "used_cached_copy": bool(s.get("used_cached_copy")),
                "is_local_file": bool(s.get("is_local_file")),
            }
            for s in sources
            if s.get("resolved_url") or s.get("url") or s.get("error")
        ]
        rec["source_files"] = saved_files
        records.append(rec)

    write_jsonl(out_path, records)
    print(f"Wrote {len(records)} records to {out_path.resolve()}")


if __name__ == "__main__":
    main()
