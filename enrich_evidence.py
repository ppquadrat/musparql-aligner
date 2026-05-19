#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import argparse
import hashlib
from datetime import datetime, timezone
import html
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse, urlunparse
from urllib.request import Request, urlopen
from pypdf import PdfReader


def load_query_records(path: Path) -> List[Dict[str, object]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    text = path.read_text(encoding="utf-8-sig")
    if text.lstrip().startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError("kg_queries.jsonl must be a JSON array or JSONL.")
        return data
    records: List[Dict[str, object]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))
    return records


def write_jsonl(path: Path, records: List[Dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def repo_dir_from_url(repo_url: str) -> Path:
    parts = repo_url.rstrip("/").split("/")
    if len(parts) < 2:
        raise ValueError(f"Bad repo URL: {repo_url}")
    owner, repo = parts[-2], parts[-1]
    return Path(f"{owner}__{repo}")


def resolve_repo_url(repo_url: str) -> str:
    parsed = urlparse(repo_url)
    return repo_url if parsed.scheme else f"https://{repo_url}"


def iter_repo_files(repo_dir: Path) -> Iterable[Path]:
    for path in repo_dir.rglob("*"):
        if path.is_file():
            yield path


def split_queries_with_starts(text: str) -> List[Dict[str, object]]:
    lines = text.splitlines()
    keyword_re = re.compile(
        r"^\s*(select|construct|ask|describe|insert|delete|with|load|clear|create|drop|copy|move|add)\b",
        re.IGNORECASE,
    )
    meta_re = re.compile(r"^\s*(prefix|base)\b", re.IGNORECASE)

    def is_meta_line(line: str) -> bool:
        stripped = line.strip()
        if not stripped:
            return True
        if stripped.startswith("#"):
            return True
        return bool(meta_re.match(stripped))

    def strip_line_comments(line: str) -> str:
        if line.lstrip().startswith("#"):
            return ""
        return line

    starts: List[int] = []
    depth = 0
    for idx, line in enumerate(lines):
        if depth == 0 and keyword_re.match(line):
            starts.append(idx)
        clean = strip_line_comments(line)
        depth += clean.count("{") - clean.count("}")

    if len(starts) <= 1:
        return [{"start": 0, "query": text}]

    adjusted: List[int] = []
    last_start = -1
    for start in starts:
        adj = start
        while adj > last_start + 1 and is_meta_line(lines[adj - 1]):
            adj -= 1
        if adj <= last_start:
            adj = start
        adjusted.append(adj)
        last_start = adj

    if adjusted[0] != 0:
        adjusted = [0] + adjusted

    adjusted = sorted(set(adjusted))
    segments: List[Dict[str, object]] = []
    for i, start in enumerate(adjusted):
        end = adjusted[i + 1] if i + 1 < len(adjusted) else None
        segment = "\n".join(lines[start:end]).strip()
        if segment:
            segments.append({"start": start, "query": segment})
    return segments or [{"start": 0, "query": text}]


def sha256_hash(text: str) -> str:
    import hashlib

    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def normalize_query(text: str) -> str:
    normalized = text.strip()
    while normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()
    lines = normalized.splitlines()
    seen_prefixes = set()
    deduped_lines: List[str] = []
    prefix_decl_re = re.compile(r"(?im)^\s*prefix\s+(\w+):")
    for line in lines:
        match = prefix_decl_re.match(line)
        if match:
            prefix_name = match.group(1).lower()
            if prefix_name in seen_prefixes:
                continue
            seen_prefixes.add(prefix_name)
        deduped_lines.append(line)
    normalized = "\n".join(deduped_lines)
    for p in ("rdf", "rdfs", "xsd", "dc", "dtl", "event", "mo", "tl", "foaf"):
        normalized = re.sub(rf"\b{p.upper()}:", f"{p}:", normalized)
    prefix_map = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "dc": "http://purl.org/dc/elements/1.1/",
        "dtl": "http://www.DTL.org/schema/properties/",
        "event": "http://purl.org/NET/c4dm/event.owl#",
        "mo": "http://purl.org/ontology/mo/",
        "tl": "http://purl.org/NET/c4dm/timeline.owl#",
        "foaf": "http://xmlns.com/foaf/0.1/",
    }
    existing = {m.group(1).lower() for m in re.finditer(r"(?im)^\s*prefix\s+(\w+):", normalized)}
    needed: List[str] = []
    for prefix, iri in prefix_map.items():
        if prefix in existing:
            continue
        if re.search(rf"\b{re.escape(prefix)}:", normalized):
            needed.append(f"PREFIX {prefix}: <{iri}>")
    if needed:
        normalized = "\n".join(needed) + "\n" + normalized
        lines = normalized.splitlines()
        seen_prefixes = set()
        deduped_lines = []
        for line in lines:
            match = prefix_decl_re.match(line)
            if match:
                prefix_name = match.group(1).lower()
                if prefix_name in seen_prefixes:
                    continue
                seen_prefixes.add(prefix_name)
            deduped_lines.append(line)
        normalized = "\n".join(deduped_lines)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


STANDARD_RDF_URI_PREFIXES = (
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "http://www.w3.org/2000/01/rdf-schema#",
    "http://www.w3.org/2001/XMLSchema#",
    "http://www.w3.org/2002/07/owl#",
)


COMMON_PREFIXES = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "dtl": "http://www.DTL.org/schema/properties/",
    "event": "http://purl.org/NET/c4dm/event.owl#",
    "mo": "http://purl.org/ontology/mo/",
    "tl": "http://purl.org/NET/c4dm/timeline.owl#",
    "foaf": "http://xmlns.com/foaf/0.1/",
}


def parse_prefixes(query: str) -> Dict[str, str]:
    prefixes: Dict[str, str] = dict(COMMON_PREFIXES)
    for match in re.finditer(r"(?im)^\s*PREFIX\s+([A-Za-z][\w-]*):\s*<([^>]+)>", query):
        prefixes[match.group(1)] = match.group(2)
    for match in re.finditer(r"(?im)^\s*BASE\s+<([^>]+)>", query):
        prefixes[""] = match.group(1)
    return prefixes


def strip_prefix_declarations(query: str) -> str:
    return "\n".join(
        line for line in query.splitlines()
        if not re.match(r"^\s*(PREFIX|BASE)\b", line, re.IGNORECASE)
    )


def curie_to_uri(curie: str, prefixes: Dict[str, str]) -> Optional[str]:
    if curie.startswith("<") and curie.endswith(">"):
        return curie[1:-1]
    if ":" not in curie:
        return None
    prefix, local = curie.split(":", 1)
    base = prefixes.get(prefix)
    if not base:
        return None
    return base + local


def uri_local_name(uri: str) -> str:
    clean = uri.rstrip("#/")
    for sep in ("#", "/", ":"):
        if sep in clean:
            return clean.rsplit(sep, 1)[1]
    return clean


def display_uri(uri: str, prefixes: Dict[str, str]) -> str:
    best_prefix = ""
    best_base = ""
    for prefix, base in prefixes.items():
        if not prefix or not uri.startswith(base) or len(base) <= len(best_base):
            continue
        best_prefix = prefix
        best_base = base
    if best_base:
        return f"{best_prefix}:{uri[len(best_base):]}"
    return f"<{uri}>"


def is_standard_rdf_uri(uri: str) -> bool:
    return uri.startswith(STANDARD_RDF_URI_PREFIXES)


def add_query_term(
    terms: Dict[str, Dict[str, object]],
    uri: Optional[str],
    token: str,
    role: str,
) -> None:
    if not uri or is_standard_rdf_uri(uri):
        return
    entry = terms.setdefault(uri, {"uri": uri, "tokens": set(), "roles": set()})
    tokens = entry.get("tokens")
    roles = entry.get("roles")
    if isinstance(tokens, set):
        tokens.add(token)
    if isinstance(roles, set):
        roles.add(role)


def extract_query_terms(query: str) -> Dict[str, Dict[str, object]]:
    prefixes = parse_prefixes(query)
    body = strip_prefix_declarations(query)
    terms: Dict[str, Dict[str, object]] = {}

    term_re = r"(?:<[^>]+>|[A-Za-z][\w-]*:[A-Za-z_][\w.\-~%]*)"
    for match in re.finditer(rf"(?:\ba\b|rdf:type)\s+({term_re})", body):
        token = match.group(1)
        add_query_term(terms, curie_to_uri(token, prefixes), token, "class")

    predicate_patterns = [
        rf"(?:^|[{{.;\[])\s*({term_re})\s+(?=[?$<\"A-Za-z_])",
        rf"\?\w+\s+({term_re})\s+(?=[?$<\"A-Za-z_])",
    ]
    for pattern in predicate_patterns:
        for match in re.finditer(pattern, body, flags=re.MULTILINE):
            token = match.group(1)
            if token in {"a", "rdf:type"}:
                continue
            add_query_term(terms, curie_to_uri(token, prefixes), token, "predicate")

    for token in re.findall(term_re, body):
        add_query_term(terms, curie_to_uri(token, prefixes), token, "term")

    return terms


def load_seed_records(path: Path) -> List[Dict[str, object]]:
    if not path.exists():
        return []
    import yaml  # type: ignore

    data = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        return []
    kgs = data.get("kgs")
    return [kg for kg in kgs if isinstance(kg, dict)] if isinstance(kgs, list) else []


def normalize_ontology_source(source: object) -> Optional[Dict[str, str]]:
    if isinstance(source, str):
        key = "local_path" if Path(source).exists() else "url"
        return {key: source}
    if not isinstance(source, dict):
        return None
    out: Dict[str, str] = {}
    for key in ("url", "local_path", "format", "role"):
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            out[key] = value.strip()
    return out if ("url" in out or "local_path" in out) else None


def load_seed_ontology_sources(seeds_path: Path) -> Dict[str, List[Dict[str, str]]]:
    by_kg: Dict[str, List[Dict[str, str]]] = {}
    for kg in load_seed_records(seeds_path):
        kg_id = kg.get("kg_id")
        raw_sources = kg.get("ontology_sources")
        if not isinstance(kg_id, str) or not isinstance(raw_sources, list):
            continue
        for raw_source in raw_sources:
            source = normalize_ontology_source(raw_source)
            if source:
                by_kg.setdefault(kg_id, []).append(source)
    return by_kg


def load_seed_datasets(seeds_path: Path) -> Dict[str, Dict[str, str]]:
    datasets: Dict[str, Dict[str, str]] = {}
    for kg in load_seed_records(seeds_path):
        kg_id = kg.get("kg_id")
        dataset = kg.get("dataset")
        if not isinstance(kg_id, str) or not isinstance(dataset, dict):
            continue
        out: Dict[str, str] = {}
        for key in ("local_path", "format", "dump_url"):
            value = dataset.get(key)
            if isinstance(value, str) and value.strip():
                out[key] = value.strip()
        if out:
            datasets[kg_id] = out
    return datasets


def load_kgs_metadata(path: Path) -> Tuple[Dict[str, List[Dict[str, str]]], Dict[str, Dict[str, str]]]:
    ontology_sources: Dict[str, List[Dict[str, str]]] = {}
    datasets: Dict[str, Dict[str, str]] = {}
    if not path.exists():
        return ontology_sources, datasets
    for kg in load_query_records(path):
        kg_id = kg.get("kg_id")
        if not isinstance(kg_id, str):
            continue
        raw_sources = kg.get("ontology_sources")
        if isinstance(raw_sources, list):
            for raw_source in raw_sources:
                source = normalize_ontology_source(raw_source)
                if source:
                    ontology_sources.setdefault(kg_id, []).append(source)
        dataset = kg.get("dataset")
        if isinstance(dataset, dict):
            out: Dict[str, str] = {}
            for key in ("local_path", "format", "dump_url"):
                value = dataset.get(key)
                if isinstance(value, str) and value.strip():
                    out[key] = value.strip()
            if out:
                datasets[kg_id] = out
    return ontology_sources, datasets


def merge_kg_maps(*maps: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for mapping in maps:
        for kg_id, value in mapping.items():
            if isinstance(value, list):
                bucket = merged.setdefault(kg_id, [])
                seen = {
                    json.dumps(item, sort_keys=True)
                    for item in bucket
                    if isinstance(item, dict)
                }
                for item in value:
                    key = json.dumps(item, sort_keys=True)
                    if key not in seen:
                        bucket.append(item)
                        seen.add(key)
            elif isinstance(value, dict):
                merged.setdefault(kg_id, {}).update(value)
    return merged


def normalize_github_blob_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc != "github.com" or "/blob/" not in parsed.path:
        return url
    parts = parsed.path.strip("/").split("/")
    if len(parts) < 5:
        return url
    owner, repo = parts[0], parts[1]
    blob_idx = parts.index("blob")
    branch = parts[blob_idx + 1]
    rest = "/".join(parts[blob_idx + 2 :])
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{rest}"


def url_without_fragment(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), parsed.params, parsed.query, ""))


def ontology_cache_path(cache_dir: Path, kg_id: str, url: str) -> Path:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    basename = Path(urlparse(url).path).name or "ontology"
    if "." not in basename:
        basename += ".rdf"
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", basename)
    return cache_dir / f"{kg_id}__{digest}__{safe}"


def fetch_ontology_source(url: str, timeout_s: int = 30) -> bytes:
    req = Request(
        url,
        headers={
            "Accept": "text/turtle, application/rdf+xml;q=0.9, application/ld+json;q=0.8, */*;q=0.1",
            "User-Agent": "kg-pipeline/0.1",
        },
    )
    with urlopen(req, timeout=timeout_s) as resp:
        return resp.read()


def infer_rdflib_format(path: Path, explicit: Optional[str] = None) -> Optional[str]:
    if explicit:
        fmt = explicit.lower()
        return {
            "ttl": "turtle",
            "turtle": "turtle",
            "rdf": "xml",
            "xml": "xml",
            "owl": "xml",
            "jsonld": "json-ld",
            "json-ld": "json-ld",
            "nt": "nt",
            "ntriples": "nt",
            "nq": "nquads",
            "nquads": "nquads",
        }.get(fmt, fmt)
    suffix = path.suffix.lower().lstrip(".")
    return infer_rdflib_format(path, suffix) if suffix else None


def parse_rdf_file(graph: object, path: Path, explicit_format: Optional[str] = None) -> bool:
    try:
        from rdflib import Graph  # type: ignore
    except Exception:
        return False
    primary = infer_rdflib_format(path, explicit_format)
    candidates: List[Optional[str]] = []
    if primary:
        candidates.append(primary)
    candidates.extend(["turtle", "xml", "json-ld", "nt", "nquads"])
    seen: Set[Optional[str]] = set()
    for fmt in candidates:
        if fmt in seen:
            continue
        seen.add(fmt)
        try:
            parsed = Graph()
            parsed.parse(str(path), format=fmt)
            graph += parsed  # type: ignore[operator]
            return True
        except Exception:
            continue
    return False


def resolve_ontology_source_path(
    kg_id: str,
    source: Dict[str, str],
    cache_dir: Path,
    allow_download: bool,
) -> Optional[Path]:
    local_path = source.get("local_path")
    if local_path:
        path = Path(local_path)
        return path if path.exists() else None
    url = source.get("url")
    if not url:
        return None
    normalized_url = normalize_github_blob_url(url_without_fragment(url))
    cache_path = ontology_cache_path(cache_dir, kg_id, normalized_url)
    if cache_path.exists():
        return cache_path
    if not allow_download:
        return None
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        cache_path.write_bytes(fetch_ontology_source(normalized_url))
    except Exception:
        return None
    return cache_path


def load_rdf_graph_from_sources(
    kg_id: str,
    sources: List[Dict[str, str]],
    cache_dir: Path,
    allow_download: bool,
) -> Tuple[Optional[object], List[str]]:
    if not sources:
        return None, []
    try:
        from rdflib import Graph  # type: ignore
    except Exception:
        return None, []

    graph = Graph()
    loaded_sources: List[str] = []
    for source in sources:
        path = resolve_ontology_source_path(kg_id, source, cache_dir, allow_download)
        if path is None:
            continue
        if not parse_rdf_file(graph, path, source.get("format")):
            continue
        loaded_sources.append(source.get("url") or source.get("local_path") or str(path))
    return (graph, loaded_sources) if loaded_sources else (None, [])


def first_literals(graph: object, subject: object, predicates: Iterable[object], limit: int = 2) -> List[str]:
    values: List[str] = []
    seen: Set[str] = set()
    for predicate in predicates:
        for obj in graph.objects(subject, predicate):  # type: ignore[attr-defined]
            text = str(obj).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            values.append(re.sub(r"\s+", " ", text))
            if len(values) >= limit:
                return values
    return values


def first_resource_labels(graph: object, resources: Iterable[object], prefixes: Dict[str, str], limit: int = 3) -> List[str]:
    try:
        from rdflib import RDFS, SKOS, URIRef  # type: ignore
    except Exception:
        return []
    values: List[str] = []
    seen: Set[str] = set()
    for resource in resources:
        if not isinstance(resource, URIRef):
            continue
        labels = first_literals(graph, resource, (RDFS.label, SKOS.prefLabel), limit=1)
        text = labels[0] if labels else display_uri(str(resource), prefixes)
        if text in seen:
            continue
        seen.add(text)
        values.append(text)
        if len(values) >= limit:
            return values
    return values


def trim_context_text(text: str, max_len: int = 180) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    return text if len(text) <= max_len else text[: max_len - 1].rstrip() + "…"


def build_ontology_context_snippet(query: str, graph: object, source_labels: List[str], max_terms: int = 8) -> str:
    try:
        from rdflib import OWL, RDF, RDFS, SKOS, URIRef  # type: ignore
    except Exception:
        return ""
    prefixes = parse_prefixes(query)
    terms = extract_query_terms(query)
    lines: List[str] = []
    for uri, info in terms.items():
        if len(lines) >= max_terms:
            break
        subject = URIRef(uri)
        if (subject, None, None) not in graph and (None, None, subject) not in graph:  # type: ignore[operator]
            continue
        name = display_uri(uri, prefixes)
        labels = first_literals(graph, subject, (RDFS.label, SKOS.prefLabel), limit=1)
        comments = first_literals(graph, subject, (RDFS.comment, SKOS.definition), limit=1)
        domains = first_resource_labels(graph, graph.objects(subject, RDFS.domain), prefixes, limit=2)  # type: ignore[attr-defined]
        ranges = first_resource_labels(graph, graph.objects(subject, RDFS.range), prefixes, limit=2)  # type: ignore[attr-defined]
        supers = first_resource_labels(graph, graph.objects(subject, RDFS.subClassOf), prefixes, limit=2)  # type: ignore[attr-defined]
        types = first_resource_labels(
            graph,
            [
                obj for obj in graph.objects(subject, RDF.type)  # type: ignore[attr-defined]
                if obj in {OWL.Class, RDF.Property, OWL.ObjectProperty, OWL.DatatypeProperty}
            ],
            prefixes,
            limit=2,
        )
        parts: List[str] = []
        if labels:
            parts.append(f"label: {trim_context_text(labels[0], 80)}")
        if comments:
            parts.append(f"definition: {trim_context_text(comments[0])}")
        if domains:
            parts.append("domain: " + ", ".join(domains))
        if ranges:
            parts.append("range: " + ", ".join(ranges))
        if supers:
            parts.append("subclass of: " + ", ".join(supers))
        if types:
            parts.append("type: " + ", ".join(types))
        if parts:
            lines.append(f"- {name}: " + "; ".join(parts))
    if not lines:
        return ""
    source_note = "; ".join(source_labels[:3])
    header = f"Ontology context from explicit seed source(s): {source_note}" if source_note else "Ontology context from explicit seed source(s)"
    return header + "\n" + "\n".join(lines)


def add_ontology_context_evidence(
    records: List[Dict[str, object]],
    ontology_sources_by_kg: Dict[str, List[Dict[str, str]]],
    cache_dir: Path,
    allow_download: bool,
    extracted_at: str,
) -> None:
    graph_cache: Dict[str, Tuple[Optional[object], List[str]]] = {}
    for rec in records:
        kg_id = rec.get("kg_id")
        query = rec.get("sparql_clean")
        if not isinstance(kg_id, str) or not isinstance(query, str):
            continue
        sources = ontology_sources_by_kg.get(kg_id, [])
        if not sources:
            continue
        if kg_id not in graph_cache:
            graph_cache[kg_id] = load_rdf_graph_from_sources(kg_id, sources, cache_dir, allow_download)
        graph, source_labels = graph_cache[kg_id]
        if graph is None:
            continue
        snippet = build_ontology_context_snippet(query, graph, source_labels)
        if not snippet:
            continue
        add_evidence(
            rec,
            "ontology_term_context",
            source_labels[0] if source_labels else "",
            "seeds.yaml:ontology_sources",
            "",
            snippet,
            extracted_at,
        )


def load_dataset_graph(dataset: Dict[str, str]) -> Optional[object]:
    local_path = dataset.get("local_path")
    if not local_path:
        return None
    path = Path(local_path)
    if not path.exists():
        return None
    try:
        from rdflib import Graph  # type: ignore
    except Exception:
        return None
    graph = Graph()
    if not parse_rdf_file(graph, path, dataset.get("format")):
        return None
    return graph


def top_counts(values: Iterable[str], limit: int = 3) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = {}
    for value in values:
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]


def build_graph_shape_context_snippet(query: str, graph: object, max_terms: int = 8, sample_limit: int = 300) -> str:
    try:
        from rdflib import RDF, RDFS, URIRef, Literal  # type: ignore
    except Exception:
        return ""
    prefixes = parse_prefixes(query)
    terms = extract_query_terms(query)
    lines: List[str] = []
    for uri, info in terms.items():
        if len(lines) >= max_terms:
            break
        roles = info.get("roles")
        role_set = roles if isinstance(roles, set) else set()
        term = URIRef(uri)
        name = display_uri(uri, prefixes)
        parts: List[str] = []
        if "class" in role_set:
            subjects = list(graph.subjects(RDF.type, term))[:sample_limit]  # type: ignore[attr-defined]
            predicate_values = []
            for subject in subjects:
                for predicate in graph.predicates(subject, None):  # type: ignore[attr-defined]
                    if predicate in {RDF.type, RDFS.label}:
                        continue
                    predicate_values.append(display_uri(str(predicate), prefixes))
            predicates = top_counts(predicate_values)
            if subjects:
                parts.append(f"{len(subjects)} sampled instances")
            if predicates:
                parts.append("common outgoing predicates: " + ", ".join(f"{p} ({c})" for p, c in predicates))
        if "predicate" in role_set or "term" in role_set:
            subject_types: List[str] = []
            object_shapes: List[str] = []
            triples_seen = 0
            for subject, obj in graph.subject_objects(term):  # type: ignore[attr-defined]
                triples_seen += 1
                for cls in graph.objects(subject, RDF.type):  # type: ignore[attr-defined]
                    subject_types.append(display_uri(str(cls), prefixes))
                if isinstance(obj, Literal):
                    object_shapes.append(f"literal:{obj.datatype or 'plain'}")
                elif isinstance(obj, URIRef):
                    obj_types = list(graph.objects(obj, RDF.type))[:2]  # type: ignore[attr-defined]
                    if obj_types:
                        object_shapes.extend(display_uri(str(cls), prefixes) for cls in obj_types)
                    else:
                        object_shapes.append("IRI")
                if triples_seen >= sample_limit:
                    break
            subj_counts = top_counts(subject_types)
            obj_counts = top_counts(object_shapes)
            if triples_seen:
                parts.append(f"{triples_seen} sampled triples")
            if subj_counts:
                parts.append("subject types: " + ", ".join(f"{v} ({c})" for v, c in subj_counts))
            if obj_counts:
                parts.append("object shapes: " + ", ".join(f"{v} ({c})" for v, c in obj_counts))
        if parts:
            lines.append(f"- {name}: " + "; ".join(parts))
    if not lines:
        return ""
    return "Observed graph shape context from local dataset\n" + "\n".join(lines)


def add_graph_shape_context_evidence(
    records: List[Dict[str, object]],
    datasets_by_kg: Dict[str, Dict[str, str]],
    extracted_at: str,
) -> None:
    graph_cache: Dict[str, Optional[object]] = {}
    for rec in records:
        kg_id = rec.get("kg_id")
        query = rec.get("sparql_clean")
        if not isinstance(kg_id, str) or not isinstance(query, str):
            continue
        dataset = datasets_by_kg.get(kg_id)
        if not dataset:
            continue
        if kg_id not in graph_cache:
            graph_cache[kg_id] = load_dataset_graph(dataset)
        graph = graph_cache[kg_id]
        if graph is None:
            continue
        snippet = build_graph_shape_context_snippet(query, graph)
        if not snippet:
            continue
        add_evidence(
            rec,
            "graph_shape_context",
            dataset.get("dump_url", ""),
            dataset.get("local_path", ""),
            "",
            snippet,
            extracted_at,
        )


def clean_md_text(text: str) -> str:
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("```"):
            continue
        if stripped.startswith("#"):
            stripped = stripped.lstrip("#").strip()
        if stripped.startswith(("-", "*")):
            stripped = stripped[1:].strip()
        stripped = re.sub(r"^\d+\.", "", stripped).strip()
        lines.append(stripped)
    return " ".join(lines).strip()


def extract_recent_text_blocks(prefix: str, limit: int = 2) -> str:
    if "<" in prefix:
        prefix = re.sub(r"<pre[^>]*>.*?</pre>", "", prefix, flags=re.DOTALL | re.IGNORECASE)
    normalized = html_to_markdownish(prefix) if "<" in prefix else prefix
    normalized = re.sub(r"```.*?```", "", normalized, flags=re.DOTALL)
    parts = [p.strip() for p in re.split(r"\n\s*\n", normalized) if p.strip()]
    if not parts:
        return ""
    return "\n".join(parts[-limit:]).strip()


def extract_md_blocks_with_desc(text: str) -> List[Dict[str, object]]:
    pattern = re.compile(r"```(?:sparql)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
    results: List[Dict[str, str]] = []
    matches = list(pattern.finditer(text))
    for match in matches:
        block = match.group(1)
        prefix = text[: match.start()]
        desc = extract_recent_text_blocks(prefix, limit=1)
        results.append({"query": block, "desc": desc, "start_idx": match.start()})
    return results


def extract_pre_blocks_with_desc(text: str) -> List[Dict[str, object]]:
    pattern = re.compile(r"<pre[^>]*>(.*?)</pre>", re.DOTALL | re.IGNORECASE)
    results: List[Dict[str, str]] = []
    matches = list(pattern.finditer(text))
    for match in matches:
        block = html.unescape(match.group(1))
        prefix = text[: match.start()]
        bullet = extract_last_bullet(prefix)
        desc = bullet or extract_recent_text_blocks(prefix, limit=2)
        results.append({"query": block, "desc": desc, "start_idx": match.start()})
    return results


def extract_preceding_comments(lines: List[str], start_idx: int) -> str:
    comments: List[str] = []
    idx = start_idx - 1
    while idx >= 0:
        line = lines[idx].strip()
        if not line:
            idx -= 1
            continue
        if line.startswith("#") or line.startswith("//"):
            comments.append(line.lstrip("#/").strip())
            idx -= 1
            continue
        if line.endswith("*/") or line.startswith("/*"):
            block_lines: List[str] = []
            while idx >= 0:
                block_line = lines[idx].strip()
                cleaned = block_line.lstrip("/*").rstrip("*/").strip()
                if cleaned:
                    block_lines.append(cleaned)
                if block_line.startswith("/*"):
                    break
                idx -= 1
            comments.extend(reversed(block_lines))
            idx -= 1
            continue
        break
    comments.reverse()
    return " ".join([c for c in comments if c]).strip()


def extract_leading_context(segment_text: str) -> str:
    lines = segment_text.splitlines()
    keyword_re = re.compile(
        r"^\s*(select|construct|ask|describe|insert|delete|with|load|clear|create|drop|copy|move|add)\b",
        re.IGNORECASE,
    )
    context_lines: List[str] = []
    in_block = False
    for line in lines:
        stripped = line.strip()
        if keyword_re.match(line):
            break
        if in_block:
            context_lines.append(line.rstrip())
            if "*/" in stripped:
                in_block = False
            continue
        if stripped.startswith("/*"):
            in_block = True
            context_lines.append(line.rstrip())
            if "*/" in stripped:
                in_block = False
            continue
        if stripped.startswith("#") or stripped.startswith("//"):
            context_lines.append(line.rstrip())
            continue
        if stripped.lower().startswith(("prefix ", "base ")):
            # Allow query-local comments interleaved with PREFIX/BASE declarations
            # until the first actual query verb.
            continue
        if not stripped:
            context_lines.append("")
            continue
        # Stop at first non-comment, non-prefix content.
        break
    return "\n".join(context_lines).strip()


def add_evidence(
    record: Dict[str, object],
    evidence_type: str,
    source_url: str,
    source_path: str,
    repo_commit: str,
    snippet: str,
    extracted_at: str,
) -> None:
    evidence = record.get("evidence")
    if not isinstance(evidence, list):
        evidence = []
    snippet = snippet.strip()
    if not snippet:
        return
    if evidence_type == "cq_item":
        snippet = clean_desc(snippet)
        if not snippet:
            return
        if re.match(r"^\s*(table|figure|algorithm)\s+\d+[:.].*competency\s+questions", snippet, re.IGNORECASE):
            return
    for ev in evidence:
        if not isinstance(ev, dict):
            continue
        if (
            ev.get("type") == evidence_type
            and ev.get("source_path") == source_path
            and ev.get("snippet") == snippet
        ):
            record["evidence"] = evidence
            return
    evidence_id = f"e{len(evidence) + 1}"
    evidence.append(
        {
            "evidence_id": evidence_id,
            "type": evidence_type,
            "source_url": source_url,
            "source_path": source_path,
            "repo_commit": repo_commit,
            "snippet": snippet,
            "extracted_at": extracted_at,
            "extractor_version": "enrich_evidence.py@v1",
        }
    )
    record["evidence"] = evidence


def parse_source_file(path: Path) -> Tuple[str, str, str]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    if text.startswith("SOURCE:"):
        parts = text.split("\n\n", 1)
        header = parts[0].strip()
        body = parts[1] if len(parts) > 1 else ""
        url = header.replace("SOURCE:", "").strip()
        return url, normalize_source_text(body), body
    return "", normalize_source_text(text), text


def extract_text_from_pdf(path: Path) -> str:
    try:
        reader = PdfReader(str(path))
    except Exception:
        return ""
    parts: List[str] = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            continue
    return "\n".join(parts)


def extract_pdf_captions(text: str) -> List[str]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    captions: List[str] = []
    caption_re = re.compile(r"^(figure|fig\.?|table|tab\.)\s*\d+", re.IGNORECASE)
    for i, line in enumerate(lines):
        if not caption_re.match(line):
            continue
        # Heuristic: caption lines are short and often end with '.' or ':'.
        if len(line.split()) < 3:
            continue
        # Capture this line plus up to 2 following lines unless they look like body text.
        collected = [line]
        for j in range(i + 1, min(i + 4, len(lines))):
            nxt = lines[j]
            if caption_re.match(nxt):
                break
            if len(nxt.split()) > 30:
                break
            collected.append(nxt)
        captions.append(" ".join(collected).strip())
    return captions


def extract_pdf_cq_captions(text: str) -> List[str]:
    # Capture captions even if line breaks split them.
    candidates: List[str] = []
    pattern = re.compile(
        r"(table\s*\d+\s*[:\-]?[\s\S]{0,80}competency\s+questions[^\n]{0,80})",
        re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        snippet = " ".join(match.group(1).split())
        candidates.append(snippet)
    return candidates


def extract_pdf_tables_for_captions(text: str, captions: List[str]) -> List[str]:
    if not text.strip() or not captions:
        return []
    lines = text.splitlines()
    line_count = len(lines)
    if line_count == 0:
        return []
    caption_set = {cap.strip() for cap in captions if cap.strip()}
    if not caption_set:
        return []
    matchers: List[Tuple[str, str, Optional[str]]] = []
    for cap in caption_set:
        match = re.search(r"(table|figure)\s*(\d+)", cap, re.IGNORECASE)
        if match:
            matchers.append(("num", match.group(1).lower(), match.group(2)))
        else:
            matchers.append(("prefix", cap.lower()[:60], None))
    if not matchers:
        return []
    blocks: List[str] = []
    caption_re = re.compile(r"^\s*(table|figure)\s+\d+[:.]", re.IGNORECASE)
    section_heading_re = re.compile(r"^\s*\d+(?:\.\d+)*\.\s+\w")
    max_lines = 60
    for idx, line in enumerate(lines):
        line_stripped = line.strip()
        if not line_stripped:
            continue
        line_lower = line_stripped.lower()
        matched = False
        for kind, token, num in matchers:
            if kind == "num":
                if re.search(rf"{re.escape(token)}\s*{re.escape(num)}\s*[:.]", line_lower):
                    matched = True
                    break
            else:
                if token and token in line_lower:
                    matched = True
                    break
        if not matched:
            continue
        start = idx
        collected: List[str] = []
        blank_run = 0
        row_count = 0
        for j in range(start, min(line_count, start + max_lines)):
            cur = lines[j].rstrip()
            if not cur.strip():
                blank_run += 1
            else:
                blank_run = 0
            if j > start and caption_re.match(cur):
                break
            if j > start and section_heading_re.match(cur):
                break
            if j > start and row_count >= 4:
                stripped = cur.strip()
                if (
                    len(stripped) > 80
                    and stripped[:1].isalpha()
                    and "." in stripped
                ):
                    break
                if (
                    stripped[:1].isalpha()
                    and not re.search(r"\d", stripped)
                    and len(stripped.split()) >= 6
                ):
                    break
            if blank_run >= 2:
                break
            if cur.strip() and cur.lstrip()[:1].isdigit():
                row_count += 1
            collected.append(cur)
        block = "\n".join(collected).strip()
        if block:
            blocks.append(block)
    return blocks


def extract_nearest_caption(lines: List[str], start_idx: int) -> Optional[str]:
    if start_idx <= 0:
        return None
    caption_re = re.compile(r"^\s*(table|figure|algorithm)\s+\d+[:.]", re.IGNORECASE)
    for i in range(start_idx - 1, max(-1, start_idx - 40), -1):
        line = lines[i].strip()
        if not line:
            continue
        if caption_re.match(line):
            return line
    return None


def has_blank_between(lines: List[str], start_idx: int, caption_idx: int) -> bool:
    if caption_idx >= start_idx:
        return False
    for i in range(caption_idx + 1, start_idx):
        if not lines[i].strip():
            return True
    return False


def extract_pdf_paragraphs(lines: List[str], start_idx: int, max_paragraphs: int = 2) -> Optional[str]:
    if start_idx <= 0:
        return None
    paragraphs: List[str] = []
    current: List[str] = []
    i = start_idx - 1
    while i >= 0 and len(paragraphs) < max_paragraphs:
        line = lines[i].strip()
        if not line:
            if current:
                paragraphs.append(" ".join(reversed(current)).strip())
                current = []
            i -= 1
            continue
        current.append(line)
        i -= 1
    if current and len(paragraphs) < max_paragraphs:
        paragraphs.append(" ".join(reversed(current)).strip())
    if not paragraphs:
        # Fallback: capture up to two non-empty lines above.
        fallback: List[str] = []
        i = start_idx - 1
        while i >= 0 and len(fallback) < 2:
            line = lines[i].strip()
            if line:
                fallback.append(line)
            i -= 1
        if not fallback:
            return None
        fallback.reverse()
        return "\n".join(fallback).strip()
    paragraphs.reverse()
    return "\n\n".join(paragraphs).strip()


def extract_nearest_cq_line(lines: List[str], start_idx: int) -> Optional[str]:
    cq_re = re.compile(r"\bCQ\d+\b", re.IGNORECASE)
    code_re = re.compile(r"\b(select|construct|ask|describe|where|prefix)\b", re.IGNORECASE)
    for i in range(start_idx - 1, max(-1, start_idx - 20), -1):
        line = lines[i].strip()
        if not line:
            continue
        if code_re.search(line):
            continue
        if cq_re.search(line) or line.endswith("?"):
            prev = None
            j = i - 1
            while j >= 0:
                prev_line = lines[j].strip()
                if not prev_line:
                    break
                if code_re.search(prev_line):
                    break
                prev = prev_line
                break
            if prev and (cq_re.search(prev) or line[:1].islower() or not cq_re.search(line)):
                return f"{prev} {line}".strip()
            return line
    return None


def extract_pdf_code_blocks(text: str) -> List[Dict[str, object]]:
    if not text.strip():
        return []
    lines = text.splitlines()
    blocks: List[Dict[str, object]] = []
    in_block = False
    current: List[str] = []
    start_idx = 0
    char_idx = 0
    start_char = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        is_code_line = bool(
            re.search(r"\b(select|construct|ask|describe|where|prefix)\b", stripped, re.IGNORECASE)
            or "{ " in stripped
            or stripped.startswith("{")
            or stripped.startswith("PREFIX")
        )
        if is_code_line:
            if not in_block:
                in_block = True
                start_idx = i
                start_char = char_idx
                current = []
            current.append(line.rstrip())
            char_idx += len(line) + 1
            continue
        if in_block:
            if len(current) >= 3:
                blocks.append({"start_idx": start_idx, "start_char": start_char, "block": "\n".join(current).strip()})
            in_block = False
            current = []
        char_idx += len(line) + 1
    if in_block and len(current) >= 3:
        blocks.append({"start_idx": start_idx, "start_char": start_char, "block": "\n".join(current).strip()})
    return blocks


def extract_pdf_query_blocks(text: str) -> List[Dict[str, object]]:
    query_start_re = re.compile(r"^\s*(prefix|base|select|construct|ask|describe)\b", re.IGNORECASE)
    query_line_re = re.compile(
        r"^\s*(prefix|base|select|construct|ask|describe|where|from|graph|optional|union|filter|bind|values|service|minus|group\s+by|order\s+by|limit|offset|having)\b",
        re.IGNORECASE,
    )
    caption_re = re.compile(r"^\s*(figure|fig\.|table|algorithm)\s+\d+\s*[:.]", re.IGNORECASE)
    cq_heading_re = re.compile(r"^\s*CQ\d+\b", re.IGNORECASE)
    page_num_re = re.compile(r"^\s*\d+\s*$")

    def is_query_continuation_line(stripped: str) -> bool:
        if not stripped:
            return True
        if stripped.startswith("#"):
            return True
        if stripped in {".", ";", ","}:
            return True
        if query_line_re.match(stripped):
            return True
        if stripped.startswith("<"):
            return True
        if stripped.startswith(("?", "{", "}", "(", "[", "]")):
            return True
        if re.match(r"^\s*[A-Za-z_][\w-]*:[^\s]*", stripped):
            return True
        return False

    lines = text.splitlines()
    blocks: List[Dict[str, object]] = []
    current: List[str] = []
    in_block = False
    depth = 0
    seen_query = False
    start_idx = 0
    start_char = 0
    char_idx = 0
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            if in_block:
                current.append("")
            char_idx += len(line) + 1
            continue
        if in_block and page_num_re.match(stripped):
            char_idx += len(line) + 1
            continue
        if in_block and (caption_re.match(stripped) or cq_heading_re.match(stripped)):
            if seen_query and depth <= 0 and current:
                blocks.append({"start_idx": start_idx, "start_char": start_char, "lines": current[:]})
            current = []
            in_block = False
            depth = 0
            seen_query = False
            char_idx += len(line) + 1
            continue
        if query_start_re.match(stripped):
            if in_block and seen_query and depth <= 0 and current:
                blocks.append({"start_idx": start_idx, "start_char": start_char, "lines": current[:]})
                current = []
                depth = 0
                seen_query = False
            if not in_block:
                in_block = True
                current = []
                depth = 0
                seen_query = False
                start_idx = idx
                start_char = char_idx
            current.append(line.rstrip())
            if re.match(r"^\s*(select|construct|ask|describe)\b", stripped, re.IGNORECASE):
                seen_query = True
            depth += line.count("{") - line.count("}")
            char_idx += len(line) + 1
            continue
        if in_block and is_query_continuation_line(stripped):
            current.append(line.rstrip())
            if re.match(r"^\s*(select|construct|ask|describe)\b", stripped, re.IGNORECASE):
                seen_query = True
            depth += line.count("{") - line.count("}")
            char_idx += len(line) + 1
            continue
        if in_block:
            if seen_query and depth <= 0 and current:
                blocks.append({"start_idx": start_idx, "start_char": start_char, "lines": current[:]})
            current = []
            in_block = False
            depth = 0
            seen_query = False
        char_idx += len(line) + 1
    if in_block and current:
        blocks.append({"start_idx": start_idx, "start_char": start_char, "lines": current[:]})

    # Normalize PREFIX lines / broken IRIs like extract_queries_from_pdf_text.
    normalized: List[Dict[str, object]] = []
    for block in blocks:
        merged: List[str] = []
        lines_block = block["lines"]
        i = 0
        while i < len(lines_block):
            line = lines_block[i].rstrip()
            if (
                line.strip().lower().startswith("prefix")
                and i + 1 < len(lines_block)
                and lines_block[i + 1].strip().startswith("<")
            ):
                line = f"{line} {lines_block[i + 1].strip()}"
                i += 2
                merged.append(line)
                continue
            if line.strip().endswith(":") and i + 1 < len(lines_block) and lines_block[i + 1].strip().startswith("<"):
                line = f"{line} {lines_block[i + 1].strip()}"
                i += 2
                merged.append(line)
                continue
            if "<" in line and ">" not in line and i + 1 < len(lines_block):
                next_line = lines_block[i + 1].strip()
                if next_line and not re.match(r"^\s*(prefix|select|construct|ask|describe)\b", next_line, re.IGNORECASE):
                    line = f"{line}{next_line}"
                    i += 2
                    merged.append(line)
                    continue
            merged.append(line)
            i += 1
        normalized.append({"start_idx": block["start_idx"], "block": "\n".join(merged).strip()})
    return normalized


def extract_pdf_cq_bullets(text: str) -> List[str]:
    lines = text.splitlines()
    bullets: List[str] = []
    header_re = re.compile(r"competency\\s+questions?", re.IGNORECASE)
    bullet_re = re.compile(r"^\\s*(?:[-*•]|\\d+\\.)\\s+")
    for i, line in enumerate(lines):
        if not header_re.search(line):
            continue
        # Look ahead for bullet list items.
        collected: List[str] = []
        for j in range(i + 1, min(i + 30, len(lines))):
            cur = lines[j].rstrip()
            if bullet_re.match(cur):
                collected.append(cur.strip())
                continue
            if collected and cur.strip() == "":
                break
            if collected and not bullet_re.match(cur):
                break
        if collected:
            bullets.append("\n".join(collected).strip())
    return bullets


def normalize_source_text(text: str) -> str:
    if "<html" in text.lower() or "markdown-body" in text.lower():
        return html_to_markdownish(text)
    return text


def html_to_markdownish(text: str) -> str:
    def extract_markdown_div(html_text: str) -> Optional[str]:
        start_match = re.search(
            r'<div[^>]*class="[^"]*markdown-body[^"]*"[^>]*>',
            html_text,
            re.IGNORECASE,
        )
        if not start_match:
            return None
        start_idx = start_match.end()
        depth = 1
        idx = start_idx
        div_open = re.compile(r"<div[^>]*>", re.IGNORECASE)
        div_close = re.compile(r"</div>", re.IGNORECASE)
        while idx < len(html_text):
            next_open = div_open.search(html_text, idx)
            next_close = div_close.search(html_text, idx)
            if not next_close:
                break
            if next_open and next_open.start() < next_close.start():
                depth += 1
                idx = next_open.end()
            else:
                depth -= 1
                idx = next_close.end()
                if depth == 0:
                    return html_text[start_idx:next_close.start()]
        return None

    match = re.search(r'<article class="markdown-body[^"]*">(.*?)</article>', text, re.DOTALL | re.IGNORECASE)
    if match:
        body = match.group(1)
    else:
        match = re.search(r'<article[^>]*itemprop="text"[^>]*>(.*?)</article>', text, re.DOTALL | re.IGNORECASE)
        if match:
            body = match.group(1)
        else:
            md_div = extract_markdown_div(text)
            if md_div:
                body = md_div
            else:
                match = re.search(r'<div[^>]*id="readme"[^>]*>(.*?)</div>', text, re.DOTALL | re.IGNORECASE)
                body = match.group(1) if match else text

    # Drop scripts/styles to reduce noise.
    body = re.sub(r"<script[^>]*>.*?</script>", "", body, flags=re.DOTALL | re.IGNORECASE)
    body = re.sub(r"<style[^>]*>.*?</style>", "", body, flags=re.DOTALL | re.IGNORECASE)

    def strip_tags(s: str) -> str:
        return html.unescape(re.sub(r"<[^>]+>", "", s))

    # Convert tables to markdown-like rows.
    def convert_tables(s: str) -> str:
        def row_to_md(row_html: str) -> str:
            cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row_html, re.DOTALL | re.IGNORECASE)
            if not cells:
                return ""
            cell_text = [strip_tags(c).strip() for c in cells]
            return "| " + " | ".join(cell_text) + " |"

        def table_repl(match_obj: re.Match) -> str:
            table_html = match_obj.group(1)
            rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL | re.IGNORECASE)
            md_rows = [row_to_md(r) for r in rows]
            md_rows = [r for r in md_rows if r]
            return "\n".join(md_rows) + "\n\n" if md_rows else ""

        return re.sub(r"<table[^>]*>(.*?)</table>", table_repl, s, flags=re.DOTALL | re.IGNORECASE)

    body = convert_tables(body)
    body = re.sub(r"<pre[^>]*><code[^>]*>", "```\n", body, flags=re.DOTALL | re.IGNORECASE)
    body = re.sub(r"</code></pre>", "\n```", body, flags=re.DOTALL | re.IGNORECASE)
    body = re.sub(r"<br\\s*/?>", "\n", body, flags=re.IGNORECASE)
    body = re.sub(r"</p>", "\n\n", body, flags=re.IGNORECASE)
    body = re.sub(r"<p[^>]*>", "", body, flags=re.IGNORECASE)

    for level in range(6, 0, -1):
        pattern = re.compile(rf"<h{level}[^>]*>(.*?)</h{level}>", re.DOTALL | re.IGNORECASE)
        body = pattern.sub(lambda m: "\n" + ("#" * level) + " " + strip_tags(m.group(1)).strip() + "\n", body)

    body = re.sub(r"<li[^>]*>(.*?)</li>", lambda m: "- " + strip_tags(m.group(1)).strip() + "\n", body, flags=re.DOTALL | re.IGNORECASE)
    body = re.sub(r"<[^>]+>", "", body)
    body = html.unescape(body)
    return body


def extract_cq_section(text: str) -> List[str]:
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if "competency question" in line.lower() or line.strip().lower().startswith("cq"):
            # capture a small subsection
            snippet_lines: List[str] = []
            for j in range(idx, min(idx + 30, len(lines))):
                snippet_lines.append(lines[j])
                if lines[j].strip().startswith("#") and j > idx:
                    break
            snippet = clean_md_text("\n".join(snippet_lines))
            if not snippet:
                return []
            return extract_cq_items_from_text(snippet)
    return []


def extract_bullet_items(lines: List[str]) -> List[str]:
    items: List[str] = []
    current: List[str] = []
    bullet_re = re.compile(r"^\s*[-*•]\s+(.*)")
    for line in lines:
        match = bullet_re.match(line)
        if match:
            if current:
                items.append(" ".join(current).strip())
                current = []
            current.append(match.group(1).strip())
            continue
        if current and line.strip():
            current.append(line.strip())
            continue
        if current:
            items.append(" ".join(current).strip())
            current = []
    if current:
        items.append(" ".join(current).strip())
    return [item for item in items if item]


def extract_cq_items_from_text(text: str) -> List[str]:
    items: List[str] = []
    label_items = extract_label_blocks(text)
    if label_items:
        items.extend(label_items)
        return items
    table_items = extract_table_blocks(text)
    if table_items:
        items.extend(table_items)
        return items
    bullet_items = extract_bullet_items(text.splitlines())
    if bullet_items:
        items.extend(bullet_items)
        return items
    for line in text.splitlines():
        if "question" in line.lower() or line.strip().lower().startswith("cq"):
            line = line.strip()
            items.extend(split_multi_cq_line(line))
    split_items: List[str] = []
    for item in items:
        split_items.extend(split_numbered_sequence(item))
    return [item for item in split_items if item]


def extract_heading_bullets(text: str) -> List[str]:
    lines = text.splitlines()
    heading_re = re.compile(r"^\s*#{1,6}\s+")
    keywords = ("competency question", "competency questions", "cqs", "questions")
    results: List[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        lower = line.strip().lower()
        if heading_re.match(line) and any(k in lower for k in keywords):
            i += 1
            block: List[str] = []
            while i < len(lines):
                if heading_re.match(lines[i]):
                    break
                block.append(lines[i].rstrip())
                i += 1
            if block:
                results.extend(extract_bullet_items(block))
        else:
            i += 1
    return results


def extract_label_blocks(text: str) -> List[str]:
    lines = text.splitlines()
    label_re = re.compile(r"^\s*[A-Z]{2,3}\d+\.\s+")
    blocks: List[str] = []
    i = 0
    while i < len(lines):
        if label_re.match(lines[i]):
            start = i
            i += 1
            while i < len(lines):
                if label_re.match(lines[i]) or lines[i].strip().startswith("```") or lines[i].strip().startswith("<pre>"):
                    break
                if lines[i].strip().startswith("#"):
                    break
                i += 1
            block = "\n".join([ln.rstrip() for ln in lines[start:i] if ln.strip()]).strip()
            if block:
                blocks.append(block)
        else:
            i += 1
    return blocks


def extract_table_blocks(text: str) -> List[str]:
    lines = text.splitlines()
    blocks: List[str] = []
    i = 0
    while i < len(lines) - 1:
        if "|" in lines[i] and "|" in lines[i + 1]:
            header = [c.strip().lower() for c in lines[i].strip("|").split("|")]
            if any("question" in h or "cq" in h or "competency" in h for h in header):
                i += 2
                rows: List[str] = []
                while i < len(lines) and "|" in lines[i]:
                    row = [c.strip() for c in lines[i].strip("|").split("|")]
                    if any(row):
                        rows.append(" | ".join([c for c in row if c]))
                    i += 1
                for row in rows:
                    if row.strip():
                        blocks.append(row.strip())
                continue
        i += 1
    return blocks


def extract_cq_block(text: str) -> List[str]:
    pattern = re.compile(
        r"(#{1,6}\s+.*competency question.*?)(?=\n\s*#{1,6}\s+|\Z)",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(text)
    if not match:
        return []
    block = match.group(1)
    lines = block.splitlines()[1:]
    cleaned = "\n".join([ln.rstrip() for ln in lines if ln.strip()]).strip()
    if not cleaned:
        return []
    return extract_cq_items_from_text(cleaned)


def split_cq_block_items(block: str) -> List[str]:
    lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
    if not lines:
        return []
    if any("|" in ln for ln in lines):
        items: List[str] = []
        for ln in lines:
            if "|" not in ln:
                continue
            row = [c.strip() for c in ln.strip("|").split("|") if c.strip()]
            if row:
                items.append(" | ".join(row))
        split_items: List[str] = []
        for item in items:
            split_items.extend(split_numbered_sequence(item))
        return [item for item in split_items if item]
    label_re = re.compile(r"^(?:[A-Z]{2,3}\d+|CQ\d+|CT\d+|DR\d+|\d+)\b", re.IGNORECASE)
    items: List[str] = []
    current: List[str] = []
    for ln in lines:
        if label_re.match(ln):
            if current:
                items.append(" ".join(current).strip())
                current = []
            current.append(ln)
            continue
        if current:
            current.append(ln)
            continue
        items.append(ln)
    if current:
        items.append(" ".join(current).strip())
    split_items: List[str] = []
    for item in items:
        split_items.extend(split_numbered_sequence(item))
    return [item for item in split_items if item]


def split_numbered_sequence(text: str) -> List[str]:
    line = text.strip()
    if not line:
        return []
    matches = list(re.finditer(r"\b\d+\s+\w", line))
    if len(matches) < 2:
        return [line]
    parts: List[str] = []
    starts = [m.start() for m in matches]
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(line)
        part = line[start:end].strip()
        if part:
            parts.append(part)
    return parts if parts else [line]


def split_multi_cq_line(line: str) -> List[str]:
    if not line:
        return []
    if len(re.findall(r"\bCQ\d+\b", line, flags=re.IGNORECASE)) < 2:
        return [line]
    parts = re.findall(r"(CQ\d+.*?)(?=\bCQ\d+\b|$)", line, flags=re.IGNORECASE)
    cleaned: List[str] = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if "?" in part:
            part = part[: part.rfind("?") + 1].strip()
        cleaned.append(part)
    return cleaned if cleaned else [line]


def extract_context_for_code(text: str, start_idx: int) -> Optional[str]:
    lines = text.splitlines()
    if start_idx > 0:
        start_idx = len(text[:start_idx].splitlines()) - 1
    if start_idx >= len(lines):
        start_idx = len(lines) - 1
    if start_idx < 0:
        return None
    label_re = re.compile(r"^\s*[A-Z]{2,3}\d+\.\s+")
    codeish_re = re.compile(
        r"^\s*(prefix|select|construct|ask|describe|where|filter|optional|bind|values)\b",
        re.IGNORECASE,
    )
    # Find nearest label above.
    label_idx = None
    i = start_idx - 1
    while i >= 0:
        if label_re.match(lines[i]):
            label_idx = i
            break
        if lines[i].strip().startswith("#"):
            break
        i -= 1
    if label_idx is not None:
        block = "\n".join([ln.rstrip() for ln in lines[label_idx:start_idx] if ln.strip()]).strip()
        return block if block else None
    # Fallback: grab up to 2 preceding non-empty paragraphs/bullets.
    collected: List[str] = []
    i = start_idx - 1
    while i >= 0 and len(collected) < 2:
        stripped = lines[i].strip()
        if not stripped:
            i -= 1
            continue
        if (
            stripped.startswith("```")
            or stripped in {"{", "}", ";", ".", "```"}
            or codeish_re.match(stripped)
            or stripped.startswith(("?", "$"))
            or (":" in stripped and stripped.endswith((".", ";", "}")))
        ):
            i -= 1
            continue
        if stripped.startswith(("-", "*")):
            collected.append(stripped)
            i -= 1
            continue
        # paragraph line
        collected.append(stripped)
        i -= 1
    if collected:
        collected.reverse()
        return "\n".join(collected).strip()
    return None


def is_probable_sparql_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped.startswith("PREFIX "):
        return True
    if re.match(r"^(SELECT|CONSTRUCT|ASK|DESCRIBE)\b", stripped):
        return True
    if re.match(r"^WHERE(?:\s|\{|$)", stripped):
        return True
    if re.match(r"^FILTER(?:\s|\(|$)", stripped):
        return True
    if stripped.startswith(("?", "<")):
        return True
    return False


def clean_desc(text: str) -> str:
    lines = []
    seen = set()
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if is_probable_sparql_line(stripped):
            continue
        if stripped.startswith("```") or stripped == "```":
            continue
        if stripped in {"{", "}", "};", ";"}:
            continue
        if "<" in stripped or ">" in stripped:
            continue
        if stripped.startswith("{") or stripped.endswith("}"):
            continue
        if stripped in seen:
            continue
        seen.add(stripped)
        lines.append(stripped)
    return "\n".join(lines).strip()


def extract_last_bullet(prefix: str) -> str:
    normalized = html_to_markdownish(prefix) if "<" in prefix else prefix
    normalized = re.sub(r"```.*?```", "", normalized, flags=re.DOTALL)
    lines = [ln.strip() for ln in normalized.splitlines() if ln.strip()]
    for line in reversed(lines):
        if line.startswith("- "):
            return line[2:].strip()
    return ""


def query_has_repo_evidence(rec: Dict[str, object]) -> bool:
    for ev in rec.get("evidence", []) or []:
        if isinstance(ev, dict) and ev.get("type") in {"repo_file", "md_fence", "md_pre"}:
            return True
    return False


def query_has_pdf_evidence(rec: Dict[str, object], pdf_path: Path) -> bool:
    evidence = rec.get("evidence")
    if not isinstance(evidence, list):
        return False
    for ev in evidence:
        if not isinstance(ev, dict):
            continue
        if ev.get("source_path") != str(pdf_path):
            continue
        if ev.get("type") in {"doc_pre", "doc_fence", "doc_pdf"}:
            return True
    return False


def normalize_query_signature(text: str) -> str:
    return re.sub(r"\s+", "", text).lower()


def query_has_doc_evidence(rec: Dict[str, object]) -> bool:
    for ev in rec.get("evidence", []) or []:
        if isinstance(ev, dict) and ev.get("type") in {"doc_pre", "doc_fence"}:
            return True
    return False


def rank_llm_context(evidence: List[Dict[str, object]]) -> List[Dict[str, object]]:
    priority_groups = [
        {"query_comment"},
        {"doc_query_desc", "web_query_desc", "readme_query_desc"},
        {"cq_item"},
        {"ontology_term_context"},
        {"graph_shape_context"},
        {"kg_summary", "doc_summary", "readme_summary", "web_summary", "repo_summary"},
    ]
    type_rank: Dict[str, int] = {}
    for idx, group in enumerate(priority_groups):
        for t in group:
            type_rank[t] = idx
    ranked: List[Dict[str, object]] = []
    for pos, ev in enumerate(evidence):
        ev_type = ev.get("type") if isinstance(ev, dict) else None
        rank = type_rank.get(ev_type, len(priority_groups))
        ranked.append((rank, pos, ev))
    ranked.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in ranked]


def infer_query_origin(evidence: List[Dict[str, object]]) -> str:
    for ev in evidence:
        if not isinstance(ev, dict):
            continue
        if ev.get("type") in {"repo_file", "md_fence", "md_pre"}:
            return "repo"
        if ev.get("type") in {"doc_pre", "doc_fence"}:
            return "doc"
    return "unknown"


def dedupe_evidence(evidence: List[Dict[str, object]]) -> List[Dict[str, object]]:
    seen = set()
    deduped: List[Dict[str, object]] = []
    for ev in evidence:
        if not isinstance(ev, dict):
            continue
        snippet = ev.get("snippet") or ""
        if not isinstance(snippet, str) or not snippet.strip():
            continue
        if ev.get("type") == "cq_item":
            cleaned = clean_desc(snippet)
            if not cleaned:
                continue
            if re.match(r"^\s*(table|figure|algorithm)\s+\d+[:.].*competency\s+questions", cleaned, re.IGNORECASE):
                continue
            ev = {**ev, "snippet": cleaned}
        if ev.get("type") in {"doc_query_desc", "web_query_desc", "readme_query_desc"}:
            bullet = extract_last_bullet(snippet)
            cleaned = clean_desc(bullet or snippet)
            if not cleaned:
                continue
            ev = {**ev, "snippet": cleaned}
        key = (ev.get("type"), ev.get("source_path"), ev.get("snippet"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(ev)
    return deduped


def expand_cq_items(evidence: List[Dict[str, object]]) -> List[Dict[str, object]]:
    expanded: List[Dict[str, object]] = []
    for ev in evidence:
        if not isinstance(ev, dict) or ev.get("type") != "cq_item":
            expanded.append(ev)
            continue
        snippet = ev.get("snippet") or ""
        if not isinstance(snippet, str) or not snippet.strip():
            continue
        parts = split_multi_cq_line(snippet)
        split_items: List[str] = []
        for part in parts:
            split_items.extend(split_numbered_sequence(part))
        if len(split_items) <= 1:
            expanded.append(ev)
            continue
        for part in split_items:
            if not part.strip():
                continue
            new_ev = dict(ev)
            new_ev["snippet"] = part.strip()
            expanded.append(new_ev)
    return expanded


def renumber_evidence(evidence: List[Dict[str, object]]) -> List[Dict[str, object]]:
    renumbered: List[Dict[str, object]] = []
    for idx, ev in enumerate(evidence, start=1):
        if not isinstance(ev, dict):
            continue
        ev = dict(ev)
        ev["evidence_id"] = f"e{idx}"
        renumbered.append(ev)
    return renumbered


def append_unique_source(kg_sources: Dict[str, List[str]], kg_id: str, source_path: str) -> None:
    bucket = kg_sources.setdefault(kg_id, [])
    if source_path not in bucket:
        bucket.append(source_path)


def ensure_missing_source_query_descs(
    by_kg_hash: Dict[tuple[str, str], Dict[str, object]],
    kg_sources: Dict[str, List[str]],
    extracted_at: str,
) -> None:
    for kg_id, source_files in kg_sources.items():
        for src_file in source_files:
            src_path = Path(src_file)
            if not src_path.is_absolute():
                if src_path.exists():
                    src_path = src_path
                elif src_path.parts and src_path.parts[0] in {"kg_sources", "curated_sources"}:
                    src_path = src_path
                else:
                    src_path = Path("kg_sources") / src_path
            if not src_path.exists() or src_path.suffix.lower() == ".pdf":
                continue
            if "api-github-com" in str(src_path):
                continue
            source_url, body, raw_body = parse_source_file(src_path)
            if not body.strip():
                continue
            blocks = extract_md_blocks_with_desc(body) + extract_pre_blocks_with_desc(raw_body)
            for block in blocks:
                desc = block.get("desc", "")
                context = extract_context_for_code(body, int(block.get("start_idx", 0)))
                if context and not desc:
                    desc = context
                cleaned_desc = clean_desc(desc)
                if not cleaned_desc:
                    continue
                for segment in split_queries_with_starts(block["query"]):
                    normalized = normalize_query(segment["query"])
                    if not normalized:
                        continue
                    q_hash = sha256_hash(normalized)
                    target = by_kg_hash.get((kg_id, q_hash))
                    if target is None:
                        continue
                    evidence = target.get("evidence", []) or []
                    if any(
                        isinstance(ev, dict)
                        and ev.get("type") in {"web_query_desc", "doc_query_desc", "readme_query_desc"}
                        and ev.get("source_path") == str(src_path)
                        for ev in evidence
                    ):
                        continue
                    add_evidence(
                        target,
                        "web_query_desc",
                        source_url or "",
                        str(src_path),
                        "",
                        cleaned_desc,
                        extracted_at,
                    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich kg_queries.jsonl with deterministic evidence.")
    parser.add_argument("--queries", default="kg_queries.jsonl")
    parser.add_argument("--repos-dir", default="repos")
    parser.add_argument("--sources-dir", default="kg_sources")
    parser.add_argument("--pdfs-dir", default="pdfs")
    parser.add_argument("--kgs", default="kgs.jsonl")
    parser.add_argument("--seeds", default="seeds.yaml")
    parser.add_argument(
        "--include-ontology-context",
        action="store_true",
        help="Add query-scoped ontology term context from ontology_sources in seeds.yaml/kgs.jsonl.",
    )
    parser.add_argument(
        "--include-graph-shape-context",
        action="store_true",
        help="Add observed graph shape context from local dataset dumps.",
    )
    parser.add_argument(
        "--download-ontology-sources",
        action="store_true",
        help="Download missing remote ontology_sources into --ontology-cache-dir.",
    )
    parser.add_argument("--ontology-cache-dir", default="ontology_sources")
    args = parser.parse_args()

    queries_path = Path(args.queries)
    repos_dir = Path(args.repos_dir)
    sources_dir = Path(args.sources_dir)
    pdfs_dir = Path(args.pdfs_dir)
    kgs_path = Path(args.kgs)
    seeds_path = Path(args.seeds)

    records = load_query_records(queries_path)
    kgs_ontology_sources, kgs_datasets = load_kgs_metadata(kgs_path)
    seed_ontology_sources = load_seed_ontology_sources(seeds_path)
    seed_datasets = load_seed_datasets(seeds_path)
    ontology_sources_by_kg = merge_kg_maps(kgs_ontology_sources, seed_ontology_sources)
    datasets_by_kg = merge_kg_maps(kgs_datasets, seed_datasets)
    by_kg_hash: Dict[tuple[str, str], Dict[str, object]] = {}
    for rec in records:
        kg_id = rec.get("kg_id")
        sparql_hash = rec.get("sparql_hash")
        if isinstance(kg_id, str) and isinstance(sparql_hash, str):
            by_kg_hash[(kg_id, sparql_hash)] = rec
        raw_hash = rec.get("raw_hash")
        if isinstance(kg_id, str) and isinstance(raw_hash, str):
            by_kg_hash[(kg_id, raw_hash)] = rec

    extracted_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    kg_repos: Dict[str, List[str]] = {}
    before_counts: Dict[str, int] = {}
    for rec in records:
        kg_id = rec.get("kg_id")
        if not isinstance(kg_id, str):
            continue
        evidence = rec.get("evidence")
        if not isinstance(evidence, list):
            continue
        before_counts[kg_id] = before_counts.get(kg_id, 0) + len(evidence)

    for rec in records:
        kg_id = rec.get("kg_id")
        if not isinstance(kg_id, str):
            continue
        evidence = rec.get("evidence")
        if not isinstance(evidence, list):
            continue
        for ev in evidence:
            if not isinstance(ev, dict):
                continue
            repo_url = ev.get("source_url")
            if ev.get("type") in {"repo_file", "md_fence", "md_pre"} and isinstance(repo_url, str):
                kg_repos.setdefault(kg_id, [])
                if repo_url not in kg_repos[kg_id]:
                    kg_repos[kg_id].append(repo_url)

    # Map KG -> source files from kgs.jsonl (if present).
    kg_sources: Dict[str, List[str]] = {}
    if kgs_path.exists():
        for kg in load_query_records(kgs_path):
            kg_id = kg.get("kg_id")
            source_files = kg.get("source_files")
            if isinstance(kg_id, str) and isinstance(source_files, list):
                for s in source_files:
                    if isinstance(s, str):
                        append_unique_source(kg_sources, kg_id, s)
            docs = kg.get("docs")
            if isinstance(kg_id, str) and isinstance(docs, list):
                for doc in docs:
                    if not isinstance(doc, str):
                        continue
                    doc_path = Path(doc)
                    if doc_path.exists():
                        append_unique_source(kg_sources, kg_id, str(doc_path))

    for rec in records:
        kg_id = rec.get("kg_id")
        if not isinstance(kg_id, str):
            continue
        evidence = rec.get("evidence")
        if not isinstance(evidence, list):
            continue
        repo_evidence = [
            e for e in evidence
            if isinstance(e, dict)
            and e.get("type") in {"repo_file", "md_fence"}
            and isinstance(e.get("source_url"), str)
        ]
        for ev in repo_evidence:
            repo_url = ev.get("source_url")
            source_path = ev.get("source_path")
            repo_commit = ev.get("repo_commit")
            if not isinstance(repo_url, str) or not isinstance(source_path, str):
                continue
            repo_url = resolve_repo_url(repo_url)
            repo_dir = repos_dir / repo_dir_from_url(repo_url)
            file_path = repo_dir / source_path
            if not file_path.exists():
                continue
            try:
                text = file_path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue

            if file_path.suffix.lower() in {".rq", ".sparql"}:
                lines = text.splitlines()
                for segment in split_queries_with_starts(text):
                    raw_query = segment["query"]
                    start_idx = int(segment["start"])
                    comment_desc = extract_preceding_comments(lines, start_idx)
                    leading_desc = extract_leading_context(raw_query)
                    if leading_desc:
                        if comment_desc:
                            comment_desc = f"{comment_desc} {leading_desc}".strip()
                        else:
                            comment_desc = leading_desc
                    if not comment_desc:
                        continue
                    normalized = normalize_query(raw_query)
                    if not normalized:
                        continue
                    q_hash = sha256_hash(normalized)
                    target = by_kg_hash.get((kg_id, q_hash))
                    if target is None:
                        continue
                    add_evidence(
                        target,
                        "query_comment",
                        repo_url,
                        source_path,
                        str(repo_commit or ""),
                        comment_desc,
                        extracted_at,
                    )
            elif file_path.suffix.lower() == ".md":
                for block in extract_md_blocks_with_desc(text):
                    for segment in split_queries_with_starts(block["query"]):
                        raw_query = segment["query"]
                        normalized = normalize_query(raw_query)
                        if not normalized:
                            continue
                        q_hash = sha256_hash(normalized)
                        target = by_kg_hash.get((kg_id, q_hash))
                        if target is None:
                            continue
                        desc = block.get("desc", "")
                        context = extract_context_for_code(text, int(block.get("start_idx", 0)))
                        if context and not desc:
                            desc = context
                        if desc:
                            add_evidence(
                                target,
                                "doc_query_desc",
                                repo_url,
                                source_path,
                                str(repo_commit or ""),
                                clean_desc(desc),
                                extracted_at,
                            )
                if file_path.name.lower().startswith("readme"):
                    cq_items = []
                    heading_blocks = extract_heading_bullets(text)
                    if heading_blocks:
                        cq_items.extend(heading_blocks)
                    else:
                        cq_items.extend(extract_label_blocks(text))
                    table_blocks = extract_table_blocks(text)
                    if table_blocks:
                        cq_items.extend(table_blocks)
                    if not cq_items:
                        cq_items.extend(extract_cq_block(text))
                    for rec2 in records:
                        if rec2.get("kg_id") != kg_id:
                            continue
                        if query_has_doc_evidence(rec2):
                            continue
                        for item in cq_items:
                            add_evidence(
                                rec2,
                                "cq_item",
                                repo_url,
                                source_path,
                                str(repo_commit or ""),
                                clean_desc(item),
                                extracted_at,
                            )

        # Parse README files explicitly for query descriptions.
        for repo_url in kg_repos.get(kg_id, []):
            repo_url = resolve_repo_url(repo_url)
            repo_dir = repos_dir / repo_dir_from_url(repo_url)
            if not repo_dir.exists():
                continue
            readmes = [p for p in repo_dir.iterdir() if p.is_file() and p.name.lower().startswith("readme")]
            for readme in readmes:
                try:
                    readme_text = readme.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue
                cq_items = []
                heading_blocks = extract_heading_bullets(readme_text)
                if heading_blocks:
                    cq_items.extend(heading_blocks)
                else:
                    cq_items.extend(extract_label_blocks(readme_text))
                table_blocks = extract_table_blocks(readme_text)
                if table_blocks:
                    cq_items.extend(table_blocks)
                if not cq_items:
                    cq_items.extend(extract_cq_block(readme_text))
                for rec2 in records:
                    if rec2.get("kg_id") != kg_id:
                        continue
                    if query_has_doc_evidence(rec2):
                        continue
                    for item in cq_items:
                        add_evidence(
                            rec2,
                            "cq_item",
                            repo_url,
                            str(readme.relative_to(repo_dir)),
                            "",
                            clean_desc(item),
                            extracted_at,
                        )
                for block in extract_md_blocks_with_desc(readme_text) + extract_pre_blocks_with_desc(readme_text):
                    for segment in split_queries_with_starts(block["query"]):
                        raw_query = segment["query"]
                        normalized = normalize_query(raw_query)
                        if not normalized:
                            continue
                        q_hash = sha256_hash(normalized)
                        target = by_kg_hash.get((kg_id, q_hash))
                        if target is None:
                            continue
                        if query_has_doc_evidence(target):
                            continue
                        desc = block.get("desc", "")
                        context = extract_context_for_code(readme_text, int(block.get("start_idx", 0)))
                        if context and not desc:
                            desc = context
                        if desc:
                            add_evidence(
                                target,
                                "readme_query_desc",
                                repo_url,
                                str(readme.relative_to(repo_dir)),
                                "",
                                clean_desc(desc),
                                extracted_at,
                            )

        # Enrich from kg_sources (web/papers) if available.
        source_files = kg_sources.get(kg_id, [])
        doc_cq_seen = False
        for src_file in source_files:
            src_path = Path(src_file)
            if not src_path.is_absolute():
                if src_path.exists():
                    src_path = src_path
                elif src_path.parts and src_path.parts[0] in {"kg_sources", "curated_sources"}:
                    src_path = src_path
                else:
                    src_path = sources_dir / src_path
            if not src_path.exists():
                continue
            if "api-github-com" in str(src_path):
                continue
            source_url, body, raw_body = parse_source_file(src_path)
            if not body.strip():
                continue
            cq_items = []
            heading_blocks = extract_heading_bullets(body)
            if heading_blocks:
                cq_items.extend(heading_blocks)
            else:
                cq_items.extend(extract_label_blocks(body))
            table_blocks = extract_table_blocks(body)
            if table_blocks:
                cq_items.extend(table_blocks)
            if not cq_items:
                cq_items.extend(extract_cq_block(body))
            for rec2 in records:
                if rec2.get("kg_id") != kg_id:
                    continue
                has_same_source = any(
                    isinstance(e, dict) and e.get("source_path") == str(src_path)
                    for e in rec2.get("evidence", []) or []
                )
                if not (has_same_source or query_has_repo_evidence(rec2)):
                    continue
                for item in cq_items:
                    add_evidence(
                        rec2,
                        "cq_item",
                        source_url or "",
                        str(src_path),
                        "",
                        clean_desc(item),
                        extracted_at,
                    )
                if table_blocks:
                    doc_cq_seen = True
            # Try to match SPARQL blocks to queries.
            for block in extract_md_blocks_with_desc(body) + extract_pre_blocks_with_desc(raw_body):
                for segment in split_queries_with_starts(block["query"]):
                    raw_query = segment["query"]
                    normalized = normalize_query(raw_query)
                    if not normalized:
                        continue
                    q_hash = sha256_hash(normalized)
                    target = by_kg_hash.get((kg_id, q_hash))
                    if target is None:
                        continue
                    desc = block.get("desc", "")
                    context = extract_context_for_code(body, int(block.get("start_idx", 0)))
                    if context and not desc:
                        desc = context
                    if desc:
                        add_evidence(
                            target,
                            "web_query_desc",
                            source_url or "",
                            str(src_path),
                            "",
                            clean_desc(desc),
                            extracted_at,
                        )
            # Ensure each matched query block has at least one local query description.
            for block in extract_md_blocks_with_desc(body) + extract_pre_blocks_with_desc(raw_body):
                for segment in split_queries_with_starts(block["query"]):
                    raw_query = segment["query"]
                    normalized = normalize_query(raw_query)
                    if not normalized:
                        continue
                    q_hash = sha256_hash(normalized)
                    target = by_kg_hash.get((kg_id, q_hash))
                    if target is None:
                        continue
                    desc = block.get("desc", "")
                    context = extract_context_for_code(body, int(block.get("start_idx", 0)))
                    if context and not desc:
                        desc = context
                    cleaned_desc = clean_desc(desc)
                    if not cleaned_desc:
                        continue
                    evidence = target.get("evidence", []) or []
                    if any(
                        isinstance(ev, dict)
                        and ev.get("type") in {"web_query_desc", "doc_query_desc", "readme_query_desc"}
                        and ev.get("source_path") == str(src_path)
                        for ev in evidence
                    ):
                        continue
                    add_evidence(
                        target,
                        "web_query_desc",
                        source_url or "",
                        str(src_path),
                        "",
                        cleaned_desc,
                        extracted_at,
                    )

            cq_section_items = extract_cq_section(body)
            if cq_section_items and not doc_cq_seen and not table_blocks:
                for rec2 in records:
                    if rec2.get("kg_id") != kg_id:
                        continue
                    if not any(
                        isinstance(e, dict) and e.get("source_path") == str(src_path)
                        for e in rec2.get("evidence", []) or []
                    ):
                        continue
                    for item in cq_section_items:
                        add_evidence(
                            rec2,
                            "cq_item",
                            source_url or "",
                            str(src_path),
                            "",
                            clean_desc(item),
                            extracted_at,
                        )

        # Extract evidence from PDFs by filename match or explicit doc paths.
        pdf_paths: List[Path] = []
        if pdfs_dir.exists():
            pdf_paths.extend(pdfs_dir.glob("*.pdf"))
        for doc_path in kg_sources.get(kg_id, []):
            path_obj = Path(doc_path)
            if path_obj.suffix.lower() == ".pdf" and path_obj.exists():
                pdf_paths.append(path_obj)
        seen_pdfs: set[Path] = set()
        for pdf_path in pdf_paths:
            if pdf_path in seen_pdfs:
                continue
            seen_pdfs.add(pdf_path)
            if not isinstance(kg_id, str):
                continue
            if kg_id.lower() not in pdf_path.name.lower() and str(pdf_path) not in kg_sources.get(kg_id, []):
                continue
            # Remove previous PDF-derived CQ evidence so we can replace it cleanly.
            for rec2 in records:
                if rec2.get("kg_id") != kg_id:
                    continue
                ev = rec2.get("evidence")
                if not isinstance(ev, list):
                    continue
                rec2["evidence"] = [
                    e for e in ev
                    if not (
                        isinstance(e, dict)
                        and e.get("source_path") == str(pdf_path)
                        and e.get("type") in {"cq_item", "doc_query_desc"}
                    )
                ]
            pdf_text = extract_text_from_pdf(pdf_path)
            if not pdf_text.strip():
                continue
            captions = extract_pdf_captions(pdf_text) + extract_pdf_cq_captions(pdf_text)
            caption_hits = []
            for cap in captions:
                cap_lower = cap.lower()
                if "table" in cap_lower and re.search(r"competency\s+questions?", cap_lower):
                    caption_hits.append(cap)
            caption_hits = sorted(set(caption_hits))
            cq_table_blocks = extract_pdf_tables_for_captions(pdf_text, caption_hits) if caption_hits else []
            cq_bullets = extract_pdf_cq_bullets(pdf_text)
            code_blocks = extract_pdf_query_blocks(pdf_text)
            # Only fall back to generic CQ extraction when no explicit CQ signal was found.
            heading_blocks = [] if (caption_hits or cq_bullets) else extract_heading_bullets(pdf_text)
            label_blocks = [] if (caption_hits or cq_bullets) else extract_label_blocks(pdf_text)
            fallback_tables = [] if (caption_hits or cq_bullets) else extract_table_blocks(pdf_text)
            cq_section_items = [] if (caption_hits or cq_bullets) else extract_cq_section(pdf_text)
            pdf_lines = pdf_text.splitlines()
            for rec2 in records:
                if rec2.get("kg_id") != kg_id:
                    continue
                if not any(
                    isinstance(e, dict) and e.get("source_path") == str(pdf_path)
                    for e in rec2.get("evidence", []) or []
                ) and not query_has_repo_evidence(rec2):
                    continue
                for tbl in cq_table_blocks:
                    for item in split_cq_block_items(tbl):
                        add_evidence(
                            rec2,
                            "cq_item",
                            "",
                            str(pdf_path),
                            "",
                            clean_desc(item),
                            extracted_at,
                        )
                for bullet_block in cq_bullets:
                    for item in extract_bullet_items(bullet_block.splitlines()):
                        add_evidence(
                            rec2,
                            "cq_item",
                            "",
                            str(pdf_path),
                            "",
                            clean_desc(item),
                            extracted_at,
                        )
                for block in heading_blocks + label_blocks:
                    add_evidence(
                        rec2,
                        "cq_item",
                        "",
                        str(pdf_path),
                        "",
                        clean_desc(block),
                        extracted_at,
                    )
                for tbl in fallback_tables:
                    for item in split_cq_block_items(tbl):
                        add_evidence(
                            rec2,
                            "cq_item",
                            "",
                            str(pdf_path),
                            "",
                            clean_desc(item),
                            extracted_at,
                        )
                for item in cq_section_items:
                    add_evidence(
                        rec2,
                        "cq_item",
                        "",
                        str(pdf_path),
                        "",
                        clean_desc(item),
                        extracted_at,
                    )

            # Attach query descriptions to matching PDF-derived queries only.
            matched_targets: List[Dict[str, object]] = []
            pdf_targets: Dict[str, Dict[str, object]] = {}
            for rec2 in records:
                if rec2.get("kg_id") != kg_id:
                    continue
                if not query_has_pdf_evidence(rec2, pdf_path):
                    continue
                sparql_clean = rec2.get("sparql_clean")
                if isinstance(sparql_clean, str):
                    pdf_targets[normalize_query_signature(sparql_clean)] = rec2
            for block in code_blocks:
                for segment in split_queries_with_starts(block.get("block", "")):
                    raw_query = segment["query"]
                    normalized = normalize_query(raw_query)
                    if not normalized:
                        continue
                    target = pdf_targets.get(normalize_query_signature(normalized))
                    if target is None:
                        continue
                    start_char = int(block.get("start_char", 0))
                    start_idx = int(block.get("start_idx", 0))
                    caption = extract_nearest_caption(pdf_lines, start_idx)
                    caption_idx = None
                    if caption:
                        for i in range(start_idx - 1, max(-1, start_idx - 40), -1):
                            if pdf_lines[i].strip() == caption:
                                caption_idx = i
                                break
                    caption_is_near = caption_idx is not None and (start_idx - caption_idx) <= 3 and not has_blank_between(pdf_lines, start_idx, caption_idx)
                    if caption and caption_is_near:
                        caption_clean = clean_desc(caption)
                        if caption_clean:
                            add_evidence(
                                target,
                                "doc_query_desc",
                                "",
                                str(pdf_path),
                                "",
                                caption_clean,
                                extracted_at,
                            )
                    else:
                        cq_line = extract_nearest_cq_line(pdf_lines, start_idx)
                        if cq_line:
                            context_clean = clean_desc(cq_line)
                        else:
                            context = extract_pdf_paragraphs(pdf_lines, start_idx, max_paragraphs=2)
                            context_clean = clean_desc(context) if context else ""
                        if context_clean:
                            add_evidence(
                                target,
                                "doc_query_desc",
                                "",
                                str(pdf_path),
                                "",
                                context_clean,
                                extracted_at,
                            )
                    matched_targets.append(target)

    ensure_missing_source_query_descs(by_kg_hash, kg_sources, extracted_at)

    optional_context_types = set()
    if args.include_ontology_context:
        optional_context_types.add("ontology_term_context")
    if args.include_graph_shape_context:
        optional_context_types.add("graph_shape_context")
    if optional_context_types:
        for rec in records:
            evidence = rec.get("evidence")
            if isinstance(evidence, list):
                rec["evidence"] = [
                    ev for ev in evidence
                    if not (isinstance(ev, dict) and ev.get("type") in optional_context_types)
                ]

    if args.include_ontology_context:
        add_ontology_context_evidence(
            records,
            ontology_sources_by_kg,
            Path(args.ontology_cache_dir),
            bool(args.download_ontology_sources),
            extracted_at,
        )

    if args.include_graph_shape_context:
        add_graph_shape_context_evidence(records, datasets_by_kg, extracted_at)

    after_counts: Dict[str, int] = {}
    type_counts: Dict[str, int] = {}
    for rec in records:
        evidence = rec.get("evidence")
        if isinstance(evidence, list):
            rec["evidence"] = renumber_evidence(dedupe_evidence(expand_cq_items(evidence)))
            for ev in rec["evidence"]:
                if isinstance(ev, dict) and ev.get("type"):
                    type_counts[ev["type"]] = type_counts.get(ev["type"], 0) + 1
            kg_id = rec.get("kg_id")
            if isinstance(kg_id, str):
                after_counts[kg_id] = after_counts.get(kg_id, 0) + len(rec["evidence"])
            rec.pop("llm_context", None)
            rec.pop("llm_context_ranked", None)
            rec.pop("cq_items", None)
            rec.pop("justification", None)
            rec.pop("comments", None)
            if "confidence" not in rec:
                rec["confidence"] = None
            if "llm_output" not in rec:
                rec["llm_output"] = {
                    "ranked_evidence_phrases": [],
                    "nl_question": None,
                    "nl_question_origin": {
                        "mode": None,
                        "evidence_ids": [],
                        "primary_evidence_id": None,
                    },
                    "confidence": None,
                    "confidence_rationale": None,
                    "needs_review": None,
                }

    write_jsonl(queries_path, records)
    print(f"Wrote {len(records)} records to {queries_path.resolve()}")
    if after_counts:
        print("\nEvidence counts by KG:")
        for kg_id in sorted(after_counts):
            before = before_counts.get(kg_id, 0)
            after = after_counts.get(kg_id, 0)
            delta = after - before
            print(f"- {kg_id}: {after} (delta={delta})")
    if type_counts:
        print("\nEvidence counts by type:")
        for etype in sorted(type_counts):
            print(f"- {etype}: {type_counts[etype]}")


if __name__ == "__main__":
    main()
