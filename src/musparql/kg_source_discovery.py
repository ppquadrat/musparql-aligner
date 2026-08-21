"""Find unverified source candidates for human review.

Discovery is deliberately separate from the authoritative source catalogue and
KG seed files.  This module can print or save a report, but it cannot promote a
candidate into either catalogue.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Iterable
from urllib.parse import urlparse

import requests


GITHUB_API = "https://api.github.com/search/repositories"
OPENALEX_API = "https://api.openalex.org/works"
BRAVE_API = "https://api.search.brave.com/res/v1/web/search"
REPORT_SCHEMA = "musparql.kg-source-discovery-report.v1"
HTTP_TIMEOUT = 15
USER_AGENT = "musparql/kg-source-discovery (+https://github.com/ppquadrat/musparql-aligner)"

GENERIC_TOKENS = {
    "knowledge", "graph", "ontology", "ontologies", "vocabulary", "data",
    "dataset", "linked", "rdf", "sparql", "project",
}
QUALIFIER_SUFFIXES = (
    "knowledge graph", "knowledge-graph", "kg", "ontology", "ontologies",
    "vocabulary", "thesaurus", "dataset", "data set", "rdf", "linked data",
)
MAX_ALIASES = 5
DOI_RE = re.compile(r"10\.\d{4,9}/[-._;()/:a-zA-Z0-9]+", re.IGNORECASE)


@dataclass
class QueryRecord:
    backend: str
    query: str
    result_count: int = 0
    status: str = "ok"
    warning: str | None = None


@dataclass
class Candidate:
    url: str
    source_kind: str
    title: str
    description: str
    relevance_score: int
    matched_tokens: list[str]
    ranking_reasons: list[str]
    review_status: str
    origins: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    duplicate_grouping: list[str] = field(default_factory=list)
    locations: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class DiscoveryReport:
    kg_name: str
    project: str | None
    created_at: str
    aliases: list[str] = field(default_factory=list)
    shortlist_limit_per_kind: int | None = 5
    candidate_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    queries: list[QueryRecord] = field(default_factory=list)
    candidates: list[Candidate] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    schema: str = REPORT_SCHEMA
    authority: str = "unverified discovery output; human review required"
    limitations: list[str] = field(default_factory=lambda: [
        "Search APIs rank and cap their results; absence from this report is not evidence that a source does not exist.",
        "Relevance scores are lexical hints, not quality or authority assessments.",
        "Title-based publication grouping is a review hint; verify grouped locations before treating them as one work.",
        "The report does not alter catalog/sources.yaml, KG seeds, or any other authoritative input.",
    ])

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def extract_core_name(kg_name: str) -> str:
    core = re.sub(r"\([^)]*\)$", "", kg_name)
    core = re.sub(r"\s+", " ", core).strip()
    return core or kg_name


def extract_short_name(kg_name: str) -> str:
    core = extract_core_name(kg_name)
    lowered = core.lower()
    for suffix in sorted(QUALIFIER_SUFFIXES, key=len, reverse=True):
        if lowered.endswith(" " + suffix):
            short = core[: len(core) - len(suffix) - 1].strip()
            if short:
                return short
    return core


def name_tokens(*names: str | None) -> list[str]:
    tokens: list[str] = []
    for name in names:
        if not name:
            continue
        for token in re.split(r"[^a-zA-Z0-9]+", name.lower()):
            if len(token) > 2 and token not in GENERIC_TOKENS and token not in tokens:
                tokens.append(token)
    return tokens


def normalise_aliases(aliases: Iterable[str]) -> list[str]:
    normalised: list[str] = []
    for raw in aliases:
        alias = re.sub(r"\s+", " ", raw).strip()
        if not alias:
            raise ValueError("aliases must not be empty")
        if len(alias) > 100:
            raise ValueError("aliases must be at most 100 characters")
        if alias.casefold() not in {item.casefold() for item in normalised}:
            normalised.append(alias)
    if len(normalised) > MAX_ALIASES:
        raise ValueError(f"at most {MAX_ALIASES} aliases may be supplied")
    return normalised


def _compact(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def relevance(
    title: str,
    context: str,
    *,
    kg_name: str,
    project: str | None,
    aliases: Iterable[str] = (),
) -> tuple[int, list[str], list[str]]:
    """Return a transparent lexical score; exact KG-name matches dominate."""
    alias_list = list(aliases)
    tokens = name_tokens(kg_name, extract_short_name(kg_name), project)
    haystack = f"{title} {context}"
    matched = [token for token in tokens if token in haystack.lower()]
    score = len(matched)
    reasons = [f"matched token: {token}" for token in matched]

    core = _compact(extract_core_name(kg_name))
    compact_title = _compact(title)
    compact_context = _compact(context)
    if core and core in compact_title:
        score += 100
        reasons.insert(0, "exact KG name in title")
    elif core and core in compact_context:
        score += 40
        reasons.insert(0, "exact KG name in description or URL")

    compact_project = _compact(project or "")
    if compact_project and compact_project in compact_title:
        score += 20
        reasons.append("project name in title")
    elif compact_project and compact_project in compact_context:
        score += 5
        reasons.append("project name in description or URL")
    for alias in alias_list:
        compact_alias = _compact(alias)
        if not compact_alias:
            continue
        if compact_alias in compact_title:
            score += 70
            reasons.append(f"alias in title: {alias}")
        elif compact_alias in compact_context:
            score += 10
            reasons.append(f"alias in description or URL: {alias}")
    return score, matched, reasons


def _append_candidate(candidates: list[Candidate], candidate: Candidate) -> None:
    """Deduplicate exact URLs while retaining every query that found the URL."""
    for existing in candidates:
        if existing.url.rstrip("/") == candidate.url.rstrip("/"):
            for origin in candidate.origins:
                if origin not in existing.origins:
                    existing.origins.append(origin)
            if candidate.relevance_score > existing.relevance_score:
                existing.relevance_score = candidate.relevance_score
                existing.matched_tokens = candidate.matched_tokens
                existing.ranking_reasons = candidate.ranking_reasons
                existing.review_status = candidate.review_status
            for key, value in candidate.metadata.items():
                existing.metadata.setdefault(key, value)
            return
    candidates.append(candidate)


def _review_status(score: int) -> str:
    return "unverified_candidate" if score > 0 else "low_lexical_relevance"


def search_github(
    kg_name: str,
    project: str | None,
    *,
    per_query: int,
    aliases: Iterable[str] = (),
    session: Any = requests,
) -> tuple[list[Candidate], list[QueryRecord], list[str]]:
    core = extract_core_name(kg_name)
    short = extract_short_name(kg_name)
    queries = [f"{short} {project} in:name,description,readme" if project else f"{short} in:name,description,readme"]
    if short.lower() != core.lower():
        queries.append(f'"{core}" {project}'.strip() if project else f'"{core}"')
    for alias in aliases:
        project_term = f' "{project}"' if project else ""
        queries.append(f'"{alias}"{project_term} in:name,description,readme')
    queries = list(dict.fromkeys(queries))
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": USER_AGENT,
    }
    if os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"

    candidates: list[Candidate] = []
    records: list[QueryRecord] = []
    warnings: list[str] = []
    for query in queries:
        record = QueryRecord(backend="github", query=query)
        records.append(record)
        try:
            response = session.get(
                GITHUB_API,
                params={"q": query, "per_page": per_query, "sort": "stars", "order": "desc"},
                headers=headers,
                timeout=HTTP_TIMEOUT,
            )
            if response.status_code != 200:
                record.status = "error"
                record.warning = f"GitHub returned HTTP {response.status_code}"
                warnings.append(f"GitHub query {query!r}: HTTP {response.status_code}")
                continue
            items = response.json().get("items", [])
            record.result_count = len(items)
            for rank, item in enumerate(items, start=1):
                url = item.get("html_url")
                if not url:
                    continue
                title = item.get("full_name") or item.get("name") or url
                description = item.get("description") or ""
                score, matched, reasons = relevance(
                    title, f"{description} {url}", kg_name=kg_name, project=project,
                    aliases=aliases,
                )
                _append_candidate(candidates, Candidate(
                    url=url,
                    source_kind="repository",
                    title=title,
                    description=description,
                    relevance_score=score,
                    matched_tokens=matched,
                    ranking_reasons=reasons,
                    review_status=_review_status(score),
                    origins=[{"backend": "github", "query": query, "rank": rank}],
                    metadata={
                        "stars": item.get("stargazers_count") or 0,
                        "archived": bool(item.get("archived")),
                        "updated_at": item.get("updated_at"),
                    },
                ))
        except (requests.RequestException, ValueError) as exc:
            record.status = "error"
            record.warning = f"GitHub request failed: {exc}"
            warnings.append(f"GitHub query {query!r} failed: {exc}")
    return candidates, records, warnings


def _openalex_url(work: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    primary = work.get("primary_location") or {}
    pdf_url = primary.get("pdf_url") if isinstance(primary, dict) else None
    landing_url = primary.get("landing_page_url") if isinstance(primary, dict) else None
    doi = work.get("doi")
    openalex_id = work.get("id")
    url = doi or landing_url or pdf_url or openalex_id
    return url, {
        "doi": doi,
        "pdf_url": pdf_url,
        "landing_page_url": landing_url,
        "openalex_id": openalex_id,
        "publication_year": work.get("publication_year"),
    }


def search_openalex(
    kg_name: str,
    project: str | None,
    *,
    per_query: int,
    aliases: Iterable[str] = (),
    session: Any = requests,
) -> tuple[list[Candidate], list[QueryRecord], list[str]]:
    core = extract_core_name(kg_name)
    short = extract_short_name(kg_name)
    queries = [f"{short} {project}" if project else short]
    if short.lower() != core.lower():
        queries.append(f"{core} {project}" if project else core)
    for alias in aliases:
        queries.append(f"{alias} {project}" if project else alias)
    queries = list(dict.fromkeys(queries))
    candidates: list[Candidate] = []
    records: list[QueryRecord] = []
    warnings: list[str] = []
    for query in queries:
        record = QueryRecord(backend="openalex", query=query)
        records.append(record)
        params: dict[str, Any] = {"search": query, "per-page": per_query}
        if os.environ.get("OPENALEX_MAILTO"):
            params["mailto"] = os.environ["OPENALEX_MAILTO"]
        try:
            response = session.get(
                OPENALEX_API,
                params=params,
                headers={"User-Agent": USER_AGENT},
                timeout=HTTP_TIMEOUT,
            )
            if response.status_code != 200:
                record.status = "error"
                record.warning = f"OpenAlex returned HTTP {response.status_code}"
                warnings.append(f"OpenAlex query {query!r}: HTTP {response.status_code}")
                continue
            works = response.json().get("results", [])
            record.result_count = len(works)
            for rank, work in enumerate(works, start=1):
                url, metadata = _openalex_url(work)
                if not url:
                    continue
                title = work.get("title") or url
                abstract_index = work.get("abstract_inverted_index") or {}
                abstract = " ".join(abstract_index) if isinstance(abstract_index, dict) else ""
                score, matched, reasons = relevance(
                    title, abstract, kg_name=kg_name, project=project, aliases=aliases
                )
                _append_candidate(candidates, Candidate(
                    url=url,
                    source_kind="publication",
                    title=title,
                    description="",
                    relevance_score=score,
                    matched_tokens=matched,
                    ranking_reasons=reasons,
                    review_status=_review_status(score),
                    origins=[{"backend": "openalex", "query": query, "rank": rank}],
                    metadata={key: value for key, value in metadata.items() if value is not None},
                ))
        except (requests.RequestException, ValueError) as exc:
            record.status = "error"
            record.warning = f"OpenAlex request failed: {exc}"
            warnings.append(f"OpenAlex query {query!r} failed: {exc}")
    return candidates, records, warnings


def search_brave(
    kg_name: str,
    project: str | None,
    *,
    per_query: int,
    aliases: Iterable[str] = (),
    session: Any = requests,
) -> tuple[list[Candidate], list[QueryRecord], list[str]]:
    key = os.environ.get("BRAVE_API_KEY")
    if not key:
        warning = "BRAVE_API_KEY is not set; web discovery was skipped."
        return [], [QueryRecord(backend="brave", query="not run", status="skipped", warning=warning)], [warning]
    core = extract_core_name(kg_name)
    short = extract_short_name(kg_name)
    context = f' "{project}"' if project else ""
    queries = [f'"{short}"{context} SPARQL', f'"{short}"{context} documentation']
    if short.lower() != core.lower():
        queries.append(f'"{core}"')
    for alias in aliases:
        queries.append(f'"{alias}"{context}')
    queries = list(dict.fromkeys(queries))
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": key,
        "User-Agent": USER_AGENT,
    }
    candidates: list[Candidate] = []
    records: list[QueryRecord] = []
    warnings: list[str] = []
    for query in queries:
        record = QueryRecord(backend="brave", query=query)
        records.append(record)
        try:
            response = session.get(
                BRAVE_API,
                params={"q": query, "count": min(per_query, 20)},
                headers=headers,
                timeout=HTTP_TIMEOUT,
            )
            if response.status_code != 200:
                record.status = "error"
                record.warning = f"Brave returned HTTP {response.status_code}"
                warnings.append(f"Brave query {query!r}: HTTP {response.status_code}")
                continue
            items = (response.json().get("web") or {}).get("results") or []
            record.result_count = len(items)
            for rank, item in enumerate(items, start=1):
                url = item.get("url")
                if not url:
                    continue
                title = item.get("title") or url
                description = item.get("description") or ""
                score, matched, reasons = relevance(
                    title, f"{description} {url}", kg_name=kg_name, project=project,
                    aliases=aliases,
                )
                domain = urlparse(url).netloc.lower()
                kind = "publication" if url.lower().endswith(".pdf") else "web_document"
                _append_candidate(candidates, Candidate(
                    url=url,
                    source_kind=kind,
                    title=title,
                    description=description,
                    relevance_score=score,
                    matched_tokens=matched,
                    ranking_reasons=reasons,
                    review_status=_review_status(score),
                    origins=[{"backend": "brave", "query": query, "rank": rank}],
                    metadata={"domain": domain},
                ))
        except (requests.RequestException, ValueError) as exc:
            record.status = "error"
            record.warning = f"Brave request failed: {exc}"
            warnings.append(f"Brave query {query!r} failed: {exc}")
    return candidates, records, warnings


def _best_origin_rank(candidate: Candidate) -> int:
    return min((int(origin["rank"]) for origin in candidate.origins), default=10_000)


def _normalise_doi(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    match = DOI_RE.search(value)
    if not match:
        return None
    return match.group(0).rstrip(".,;)").lower()


def _publication_title_keys(title: str) -> set[str]:
    value = re.sub(r"^\s*\(pdf\)\s*", "", title, flags=re.IGNORECASE)
    variants = [value, *re.split(r"\s+-\s+", value)]
    keys: set[str] = set()
    for variant in variants:
        words = re.findall(r"[a-z0-9]+", variant.lower())
        if len(words) >= 7:
            keys.add(" ".join(words))
    return keys


def _publication_identity_keys(candidate: Candidate) -> set[str]:
    searchable = " ".join([
        candidate.url,
        candidate.title,
        candidate.description,
        *(str(value) for value in candidate.metadata.values() if isinstance(value, str)),
    ])
    keys: set[str] = set()
    for match in DOI_RE.finditer(searchable):
        doi = _normalise_doi(match.group(0))
        if doi:
            keys.add(f"doi:{doi}")
    openalex_id = candidate.metadata.get("openalex_id")
    if isinstance(openalex_id, str) and openalex_id:
        keys.add(f"openalex:{openalex_id.rstrip('/').lower()}")
    keys.update(f"title:{title_key}" for title_key in _publication_title_keys(candidate.title))
    return keys


def _location(candidate: Candidate) -> dict[str, Any]:
    return {
        "url": candidate.url,
        "title": candidate.title,
        "description": candidate.description,
        "origins": deepcopy(candidate.origins),
        "metadata": deepcopy(candidate.metadata),
    }


def _representative_priority(candidate: Candidate) -> tuple[int, int, int]:
    has_doi = bool(_normalise_doi(candidate.metadata.get("doi")))
    from_openalex = any(origin.get("backend") == "openalex" for origin in candidate.origins)
    return (int(has_doi), int(from_openalex), candidate.relevance_score)


def group_publication_locations(candidates: Iterable[Candidate]) -> list[Candidate]:
    """Group confident duplicate publication locations without discarding URLs."""
    items = list(candidates)
    parent = list(range(len(items)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    key_owner: dict[str, int] = {}
    item_keys: list[set[str]] = []
    for index, candidate in enumerate(items):
        keys = _publication_identity_keys(candidate)
        item_keys.append(keys)
        for key in keys:
            if key in key_owner:
                union(index, key_owner[key])
            else:
                key_owner[key] = index

    components: dict[int, list[int]] = {}
    for index in range(len(items)):
        components.setdefault(find(index), []).append(index)

    grouped: list[Candidate] = []
    for indexes in components.values():
        members = [items[index] for index in indexes]
        shared_identity = set().union(*(item_keys[index] for index in indexes))
        has_confirmed_identifier = any(
            key.startswith(("doi:", "openalex:"))
            and sum(key in item_keys[index] for index in indexes) > 1
            for key in shared_identity
        )
        is_publication_group = any(item.source_kind == "publication" for item in members)
        if len(members) == 1 or not (has_confirmed_identifier or is_publication_group):
            grouped.extend(deepcopy(members))
            continue

        representative = deepcopy(max(members, key=_representative_priority))
        representative.source_kind = "publication"
        representative.locations = [_location(member) for member in members]
        reasons: list[str] = []
        if has_confirmed_identifier:
            reasons.append("confirmed shared DOI or OpenAlex identifier")
        if any(
            key.startswith("title:")
            and sum(key in item_keys[index] for index in indexes) > 1
            for key in shared_identity
        ):
            reasons.append("probable duplicate: exact normalized long title")
        representative.duplicate_grouping = reasons
        representative.relevance_score = max(item.relevance_score for item in members)
        representative.ranking_reasons = list(dict.fromkeys(
            reason for item in members for reason in item.ranking_reasons
        ))
        grouped.append(representative)
    return grouped


def shortlist_candidates(
    candidates: Iterable[Candidate], limit_per_kind: int | None
) -> tuple[list[Candidate], dict[str, dict[str, int]]]:
    """Rank and cap each source kind, retaining counts for omitted candidates."""
    grouped: dict[str, list[Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.source_kind, []).append(candidate)

    selected: list[Candidate] = []
    counts: dict[str, dict[str, int]] = {}
    for kind in sorted(grouped):
        ranked = sorted(
            grouped[kind],
            key=lambda item: (-item.relevance_score, _best_origin_rank(item), item.title.lower()),
        )
        shown = ranked if limit_per_kind is None else ranked[:limit_per_kind]
        selected.extend(shown)
        counts[kind] = {
            "found": len(ranked),
            "shown": len(shown),
            "omitted": len(ranked) - len(shown),
            "locations": sum(len(item.locations) or 1 for item in ranked),
            "grouped_duplicates": sum(max(0, len(item.locations) - 1) for item in ranked),
        }
    return selected, counts


def discover_sources(
    kg_name: str,
    project: str | None = None,
    *,
    per_query: int = 20,
    backends: Iterable[str] = ("github", "openalex", "brave"),
    max_candidates_per_kind: int | None = 5,
    aliases: Iterable[str] = (),
) -> DiscoveryReport:
    alias_list = normalise_aliases(aliases)
    report = DiscoveryReport(
        kg_name=kg_name,
        project=project,
        created_at=datetime.now(timezone.utc).isoformat(),
        aliases=alias_list,
        shortlist_limit_per_kind=max_candidates_per_kind,
    )
    functions = {
        "github": search_github,
        "openalex": search_openalex,
        "brave": search_brave,
    }
    candidates: list[Candidate] = []
    for backend in backends:
        found, queries, warnings = functions[backend](
            kg_name, project, per_query=per_query, aliases=alias_list
        )
        for candidate in found:
            _append_candidate(candidates, candidate)
        report.queries.extend(queries)
        report.warnings.extend(warnings)
    grouped_candidates = group_publication_locations(candidates)
    report.candidates, report.candidate_counts = shortlist_candidates(
        grouped_candidates, max_candidates_per_kind
    )
    return report


def format_text(report: DiscoveryReport) -> str:
    lines = [
        "UNVERIFIED KG SOURCE DISCOVERY REPORT",
        f"Knowledge graph: {report.kg_name}",
        f"Project: {report.project or '(not supplied)'}",
        f"Aliases: {', '.join(report.aliases) if report.aliases else '(none)'}",
        f"Generated: {report.created_at}",
        "",
        "Queries run:",
    ]
    for query in report.queries:
        lines.append(f"- [{query.status}] {query.backend}: {query.query} ({query.result_count} results)")
        if query.warning:
            lines.append(f"  Warning: {query.warning}")
    total_found = sum(item["found"] for item in report.candidate_counts.values())
    lines.extend(["", f"Candidates ({len(report.candidates)} shown of {total_found} unique; none are approved):"])
    for kind, counts in report.candidate_counts.items():
        lines.append(
            f"- {kind}: {counts['shown']} shown, {counts['omitted']} omitted; "
            f"{counts['locations']} locations with {counts['grouped_duplicates']} grouped duplicates"
        )
    for index, candidate in enumerate(report.candidates, start=1):
        lines.extend([
            f"{index}. [{candidate.source_kind}; {candidate.review_status}; score={candidate.relevance_score}] {candidate.title}",
            f"   {candidate.url}",
            f"   Ranking: " + "; ".join(candidate.ranking_reasons or ["no positive lexical signal"]),
            f"   Found by: " + "; ".join(
                f"{origin['backend']} / {origin['query']} / API rank {origin['rank']}"
                for origin in candidate.origins
            ),
        ])
        if candidate.description:
            lines.append(f"   {candidate.description}")
        if candidate.metadata:
            lines.append(f"   Metadata: {json.dumps(candidate.metadata, ensure_ascii=False, sort_keys=True)}")
        if candidate.locations:
            lines.append(
                f"   Grouped locations ({len(candidate.locations)}): "
                + "; ".join(location["url"] for location in candidate.locations)
            )
            lines.append(f"   Grouping: {'; '.join(candidate.duplicate_grouping)}")
    if report.warnings:
        lines.extend(["", "Warnings:"] + [f"- {warning}" for warning in report.warnings])
    lines.extend(["", "Limitations:"] + [f"- {item}" for item in report.limitations])
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Find unverified KG source candidates for manual review; never update catalogues or seeds."
    )
    parser.add_argument("--name", required=True, help="Knowledge graph name")
    parser.add_argument("--project", help="Optional project context")
    parser.add_argument(
        "--alias", action="append", default=[],
        help=f"Alternative KG or project name; repeat up to {MAX_ALIASES} times",
    )
    parser.add_argument("--per-query", type=int, default=20, choices=range(1, 101), metavar="1-100")
    parser.add_argument(
        "--expanded", action="store_true",
        help="Include every returned unique candidate instead of five per source category",
    )
    parser.add_argument(
        "--backend", action="append", choices=("github", "openalex", "brave"),
        help="Backend to run; repeat to select several (default: all)",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of the human-readable report")
    parser.add_argument("--output", type=Path, help="Save a new JSON report; must use a .json filename")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = discover_sources(
            args.name,
            args.project,
            per_query=args.per_query,
            backends=args.backend or ("github", "openalex", "brave"),
            max_candidates_per_kind=None if args.expanded else 5,
            aliases=args.alias,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    payload = json.dumps(report.as_dict(), indent=2, ensure_ascii=False) + "\n"
    if args.output:
        if args.output.suffix.lower() != ".json":
            raise SystemExit("--output must be a .json report, not a catalogue or seed file")
        if args.output.exists():
            raise SystemExit(f"refusing to overwrite existing report: {args.output}")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
        print(f"Saved unverified discovery report to {args.output}", file=sys.stderr)
    print(payload if args.json else format_text(report), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
