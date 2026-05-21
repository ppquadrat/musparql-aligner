#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import yaml


GITHUB_API = "https://api.github.com/search/repositories"
OPENALEX_API = "https://api.openalex.org/works"
BRAVE_API = "https://api.search.brave.com/res/v1/web/search"

HTTP_TIMEOUT = 15
USER_AGENT = "musparql-aligner/discover_kg_sources (+https://github.com/polifonia-project)"

# Generic words that should not gate relevance on their own (they appear in
# nearly any KG description and would let through irrelevant hits).
_GENERIC_TOKENS = {
    "knowledge",
    "graph",
    "ontology",
    "vocabulary",
    "data",
    "dataset",
    "linked",
    "rdf",
    "sparql",
    "project",
}

SKIP_DOMAINS_FOR_DOCS = {
    "github.com",
    "gitlab.com",
    "bitbucket.org",
    "duckduckgo.com",
    "google.com",
    "www.google.com",
    "twitter.com",
    "x.com",
    "facebook.com",
    "linkedin.com",
}


@dataclass
class KGSourceDiscovery:
    kg_name: str
    project: Optional[str] = None
    github_repos: List[str] = field(default_factory=list)
    docs_websites: List[str] = field(default_factory=list)
    pdf_papers: List[str] = field(default_factory=list)
    search_queries: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


def extract_core_name(kg_name: str) -> str:
    """Strip trailing parentheticals and collapse whitespace."""
    core = re.sub(r"\([^)]*\)$", "", kg_name)
    core = re.sub(r"\s+", " ", core).strip()
    return core or kg_name


_QUALIFIER_SUFFIXES = (
    "knowledge graph",
    "knowledge-graph",
    "kg",
    "ontology",
    "ontologies",
    "vocabulary",
    "thesaurus",
    "dataset",
    "data set",
    "rdf",
    "linked data",
)


def extract_short_name(kg_name: str) -> str:
    """
    Return the distinctive part of the name by stripping common KG qualifier
    suffixes. For 'MUSOW Knowledge Graph' returns 'MUSOW'; for 'Jazz Ontology'
    returns 'Jazz'. Falls back to the core name when stripping would leave nothing.
    """
    core = extract_core_name(kg_name)
    low = core.lower()
    for suffix in sorted(_QUALIFIER_SUFFIXES, key=len, reverse=True):
        if low.endswith(" " + suffix):
            cut = core[: len(core) - len(suffix) - 1].strip()
            if cut:
                return cut
    return core


def _name_tokens(*names: str, drop_generic: bool = True) -> List[str]:
    """Lowercase alphanumeric tokens with length > 2, used for relevance scoring."""
    toks: List[str] = []
    for n in names:
        if not n:
            continue
        for t in re.split(r"[^a-zA-Z0-9]+", n.lower()):
            if len(t) <= 2:
                continue
            if drop_generic and t in _GENERIC_TOKENS:
                continue
            if t not in toks:
                toks.append(t)
    return toks


def search_github_repos(kg_name: str, project: Optional[str]) -> Tuple[List[str], List[str]]:
    """
    Search GitHub for repositories matching the KG name (and optionally the project).
    Returns (repo_urls, warnings).
    """
    core = extract_core_name(kg_name)
    short = extract_short_name(kg_name)

    # Keep query count small: GitHub unauthenticated allows ~10 req/min total.
    queries: List[str] = []
    if project:
        queries.append(f'{short} {project} in:name,description,readme')
        if short.lower() != core.lower():
            queries.append(f'"{core}" {project}')
    else:
        queries.append(f'{short} in:name,description,readme')
        if short.lower() != core.lower():
            queries.append(f'"{core}"')
    queries = list(dict.fromkeys(queries))[:3]

    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": USER_AGENT,
    }
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    seen: List[str] = []
    warnings: List[str] = []
    scored: List[Tuple[int, str]] = []
    # Include short name as a relevance signal alongside full name + project.
    tokens = _name_tokens(kg_name, short, project or "")

    for q in queries:
        try:
            r = requests.get(
                GITHUB_API,
                params={"q": q, "per_page": 10, "sort": "stars", "order": "desc"},
                headers=headers,
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException as e:
            warnings.append(f"GitHub request failed for {q!r}: {e}")
            continue

        if r.status_code == 403 and "rate limit" in r.text.lower():
            warnings.append(
                "GitHub rate-limit reached. Set GITHUB_TOKEN to raise the limit."
            )
            break
        if r.status_code != 200:
            warnings.append(f"GitHub returned HTTP {r.status_code} for {q!r}")
            continue

        for item in r.json().get("items", []):
            url = item.get("html_url")
            if not url or url in seen:
                continue
            seen.append(url)

            haystack = " ".join(
                [
                    item.get("name") or "",
                    item.get("full_name") or "",
                    item.get("description") or "",
                ]
            ).lower()
            score = sum(1 for t in tokens if t in haystack)
            score += int((item.get("stargazers_count") or 0) > 0)
            scored.append((score, url))
        time.sleep(1)

    scored.sort(key=lambda x: -x[0])
    # Keep only relevant hits (at least one token match)
    return [u for s, u in scored if s > 0], warnings


def search_openalex_pdfs(kg_name: str, project: Optional[str]) -> Tuple[List[str], List[str]]:
    """
    Search OpenAlex for academic works mentioning the KG; return PDF URLs.
    """
    core = extract_core_name(kg_name)
    short = extract_short_name(kg_name)

    queries: List[str] = []
    if project:
        queries.append(f"{short} {project}")
        if short.lower() != core.lower():
            queries.append(f"{core} {project}")
    else:
        queries.append(short)
        if short.lower() != core.lower():
            queries.append(core)
    queries = list(dict.fromkeys(queries))[:3]

    params_base = {"per_page": 10}
    mailto = os.environ.get("OPENALEX_MAILTO")
    if mailto:
        params_base["mailto"] = mailto

    pdfs: List[str] = []
    warnings: List[str] = []
    # Use non-generic tokens for relevance; require any to appear in title OR abstract.
    tokens = _name_tokens(kg_name, project or "")

    for q in queries:
        params = {**params_base, "search": q}
        try:
            r = requests.get(
                OPENALEX_API,
                params=params,
                headers={"User-Agent": USER_AGENT},
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException as e:
            warnings.append(f"OpenAlex request failed for {q!r}: {e}")
            continue

        if r.status_code != 200:
            warnings.append(f"OpenAlex returned HTTP {r.status_code} for {q!r}")
            continue

        for w in r.json().get("results", []):
            title = (w.get("title") or "").lower()
            # Reconstruct abstract from OpenAlex inverted index if present.
            abstract_idx = w.get("abstract_inverted_index") or {}
            abstract = " ".join(abstract_idx.keys()).lower() if abstract_idx else ""
            haystack = f"{title} {abstract}"

            if tokens and not any(t in haystack for t in tokens):
                continue

            pdf_url = None
            primary = w.get("primary_location") or {}
            if isinstance(primary, dict):
                pdf_url = primary.get("pdf_url")
            if not pdf_url:
                oa = w.get("open_access") or {}
                if isinstance(oa, dict):
                    pdf_url = oa.get("oa_url")
            if not pdf_url:
                continue
            if pdf_url not in pdfs:
                pdfs.append(pdf_url)

        time.sleep(0.5)

    return pdfs, warnings


def search_brave_docs(kg_name: str, project: Optional[str]) -> Tuple[List[str], List[str]]:
    """
    Search the Brave Web Search API for documentation-style pages. Requires
    BRAVE_API_KEY. Returns (urls, warnings).
    """
    api_key = os.environ.get("BRAVE_API_KEY")
    if not api_key:
        return [], [
            "BRAVE_API_KEY not set; skipping web/docs search. "
            "Get a free key at https://api.search.brave.com/."
        ]

    core = extract_core_name(kg_name)
    short = extract_short_name(kg_name)
    queries: List[str] = []
    if project:
        queries.append(f'"{short}" "{project}" SPARQL')
        queries.append(f'"{short}" "{project}" documentation')
    else:
        queries.append(f'"{short}" SPARQL')
        queries.append(f'"{short}" ontology documentation')
    if short.lower() != core.lower():
        queries.append(f'"{core}"')
    queries = list(dict.fromkeys(queries))[:3]

    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": api_key,
        "User-Agent": USER_AGENT,
    }

    tokens = _name_tokens(kg_name, project or "")
    doc_hints = (
        "doc",
        "documentation",
        "wiki",
        "sparql",
        "endpoint",
        "ontology",
        "api",
        "query",
    )

    urls: List[str] = []
    warnings: List[str] = []

    for q in queries:
        try:
            r = requests.get(
                BRAVE_API,
                params={"q": q, "count": 10},
                headers=headers,
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException as e:
            warnings.append(f"Brave request failed for {q!r}: {e}")
            continue

        if r.status_code == 401:
            warnings.append("Brave API key rejected (HTTP 401).")
            break
        if r.status_code == 429:
            warnings.append("Brave rate-limited (HTTP 429); stopping.")
            break
        if r.status_code != 200:
            warnings.append(f"Brave returned HTTP {r.status_code} for {q!r}")
            continue

        results = (r.json().get("web") or {}).get("results") or []
        for item in results:
            url = item.get("url")
            if not url:
                continue
            domain = urlparse(url).netloc.lower()
            if domain in SKIP_DOMAINS_FOR_DOCS:
                continue
            if url.lower().endswith(".pdf"):
                continue  # PDFs handled by OpenAlex
            haystack = " ".join(
                [item.get("title") or "", item.get("description") or "", url]
            ).lower()
            if tokens and not any(t in haystack for t in tokens):
                continue
            if any(h in haystack for h in doc_hints) and url not in urls:
                urls.append(url)

        time.sleep(0.5)

    return urls, warnings


def discover_kg_sources(kg_name: str, project: Optional[str] = None) -> KGSourceDiscovery:
    discovery = KGSourceDiscovery(kg_name=kg_name, project=project)

    print("Searching GitHub for repositories…", file=sys.stderr)
    repos, w1 = search_github_repos(kg_name, project)
    discovery.github_repos = repos[:5]
    discovery.warnings.extend(w1)

    print("Searching OpenAlex for academic papers…", file=sys.stderr)
    pdfs, w2 = search_openalex_pdfs(kg_name, project)
    discovery.pdf_papers = pdfs[:5]
    discovery.warnings.extend(w2)

    print("Searching Brave for documentation websites…", file=sys.stderr)
    docs, w3 = search_brave_docs(kg_name, project)
    discovery.docs_websites = docs[:5]
    discovery.warnings.extend(w3)

    # Record the human-readable backends used (helpful for the report)
    discovery.search_queries = [
        f"GitHub Search API (kg='{kg_name}', project='{project or ''}')",
        f"OpenAlex API (kg='{kg_name}', project='{project or ''}')",
        f"Brave Web Search API (kg='{kg_name}', project='{project or ''}')",
    ]
    return discovery


def generate_kg_id(kg_name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", kg_name).strip("_").lower()


def generate_yaml_entry(discovery: KGSourceDiscovery) -> Dict[str, Any]:
    kg_id = generate_kg_id(discovery.kg_name)
    entry: Dict[str, Any] = {
        "kg_id": kg_id,
        "name": discovery.kg_name,
        "project": discovery.project,
        "description_hint": (
            f"Knowledge graph for {discovery.kg_name}."
            + (f" Part of the {discovery.project} project." if discovery.project else "")
        ),
    }
    if discovery.github_repos:
        entry["repos"] = discovery.github_repos[:5]
    if discovery.docs_websites or discovery.pdf_papers:
        docs_list: List[str] = []
        docs_list.extend(discovery.docs_websites[:3])
        docs_list.extend(discovery.pdf_papers[:3])
        if docs_list:
            entry["docs"] = docs_list
    entry["priority"] = "medium"
    entry["notes"] = f"Auto-discovered sources. KG ID: {kg_id}"
    return entry


def print_discovery_report(discovery: KGSourceDiscovery) -> None:
    print("\n" + "=" * 60)
    print(f"Knowledge Graph: {discovery.kg_name}")
    if discovery.project:
        print(f"Project: {discovery.project}")
    print("=" * 60)

    print(f"\nGitHub Repositories ({len(discovery.github_repos)}):")
    if discovery.github_repos:
        for i, repo in enumerate(discovery.github_repos, 1):
            print(f"  {i}. {repo}")
    else:
        print("  None found")

    print(f"\nDocumentation Websites ({len(discovery.docs_websites)}):")
    if discovery.docs_websites:
        for i, url in enumerate(discovery.docs_websites, 1):
            print(f"  {i}. {url}")
    else:
        print("  None found")

    print(f"\nAcademic Papers (PDFs) ({len(discovery.pdf_papers)}):")
    if discovery.pdf_papers:
        for i, url in enumerate(discovery.pdf_papers, 1):
            print(f"  {i}. {url}")
    else:
        print("  None found")

    print("\nBackends:")
    for i, q in enumerate(discovery.search_queries, 1):
        print(f"  {i}. {q}")

    if discovery.warnings:
        print("\nWarnings:")
        for w in discovery.warnings:
            print(f"  - {w}")


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "Usage: python discover_kg_sources.py --name '<KG Name>' "
            "[--project '<Project Name>'] [--yaml]"
        )
        print(
            "Example: python discover_kg_sources.py --name 'MUSOW Knowledge Graph' "
            "--project 'Polifonia' --yaml"
        )
        print("\nEnvironment variables:")
        print("  GITHUB_TOKEN       (optional) raise GitHub rate limit")
        print("  OPENALEX_MAILTO    (optional) email for OpenAlex polite pool")
        print("  BRAVE_API_KEY      (optional) enables web/docs search via Brave")
        sys.exit(1)

    kg_name: Optional[str] = None
    project: Optional[str] = None
    output_yaml = False

    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "--name" and i + 1 < len(sys.argv):
            kg_name = sys.argv[i + 1]
            i += 2
        elif arg == "--project" and i + 1 < len(sys.argv):
            project = sys.argv[i + 1]
            i += 2
        elif arg == "--yaml":
            output_yaml = True
            i += 1
        else:
            i += 1

    if not kg_name:
        print("Error: --name is required")
        sys.exit(1)

    print(f"\nDiscovering sources for: {kg_name}")
    if project:
        print(f"Project context: {project}")

    discovery = discover_kg_sources(kg_name, project)
    print_discovery_report(discovery)

    if output_yaml:
        yaml_entry = generate_yaml_entry(discovery)
        print("\n" + "=" * 60)
        print("Suggested YAML Entry for seeds.yaml:")
        print("=" * 60)
        print(yaml.dump({"kgs": [yaml_entry]}, default_flow_style=False, sort_keys=False))


if __name__ == "__main__":
    main()
