"""Refresh the tracked English EuroSciVoc expertise-search snapshot."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date
import os
from pathlib import Path

import requests
import yaml


ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
SCHEME = "http://data.europa.eu/8mn/euroscivoc/40c0f173-baa3-48a3-9fe6-d6e8fb366a00"
SOURCE_ID = "euroscivoc-reference"


def _query(query: str) -> list[dict[str, dict[str, str]]]:
    response = requests.get(
        ENDPOINT,
        params={"query": query},
        headers={
            "Accept": "application/sparql-results+json",
            "User-Agent": "Musparql-EuroSciVoc-snapshot/1",
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["results"]["bindings"]


def refresh(path: Path) -> None:
    version_rows = _query(
        "PREFIX owl: <http://www.w3.org/2002/07/owl#> "
        f"SELECT ?version WHERE {{ <{SCHEME}> owl:versionInfo ?version }}"
    )
    if len(version_rows) != 1:
        raise ValueError("EuroSciVoc scheme did not expose exactly one version")
    version = version_rows[0]["version"]["value"]
    rows = _query(
        "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
        "SELECT ?concept ?label ?alt WHERE { "
        f"?concept skos:inScheme <{SCHEME}> ; skos:prefLabel ?label . "
        'FILTER(LANGMATCHES(LANG(?label), "en")) '
        "OPTIONAL { ?concept skos:altLabel ?alt . "
        'FILTER(LANGMATCHES(LANG(?alt), "en")) } } '
        "ORDER BY LCASE(STR(?label)) ?concept"
    )
    concepts: dict[str, dict[str, object]] = {}
    alternatives: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        uri = row["concept"]["value"]
        label = row["label"]["value"]
        concepts.setdefault(
            uri,
            {
                "suggestion_id": "euroscivoc-" + uri.rsplit("/", 1)[-1],
                "preferred_label": label,
                "alternative_labels": [],
                "language": "en",
                "broader_suggestion_ids": [],
                "source_id": SOURCE_ID,
                "vocabulary_concept_uri": uri,
                "vocabulary_version": version,
            },
        )
        if row.get("alt") and row["alt"]["value"] != label:
            alternatives[uri].add(row["alt"]["value"])
    if len(concepts) < 1000:
        raise ValueError("EuroSciVoc query returned an unexpectedly small concept set")
    for uri, values in alternatives.items():
        concepts[uri]["alternative_labels"] = sorted(values, key=str.casefold)

    payload = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    source = next(item for item in payload["sources"] if item["source_id"] == SOURCE_ID)
    source["source_version"] = version
    source["usage"] = "suggestion_entries"
    source["note"] = (
        "English preferred and alternative labels imported from the official "
        "Publications Office SPARQL endpoint for local, reproducible search."
    )
    project_terms = [
        item for item in payload["suggestions"] if item["source_id"] != SOURCE_ID
    ]
    protected_labels = {
        label.casefold()
        for item in project_terms
        for label in (item["preferred_label"], *item["alternative_labels"])
    }
    euroscivoc = [
        item
        for item in concepts.values()
        if item["preferred_label"].casefold() not in protected_labels
    ]
    euroscivoc.sort(key=lambda item: (item["preferred_label"].casefold(), item["suggestion_id"]))
    payload["snapshot_id"] = f"musparql-expertise-domains-euroscivoc-{version}"
    payload["created_on"] = date.today().isoformat()
    payload["suggestions"] = project_terms + euroscivoc
    rendered = yaml.safe_dump(payload, allow_unicode=True, sort_keys=False, width=100)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    refresh(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
