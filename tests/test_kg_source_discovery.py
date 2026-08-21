from __future__ import annotations

import json

import pytest

from musparql.kg_source_discovery import (
    DiscoveryReport,
    extract_short_name,
    main,
    search_github,
)


class Response:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class GitHubSession:
    def __init__(self):
        self.calls = []

    def get(self, _url, **kwargs):
        self.calls.append(kwargs)
        return Response({"items": [
            {
                "html_url": "https://github.com/example/jazz-ontology",
                "full_name": "example/jazz-ontology",
                "description": "An RDF model for jazz performances",
                "stargazers_count": 3,
                "archived": False,
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "html_url": "https://github.com/example/unrelated",
                "full_name": "example/unrelated",
                "description": "A deliberately unrelated result",
                "stargazers_count": 0,
                "archived": False,
                "updated_at": "2025-01-01T00:00:00Z",
            },
        ]})


def test_github_report_retains_low_relevance_results_and_query_provenance(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    session = GitHubSession()
    candidates, queries, warnings = search_github(
        "Jazz Ontology", None, per_query=20, session=session
    )

    assert not warnings
    assert len(queries) == 2
    assert len(candidates) == 2
    by_url = {candidate.url: candidate for candidate in candidates}
    assert by_url["https://github.com/example/jazz-ontology"].review_status == "unverified_candidate"
    assert by_url["https://github.com/example/unrelated"].review_status == "low_lexical_relevance"
    assert len(by_url["https://github.com/example/jazz-ontology"].origins) == 2
    assert "Authorization" not in session.calls[0]["headers"]


def test_cli_saves_only_new_json_reports(monkeypatch, tmp_path, capsys):
    report = DiscoveryReport(
        kg_name="Synthetic KG",
        project=None,
        created_at="2026-08-21T00:00:00+00:00",
    )
    monkeypatch.setattr("musparql.kg_source_discovery.discover_sources", lambda *args, **kwargs: report)
    output = tmp_path / "report.json"

    assert main(["--name", "Synthetic KG", "--output", str(output)]) == 0
    assert json.loads(output.read_text())["authority"].startswith("unverified")
    assert "UNVERIFIED" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="refusing to overwrite"):
        main(["--name", "Synthetic KG", "--output", str(output)])
    with pytest.raises(SystemExit, match="must be a .json"):
        main(["--name", "Synthetic KG", "--output", str(tmp_path / "seeds.yaml")])


def test_name_normalisation_preserves_distinctive_part():
    assert extract_short_name("Jazz Ontology") == "Jazz"
    assert extract_short_name("MUSOW Knowledge Graph (Polifonia)") == "MUSOW"
