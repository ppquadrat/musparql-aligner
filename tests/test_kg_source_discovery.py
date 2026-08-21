from __future__ import annotations

import json

import pytest

from musparql.kg_source_discovery import (
    Candidate,
    DiscoveryReport,
    extract_short_name,
    group_publication_locations,
    main,
    normalise_aliases,
    search_github,
    shortlist_candidates,
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
    assert by_url["https://github.com/example/jazz-ontology"].relevance_score > by_url["https://github.com/example/unrelated"].relevance_score
    assert "exact KG name in title" in by_url["https://github.com/example/jazz-ontology"].ranking_reasons
    assert by_url["https://github.com/example/unrelated"].review_status == "low_lexical_relevance"
    assert len(by_url["https://github.com/example/jazz-ontology"].origins) == 2
    assert by_url["https://github.com/example/jazz-ontology"].origins[0]["rank"] == 1
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
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Saved unverified discovery report" in captured.err
    with pytest.raises(SystemExit, match="refusing to overwrite"):
        main(["--name", "Synthetic KG", "--output", str(output)])
    with pytest.raises(SystemExit, match="must be a .json"):
        main(["--name", "Synthetic KG", "--output", str(tmp_path / "seeds.yaml")])


def test_cli_can_explicitly_print_a_saved_report(monkeypatch, tmp_path, capsys):
    report = DiscoveryReport(
        kg_name="Synthetic KG",
        project=None,
        created_at="2026-08-21T00:00:00+00:00",
    )
    monkeypatch.setattr("musparql.kg_source_discovery.discover_sources", lambda *args, **kwargs: report)

    assert main([
        "--name", "Synthetic KG", "--output", str(tmp_path / "report.json"), "--also-print"
    ]) == 0
    assert "UNVERIFIED" in capsys.readouterr().out


def test_name_normalisation_preserves_distinctive_part():
    assert extract_short_name("Jazz Ontology") == "Jazz"
    assert extract_short_name("MUSOW Knowledge Graph (Polifonia)") == "MUSOW"


def test_shortlist_caps_each_source_kind_and_reports_omissions():
    candidates = [
        Candidate(
            url=f"https://example.test/{kind}/{number}",
            source_kind=kind,
            title=f"Candidate {number}",
            description="",
            relevance_score=number,
            matched_tokens=[],
            ranking_reasons=[],
            review_status="unverified_candidate",
            origins=[{"backend": "synthetic", "query": "synthetic", "rank": 7 - number}],
        )
        for kind in ("publication", "repository")
        for number in range(7)
    ]

    shown, counts = shortlist_candidates(candidates, 5)

    assert len(shown) == 10
    assert counts == {
        "publication": {"found": 7, "shown": 5, "omitted": 2, "locations": 7, "grouped_duplicates": 0},
        "repository": {"found": 7, "shown": 5, "omitted": 2, "locations": 7, "grouped_duplicates": 0},
    }
    assert [item.relevance_score for item in shown[:5]] == [6, 5, 4, 3, 2]


def test_aliases_are_normalised_limited_and_added_to_queries(monkeypatch):
    assert normalise_aliases([" DTL1000 ", "dtl1000", "100 years of jazz"]) == [
        "DTL1000", "100 years of jazz"
    ]
    with pytest.raises(ValueError, match="at most 5"):
        normalise_aliases([f"alias-{number}" for number in range(6)])

    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    session = GitHubSession()
    _candidates, queries, _warnings = search_github(
        "Jazz Ontology", None, aliases=["DTL1000"], per_query=20, session=session
    )
    assert [record.query for record in queries][-1] == '"DTL1000" in:name,description,readme'


def test_publication_locations_group_by_doi_or_exact_long_title():
    title = "The Jazz Ontology: A semantic model and large-scale RDF repositories for jazz"
    canonical = Candidate(
        url="https://doi.org/10.1016/j.websem.2022.100735",
        source_kind="publication",
        title=title,
        description="",
        relevance_score=101,
        matched_tokens=["jazz"],
        ranking_reasons=["exact KG name in title"],
        review_status="unverified_candidate",
        origins=[{"backend": "openalex", "query": "Jazz Ontology", "rank": 3}],
        metadata={"doi": "https://doi.org/10.1016/j.websem.2022.100735"},
    )
    title_copy = Candidate(
        url="https://example.test/institutional-copy",
        source_kind="web_document",
        title=title + " - Institutional Repository",
        description="",
        relevance_score=101,
        matched_tokens=["jazz"],
        ranking_reasons=["exact KG name in title"],
        review_status="unverified_candidate",
        origins=[{"backend": "brave", "query": '"Jazz Ontology"', "rank": 2}],
    )
    doi_copy = Candidate(
        url="https://example.test/doi/10.1016/j.websem.2022.100735",
        source_kind="web_document",
        title="Publisher index entry",
        description="",
        relevance_score=1,
        matched_tokens=["jazz"],
        ranking_reasons=["matched token: jazz"],
        review_status="unverified_candidate",
        origins=[{"backend": "brave", "query": '"Jazz Ontology"', "rank": 4}],
    )
    unrelated = Candidate(
        url="https://example.test/unrelated",
        source_kind="publication",
        title="A different long publication title about jazz performance practice",
        description="",
        relevance_score=1,
        matched_tokens=["jazz"],
        ranking_reasons=["matched token: jazz"],
        review_status="unverified_candidate",
        origins=[{"backend": "openalex", "query": "Jazz", "rank": 1}],
    )

    grouped = group_publication_locations([canonical, title_copy, doi_copy, unrelated])

    assert len(grouped) == 2
    jazz = next(item for item in grouped if item.url.startswith("https://doi.org/"))
    assert len(jazz.locations) == 3
    assert jazz.duplicate_grouping == [
        "confirmed shared DOI or OpenAlex identifier",
        "probable duplicate: exact normalized long title",
    ]
    assert {location["url"] for location in jazz.locations} == {
        canonical.url, title_copy.url, doi_copy.url,
    }
