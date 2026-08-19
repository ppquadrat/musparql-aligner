from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import (
    ExpertiseDomain,
    KgSeedFamiliarityScope,
    KgSeedReviewDomain,
    KgSeedSnapshot,
    Reviewer,
    ReviewerDomainExpertise,
    ReviewerExperience,
    ReviewerKgDomainAssessment,
    ReviewerLanguage,
    ReviewerResourceFamiliarityAssessment,
)
from musparql.web import create_app
from musparql.web.auth import timestamp, utc_now
from musparql.web.email import SyntheticEmailSender


ROOT = Path(__file__).resolve().parents[1]
SECRET = "synthetic-phase5-secret-that-is-at-least-32-bytes"
OWNER_ID = "reviewer-0001"
REVIEWER_ID = "reviewer-0042"
OTHER_ID = "reviewer-0043"


def _reviewer(reviewer_id: str, email: str, *, notice: bool = False) -> Reviewer:
    now = timestamp(utc_now())
    return Reviewer(
        id=reviewer_id,
        name=f"Synthetic {reviewer_id}",
        affiliation="Synthetic Institute",
        email_display=email,
        email_normalized=email,
        status="active",
        created_at=now,
        updated_at=now,
        privacy_notice_version="synthetic-phase5-v1" if notice else None,
        privacy_notice_acknowledged_at=now if notice else None,
    )


def _complete_profile(session, reviewer_id: str) -> None:
    now = timestamp(utc_now())
    session.add(
        ReviewerExperience(
            reviewer_id=reviewer_id,
            kg_ontology_experience="regular",
            sparql_experience="regular",
            nlp_llm_experience="regular",
            assessed_at=now,
        )
    )
    session.add(
        ReviewerLanguage(
            reviewer_id=reviewer_id,
            language_tag="en",
            level="native",
            first_asserted_at=now,
            updated_at=now,
        )
    )
    domain_id = f"synthetic-profile-domain-{reviewer_id}"
    session.add(
        ExpertiseDomain(
            id=domain_id,
            entered_label="Synthetic profile expertise",
            normalized_label=f"synthetic profile expertise {reviewer_id}",
            vocabulary_name=None,
            vocabulary_concept_uri=None,
            vocabulary_version=None,
            created_by="reviewer",
        )
    )
    session.add(
        ReviewerDomainExpertise(
            id=f"synthetic-profile-assertion-{reviewer_id}",
            reviewer_id=reviewer_id,
            domain_id=domain_id,
            expertise_level="working",
            asserted_at=now,
            supersedes_id=None,
        )
    )


def _bundle(path: Path, *, reviewer_id: str | None = None) -> None:
    payload = {
        "schema": "musparql.review-bundle.v2",
        "mode": "initial",
        "dataset_id": "synthetic-phase5-dataset",
        "built_at": "2026-08-19T12:00:00Z",
        "holdout_input_policy": "no_holdout",
        "record_count": 1,
        "records": [
            {
                "review_id": "synthetic-kg::synthetic-query::one",
                "kg_id": "synthetic-kg",
            }
        ],
    }
    if reviewer_id is not None:
        payload["reviewer_id"] = reviewer_id
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "window.REVIEW_DATA = " + json.dumps(payload) + ";\n", encoding="utf-8"
    )


def csrf(client) -> str:
    cookie = client.get_cookie("musparql_csrf", path="/")
    if cookie is None:
        client.get("/")
        cookie = client.get_cookie("musparql_csrf", path="/")
    assert cookie is not None
    return cookie.value


def login(client, app, sender, email: str) -> None:
    position = sender.position()
    response = client.post(
        "/auth/login", data={"csrf_token": csrf(client), "email": email}
    )
    assert response.status_code == 302
    message = sender.wait_for("login_code", email, after_index=position)
    app.extensions["musparql_email_dispatcher"].wait_for_idle()
    response = client.post(
        "/auth/verify", data={"csrf_token": csrf(client), "code": message.value}
    )
    assert response.status_code == 302


def _create_assignment(owner, *, bundle_name: str = "neutral.js") -> str:
    response = owner.post(
        "/owner/assignments",
        data={
            "csrf_token": csrf(owner),
            "reviewer_id": REVIEWER_ID,
            "mode": "initial",
            "bundle_name": bundle_name,
            "processing_recipe": "validate_initial_review",
            "seed_key": "synthetic-kg|synthetic-seed-v1",
        },
    )
    assert response.status_code == 302
    assert "result=assignment-" in response.location
    return response.location.split("result=", 1)[1]


def _assessment_form(client) -> dict[str, str]:
    return {
        "csrf_token": csrf(client),
        "domain_level": "advanced",
        "familiarity_level": "worked",
        "confirmed": "yes",
    }


def test_owner_creation_assessment_gate_attribution_and_isolation(tmp_path: Path) -> None:
    database_path = tmp_path / "phase5.sqlite3"
    bundle_root = tmp_path / "bundles"
    _bundle(bundle_root / "neutral.js")
    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    now = timestamp(utc_now())
    with sessions.begin() as session:
        session.add(_reviewer(OWNER_ID, "owner@example.invalid"))
        session.add(_reviewer(REVIEWER_ID, "reviewer@example.invalid", notice=True))
        session.add(_reviewer(OTHER_ID, "other@example.invalid", notice=True))
        _complete_profile(session, REVIEWER_ID)
        _complete_profile(session, OTHER_ID)
        session.add(
            KgSeedSnapshot(
                kg_id="synthetic-kg",
                seed_version="synthetic-seed-v1",
                seed_digest="sha256:" + "a" * 64,
                previous_seed_digest=None,
                seed_json={"name": "Synthetic graph"},
            )
        )
        session.flush()
        session.add(
            KgSeedReviewDomain(
                kg_id="synthetic-kg",
                seed_version="synthetic-seed-v1",
                domain_id="synthetic-domain",
                label="Synthetic music domain",
                description="Fictional expertise prompt.",
            )
        )
        session.add(
            KgSeedFamiliarityScope(
                kg_id="synthetic-kg",
                seed_version="synthetic-seed-v1",
                scope_id="synthetic-resource",
                label="Synthetic Music Knowledge Graph",
                description="Fictional resource prompt.",
            )
        )
    engine.dispose()
    sender = SyntheticEmailSender()
    app = create_app(
        {
            "TESTING": True,
            "DATABASE_PATH": database_path,
            "APP_SECRET": SECRET,
            "OWNER_REVIEWER_ID": OWNER_ID,
            "COOKIE_SECURE": False,
            "EMAIL_SENDER": sender,
            "EXPERTISE_SUGGESTIONS_PATH": ROOT
            / "catalog/expertise_domain_suggestions.yaml",
            "ASSIGNMENT_BUNDLE_ROOT": bundle_root,
            "PRIVACY_NOTICE_VERSION": "synthetic-phase5-v1",
            "PRIVACY_NOTICE_BODY": "Synthetic notice. Do not enter real data.",
        }
    )
    try:
        owner = app.test_client()
        login(owner, app, sender, "owner@example.invalid")
        assignment_id = _create_assignment(owner)

        reviewer = app.test_client()
        login(reviewer, app, sender, "reviewer@example.invalid")
        assert assignment_id.encode() in reviewer.get("/").data
        page = reviewer.get(f"/assignments/{assignment_id}")
        assert page.status_code == 200
        assert b"Synthetic music domain" in page.data
        assert b"Fictional expertise prompt" in page.data
        assert reviewer.get(f"/assignments/{assignment_id}/bundle").status_code == 403

        other = app.test_client()
        login(other, app, sender, "other@example.invalid")
        assert other.get(f"/assignments/{assignment_id}").status_code == 404
        assert other.get(f"/assignments/{assignment_id}/bundle").status_code == 404

        response = reviewer.post(
            f"/assignments/{assignment_id}", data=_assessment_form(reviewer)
        )
        assert response.status_code == 302
        payload = reviewer.get(f"/assignments/{assignment_id}/bundle").get_json()
        assert payload["reviewer_id"] == REVIEWER_ID
        assert payload["assignment_id"] == assignment_id
        assert payload["bundle_digest"].startswith("sha256:")

        # A later assignment asks again, preselects prior values, and appends history.
        second_id = _create_assignment(owner)
        second_page = reviewer.get(f"/assignments/{second_id}")
        assert b'value="advanced" selected' in second_page.data
        assert b'value="worked" selected' in second_page.data
        assert reviewer.post(
            f"/assignments/{second_id}", data=_assessment_form(reviewer)
        ).status_code == 302

        check_engine = create_database_engine(database_path)
        check_sessions = session_factory(check_engine)
        try:
            with check_sessions() as session:
                domains = list(
                    session.scalars(
                        select(ReviewerKgDomainAssessment)
                        .where(ReviewerKgDomainAssessment.reviewer_id == REVIEWER_ID)
                        .order_by(ReviewerKgDomainAssessment.assessed_at)
                    )
                )
                familiarities = list(
                    session.scalars(
                        select(ReviewerResourceFamiliarityAssessment)
                        .where(
                            ReviewerResourceFamiliarityAssessment.reviewer_id
                            == REVIEWER_ID
                        )
                        .order_by(ReviewerResourceFamiliarityAssessment.assessed_at)
                    )
                )
                assert len(domains) == len(familiarities) == 2
                assert domains[1].previous_assessment_id == domains[0].id
                assert familiarities[1].previous_assessment_id == familiarities[0].id
        finally:
            check_engine.dispose()

        # File mutation after issue is detected before any data is served.
        _bundle(bundle_root / "neutral.js")
        with (bundle_root / "neutral.js").open("a", encoding="utf-8") as handle:
            handle.write(" \n")
        assert reviewer.get(f"/assignments/{assignment_id}/bundle").status_code == 409
    finally:
        app.extensions["musparql_email_dispatcher"].shutdown()
        app.extensions["musparql_engine"].dispose()


def test_assignment_creation_rejects_identity_paths_and_unfiltered_bundles(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundles"
    _bundle(bundle_root / "identified.js", reviewer_id=REVIEWER_ID)
    holdout_payload = {
        "schema": "musparql.review-bundle.v2",
        "mode": "initial",
        "dataset_id": "synthetic-holdout-dataset",
        "holdout_input_policy": "no_holdout",
        "record_count": 1,
        "records": [{"kg_id": "synthetic-kg", "split": "holdout"}],
    }
    (bundle_root / "marked.json").write_text(
        json.dumps(holdout_payload), encoding="utf-8"
    )
    service_path = tmp_path / "database.sqlite3"
    upgrade_database(service_path)
    engine = create_database_engine(service_path)
    sessions = session_factory(engine)
    from musparql.web.assignments import AssignmentService

    service = AssignmentService(sessions, bundle_root)
    for bundle_name in (
        "identified.js",
        "marked.json",
        "../identified.js",
        str(bundle_root / "identified.js"),
    ):
        try:
            service.create(
                reviewer_id=REVIEWER_ID,
                mode="initial",
                bundle_name=bundle_name,
                processing_recipe="validate_initial_review",
                seed_keys=["synthetic-kg|synthetic-seed-v1"],
            )
        except ValueError:
            pass
        else:
            raise AssertionError(f"unsafe bundle accepted: {bundle_name}")
    engine.dispose()
