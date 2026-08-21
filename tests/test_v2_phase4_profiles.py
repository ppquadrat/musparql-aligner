from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import select
from werkzeug.datastructures import MultiDict

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import (
    ExpertiseDomain,
    Reviewer,
    ReviewerDomainExpertise,
    ReviewerExperience,
    ReviewerLanguage,
)
from musparql.web import create_app
from musparql.web.auth import timestamp, utc_now
from musparql.web.email import SyntheticEmailSender


ROOT = Path(__file__).resolve().parents[1]
SECRET = "synthetic-phase4-secret-that-is-at-least-32-bytes"
OWNER_ID = "reviewer-0001"
REVIEWER_ID = "reviewer-0042"


def reviewer(reviewer_id: str, email: str, *, status: str = "active") -> Reviewer:
    now = timestamp(utc_now())
    return Reviewer(
        id=reviewer_id,
        name=f"Synthetic {reviewer_id}",
        affiliation="",
        email_display=email,
        email_normalized=email,
        status=status,
        created_at=now,
        updated_at=now,
        privacy_notice_version=None,
        privacy_notice_acknowledged_at=None,
    )


@pytest.fixture
def phase4_app(tmp_path: Path):
    database_path = tmp_path / "phase4.sqlite3"
    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        session.add(reviewer(OWNER_ID, "owner@example.invalid"))
        session.add(reviewer(REVIEWER_ID, "reviewer@example.invalid"))
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
            "EXPERTISE_SUGGESTIONS_PATH": ROOT / "catalog/expertise_domain_suggestions.yaml",
            "LANGUAGE_OPTIONS_PATH": ROOT / "catalog/language_options.json",
            "PRIVACY_NOTICE_VERSION": "synthetic-phase4-v1",
            "PRIVACY_NOTICE_BODY": "Synthetic Phase 4 notice. Do not enter real data.",
        }
    )
    yield app, sender, database_path
    app.extensions["musparql_email_dispatcher"].shutdown()
    app.extensions["musparql_engine"].dispose()


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


def profile_form(client, *, domain_level: str = "advanced", name: str = "Synthetic Reviewer") -> MultiDict:
    return MultiDict(
        [
            ("csrf_token", csrf(client)),
            ("notice_acknowledged", "yes"),
            ("name", name),
            ("affiliation", "Synthetic Research Institute"),
            ("kg_ontology_experience", "regular"),
            ("sparql_experience", "expert"),
            ("nlp_llm_experience", "occasional"),
            ("language_tag", "en"),
            ("language_level", "native"),
            ("language_tag", "fr"),
            ("language_level", "advanced"),
            ("new_domain_label", "Computational musicology"),
            ("new_domain_level", domain_level),
        ]
    )


def test_incomplete_reviewer_is_redirected_and_completes_onboarding(phase4_app) -> None:
    app, sender, database_path = phase4_app
    client = app.test_client()
    login(client, app, sender, "reviewer@example.invalid")

    response = client.get("/")
    assert response.status_code == 302
    assert response.location.endswith("/profile")
    page = client.get("/profile")
    assert b"Complete your profile" in page.data
    assert b"Synthetic Phase 4 notice" in page.data
    assert b"Computational musicology" in page.data
    assert b"Add as written" not in page.data
    assert b"special-category personal data" not in page.data
    assert b"English (en)" in page.data
    assert b'id="add-language"' in page.data
    assert b'id="add-domain"' in page.data
    assert b"1064 EuroSciVoc concepts locally" in page.data
    assert b'id="domain-suggestion-data"' in page.data
    assert b'<datalist' not in page.data
    assert b'class="domain-search"' in page.data
    assert b'autocomplete="off"' in page.data
    assert b"2 \xe2\x80\x94 Working knowledge" in page.data

    response = client.post("/profile", data=profile_form(client))
    assert response.status_code == 302
    assert response.location.endswith("/profile?saved=yes")
    assert b"Your authenticated Musparql session is active" in client.get("/").data

    owner = app.test_client()
    login(owner, app, sender, "owner@example.invalid")
    assert b"Complete" in owner.get("/owner/reviewers").data

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            stored = session.get(Reviewer, REVIEWER_ID)
            assert stored is not None
            assert stored.name == "Synthetic Reviewer"
            assert stored.privacy_notice_version == "synthetic-phase4-v1"
            assert stored.privacy_notice_acknowledged_at is not None
            experience = session.get(ReviewerExperience, REVIEWER_ID)
            assert experience is not None and experience.sparql_experience == "expert"
            languages = list(
                session.scalars(
                    select(ReviewerLanguage).where(ReviewerLanguage.reviewer_id == REVIEWER_ID)
                )
            )
            assert {(item.language_tag, item.level) for item in languages} == {
                ("en", "native"),
                ("fr", "advanced"),
            }
            assertion = session.scalar(
                select(ReviewerDomainExpertise).where(
                    ReviewerDomainExpertise.reviewer_id == REVIEWER_ID
                )
            )
            domain = session.get(ExpertiseDomain, assertion.domain_id)
            assert domain is not None
            assert domain.entered_label == "Computational musicology"
            assert domain.normalized_label == "computational musicology"
            assert domain.vocabulary_name is None
    finally:
        engine.dispose()


def test_profile_correction_appends_domain_history_and_preserves_first_language_date(
    phase4_app,
) -> None:
    app, sender, database_path = phase4_app
    client = app.test_client()
    login(client, app, sender, "reviewer@example.invalid")
    client.post("/profile", data=profile_form(client, domain_level="working"))

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        first = session.scalar(
            select(ReviewerDomainExpertise).where(
                ReviewerDomainExpertise.reviewer_id == REVIEWER_ID
            )
        )
        first_language = session.get(ReviewerLanguage, (REVIEWER_ID, "en"))
        assert first is not None and first_language is not None
        domain_id = first.domain_id
        first_id = first.id
        first_language_date = first_language.first_asserted_at
    engine.dispose()

    corrected = profile_form(client, name="Corrected Synthetic Reviewer")
    corrected.setlist("notice_acknowledged", [])
    corrected.setlist("new_domain_label", [])
    corrected.setlist("new_domain_level", [])
    corrected.add("existing_domain_id", domain_id)
    corrected.add("existing_assertion_id", first_id)
    corrected.add("existing_domain_level", "expert")
    response = client.post("/profile", data=corrected)
    assert response.status_code == 302

    stale = profile_form(client, name="Stale Synthetic Reviewer")
    stale.setlist("notice_acknowledged", [])
    stale.setlist("new_domain_label", [])
    stale.setlist("new_domain_level", [])
    stale.add("existing_domain_id", domain_id)
    stale.add("existing_assertion_id", first_id)
    stale.add("existing_domain_level", "basic")
    response = client.post("/profile", data=stale)
    assert response.status_code == 200
    assert b"Profile not saved: The profile changed in another request" in response.data
    assert b"Stale Synthetic Reviewer" in response.data

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            history = list(
                session.scalars(
                    select(ReviewerDomainExpertise)
                    .where(ReviewerDomainExpertise.reviewer_id == REVIEWER_ID)
                    .order_by(ReviewerDomainExpertise.asserted_at)
                )
            )
            assert len(history) == 2
            assert history[1].supersedes_id == first_id
            assert history[1].expertise_level == "expert"
            language = session.get(ReviewerLanguage, (REVIEWER_ID, "en"))
            assert language is not None and language.first_asserted_at == first_language_date
            assert session.get(Reviewer, REVIEWER_ID).name == "Corrected Synthetic Reviewer"
    finally:
        engine.dispose()


def test_invalid_or_stale_profile_is_atomic(phase4_app) -> None:
    app, sender, database_path = phase4_app
    client = app.test_client()
    login(client, app, sender, "reviewer@example.invalid")
    invalid = profile_form(client, name="Must Not Persist")
    invalid.add("existing_domain_id", "domain-not-owned")
    invalid.add("existing_assertion_id", "assertion-not-owned")
    invalid.add("existing_domain_level", "expert")
    response = client.post("/profile", data=invalid)
    assert response.status_code == 200
    assert b"Profile not saved: The submitted domain set is stale or invalid" in response.data
    assert b"Must Not Persist" in response.data

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            stored = session.get(Reviewer, REVIEWER_ID)
            assert stored is not None and stored.name != "Must Not Persist"
            assert session.scalar(select(ReviewerExperience)) is None
            assert session.scalar(select(ReviewerDomainExpertise)) is None
    finally:
        engine.dispose()


def test_identity_erasure_removes_profile_and_reviewer_only_domain(phase4_app) -> None:
    app, sender, database_path = phase4_app
    client = app.test_client()
    login(client, app, sender, "reviewer@example.invalid")
    assert client.post("/profile", data=profile_form(client)).status_code == 302

    current = app.extensions["musparql_profiles"].load(REVIEWER_ID).domains[0]
    corrected = profile_form(client)
    corrected.setlist("notice_acknowledged", [])
    corrected.setlist("new_domain_label", [])
    corrected.setlist("new_domain_level", [])
    corrected.add("existing_domain_id", current.domain_id)
    corrected.add("existing_assertion_id", current.assertion_id)
    corrected.add("existing_domain_level", "expert")
    assert client.post("/profile", data=corrected).status_code == 302

    app.extensions["musparql_auth"].delete_reviewer_identity(OWNER_ID, REVIEWER_ID)

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            stored = session.get(Reviewer, REVIEWER_ID)
            assert stored is not None and stored.status == "withdrawn"
            assert session.get(ReviewerExperience, REVIEWER_ID) is None
            assert session.scalar(
                select(ReviewerLanguage).where(ReviewerLanguage.reviewer_id == REVIEWER_ID)
            ) is None
            assert session.scalar(
                select(ReviewerDomainExpertise).where(
                    ReviewerDomainExpertise.reviewer_id == REVIEWER_ID
                )
            ) is None
            assert session.scalar(select(ExpertiseDomain)) is None
    finally:
        engine.dispose()


def test_notice_version_change_requires_new_acknowledgement(phase4_app) -> None:
    app, sender, _database_path = phase4_app
    client = app.test_client()
    login(client, app, sender, "reviewer@example.invalid")
    client.post("/profile", data=profile_form(client))
    service = app.extensions["musparql_profiles"]
    service.notice_version = "synthetic-phase4-v2"
    app.config["PRIVACY_NOTICE_VERSION"] = "synthetic-phase4-v2"
    app.config["PRIVACY_NOTICE_BODY"] = "Updated synthetic notice."

    assert client.get("/").status_code == 302
    page = client.get("/profile")
    assert b"Updated synthetic notice" in page.data
    current = service.load(REVIEWER_ID)
    form = profile_form(client)
    form.setlist("notice_acknowledged", [])
    form.setlist("new_domain_label", [])
    form.setlist("new_domain_level", [])
    form.add("existing_domain_id", current.domains[0].domain_id)
    form.add("existing_assertion_id", current.domains[0].assertion_id)
    form.add("existing_domain_level", current.domains[0].expertise_level)
    response = client.post("/profile", data=form)
    assert response.status_code == 200
    assert b"Profile not saved: The current privacy notice must be acknowledged" in response.data


def test_invalid_new_domain_preserves_all_submitted_profile_fields(phase4_app) -> None:
    app, sender, _database_path = phase4_app
    client = app.test_client()
    login(client, app, sender, "reviewer@example.invalid")
    form = profile_form(client, name="Retained Synthetic Reviewer")
    form.setlist("new_domain_label", ["Retained synthetic domain"])
    form.setlist("new_domain_level", [""])

    response = client.post("/profile", data=form)

    assert response.status_code == 200
    assert b"Profile not saved: Choose an expertise level for Retained synthetic domain" in response.data
    assert b'value="Retained Synthetic Reviewer"' in response.data
    assert b'value="Retained synthetic domain"' in response.data
    assert b'<option value="en" selected>English (en)</option>' in response.data
    assert b'<option value="native" selected>Native</option>' in response.data
    assert b'name="notice_acknowledged" value="yes" checked' in response.data


def test_language_snapshot_and_profile_javascript_are_available(phase4_app) -> None:
    app, sender, _database_path = phase4_app
    service = app.extensions["musparql_profiles"]
    assert len(service.language_options) >= 180
    assert service.euroscivoc_suggestion_count >= 1000

    client = app.test_client()
    login(client, app, sender, "reviewer@example.invalid")
    script = client.get("/static/profile.js")
    assert script.status_code == 200
    assert b"configureRepeatRows" in script.data
    assert b"configureDomainAutocomplete" in script.data
    assert b"domainMatches" in script.data

    stylesheet = client.get("/static/portal.css")
    assert stylesheet.status_code == 200
    assert b"minmax(0, 1fr)" in stylesheet.data
    assert b".domain-results" in stylesheet.data
    assert b'input[type="checkbox"] { width: auto; }' in stylesheet.data


def test_owner_sees_only_pseudonymous_completion_status(phase4_app) -> None:
    app, sender, _database_path = phase4_app
    owner = app.test_client()
    login(owner, app, sender, "owner@example.invalid")
    page = owner.get("/owner/reviewers")
    assert page.status_code == 200
    assert b"Awaiting onboarding" in page.data
    assert b"Not applicable" in page.data


def test_app_fails_closed_without_approved_notice(tmp_path: Path) -> None:
    database_path = tmp_path / "no-notice.sqlite3"
    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        session.add(reviewer(OWNER_ID, "owner@example.invalid"))
    engine.dispose()
    with pytest.raises(RuntimeError, match="controller-approved privacy notice"):
        create_app(
            {
                "TESTING": False,
                "DATABASE_PATH": database_path,
                "APP_SECRET": SECRET,
                "OWNER_REVIEWER_ID": OWNER_ID,
                "COOKIE_SECURE": False,
                "EMAIL_SENDER": SyntheticEmailSender(),
                "EXPERTISE_SUGGESTIONS_PATH": ROOT
                / "catalog/expertise_domain_suggestions.yaml",
                "PRIVACY_NOTICE_VERSION": None,
                "PRIVACY_NOTICE_BODY": None,
            }
        )
