"""Phase 3 login, session, and owner-control HTTP routes."""
from __future__ import annotations

from dataclasses import replace
from functools import wraps
import json
from pathlib import Path
from typing import Any, Callable, TypeVar, cast

from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)

from .workshops import WorkshopAccessError, WorkshopUnavailable
from .assignments import has_current_consent


portal = Blueprint("portal", __name__)
View = TypeVar("View", bound=Callable[..., Any])


def login_required(view: View) -> View:
    @wraps(view)
    def wrapped(*args: Any, **kwargs: Any):
        if g.current_reviewer is None:
            return redirect(url_for("portal.login"))
        return view(*args, **kwargs)

    return cast(View, wrapped)


def owner_required(view: View) -> View:
    @wraps(view)
    @login_required
    def wrapped(*args: Any, **kwargs: Any):
        if g.current_reviewer.id != current_app.config["OWNER_REVIEWER_ID"]:
            abort(403)
        auth = current_app.extensions["musparql_auth"]
        if not auth.owner_is_recent(g.auth_session):
            return render_template("login.html", reauthenticate=True), 401
        return view(*args, **kwargs)

    return cast(View, wrapped)


def consent_required(view: View) -> View:
    """Keep participant data routes behind the configured consent version."""

    @wraps(view)
    @login_required
    def wrapped(*args: Any, **kwargs: Any):
        if (
            g.current_reviewer.id != current_app.config["OWNER_REVIEWER_ID"]
            and current_app.config["CONSENT_STATEMENT_VERSION"]
            and not _has_current_consent()
        ):
            return redirect(url_for("portal.consent_pending"))
        return view(*args, **kwargs)

    return cast(View, wrapped)


def complete_profile_required(view: View) -> View:
    """Keep participant mutation/API routes behind the complete-profile gate."""

    @wraps(view)
    @consent_required
    def wrapped(*args: Any, **kwargs: Any):
        if g.current_reviewer.id != current_app.config["OWNER_REVIEWER_ID"] and not (
            current_app.extensions["musparql_profiles"].is_complete(
                g.current_reviewer.id
            )
        ):
            abort(403)
        return view(*args, **kwargs)

    return cast(View, wrapped)


def _request_context() -> str:
    # ProxyFix has already replaced this with the trusted client address when
    # the explicitly configured single reverse proxy is in use. Do not include
    # client-controlled headers such as User-Agent in a security bucket key.
    return request.remote_addr or "unknown"


def _has_current_consent() -> bool:
    return has_current_consent(
        g.current_reviewer,
        current_app.config["CONSENT_STATEMENT_VERSION"],
        current_app.config["PRIVACY_NOTICE_VERSION"],
    )


def _set_auth_cookie(response: Response, token: str, remembered: bool) -> None:
    max_age = current_app.config["REMEMBERED_ABSOLUTE_SECONDS"] if remembered else None
    response.set_cookie(
        current_app.config["AUTH_COOKIE_NAME"],
        token,
        max_age=max_age,
        secure=current_app.config["COOKIE_SECURE"],
        httponly=True,
        samesite="Lax",
        path="/",
    )


def _clear_auth_cookies(response: Response) -> None:
    for name in (
        current_app.config["AUTH_COOKIE_NAME"],
        current_app.config["LOGIN_CHALLENGE_COOKIE_NAME"],
    ):
        response.delete_cookie(
            name,
            secure=current_app.config["COOKIE_SECURE"],
            httponly=True,
            samesite="Lax",
            path="/",
        )
    g.rotate_csrf = True


@portal.get("/")
def index():
    if (
        g.current_reviewer is not None
        and g.current_reviewer.id != current_app.config["OWNER_REVIEWER_ID"]
        and current_app.config["CONSENT_STATEMENT_VERSION"]
        and not _has_current_consent()
    ):
        return redirect(url_for("portal.consent_pending"))
    if (
        g.current_reviewer is not None
        and g.current_reviewer.id != current_app.config["OWNER_REVIEWER_ID"]
        and not current_app.extensions["musparql_profiles"].is_complete(
            g.current_reviewer.id
        )
    ):
        return redirect(url_for("portal.profile"))
    assignments = []
    workshop_available = False
    if (
        g.current_reviewer is not None
        and g.current_reviewer.id != current_app.config["OWNER_REVIEWER_ID"]
    ):
        assignments = current_app.extensions["musparql_assignments"].list_for_reviewer(
            g.current_reviewer.id
        )
        assignments.extend(
            current_app.extensions[
                "musparql_assignments"
            ].list_group_assignments_for_reviewer(g.current_reviewer.id)
        )
        workshop_available = current_app.extensions[
            "musparql_workshops"
        ].available(g.current_reviewer.id)
    return render_template(
        "index.html",
        assignments=assignments,
        workshop_available=workshop_available,
        profile_saved=request.args.get("profile_saved") == "yes",
    )


@portal.get("/workshop")
@consent_required
def workshop():
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    if not current_app.extensions["musparql_profiles"].is_complete(
        g.current_reviewer.id
    ):
        return redirect(url_for("portal.profile"))
    try:
        value = current_app.extensions["musparql_workshops"].dashboard(
            g.current_reviewer.id, request.args.get("group_id")
        )
    except WorkshopAccessError:
        abort(403)
    except WorkshopUnavailable:
        abort(404)
    messages = {
        "profile-saved": "Your profile is complete. Choose a review batch to begin.",
        "group-created": "Your team is ready.",
        "group-joined": "You joined the team.",
        "package-claimed": "The review batch is ready for setup.",
        "assignment-abandoned": "The review was closed without a submission.",
    }
    errors = {
        "invalid-group-code": "That team code is not available.",
        "claim-unavailable": "That review batch could not be started by this team.",
    }
    return render_template(
        "workshop.html",
        value=value,
        result=messages.get(request.args.get("result", ""), ""),
        error=errors.get(request.args.get("error", ""), ""),
    )


@portal.post("/workshop/groups")
@consent_required
def create_workshop_group():
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    if not current_app.extensions["musparql_profiles"].is_complete(
        g.current_reviewer.id
    ):
        abort(403)
    try:
        current_app.extensions["musparql_workshops"].create_group(
            g.current_reviewer.id
        )
    except WorkshopAccessError:
        abort(403)
    except WorkshopUnavailable:
        abort(404)
    return redirect(url_for("portal.workshop", result="group-created"))


@portal.post("/workshop/groups/join")
@consent_required
def join_workshop_group():
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    if not current_app.extensions["musparql_profiles"].is_complete(
        g.current_reviewer.id
    ):
        abort(403)
    try:
        group_id = current_app.extensions["musparql_workshops"].join_group(
            g.current_reviewer.id, request.form.get("join_code", "")
        )
    except WorkshopAccessError:
        return redirect(url_for("portal.workshop", error="invalid-group-code"))
    except WorkshopUnavailable:
        abort(404)
    return redirect(
        url_for("portal.workshop", result="group-joined", group_id=group_id)
    )


@portal.post("/workshop/groups/<group_id>/packages/<package_id>/claim")
@consent_required
def claim_workshop_package(group_id: str, package_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    if not current_app.extensions["musparql_profiles"].is_complete(
        g.current_reviewer.id
    ):
        abort(403)
    try:
        assignment_id = current_app.extensions["musparql_workshops"].claim_package(
            reviewer_id=g.current_reviewer.id,
            group_id=group_id,
            package_id=package_id,
        )
    except WorkshopAccessError:
        return redirect(url_for("portal.workshop", error="claim-unavailable"))
    except WorkshopUnavailable:
        abort(404)
    except ValueError:
        current_app.logger.error("Workshop package failed integrity validation")
        return redirect(url_for("portal.workshop", error="claim-unavailable"))
    return redirect(
        url_for("portal.assignment", assignment_id=assignment_id)
    )


@portal.post("/workshop/packages/<package_id>/claim")
@consent_required
def start_workshop_package(package_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    if not current_app.extensions["musparql_profiles"].is_complete(
        g.current_reviewer.id
    ):
        abort(403)
    try:
        assignment_id = current_app.extensions["musparql_workshops"].claim_package(
            reviewer_id=g.current_reviewer.id,
            group_id=None,
            package_id=package_id,
        )
    except WorkshopAccessError:
        return redirect(url_for("portal.workshop", error="claim-unavailable"))
    except WorkshopUnavailable:
        abort(404)
    except ValueError:
        current_app.logger.error("Workshop package failed integrity validation")
        return redirect(url_for("portal.workshop", error="claim-unavailable"))
    return redirect(url_for("portal.assignment", assignment_id=assignment_id))


@portal.post("/assignments/<assignment_id>/abandon")
@consent_required
def abandon_workshop_assignment(assignment_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    if not current_app.extensions["musparql_profiles"].is_complete(
        g.current_reviewer.id
    ):
        abort(403)
    try:
        current_app.extensions["musparql_workshops"].abandon_assignment(
            reviewer_id=g.current_reviewer.id,
            assignment_id=assignment_id,
        )
    except WorkshopAccessError:
        abort(403)
    except WorkshopUnavailable:
        abort(404)
    return redirect(url_for("portal.workshop", result="assignment-abandoned"))


@portal.route("/profile", methods=["GET", "POST"])
@consent_required
def profile():
    service = current_app.extensions["musparql_profiles"]
    error = ""
    submitted = request.method == "POST"
    if request.method == "POST":
        try:
            service.update(
                g.current_reviewer.id,
                request.form,
                existing_domain_ids=request.form.getlist("existing_domain_id"),
                existing_assertion_ids=request.form.getlist("existing_assertion_id"),
                existing_domain_levels=request.form.getlist("existing_domain_level"),
                new_domain_labels=request.form.getlist("new_domain_label"),
                new_domain_levels=request.form.getlist("new_domain_level"),
                language_tags=request.form.getlist("language_tag"),
                language_levels=request.form.getlist("language_level"),
            )
        except ValueError as exc:
            error = f"Profile not saved: {exc}"
        else:
            assignments = current_app.extensions[
                "musparql_assignments"
            ].list_for_reviewer(g.current_reviewer.id)
            if assignments:
                return redirect(
                    url_for(
                        "portal.assignment",
                        assignment_id=assignments[0].id,
                        profile_saved="yes",
                    )
                )
            if current_app.extensions["musparql_workshops"].available(
                g.current_reviewer.id
            ):
                return redirect(url_for("portal.workshop", result="profile-saved"))
            return redirect(url_for("portal.index", profile_saved="yes"))
    value = service.load(g.current_reviewer.id)
    new_domain_rows: list[tuple[str, str]]
    notice_acknowledged = False
    if submitted and error:
        submitted_existing_levels = dict(
            zip(
                request.form.getlist("existing_domain_id"),
                request.form.getlist("existing_domain_level"),
            )
        )
        value = replace(
            value,
            title=request.form.get("title", ""),
            first_name=request.form.get("first_name", ""),
            last_name=request.form.get("last_name", ""),
            affiliation=request.form.get("affiliation", ""),
            email=request.form.get("contact_email", value.email),
            future_review_contact_allowed=(
                request.form.get("future_review_contact_allowed") == "yes"
            ),
            kg_ontology_experience=request.form.get("kg_ontology_experience", ""),
            sparql_experience=request.form.get("sparql_experience", ""),
            nlp_llm_experience=request.form.get("nlp_llm_experience", ""),
            domains=tuple(
                replace(
                    domain,
                    expertise_level=submitted_existing_levels.get(
                        domain.domain_id, domain.expertise_level
                    ),
                )
                for domain in value.domains
            ),
        )
        language_rows = list(
            zip(
                request.form.getlist("language_tag"),
                request.form.getlist("language_level"),
            )
        )
        new_domain_rows = list(
            zip(
                request.form.getlist("new_domain_label"),
                request.form.getlist("new_domain_level"),
            )
        )
        notice_acknowledged = request.form.get("notice_acknowledged") == "yes"
    else:
        language_rows = list(value.languages)
        new_domain_rows = []
    language_rows += [("", "")] * max(0, 2 - len(language_rows))
    new_domain_rows += [("", "")] * max(0, 1 - len(new_domain_rows))
    return render_template(
        "profile.html",
        profile=value,
        profile_complete=service.is_complete(g.current_reviewer.id),
        language_rows=language_rows,
        new_domain_rows=new_domain_rows,
        suggestions=service.suggestions,
        suggestion_snapshot_id=service.suggestion_snapshot_id,
        euroscivoc_suggestion_count=service.euroscivoc_suggestion_count,
        language_options=service.language_options,
        language_snapshot_id=service.language_snapshot_id,
        technical_levels=("none", "occasional", "regular", "expert"),
        subject_levels=("none", "basic", "working", "advanced", "expert"),
        language_levels=("basic", "advanced", "fluent", "native"),
        notice_version=current_app.config["PRIVACY_NOTICE_VERSION"],
        notice_body=current_app.config["PRIVACY_NOTICE_BODY"],
        contact_email=current_app.config["PARTICIPANT_CONTACT_EMAIL"],
        notice_acknowledged=notice_acknowledged,
        error=error,
    )


@portal.route("/auth/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template(
            "login.html",
            reauthenticate=False,
            workshop_code_available=current_app.extensions[
                "musparql_workshop_admission"
            ].available(),
        )
    auth = current_app.extensions["musparql_auth"]
    try:
        challenge_id = auth.request_login_code(
            request.form.get("email", "")[:512], _request_context()
        )
    except Exception:
        current_app.logger.error("Login-code delivery failed")
        challenge_id = "unavailable"
    response = redirect(url_for("portal.verify"))
    response.set_cookie(
        current_app.config["LOGIN_CHALLENGE_COOKIE_NAME"],
        challenge_id,
        max_age=current_app.config["LOGIN_CODE_TTL_SECONDS"],
        secure=current_app.config["COOKIE_SECURE"],
        httponly=True,
        samesite="Strict",
        path="/auth",
    )
    return response


@portal.post("/auth/workshop")
def workshop_login():
    try:
        result = current_app.extensions["musparql_auth"].redeem_workshop_code(
            request.form.get("code", "")[:32],
            _request_context(),
            current_token=request.cookies.get(current_app.config["AUTH_COOKIE_NAME"]),
            admission_nonce=request.form.get("csrf_token", "")[:256],
        )
    except Exception:
        current_app.logger.error("Workshop admission failed")
        result = None
    if result is None:
        return render_template(
            "login.html",
            reauthenticate=False,
            workshop_code_available=current_app.extensions[
                "musparql_workshop_admission"
            ].available(),
            workshop_code_invalid=True,
        ), 200
    token, _reviewer = result
    response = redirect(url_for("portal.consent_pending"))
    _set_auth_cookie(response, token, remembered=False)
    g.rotate_csrf = True
    return response


@portal.post("/auth/workshop/recover")
def workshop_recover():
    result = current_app.extensions["musparql_auth"].recover_workshop_session(
        request.form.get("reviewer_id", "")[:64],
        request.form.get("code", "")[:32],
        _request_context(),
        current_token=request.cookies.get(current_app.config["AUTH_COOKIE_NAME"]),
    )
    if result is None:
        return render_template(
            "login.html",
            reauthenticate=False,
            workshop_code_available=current_app.extensions[
                "musparql_workshop_admission"
            ].available(),
            workshop_recovery_invalid=True,
        ), 200
    token, reviewer = result
    response = redirect(
        url_for("portal.owner_workshop_entry")
        if reviewer.id == current_app.config["OWNER_REVIEWER_ID"]
        else url_for("portal.consent_pending")
        if not has_current_consent(
            reviewer,
            current_app.config["CONSENT_STATEMENT_VERSION"],
            current_app.config["PRIVACY_NOTICE_VERSION"],
        )
        else url_for("portal.index")
    )
    _set_auth_cookie(response, token, remembered=False)
    g.rotate_csrf = True
    return response


@portal.route("/consent", methods=["GET", "POST"])
@login_required
def consent_pending():
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        return redirect(url_for("portal.index"))
    if not current_app.config["CONSENT_STATEMENT_VERSION"]:
        return redirect(url_for("portal.index"))
    if _has_current_consent():
        return redirect(url_for("portal.index"))
    error = ""
    if request.method == "POST":
        try:
            current_app.extensions["musparql_consent"].accept(
                g.current_reviewer.id,
                affirmed=request.form.get("consent_affirmed") == "yes",
            )
        except ValueError as exc:
            error = str(exc)
        except LookupError:
            abort(403)
        else:
            return redirect(url_for("portal.profile"))
    return render_template(
        "consent_pending.html",
        notice_version=current_app.config["PRIVACY_NOTICE_VERSION"],
        statement_version=current_app.config["CONSENT_STATEMENT_VERSION"],
        summary_body=current_app.config["CONSENT_SUMMARY_BODY"],
        statement_body=current_app.config["CONSENT_STATEMENT_BODY"],
        contact_email=current_app.config["PARTICIPANT_CONTACT_EMAIL"],
        error=error,
    )


@portal.get("/participant-notice")
def participant_notice():
    return render_template(
        "participant_notice.html",
        notice_version=current_app.config["PRIVACY_NOTICE_VERSION"],
        notice_body=current_app.config["PRIVACY_NOTICE_BODY"],
        contact_email=current_app.config["PARTICIPANT_CONTACT_EMAIL"],
    )


@portal.get("/consent/withdrawal")
@login_required
def consent_withdrawal():
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        return redirect(url_for("portal.index"))
    return render_template(
        "consent_withdrawal.html",
        contact_email=current_app.config["PARTICIPANT_CONTACT_EMAIL"],
    )


@portal.route("/auth/verify", methods=["GET", "POST"])
def verify():
    if request.method == "GET":
        return render_template("verify.html", invalid=False)
    auth = current_app.extensions["musparql_auth"]
    remembered = request.form.get("remembered") == "yes"
    result = auth.verify_login_code(
        request.cookies.get(current_app.config["LOGIN_CHALLENGE_COOKIE_NAME"], ""),
        request.form.get("code", "")[:32],
        remembered=remembered,
        current_token=request.cookies.get(current_app.config["AUTH_COOKIE_NAME"]),
    )
    if result is None:
        return render_template("verify.html", invalid=True), 200
    token, reviewer = result
    response = redirect(
        url_for("portal.consent_pending")
        if (
            reviewer.id != current_app.config["OWNER_REVIEWER_ID"]
            and current_app.config["CONSENT_STATEMENT_VERSION"]
            and not has_current_consent(
                reviewer,
                current_app.config["CONSENT_STATEMENT_VERSION"],
                current_app.config["PRIVACY_NOTICE_VERSION"],
            )
        )
        else url_for("portal.index")
    )
    _set_auth_cookie(
        response,
        token,
        remembered=remembered and reviewer.id != current_app.config["OWNER_REVIEWER_ID"],
    )
    response.delete_cookie(
        current_app.config["LOGIN_CHALLENGE_COOKIE_NAME"],
        secure=current_app.config["COOKIE_SECURE"],
        httponly=True,
        samesite="Strict",
        path="/auth",
    )
    g.rotate_csrf = True
    return response


@portal.post("/auth/logout")
@login_required
def logout():
    current_app.extensions["musparql_auth"].logout(
        request.cookies.get(current_app.config["AUTH_COOKIE_NAME"])
    )
    response = redirect(url_for("portal.login"))
    _clear_auth_cookies(response)
    return response


@portal.post("/auth/logout-all")
@login_required
def logout_all():
    current_app.extensions["musparql_auth"].logout_all(g.current_reviewer.id)
    response = redirect(url_for("portal.login"))
    _clear_auth_cookies(response)
    return response


@portal.get("/owner/reviewers")
@login_required
def owner_reviewers():
    if g.current_reviewer.id != current_app.config["OWNER_REVIEWER_ID"]:
        abort(403)
    reviewers = current_app.extensions["musparql_auth"].list_reviewers()
    completion = current_app.extensions["musparql_profiles"].completion_by_reviewer()
    return render_template(
        "owner_reviewers.html",
        reviewers=reviewers,
        owner_id=current_app.config["OWNER_REVIEWER_ID"],
        profile_completion=completion,
        result=request.args.get("result", ""),
        error=request.args.get("error", ""),
    )


@portal.get("/owner/workshop-entry")
@login_required
def owner_workshop_entry():
    if g.current_reviewer.id != current_app.config["OWNER_REVIEWER_ID"]:
        abort(403)
    return render_template(
        "owner_workshop_entry.html",
        rounds=current_app.extensions["musparql_workshop_admission"].list_rounds(),
        workshop_reviewers=current_app.extensions[
            "musparql_auth"
        ].list_workshop_reviewers(),
        error=request.args.get("error", ""),
    )


@portal.post("/owner/workshop-entry/<round_id>/issue")
@owner_required
def owner_issue_workshop_entry(round_id: str):
    try:
        plaintext_code = current_app.extensions[
            "musparql_workshop_admission"
        ].issue(round_id)
    except ValueError:
        return redirect(url_for("portal.owner_workshop_entry", error="issue-failed"))
    return render_template(
        "owner_workshop_entry.html",
        rounds=current_app.extensions["musparql_workshop_admission"].list_rounds(),
        workshop_reviewers=current_app.extensions[
            "musparql_auth"
        ].list_workshop_reviewers(),
        plaintext_code=plaintext_code,
    )


@portal.post("/owner/workshop-entry/reviewers/<reviewer_id>/reset")
@owner_required
def owner_reset_workshop_session(reviewer_id: str):
    try:
        recovery_code = current_app.extensions[
            "musparql_auth"
        ].issue_workshop_recovery_code(g.current_reviewer.id, reviewer_id)
    except ValueError:
        return redirect(url_for("portal.owner_workshop_entry", error="reset-failed"))
    return render_template(
        "owner_workshop_entry.html",
        rounds=current_app.extensions["musparql_workshop_admission"].list_rounds(),
        workshop_reviewers=current_app.extensions[
            "musparql_auth"
        ].list_workshop_reviewers(),
        recovery_code=recovery_code,
        recovery_reviewer_id=reviewer_id,
    )


@portal.post("/owner/workshop-entry/<code_id>/revoke")
@owner_required
def owner_revoke_workshop_entry(code_id: str):
    try:
        current_app.extensions["musparql_workshop_admission"].revoke(code_id)
    except ValueError:
        return redirect(url_for("portal.owner_workshop_entry", error="revoke-failed"))
    return redirect(url_for("portal.owner_workshop_entry"))


@portal.post("/owner/invitations")
@owner_required
def invite():
    try:
        current_app.extensions["musparql_auth"].invite(
            g.current_reviewer.id,
            request.form.get("title", ""),
            request.form.get("first_name", ""),
            request.form.get("last_name", ""),
            request.form.get("email", ""),
        )
    except ValueError:
        return redirect(url_for("portal.owner_reviewers", error="invalid-invitation"))
    except Exception:
        current_app.logger.error("Invitation delivery failed")
        return redirect(url_for("portal.owner_reviewers", error="delivery-failed"))
    return redirect(url_for("portal.owner_reviewers", result="invited"))


@portal.post("/owner/reviewers/<reviewer_id>/<action>")
@owner_required
def owner_account_action(reviewer_id: str, action: str):
    auth = current_app.extensions["musparql_auth"]
    try:
        if action == "delete":
            if request.form.get("confirm") != reviewer_id:
                raise ValueError("Deletion confirmation did not match")
            auth.delete_reviewer_identity(g.current_reviewer.id, reviewer_id)
        else:
            auth.change_reviewer_status(g.current_reviewer.id, reviewer_id, action)
    except ValueError:
        return redirect(url_for("portal.owner_reviewers", error="invalid-account-action"))
    return redirect(url_for("portal.owner_reviewers", result=action))


@portal.get("/owner/assignments")
@login_required
def owner_assignments():
    if g.current_reviewer.id != current_app.config["OWNER_REVIEWER_ID"]:
        abort(403)
    service = current_app.extensions["musparql_assignments"]
    reviewers, seeds = service.owner_choices()
    return render_template(
        "owner_assignments.html",
        assignments=service.list_all(),
        reviewers=[item for item in reviewers if item.id != g.current_reviewer.id],
        seeds=seeds,
        result=request.args.get("result", ""),
        error=request.args.get("error", ""),
    )


@portal.get("/owner/processing")
@login_required
def owner_processing():
    if g.current_reviewer.id != current_app.config["OWNER_REVIEWER_ID"]:
        abort(403)
    return render_template(
        "owner_processing.html",
        rows=current_app.extensions["musparql_processing"].dashboard(),
        combined_jobs=current_app.extensions["musparql_processing"].combined_jobs(),
        item_ids=current_app.extensions["musparql_processing"].dashboard_items(),
        result=request.args.get("result", ""),
        error=request.args.get("error", ""),
    )


@portal.post("/owner/candidates")
@owner_required
def create_combined_candidate():
    try:
        job_id = current_app.extensions["musparql_processing"].create_combined_candidate(
            request.form.getlist("receipt_id")
        )
    except (LookupError, ValueError):
        return redirect(url_for("portal.owner_processing", error="invalid-candidate-selection"))
    return redirect(url_for("portal.owner_processing", result=job_id))


@portal.post("/owner/submissions/<receipt_id>/<decision>")
@owner_required
def owner_submission_decision(receipt_id: str, decision: str):
    try:
        current_app.extensions["musparql_processing"].decide_inclusion(
            receipt_id, g.current_reviewer.id, decision, request.form.get("reason", "")
        )
    except (LookupError, ValueError):
        return redirect(url_for("portal.owner_processing", error="invalid-inclusion-decision"))
    return redirect(url_for("portal.owner_processing", result=decision))


@portal.post("/owner/submissions/<receipt_id>/items/<path:item_id>/<decision>")
@owner_required
def owner_item_decision(receipt_id: str, item_id: str, decision: str):
    try:
        current_app.extensions["musparql_processing"].decide_item(
            receipt_id, item_id, g.current_reviewer.id, decision,
            request.form.get("reason", ""),
        )
    except (LookupError, ValueError):
        return redirect(url_for("portal.owner_processing", error="invalid-item-decision"))
    return redirect(url_for("portal.owner_processing", result=decision))


@portal.post("/owner/candidates/<job_id>/<decision>")
@owner_required
def owner_candidate_decision(job_id: str, decision: str):
    try:
        current_app.extensions["musparql_processing"].decide_candidate(
            job_id, g.current_reviewer.id, decision, request.form.get("reason", "")
        )
    except (LookupError, ValueError):
        return redirect(url_for("portal.owner_processing", error="invalid-candidate-decision"))
    return redirect(url_for("portal.owner_processing", result=decision))


@portal.post("/owner/assignments")
@owner_required
def create_assignment():
    service = current_app.extensions["musparql_assignments"]
    try:
        assignment_id = service.create(
            reviewer_id=request.form.get("reviewer_id", ""),
            mode=request.form.get("mode", ""),
            bundle_name=request.form.get("bundle_name", ""),
            processing_recipe=request.form.get("processing_recipe", ""),
            seed_keys=request.form.getlist("seed_key"),
            previous_benchmark_path=request.form.get("previous_benchmark_path") or None,
        )
    except ValueError:
        return redirect(url_for("portal.owner_assignments", error="invalid-assignment"))
    return redirect(
        url_for("portal.owner_assignments", result=assignment_id)
    )


@portal.route("/assignments/<assignment_id>", methods=["GET", "POST"])
@consent_required
def assignment(assignment_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    profiles = current_app.extensions["musparql_profiles"]
    if not profiles.is_complete(g.current_reviewer.id):
        return redirect(url_for("portal.profile"))
    expected_reviewer_id = request.args.get("expected_reviewer_id", "")
    if expected_reviewer_id and expected_reviewer_id != g.current_reviewer.id:
        try:
            team = current_app.extensions["musparql_workshops"].workbench_team(
                g.current_reviewer.id, assignment_id
            )
        except WorkshopAccessError:
            abort(404)
        if expected_reviewer_id not in team["missing_assessment_reviewer_ids"]:
            abort(404)
        return render_template(
            "assessment_handoff.html",
            assignment_id=assignment_id,
            expected_reviewer_id=expected_reviewer_id,
        )
    service = current_app.extensions["musparql_assignments"]
    error = ""
    try:
        if request.method == "POST":
            before = service.view(assignment_id, g.current_reviewer.id)
            if before.assignment.review_group_id is not None:
                current_app.extensions[
                    "musparql_workshops"
                ].remember_batch_assignment(g.current_reviewer.id, assignment_id)
            service.assess(
                assignment_id,
                g.current_reviewer.id,
                request.form.getlist("domain_level"),
                request.form.getlist("familiarity_level"),
                confirmed=request.form.get("confirmed") == "yes",
            )
            if before.assignment.review_group_id is not None:
                if before.assignment.participant_status not in {"not_started", "active"}:
                    return redirect(url_for("portal.workshop"))
                return redirect(
                    url_for("portal.assignment_workbench", assignment_id=assignment_id)
                )
            return redirect(url_for("portal.assignment", assignment_id=assignment_id))
        value = service.view(assignment_id, g.current_reviewer.id)
        if value.assignment.review_group_id is not None:
            current_app.extensions["musparql_workshops"].remember_batch_assignment(
                g.current_reviewer.id, assignment_id
            )
            if value.assessment_reused and value.workbench_available:
                return redirect(
                    url_for(
                        "portal.assignment_workbench",
                        assignment_id=assignment_id,
                    )
                )
    except LookupError:
        abort(404)
    except ValueError:
        error = "Please answer every prompt and confirm your answers."
        try:
            value = service.view(assignment_id, g.current_reviewer.id)
        except LookupError:
            abort(404)
    return render_template(
        "assignment.html",
        value=value,
        error=error,
        profile_saved=request.args.get("profile_saved") == "yes",
        joined=request.args.get("joined") == "yes",
        subject_levels=("none", "basic", "working", "advanced", "expert"),
        familiarity_levels=("none", "inspected", "worked", "regular_user", "creator"),
    )


@portal.post("/assignments/<assignment_id>/assessment/skip")
@complete_profile_required
def skip_assignment_assessment(assignment_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    try:
        current_app.extensions["musparql_assignments"].defer_assessment(
            assignment_id, g.current_reviewer.id
        )
    except LookupError:
        abort(404)
    except PermissionError:
        abort(403)
    return redirect(
        url_for("portal.assignment_workbench", assignment_id=assignment_id)
    )


@portal.get("/assignments/<assignment_id>/bundle")
@complete_profile_required
def assignment_bundle(assignment_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    try:
        payload = current_app.extensions[
            "musparql_assignments"
        ].attributed_bundle(assignment_id, g.current_reviewer.id)
    except LookupError:
        abort(404)
    except PermissionError:
        abort(403)
    except ValueError:
        current_app.logger.error("Assignment bundle failed integrity validation")
        abort(409)
    return jsonify(payload)


@portal.post("/assignments/<assignment_id>/submissions")
@complete_profile_required
def submit_assignment(assignment_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "A JSON object is required."}), 400
    try:
        requested_completion = request.args.get("completion", "completed")
        receipt = current_app.extensions["musparql_submissions"].submit(
            assignment_id,
            g.current_reviewer.id,
            payload,
            completion_type=requested_completion,
        )
    except LookupError:
        abort(404)
    except PermissionError:
        abort(403)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 422
    response = receipt.as_dict()
    try:
        team = current_app.extensions["musparql_workshops"].workbench_team(
            g.current_reviewer.id, assignment_id
        )
    except WorkshopAccessError:
        pass
    else:
        missing = team["missing_assessment_reviewer_ids"]
        response["missing_assessment_reviewer_ids"] = missing
        response["assessment_url"] = (
            url_for(
                "portal.assignment",
                assignment_id=assignment_id,
                expected_reviewer_id=missing[0],
            )
            if missing
            else None
        )
        response["missing_assessment_links"] = [
            {
                "reviewer_id": reviewer_id,
                "url": url_for(
                    "portal.assignment",
                    assignment_id=assignment_id,
                    expected_reviewer_id=reviewer_id,
                ),
            }
            for reviewer_id in missing
        ]
        # Retained as a stable participant API endpoint, but no longer linked
        # from the workshop interface.
        response["submission_url"] = url_for(
            "portal.assignment_submission", assignment_id=assignment_id
        )
    return jsonify(response), 200 if receipt.duplicate else 202


@portal.post("/assignments/<assignment_id>/team/members")
@complete_profile_required
def add_assignment_team_member(assignment_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    try:
        added = current_app.extensions["musparql_workshops"].add_assignment_member(
            g.current_reviewer.id,
            assignment_id,
            request.form.get("reviewer_id", "")[:64],
        )
        team = current_app.extensions["musparql_workshops"].workbench_team(
            g.current_reviewer.id, assignment_id
        )
    except WorkshopAccessError as exc:
        return jsonify({"error": str(exc)}), 422
    return jsonify(
        {
            "added": added,
            "member_count": team["member_count"],
            "member_reviewer_ids": team["member_reviewer_ids"],
            "missing_assessment_reviewer_ids": team[
                "missing_assessment_reviewer_ids"
            ],
        }
    )


@portal.get("/assignments/<assignment_id>/submission")
@complete_profile_required
def assignment_submission(assignment_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    try:
        receipt = current_app.extensions["musparql_submissions"].participant_receipt(
            assignment_id, g.current_reviewer.id
        )
    except LookupError:
        abort(404)
    except PermissionError:
        abort(403)
    return render_template("submission_receipt.html", receipt=receipt)


def _hosted_assignment_bundle(assignment_id: str) -> dict[str, Any]:
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    try:
        return current_app.extensions["musparql_assignments"].attributed_bundle(
            assignment_id, g.current_reviewer.id
        )
    except LookupError:
        abort(404)
    except PermissionError:
        abort(403)
    except ValueError:
        current_app.logger.error("Assignment workbench failed integrity validation")
        abort(409)


@portal.get("/assignments/<assignment_id>/workbench/")
@complete_profile_required
def assignment_workbench(assignment_id: str):
    try:
        current_app.extensions["musparql_workshops"].remember_batch_assignment(
            g.current_reviewer.id, assignment_id
        )
    except LookupError:
        abort(404)
    except WorkshopAccessError:
        abort(403)
    payload = _hosted_assignment_bundle(assignment_id)
    root_key = (
        "LINGUISTIC_WORKBENCH_ROOT"
        if payload.get("mode") == "linguistic"
        else "REVIEW_WORKBENCH_ROOT"
    )
    return send_from_directory(
        Path(current_app.config[root_key]).expanduser().resolve(),
        "index.html",
    )


@portal.get("/assignments/<assignment_id>/workbench/<asset_name>")
@complete_profile_required
def assignment_workbench_asset(assignment_id: str, asset_name: str):
    payload = _hosted_assignment_bundle(assignment_id)
    linguistic = payload.get("mode") == "linguistic"
    if asset_name == "review_data.js":
        body = "window.REVIEW_DATA = " + json.dumps(
            payload, ensure_ascii=True, separators=(",", ":")
        ) + ";\n"
        return Response(body, mimetype="application/javascript")
    if asset_name == "host_context.js":
        context = {
            "assignment_id": assignment_id,
            "reviewer_id": g.current_reviewer.id,
            "draft_owner_id": payload.get("review_group_id")
            or g.current_reviewer.id,
            "holdout_capability": False,
            "assignment_url": url_for(
                "portal.assignment", assignment_id=assignment_id
            ),
            "profile_url": url_for("portal.profile"),
            "assignments_url": url_for("portal.index"),
            "logout_url": url_for("portal.logout"),
            "csrf_token": g.csrf_token,
            "submission_url": (
                None
                if linguistic and payload.get("review_group_id")
                else url_for(
                    "portal.submit_assignment", assignment_id=assignment_id
                )
            ),
        }
        if payload.get("review_group_id"):
            team = current_app.extensions["musparql_workshops"].workbench_team(
                g.current_reviewer.id, assignment_id
            )
            context.update(
                assignments_url=url_for("portal.workshop"),
                partial_submission_url=url_for(
                    "portal.submit_assignment",
                    assignment_id=assignment_id,
                    completion="partial",
                ),
                workshop_url=url_for("portal.workshop"),
                workshop_mode=True,
                team_join_code=team["join_code"],
                team_member_count=team["member_count"],
                team_member_reviewer_ids=team["member_reviewer_ids"],
                batch_name=team["batch_name"],
                missing_assessment_reviewer_ids=team[
                    "missing_assessment_reviewer_ids"
                ],
                submission_closes_review=False,
                add_team_member_url=url_for(
                    "portal.add_assignment_team_member",
                    assignment_id=assignment_id,
                ),
            )
        body = "window.MUSPARQL_HOSTED_CONTEXT = " + json.dumps(
            context, ensure_ascii=True, separators=(",", ":")
        ) + ";\n"
        return Response(body, mimetype="application/javascript")
    if asset_name not in {"app.js", "styles.css"}:
        abort(404)
    root_key = "LINGUISTIC_WORKBENCH_ROOT" if linguistic else "REVIEW_WORKBENCH_ROOT"
    return send_from_directory(
        Path(current_app.config[root_key]).expanduser().resolve(),
        asset_name,
    )
