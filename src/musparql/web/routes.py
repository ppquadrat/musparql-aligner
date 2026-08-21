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


def _request_context() -> str:
    remote = request.remote_addr or "unknown"
    user_agent = request.user_agent.string[:512]
    return f"{remote}\0{user_agent}"


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
        and not current_app.extensions["musparql_profiles"].is_complete(
            g.current_reviewer.id
        )
    ):
        return redirect(url_for("portal.profile"))
    assignments = []
    if (
        g.current_reviewer is not None
        and g.current_reviewer.id != current_app.config["OWNER_REVIEWER_ID"]
    ):
        assignments = current_app.extensions["musparql_assignments"].list_for_reviewer(
            g.current_reviewer.id
        )
    return render_template("index.html", assignments=assignments)


@portal.route("/profile", methods=["GET", "POST"])
@login_required
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
            return redirect(url_for("portal.profile", saved="yes"))
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
            name=request.form.get("name", ""),
            affiliation=request.form.get("affiliation", ""),
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
        notice_acknowledged=notice_acknowledged,
        error=error,
        saved=request.args.get("saved") == "yes",
    )


@portal.route("/auth/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html", reauthenticate=False)
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
    response = redirect(url_for("portal.index"))
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


@portal.post("/owner/invitations")
@owner_required
def invite():
    try:
        current_app.extensions["musparql_auth"].invite(
            g.current_reviewer.id,
            request.form.get("name", ""),
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
@login_required
def assignment(assignment_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    profiles = current_app.extensions["musparql_profiles"]
    if not profiles.is_complete(g.current_reviewer.id):
        return redirect(url_for("portal.profile"))
    service = current_app.extensions["musparql_assignments"]
    error = ""
    try:
        if request.method == "POST":
            service.assess(
                assignment_id,
                g.current_reviewer.id,
                request.form.getlist("domain_level"),
                request.form.getlist("familiarity_level"),
                confirmed=request.form.get("confirmed") == "yes",
            )
            return redirect(url_for("portal.assignment", assignment_id=assignment_id))
        value = service.view(assignment_id, g.current_reviewer.id)
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
        subject_levels=("none", "basic", "working", "advanced", "expert"),
        familiarity_levels=("none", "inspected", "worked", "regular_user", "creator"),
    )


@portal.get("/assignments/<assignment_id>/bundle")
@login_required
def assignment_bundle(assignment_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    if not current_app.extensions["musparql_profiles"].is_complete(
        g.current_reviewer.id
    ):
        abort(403)
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
@login_required
def submit_assignment(assignment_id: str):
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "A JSON object is required."}), 400
    try:
        receipt = current_app.extensions["musparql_submissions"].submit(
            assignment_id, g.current_reviewer.id, payload
        )
    except LookupError:
        abort(404)
    except PermissionError:
        abort(403)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 422
    return jsonify(receipt.as_dict()), 200 if receipt.duplicate else 202


def _hosted_assignment_bundle(assignment_id: str) -> dict[str, Any]:
    if g.current_reviewer.id == current_app.config["OWNER_REVIEWER_ID"]:
        abort(404)
    if not current_app.extensions["musparql_profiles"].is_complete(
        g.current_reviewer.id
    ):
        abort(403)
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
@login_required
def assignment_workbench(assignment_id: str):
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
@login_required
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
            "holdout_capability": False,
            "assignment_url": url_for(
                "portal.assignment", assignment_id=assignment_id
            ),
            "profile_url": url_for("portal.profile"),
            "logout_url": url_for("portal.logout"),
            "csrf_token": g.csrf_token,
            "submission_url": url_for(
                "portal.submit_assignment", assignment_id=assignment_id
            ),
        }
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
