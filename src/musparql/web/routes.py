"""Phase 3 login, session, and owner-control HTTP routes."""
from __future__ import annotations

from functools import wraps
from typing import Any, Callable, TypeVar, cast

from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    g,
    redirect,
    render_template,
    request,
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
    return render_template("index.html")


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
    return render_template(
        "owner_reviewers.html",
        reviewers=reviewers,
        owner_id=current_app.config["OWNER_REVIEWER_ID"],
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
