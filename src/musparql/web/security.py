"""Request authentication, CSRF validation, cookies, and response hardening."""
from __future__ import annotations

import hmac
import secrets

from flask import Flask, abort, g, request


SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def install_security(app: Flask) -> None:
    @app.before_request
    def load_security_context() -> None:
        if request.method == "TRACE":
            abort(405)
        csrf_cookie = request.cookies.get(app.config["CSRF_COOKIE_NAME"])
        if not csrf_cookie or len(csrf_cookie) > 256:
            csrf_cookie = secrets.token_urlsafe(32)
            g.set_csrf_cookie = True
        g.csrf_token = csrf_cookie

        if request.method not in SAFE_METHODS:
            supplied = request.form.get("csrf_token", "") or request.headers.get(
                "X-CSRF-Token", ""
            )
            if not supplied or not hmac.compare_digest(csrf_cookie, supplied):
                abort(400, description="Invalid or missing request token")

        auth = app.extensions["musparql_auth"]
        raw_token = request.cookies.get(app.config["AUTH_COOKIE_NAME"])
        authenticated = auth.authenticate(raw_token)
        g.current_reviewer = authenticated[0] if authenticated else None
        g.auth_session = authenticated[1] if authenticated else None

    @app.after_request
    def apply_security_headers(response):  # type: ignore[no-untyped-def]
        if getattr(g, "set_csrf_cookie", False) or getattr(g, "rotate_csrf", False):
            token = secrets.token_urlsafe(32) if getattr(g, "rotate_csrf", False) else g.csrf_token
            response.set_cookie(
                app.config["CSRF_COOKIE_NAME"],
                token,
                secure=app.config["COOKIE_SECURE"],
                httponly=True,
                samesite="Strict",
                path="/",
            )
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; base-uri 'none'; form-action 'self'; "
            "frame-ancestors 'none'; object-src 'none'; style-src 'self'"
        )
        response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
        response.headers["Cross-Origin-Resource-Policy"] = "same-origin"
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=(), payment=(), usb=()"
        )
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        if app.config["COOKIE_SECURE"]:
            response.headers["Strict-Transport-Security"] = "max-age=31536000"
        return response

    @app.context_processor
    def security_template_context() -> dict[str, object]:
        return {
            "csrf_token": lambda: g.csrf_token,
            "current_reviewer": g.current_reviewer,
        }
