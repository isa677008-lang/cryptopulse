"""Авторизация управляющих API-маршрутов."""
import hmac
import os
from functools import wraps
from flask import request, jsonify

_ADMIN_SECRET = os.getenv("BOT_ADMIN_SECRET", "").strip()


def _get_secret_from_request() -> str:
    return (request.headers.get("X-Bot-Secret") or request.args.get("secret") or "").strip()


def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _ADMIN_SECRET:
            return jsonify({"success": False, "error": "admin API disabled: BOT_ADMIN_SECRET not set"}), 503
        provided = _get_secret_from_request()
        if not provided or not hmac.compare_digest(provided, _ADMIN_SECRET):
            return jsonify({"success": False, "error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper
