"""Connection pool, auth dependencies, and scope helpers."""

from fastapi import HTTPException

from mees_shared.db import get_conn, init_pool as _init_pool, close_pool  # noqa: F401
from mees_shared.auth import CurrentUser, get_current_user as _make_get_user, make_require_admin  # noqa: F401

from config.settings import settings


def init_pool() -> None:
    _init_pool(settings.dsn, settings.db_pool_min, settings.db_pool_max)


# ── Auth ──────────────────────────────────────────────────────────────────────

get_current_user = _make_get_user(settings.auth_enabled, settings.dev_user_email, has_scopes=True)
require_admin = make_require_admin(get_current_user)


# ── Scope helpers ─────────────────────────────────────────────────────────────


def validate_scope(scope: str | None, user: CurrentUser) -> str | None:
    """Validate requested scope against user's allowed_scopes.

    Returns the effective scope to filter by, or None for 'all'.
    Raises 403 if user requests a scope they lack access to.
    """
    if not scope or scope == "all":
        return None

    if scope not in ("personal", "business"):
        raise HTTPException(400, f"Invalid scope: {scope}")

    if scope not in user.allowed_scopes:
        raise HTTPException(403, f"You do not have access to scope '{scope}'")

    return scope


def scope_condition(
    scope: str | None, user: CurrentUser, alias: str = "a"
) -> tuple[str, dict]:
    """Return a SQL WHERE fragment + params dict for scope filtering.

    If scope is a specific value, filter to that scope.
    If scope is None (all), filter to all of the user's allowed scopes.
    """
    if scope:
        return f"({alias}.scope = %(scope)s)", {"scope": scope}
    else:
        return f"({alias}.scope = ANY(%(allowed_scopes)s))", {
            "allowed_scopes": user.allowed_scopes,
        }
