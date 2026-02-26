"""Connection pool, auth dependencies, and scope helpers."""

from collections.abc import Generator
from dataclasses import dataclass

from fastapi import Depends, HTTPException, Request
from psycopg2.pool import ThreadedConnectionPool

from config.settings import settings

pool: ThreadedConnectionPool | None = None


def init_pool() -> None:
    """Create the connection pool. Called once at app startup."""
    global pool
    pool = ThreadedConnectionPool(
        settings.db_pool_min,
        settings.db_pool_max,
        settings.dsn,
    )


def close_pool() -> None:
    """Close all connections. Called at app shutdown."""
    global pool
    if pool:
        pool.closeall()
        pool = None


def get_conn() -> Generator:
    """FastAPI dependency — yields a psycopg2 connection, returns it to pool after."""
    assert pool is not None, "Connection pool not initialised"
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


# ── Auth ──────────────────────────────────────────────────────────────────────


@dataclass
class CurrentUser:
    email: str
    display_name: str
    allowed_scopes: list[str]
    role: str  # 'admin' | 'readonly'


def get_current_user(request: Request, conn=Depends(get_conn)) -> CurrentUser:
    """Extract authenticated user from Authelia headers or dev fallback."""
    if not settings.auth_enabled:
        email = settings.dev_user_email
    else:
        email = request.headers.get("Remote-Email")
        if not email:
            raise HTTPException(401, "Not authenticated")

    cur = conn.cursor()
    cur.execute(
        "SELECT email, display_name, allowed_scopes, role FROM app_user WHERE email = %s",
        (email,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(403, f"User {email} is not authorised for this application")

    display_name_header = request.headers.get("Remote-Name")
    return CurrentUser(
        email=row[0],
        display_name=display_name_header or row[1],
        allowed_scopes=row[2],
        role=row[3],
    )


def require_admin(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
    """Dependency that ensures the user has admin role."""
    if user.role != "admin":
        raise HTTPException(403, "Admin access required")
    return user


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
