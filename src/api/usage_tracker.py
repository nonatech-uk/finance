"""Lightweight feature-usage tracker — sync variant (psycopg2).

Buffers API hits and page-views in memory, flushing to a dedicated
``usage`` PostgreSQL database every FLUSH_INTERVAL seconds via a
background daemon thread.  Completely opt-in: if *dsn* is empty the
tracker is a silent no-op.
"""

import logging
import re
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import psycopg2
from fastapi import APIRouter, Request

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FLUSH_INTERVAL = 30  # seconds
_SKIP_PREFIXES = ("/health", "/assets", "/.well-known", "/caldav")
_SKIP_EXTENSIONS = (".js", ".css", ".ico", ".png", ".jpg", ".svg", ".woff", ".woff2", ".map")
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I,
)
_INT_SEGMENT_RE = re.compile(r"(?<=/)\d+(?=/|$)")

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------
_app_name: str = ""
_conn = None
_lock = threading.Lock()
_api_buf: dict[tuple, int] = defaultdict(int)
_pv_buf: dict[tuple, int] = defaultdict(int)
_flush_thread: threading.Thread | None = None
_running = False


# ---------------------------------------------------------------------------
# Path normalisation
# ---------------------------------------------------------------------------
def _normalise(path: str) -> str:
    path = _UUID_RE.sub("{id}", path)
    path = _INT_SEGMENT_RE.sub("{id}", path)
    return path


def _current_hour() -> datetime:
    now = datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Background flush
# ---------------------------------------------------------------------------
def _flush() -> None:
    global _conn
    with _lock:
        api_snap = dict(_api_buf)
        pv_snap = dict(_pv_buf)
        _api_buf.clear()
        _pv_buf.clear()

    if not api_snap and not pv_snap:
        return

    try:
        if _conn is None or _conn.closed:
            return
        cur = _conn.cursor()
        for (app, method, path, email, status, hour), count in api_snap.items():
            cur.execute(
                """INSERT INTO api_usage (app, method, path, user_email, status_code, hour, count)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (app, method, path, user_email, status_code, hour)
                   DO UPDATE SET count = api_usage.count + EXCLUDED.count""",
                (app, method, path, email, status, hour, count),
            )
        for (app, path, email, hour), count in pv_snap.items():
            cur.execute(
                """INSERT INTO page_views (app, path, user_email, hour, count)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (app, path, user_email, hour)
                   DO UPDATE SET count = page_views.count + EXCLUDED.count""",
                (app, path, email, hour, count),
            )
        _conn.commit()
    except Exception:
        log.exception("usage_tracker: flush failed")
        try:
            _conn.rollback()
        except Exception:
            pass


def _flush_loop() -> None:
    while _running:
        time.sleep(FLUSH_INTERVAL)
        _flush()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def init_usage_tracker(app_name: str, dsn: str) -> None:
    """Initialise the tracker.  *dsn* may be empty to disable tracking."""
    global _app_name, _conn, _flush_thread, _running
    _app_name = app_name
    if not dsn:
        log.info("usage_tracker: disabled (no USAGE_DSN)")
        return
    try:
        _conn = psycopg2.connect(dsn)
        _conn.autocommit = False
        _running = True
        _flush_thread = threading.Thread(target=_flush_loop, daemon=True)
        _flush_thread.start()
        log.info("usage_tracker: enabled for %s", app_name)
    except Exception:
        log.exception("usage_tracker: failed to connect — tracking disabled")
        _conn = None


def shutdown_usage_tracker() -> None:
    global _running, _conn
    _running = False
    if _flush_thread is not None:
        _flush_thread.join(timeout=5)
    _flush()  # final drain
    if _conn is not None and not _conn.closed:
        _conn.close()


async def track_usage_middleware(request: Request, call_next):
    """FastAPI HTTP middleware that records endpoint usage."""
    path = request.url.path
    if not _conn or any(path.startswith(p) for p in _SKIP_PREFIXES):
        return await call_next(request)
    if any(path.endswith(ext) for ext in _SKIP_EXTENSIONS):
        return await call_next(request)

    response = await call_next(request)

    norm = _normalise(path)
    email = request.headers.get("Remote-Email", "")
    key = (_app_name, request.method, norm, email, response.status_code, _current_hour())
    with _lock:
        _api_buf[key] += 1
    return response


# ---------------------------------------------------------------------------
# Pageview endpoint
# ---------------------------------------------------------------------------
usage_pageview_router = APIRouter()


@usage_pageview_router.post("/usage/pageview", status_code=204)
async def record_pageview(request: Request):
    if not _conn:
        return
    body = await request.json()
    pv_path = body.get("path", "")
    if not pv_path:
        return
    email = request.headers.get("Remote-Email", "")
    key = (_app_name, pv_path, email, _current_hour())
    with _lock:
        _pv_buf[key] += 1
