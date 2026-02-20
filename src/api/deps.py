"""Connection pool and FastAPI dependencies."""

from collections.abc import Generator

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
