"""
Lakebase (Databricks-managed Postgres) connection helper.

Connects using a single LAKEBASE_URL (a standard Postgres connection URL,
e.g. postgresql://role:password@host:5432/databricks_postgres?sslmode=require)
pointing at a native Postgres role with a static, non-expiring password.
This keeps setup to a single secret instead of five separate env vars.

The secret is fetched once and cached, and connections are pulled from a
small pool instead of opening a brand new connection (and re-fetching the
secret) on every single query - which was slow enough under real usage to
cause request timeouts.
"""

import base64
import logging
import os
import threading
from contextlib import contextmanager

import psycopg2
from databricks.sdk import WorkspaceClient
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine

logger = logging.getLogger("lakebase")

_w = WorkspaceClient()

_SCOPE = os.environ.get("LAKEBASE_SECRET_SCOPE", "database")
_KEY = os.environ.get("LAKEBASE_SECRET_KEY", "lakebase-url")

_url_lock = threading.Lock()
_cached_url: str | None = None

_pool_lock = threading.Lock()
_connection_pool: pg_pool.ThreadedConnectionPool | None = None


def _lakebase_url() -> str:
    """
    Fetch and decode the Lakebase connection URL from the Databricks secret
    scope, once per process. Subsequent calls reuse the cached value instead
    of hitting the secrets API again.
    """
    global _cached_url
    if _cached_url is None:
        with _url_lock:
            if _cached_url is None:
                secret = _w.secrets.get_secret(scope=_SCOPE, key=_KEY)
                _cached_url = base64.b64decode(secret.value).decode("utf-8")
    return _cached_url


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    """Return the process-wide connection pool, creating it on first use."""
    global _connection_pool
    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                _connection_pool = pg_pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=10,
                    dsn=_lakebase_url(),
                    cursor_factory=RealDictCursor,
                )
    return _connection_pool


@contextmanager
def get_connection():
    """Yield a pooled psycopg2 connection with a RealDictCursor factory."""
    conn_pool = _get_pool()
    conn = conn_pool.getconn()
    # Autocommit means every statement closes its own transaction
    # immediately. Without this, a plain SELECT (run_query) leaves an open,
    # uncommitted transaction on the connection when it's returned to the
    # pool - the next request to reuse that same pooled connection then
    # inherits a stale/half-open transaction, which can silently break
    # (or outright fail) the write that follows. Autocommit avoids that
    # class of bug entirely for this app, since we don't need multi
    # statement transactions.
    conn.autocommit = True
    try:
        yield conn
    except Exception:
        # Still roll back defensively in case autocommit was overridden
        # somewhere, so a failed request never leaves the connection in a
        # broken transaction state for the next request to inherit.
        conn.rollback()
        raise
    finally:
        conn_pool.putconn(conn)


def get_engine():
    """Return a SQLAlchemy engine for Lakebase."""
    return create_engine(_lakebase_url())


def run_query(sql: str, params: tuple | dict | None = None) -> list[dict]:
    """Run a read query against Lakebase and return rows as list[dict]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def run_write(sql: str, params: tuple | dict | None = None) -> int:
    """Run an INSERT/UPDATE/DELETE against Lakebase, return affected row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount
