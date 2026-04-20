"""Database connection and query utilities for Lakebase PostgreSQL."""
import logging
import time
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from backend.config import settings, get_lakebase_password, get_lakebase_user

logger = logging.getLogger(__name__)

_SLOW_QUERY_THRESHOLD = 1.0  # seconds


class DatabasePool:
    """Connection pool for PostgreSQL.

    When running inside a Databricks App the Lakebase OAuth token is short-lived
    (~1 h).  If a connection attempt fails with an auth error we automatically
    recreate the pool with a fresh token.
    """

    _pool: Optional[pool.ThreadedConnectionPool] = None

    @classmethod
    def _create_pool(cls) -> pool.ThreadedConnectionPool:
        """Create a new connection pool using a (possibly refreshed) password."""
        user = get_lakebase_user()
        logger.info("Creating Lakebase connection pool: host=%s, db=%s, user=%s",
                     settings.lakebase_dns, settings.lakebase_database, user)
        return pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=settings.lakebase_dns,
            port=settings.lakebase_port,
            database=settings.lakebase_database,
            user=user,
            password=get_lakebase_password(),
            sslmode="require",
            connect_timeout=15,
            options="-c statement_timeout=30000",
        )

    @classmethod
    def get_pool(cls) -> pool.ThreadedConnectionPool:
        """Get or create connection pool."""
        if cls._pool is None:
            cls._pool = cls._create_pool()
        return cls._pool

    @classmethod
    @contextmanager
    def get_connection(cls):
        """Get a connection from the pool, auto-refreshing on auth failure."""
        try:
            p = cls.get_pool()
            conn = p.getconn()
        except psycopg2.OperationalError as exc:
            # Token likely expired – recreate pool with fresh token
            logger.warning("Lakebase connection failed (refreshing pool): %s", exc)
            cls.close_pool()
            p = cls.get_pool()
            conn = p.getconn()
        try:
            yield conn
        except psycopg2.OperationalError as exc:
            # Mid-query auth error – reset pool so next call gets fresh token
            logger.warning("Lakebase mid-query auth error (resetting pool): %s", exc)
            cls.close_pool()
            raise
        finally:
            try:
                p.putconn(conn)
            except Exception:
                pass

    @classmethod
    def close_pool(cls):
        """Close all connections in the pool."""
        if cls._pool:
            try:
                cls._pool.closeall()
            except Exception:
                pass
            cls._pool = None


def execute_query(query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """
    Execute a SELECT query and return results as list of dictionaries.

    Args:
        query: SQL query string
        params: Query parameters tuple

    Returns:
        List of dictionaries with query results
    """
    t0 = time.monotonic()
    with DatabasePool.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = [dict(row) for row in cur.fetchall()]
    elapsed = time.monotonic() - t0
    if elapsed > _SLOW_QUERY_THRESHOLD:
        logger.warning("Slow query (%.2fs, %d rows): %s", elapsed, len(rows), query[:200])
    return rows


def execute_one(query: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
    """
    Execute a SELECT query and return a single result.
    
    Args:
        query: SQL query string
        params: Query parameters tuple
        
    Returns:
        Dictionary with query result or None
    """
    results = execute_query(query, params)
    return results[0] if results else None


def execute_update(query: str, params: Optional[tuple] = None) -> int:
    """
    Execute an INSERT/UPDATE/DELETE query.

    Args:
        query: SQL query string
        params: Query parameters tuple

    Returns:
        Number of rows affected
    """
    t0 = time.monotonic()
    with DatabasePool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
            rowcount = cur.rowcount
    elapsed = time.monotonic() - t0
    if elapsed > _SLOW_QUERY_THRESHOLD:
        logger.warning("Slow update (%.2fs, %d rows): %s", elapsed, rowcount, query[:200])
    return rowcount


def execute_many(query: str, params_list: List[tuple]) -> int:
    """
    Execute a query multiple times with different parameters.
    
    Args:
        query: SQL query string
        params_list: List of parameter tuples
        
    Returns:
        Number of rows affected
    """
    with DatabasePool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, params_list)
            conn.commit()
            return cur.rowcount
