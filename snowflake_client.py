"""
Snowflake connection management and query execution.

Uses st.cache_resource so the connection is shared across Streamlit sessions
(one connection pool per Streamlit worker process). Handles stale connections
automatically by clearing the cache and reconnecting.

Query results are cached in-process for CACHE_TTL_SECONDS (1 hour) to avoid
redundant Snowflake calls for repeated identical questions. Cache is keyed on
the exact SQL string (SHA-256). Only successful results are cached.

Two-level caching strategy:
  - st.cache_resource: connection object, shared across all users on this worker
  - _QUERY_CACHE (in-process dict): result rows, keyed by SHA-256 of SQL string
    Benefits: repeated questions return instantly, consume zero Snowflake credits.
    Limitation: lost on worker restart. Redis/Postgres would be needed for persistence.
"""

import hashlib
import time
import streamlit as st
import snowflake.connector
from snowflake.connector import ProgrammingError, OperationalError, DatabaseError
from typing import Any

DB = "US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET"
SCHEMA = "PUBLIC"
# 50s leaves ~10s buffer before the assignment's 60s hard deadline.
# Snowflake's STATEMENT_TIMEOUT_IN_SECONDS is set to this value in the session params.
QUERY_TIMEOUT_SECONDS = 50

# ---------------------------------------------------------------------------
# Query result cache (in-process, shared across Streamlit sessions)
# ---------------------------------------------------------------------------
# Format: { sha256_of_sql: (rows, columns, cached_at_timestamp) }
# Only successful results are cached — errors always execute fresh so transient
# Snowflake issues don't get stuck in cache.
_QUERY_CACHE: dict[str, tuple[list, list, float]] = {}
_CACHE_TTL_SECONDS = 3600  # 1 hour — long enough for a session, short enough to stay fresh


@st.cache_resource(show_spinner=False)
def _get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create and cache a Snowflake connection using Streamlit secrets.

    @st.cache_resource means this function runs once per Streamlit worker process
    and the connection object is reused for all sessions. show_spinner=False prevents
    an ugly "Running..." banner appearing every time the connection is checked.

    Supports both externalbrowser (SSO) and password auth, controlled by the
    'authenticator' key in secrets.toml. externalbrowser requires a browser popup
    and cannot be used on Streamlit Cloud — use password auth for deployment.
    """
    sf = st.secrets["connections"]["snowflake"]

    params: dict = {
        "account": sf["account"],
        "user": sf["user"],
        "database": DB,
        "schema": SCHEMA,
        "login_timeout": 60,
        "network_timeout": QUERY_TIMEOUT_SECONDS + 5,  # slightly above query timeout
        "session_parameters": {
            # QUERY_TAG makes these queries identifiable in Snowflake's query history/audit logs.
            "QUERY_TAG": "census_chat_agent",
            # Server-side timeout is the true enforcement mechanism — the Python timeout
            # is a best-effort client-side guard that may fire slightly later.
            "STATEMENT_TIMEOUT_IN_SECONDS": str(QUERY_TIMEOUT_SECONDS),
        },
    }

    # Role (optional)
    role = sf.get("role", "")
    if role:
        params["role"] = role

    # Warehouse (optional — Snowflake uses user's default if omitted)
    warehouse = sf.get("warehouse", "")
    if warehouse and warehouse not in ("<none selected>", "FILL_IN_YOUR_WAREHOUSE"):
        params["warehouse"] = warehouse

    # Auth: externalbrowser (SSO) or username+password
    authenticator = sf.get("authenticator", "")
    if authenticator:
        params["authenticator"] = authenticator
    else:
        params["password"] = sf["password"]

    return snowflake.connector.connect(**params)


def execute_query(sql: str) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Execute a SQL query against Snowflake, with in-process result caching.

    Results are cached by SHA-256 of the SQL string for CACHE_TTL_SECONDS.
    Only successful results are cached; errors are never cached.

    Returns:
        (rows, column_names) where rows is a list of dicts.

    Raises:
        ProgrammingError: SQL syntax or logic error — caller should show to Claude for retry.
        TimeoutError: Query exceeded QUERY_TIMEOUT_SECONDS.
        RuntimeError: Any other database/connection error.
    """
    # SHA-256 of the exact SQL string is the cache key. temperature=0 in SQL generation
    # makes identical questions produce identical SQL, so this cache is highly effective.
    cache_key = hashlib.sha256(sql.strip().encode()).hexdigest()
    now = time.time()
    if cache_key in _QUERY_CACHE:
        rows, columns, cached_at = _QUERY_CACHE[cache_key]
        if now - cached_at < _CACHE_TTL_SECONDS:
            return rows, columns
        del _QUERY_CACHE[cache_key]  # expired — remove so stale data isn't served

    conn = _try_get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]

        # Cache successful result
        _QUERY_CACHE[cache_key] = (rows, columns, time.time())
        return rows, columns

    except ProgrammingError as e:
        # ProgrammingError = bad SQL (wrong column name, syntax error, etc.).
        # Re-raise without caching so agent.py can send the error to Claude for retry.
        raise ProgrammingError(str(e)) from e

    except OperationalError as e:
        msg = str(e).lower()
        if "timeout" in msg or "statement timeout" in msg:
            # Convert to TimeoutError so agent.py can show a friendlier message
            # and avoid retrying (a retry would just time out again).
            raise TimeoutError(
                "Query exceeded the 50-second time limit. Try a more specific geographic area or simpler query."
            ) from e
        # Non-timeout OperationalError typically means the connection went stale
        # (idle too long, Streamlit worker recycled, network blip). Reset so the
        # next call gets a fresh connection automatically.
        _reset_connection()
        raise RuntimeError(f"Database connection error: {e}") from e

    except DatabaseError as e:
        raise RuntimeError(f"Database error: {e}") from e


def clear_query_cache() -> int:
    """Clear all cached query results. Returns number of entries removed."""
    count = len(_QUERY_CACHE)
    _QUERY_CACHE.clear()
    return count


def _try_get_connection() -> snowflake.connector.SnowflakeConnection:
    """Get connection; if stale/closed, reset cache and reconnect.

    The SELECT 1 liveness ping adds ~1ms but prevents cryptic errors when the
    connection has gone idle (common after Streamlit Cloud scales down an inactive
    worker). Cheaper than reconnecting on every query error.
    """
    try:
        conn = _get_connection()
        conn.cursor().execute("SELECT 1")
        return conn
    except Exception:
        # Most common cause: stale cached connector after worker idle time.
        _reset_connection()
        return _get_connection()


def _reset_connection() -> None:
    """Clear the st.cache_resource entry so the next _get_connection() call
    creates a fresh Snowflake connection instead of returning the stale cached one."""
    _get_connection.clear()


def test_connection() -> tuple[bool, str]:
    """
    Test that the Snowflake connection works and the census database is accessible.
    Returns (success, message).
    """
    try:
        rows, _ = execute_query(
            f'SELECT COUNT(*) AS cnt FROM "{DB}"."{SCHEMA}"."2019_CBG_B01" LIMIT 1'
        )
        return True, "Connection successful"
    except Exception as e:
        return False, str(e)
