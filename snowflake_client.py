"""
Snowflake connection management and query execution.

Uses st.cache_resource so the connection is shared across Streamlit sessions
(one connection pool per Streamlit worker process). Handles stale connections
automatically by clearing the cache and reconnecting.

Query results are cached in-process for CACHE_TTL_SECONDS (1 hour) to avoid
redundant Snowflake calls for repeated identical questions. Cache is keyed on
the exact SQL string (SHA-256). Only successful results are cached.
"""

import hashlib
import time
import streamlit as st
import snowflake.connector
from snowflake.connector import ProgrammingError, OperationalError, DatabaseError
from typing import Any

DB = "US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET"
SCHEMA = "PUBLIC"
QUERY_TIMEOUT_SECONDS = 50  # leaves buffer for Claude API latency

# ---------------------------------------------------------------------------
# Query result cache (in-process, shared across Streamlit sessions)
# ---------------------------------------------------------------------------
_QUERY_CACHE: dict[str, tuple[list, list, float]] = {}
_CACHE_TTL_SECONDS = 3600  # 1 hour


@st.cache_resource(show_spinner=False)
def _get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create and cache a Snowflake connection using Streamlit secrets.

    Supports both externalbrowser (SSO) and password authentication,
    controlled by the 'authenticator' key in secrets.toml.
    """
    sf = st.secrets["connections"]["snowflake"]

    params: dict = {
        "account": sf["account"],
        "user": sf["user"],
        "database": DB,
        "schema": SCHEMA,
        "login_timeout": 60,
        "network_timeout": QUERY_TIMEOUT_SECONDS + 5,
        "session_parameters": {
            "QUERY_TAG": "census_chat_agent",
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
    # Check cache
    cache_key = hashlib.sha256(sql.strip().encode()).hexdigest()
    now = time.time()
    if cache_key in _QUERY_CACHE:
        rows, columns, cached_at = _QUERY_CACHE[cache_key]
        if now - cached_at < _CACHE_TTL_SECONDS:
            return rows, columns
        # Expired — remove stale entry
        del _QUERY_CACHE[cache_key]

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
        # SQL syntax / logic error — propagate so the caller can retry with error info
        raise ProgrammingError(str(e)) from e

    except OperationalError as e:
        msg = str(e).lower()
        if "timeout" in msg or "statement timeout" in msg:
            raise TimeoutError(
                "Query exceeded the 50-second time limit. Try a more specific geographic area or simpler query."
            ) from e
        # Connection likely went stale — clear cache and re-raise
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
    """Get connection; if stale/closed, reset cache and reconnect."""
    try:
        conn = _get_connection()
        # Quick liveness check
        conn.cursor().execute("SELECT 1")
        return conn
    except Exception:
        _reset_connection()
        return _get_connection()


def _reset_connection() -> None:
    """Clear the cached connection so the next call creates a fresh one."""
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
