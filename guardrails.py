"""
Guardrails: topic classification, SQL validation, SQL extraction, limit enforcement.

Four layers of protection — defense in depth, no single layer is bulletproof:
1. NSFW blocklist — instant string match, rejects before any API call
2. Topic classification — keyword scoring, zero LLM cost, handles ~99% of off-topic cases
3. SQL validation — regex blocks DML/DDL; whitelist ensures only census tables are queried
4. Limit enforcement — auto-appends LIMIT to prevent full 220K-row table scans
"""

import re
from schema_metadata import TABLES

# ---------------------------------------------------------------------------
# Layer 1: NSFW blocklist (check before Claude sees the input)
# ---------------------------------------------------------------------------
# frozenset for O(1) membership tests. Checked before any API call to avoid
# sending harmful content to the LLM and wasting tokens/cost.
# This is a simple substring blocklist — not exhaustive, but fast and free.
NSFW_BLOCKLIST: frozenset[str] = frozenset(
    [
        "porn", "pornography", "nude", "naked", "sex tape", "sexual assault",
        "rape", "molest", "pedophil", "child porn", "cp ", "snuff",
        "murder", "kill", "genocide", "terrorist", "bomb", "explosive",
        "drug deal", "cocaine", "heroin", "meth recipe", "hack", "phish",
        "credit card fraud", "identity theft", "ssn ", "social security number",
        "doxx", "doxing", "fuck", "fucking", "shit", "shitting"
    ]
)

# ---------------------------------------------------------------------------
# Layer 2: Topic keywords — question must relate to at least one of these
# ---------------------------------------------------------------------------
# A broad list intentionally — it's better to let a borderline question through
# and let the SQL system prompt refuse it, than to block a valid census question.
# State names and major cities are included so "What is the population of Texas?"
# passes even without the word "population" being present.
TOPIC_KEYWORDS: frozenset[str] = frozenset(
    [
        # Core census concepts
        "census", "acs", "american community survey", "cbg", "census block",
        "fips", "demographic", "demographics",
        # Population
        "population", "people", "residents", "inhabitants", "citizens",
        "how many", "how large", "size of",
        # Age & sex
        "age", "median age", "elderly", "senior", "child", "children",
        "youth", "young", "adult", "male", "female", "men", "women",
        "sex", "gender",
        # Race & ethnicity
        "race", "racial", "ethnicity", "ethnic", "white", "black",
        "african american", "asian", "hispanic", "latino", "latina",
        "native american", "indigenous", "pacific islander", "multiracial",
        # Income & poverty
        "income", "earnings", "salary", "wages", "household income",
        "per capita", "poverty", "poor", "low income", "welfare",
        "median income", "wealth", "rich", "affluent", "economic",
        # Education
        "education", "college", "university", "degree", "bachelor",
        "graduate", "high school", "diploma", "ged", "literacy",
        "attainment", "dropout",
        # Housing
        "housing", "house", "home", "apartment", "rent", "renter",
        "homeowner", "owner", "mortgage", "vacancy", "vacant",
        "occupied", "housing unit", "dwelling",
        # Households
        "household", "family", "married", "single", "living alone",
        "nonfamily",
        # Geography
        "state", "county", "city", "neighborhood", "region", "area",
        "district", "zip", "metro", "urban", "rural", "suburban",
        # Comparative / analytical
        "highest", "lowest", "most", "least", "compare", "comparison",
        "ranking", "rank", "top", "bottom", "average", "median", "total",
        "percent", "percentage", "ratio", "rate", "distribution",
        # State names (partial list of most common)
        "california", "texas", "florida", "new york", "pennsylvania",
        "illinois", "ohio", "georgia", "michigan", "north carolina",
        "new jersey", "virginia", "washington", "arizona", "massachusetts",
        "tennessee", "indiana", "missouri", "maryland", "colorado",
        "wisconsin", "minnesota", "south carolina", "alabama", "louisiana",
        "kentucky", "oregon", "oklahoma", "connecticut", "iowa",
        "mississippi", "arkansas", "utah", "nevada", "new mexico",
        "west virginia", "nebraska", "kansas", "idaho", "hawaii",
        "maine", "montana", "rhode island", "delaware", "south dakota",
        "north dakota", "alaska", "vermont", "wyoming",
        # Major cities
        "los angeles", "chicago", "houston", "phoenix", "philadelphia",
        "san antonio", "san diego", "dallas", "san jose", "austin",
        "jacksonville", "san francisco", "indianapolis", "seattle",
        "denver", "nashville", "miami", "atlanta", "boston", "portland",
        "detroit", "memphis", "baltimore",
    ]
)


def classify_topic(question: str) -> tuple[bool, str]:
    """
    Determine if a question is on-topic (US Census/demographics) and safe.

    Returns:
        (is_allowed, reason) where is_allowed=True means proceed.
    """
    q_lower = question.lower().strip()

    # NSFW check runs first — it's cheaper and we never want those terms reaching Claude.
    for term in NSFW_BLOCKLIST:
        if term in q_lower:
            return False, "This content is not appropriate. Please ask about US Census or demographic data."

    for keyword in TOPIC_KEYWORDS:
        if keyword in q_lower:
            return True, "on-topic"

    # Short follow-up questions like "How about Texas?" or "What's the average?"
    # won't match any keyword but are clearly continuations of an on-topic conversation.
    # Allowing them here avoids false rejections; the SQL prompt will refuse if truly off-topic.
    word_count = len(q_lower.split())
    if word_count <= 8:
        return True, "short question — assumed follow-up"

    return (
        False,
        "I can only answer questions about US Census data, demographics, population, income, "
        "housing, education, and related topics. Your question appears to be outside this scope.",
    )


# ---------------------------------------------------------------------------
# Layer 3: SQL Validation
# ---------------------------------------------------------------------------
# Regex is not a full SQL parser — a sophisticated attacker could obfuscate keywords.
# The real protection is the read-only Snowflake role; this layer blocks obvious
# injection attempts and acts as an early warning system in logs.
FORBIDDEN_SQL_KEYWORDS = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|CREATE|ALTER|TRUNCATE|EXEC|EXECUTE|GRANT|REVOKE|MERGE|CALL)\b",
    re.IGNORECASE,
)

# Derived directly from schema_metadata.TABLES so the allowlist stays in sync
# automatically whenever a new table is added to the schema metadata.
VALID_TABLES: frozenset[str] = frozenset(TABLES.keys())


def validate_sql(sql: str) -> tuple[bool, str]:
    """
    Validate generated SQL for safety.

    Returns:
        (is_valid, error_reason). If is_valid=False, do not execute.
    """
    # Check for forbidden DML/DDL keywords
    match = FORBIDDEN_SQL_KEYWORDS.search(sql)
    if match:
        return False, f"SQL contains forbidden keyword: {match.group()}"

    # Check that at least one known table is referenced
    sql_upper = sql.upper()
    found_valid_table = any(tbl.upper() in sql_upper for tbl in VALID_TABLES)
    if not found_valid_table:
        return False, "SQL does not reference any known census data table"

    return True, ""


def extract_sql(response: str) -> str | None:
    """
    Extract SQL from a Claude response that wraps it in a ```sql ... ``` code fence.
    Returns the SQL string, or None if no code fence found.

    The system prompt tells Claude to return ONLY a code fence — no preamble.
    If Claude adds extra text outside the fence anyway, the SQL is still extracted
    correctly because the regex matches the innermost fenced block.
    """
    match = re.search(r"```(?:sql)?\s*\n?(.*?)```", response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def is_refusal(response: str) -> tuple[bool, str]:
    """
    Check if Claude's response is a refusal (starts with REFUSAL:).
    Returns (is_refusal, refusal_message).

    The system prompt instructs Claude to prefix off-topic responses with 'REFUSAL:'
    followed by a one-sentence explanation. This sentinel prefix lets the UI show
    a polite info banner instead of an error, and avoids treating refusals as bugs.
    """
    stripped = response.strip()
    # Prompt contract: refusals must start with "REFUSAL:".
    if stripped.upper().startswith("REFUSAL:"):
        msg = stripped[len("REFUSAL:"):].strip()
        return True, msg
    return False, ""


def enforce_limit(sql: str, limit: int = 100) -> str:
    """
    If SQL has no LIMIT clause and no GROUP BY (i.e., it's a row-returning query),
    append LIMIT to prevent accidentally returning millions of rows.

    Aggregation queries (GROUP BY) naturally collapse 220K rows into ~50,
    so they don't need an artificial cap. Non-aggregated SELECTs do.
    """
    sql_upper = sql.upper()

    if re.search(r"\bLIMIT\b", sql_upper):
        return sql

    # GROUP BY queries collapse 220K CBG rows down to ~50 state/county rows —
    # adding LIMIT would incorrectly truncate those ranked results.
    if re.search(r"\bGROUP\s+BY\b", sql_upper):
        return sql

    # Strip trailing semicolons before appending; double semicolons cause Snowflake errors.
    sql = sql.rstrip().rstrip(";")
    return f"{sql}\nLIMIT {limit}"
