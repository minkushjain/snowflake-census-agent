"""
Guardrails: topic classification, SQL validation, SQL extraction, limit enforcement.

Four layers of protection:
1. NSFW blocklist — rejects at input, before reaching Claude
2. Topic classification — keyword-based, fast, zero cost
3. SQL validation — regex-based, prevents dangerous/out-of-scope queries
4. Limit enforcement — auto-appends LIMIT if missing on row-returning queries
"""

import re
from schema_metadata import TABLES

# ---------------------------------------------------------------------------
# Layer 1: NSFW blocklist (check before Claude sees the input)
# ---------------------------------------------------------------------------
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

    # Check NSFW blocklist first
    for term in NSFW_BLOCKLIST:
        if term in q_lower:
            return False, "This content is not appropriate. Please ask about US Census or demographic data."

    # Check topic relevance
    for keyword in TOPIC_KEYWORDS:
        if keyword in q_lower:
            return True, "on-topic"

    # Short numeric questions might still be valid (e.g. "How many?") — but too vague
    # Give benefit of the doubt for short follow-up questions (<=8 words) that could
    # be continuations of an on-topic conversation
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
FORBIDDEN_SQL_KEYWORDS = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|CREATE|ALTER|TRUNCATE|EXEC|EXECUTE|GRANT|REVOKE|MERGE|CALL)\b",
    re.IGNORECASE,
)

# Known valid table names (without quotes for matching)
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
    """
    # Match ```sql ... ``` with optional whitespace
    match = re.search(r"```(?:sql)?\s*\n?(.*?)```", response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def is_refusal(response: str) -> tuple[bool, str]:
    """
    Check if Claude's response is a refusal (starts with REFUSAL:).
    Returns (is_refusal, refusal_message).
    """
    stripped = response.strip()
    if stripped.upper().startswith("REFUSAL:"):
        # Return everything after "REFUSAL:"
        msg = stripped[len("REFUSAL:"):].strip()
        return True, msg
    return False, ""


def enforce_limit(sql: str, limit: int = 100) -> str:
    """
    If SQL has no LIMIT clause and no GROUP BY (i.e., it's a row-returning query),
    append LIMIT to prevent accidentally returning millions of rows.
    """
    sql_upper = sql.upper()

    # Don't add LIMIT if already present
    if re.search(r"\bLIMIT\b", sql_upper):
        return sql

    # Don't add LIMIT to pure aggregation queries (they return few rows)
    if re.search(r"\bGROUP\s+BY\b", sql_upper):
        return sql

    # Strip trailing semicolon and whitespace, then add limit
    sql = sql.rstrip().rstrip(";")
    return f"{sql}\nLIMIT {limit}"
