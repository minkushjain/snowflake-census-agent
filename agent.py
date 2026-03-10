"""
Core agent orchestration: NL → SQL → execute → NL answer pipeline.

Flow per turn:
  1. Guardrail check (topic + NSFW)
  2. Schema context selection (keyword scoring)
  3. Claude #1: generate SQL  (deterministic, temperature=0)
  4. SQL validation + limit enforcement
  5. Snowflake query execution (with one retry on SQL error)
  6. Claude #2: synthesize answer  (streaming, temperature=0.3)
  7. Return structured AgentResponse

Public API:
  run_query_phase(question, history) -> (_QueryPhase | None, AgentResponse | None)
  stream_answer(phase)               -> Generator[str]
  run_agent(question, history)       -> AgentResponse  (non-streaming, for testing)
"""

import time
import sys
from dataclasses import dataclass, field
from typing import Generator

import anthropic
import streamlit as st
from snowflake.connector import ProgrammingError

import guardrails
import snowflake_client
from schema_metadata import (
    get_relevant_schema,
    get_city_county_fips,
    GEOGRAPHIC_NOTES,
    MAJOR_CITY_TO_COUNTY_FIPS,
)
from prompts import (
    SYSTEM_PROMPT_SQL,
    SYSTEM_PROMPT_ANSWER,
    SYSTEM_PROMPT_FOLLOWUP,
    USER_PROMPT_SQL_TEMPLATE,
    USER_PROMPT_ANSWER_TEMPLATE,
    USER_PROMPT_ANSWER_EMPTY_TEMPLATE,
    USER_PROMPT_SQL_RETRY_TEMPLATE,
    USER_PROMPT_FOLLOWUP_TEMPLATE,
)
from utils import (
    results_to_markdown_table,
    trim_conversation_history,
    format_conversation_for_prompt,
)

MODEL = "claude-sonnet-4-6"
MAX_TOKENS_SQL = 1024
MAX_TOKENS_ANSWER = 1536


# ---------------------------------------------------------------------------
# Response dataclasses
# ---------------------------------------------------------------------------

@dataclass
class AgentResponse:
    answer: str
    sql: str | None = None
    row_count: int = 0
    elapsed_ms: int = 0
    error: bool = False
    refusal: bool = False
    rows: list[dict] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)


@dataclass
class _QueryPhase:
    """Intermediate result: SQL generated and executed, ready for answer synthesis."""
    client: anthropic.Anthropic
    question: str
    sql: str
    rows: list[dict]
    columns: list[str]
    answer_user_prompt: str   # Pre-built prompt for synthesis step
    start_ms: int


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    """Print a timestamped log line to stderr (visible in the Streamlit terminal)."""
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stderr, flush=True)


def _get_anthropic_client() -> anthropic.Anthropic:
    """Initialize Anthropic client from Streamlit secrets (never at module import)."""
    return anthropic.Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])


def _call_claude(
    client: anthropic.Anthropic,
    system: str,
    user_message: str,
    max_tokens: int,
    temperature: float,
) -> str:
    """Call Claude synchronously and return the response text."""
    response = client.messages.create(
        model=MODEL,
        max_tokens=max_tokens,
        temperature=temperature,
        system=system,
        messages=[{"role": "user", "content": user_message}],
    )
    return response.content[0].text


def _build_city_note(question: str) -> str:
    """If question mentions a known city, return a FIPS hint for SQL generation."""
    fips = get_city_county_fips(question)
    if fips:
        q_lower = question.lower()
        city_name = next(
            (city for city in MAJOR_CITY_TO_COUNTY_FIPS if city in q_lower), "the city"
        )
        return (
            f"CITY APPROXIMATION HINT: For '{city_name}', use county FIPS {fips}.\n"
            f"Filter with: WHERE LEFT(census_block_group, 5) = '{fips}'\n"
            f"Include a SQL comment noting this is a county-level approximation for the city."
        )
    return ""


def _execute_with_retry(
    client: anthropic.Anthropic,
    sql: str,
    question: str,
    schema_context: str,
    city_note: str,
    history_text: str,
) -> tuple[list[dict] | str, list[str], str]:
    """
    Execute SQL against Snowflake. On ProgrammingError, ask Claude to fix it and retry once.

    Returns:
        (rows, columns, final_sql)  on success  — rows is a list of dicts
        (error_message, [], sql)    on failure  — rows is a str error message
    """
    try:
        rows, columns = snowflake_client.execute_query(sql)
        return rows, columns, sql

    except ProgrammingError as e:
        error_msg = str(e)
        _log(f"  ✗ SQL error (will retry): {error_msg[:120]}")

        retry_prompt = USER_PROMPT_SQL_RETRY_TEMPLATE.format(
            schema_context=schema_context,
            geographic_notes=GEOGRAPHIC_NOTES,
            city_note=city_note,
            history=history_text,
            question=question,
            failed_sql=sql,
            error_message=error_msg,
        )

        try:
            retry_response = _call_claude(
                client, SYSTEM_PROMPT_SQL, retry_prompt, MAX_TOKENS_SQL, 0
            )
            fixed_sql = guardrails.extract_sql(retry_response)
            if not fixed_sql:
                return (
                    "I had trouble generating a valid SQL query. "
                    "Please try rephrasing with a specific state, county, or topic.",
                    [],
                    sql,
                )

            fixed_sql = guardrails.enforce_limit(fixed_sql)
            _log(f"  → Snowflake retry with fixed SQL:\n{fixed_sql}\n")
            rows, columns = snowflake_client.execute_query(fixed_sql)
            _log(f"  ✓ Retry succeeded: {len(rows)} row(s)")
            return rows, columns, fixed_sql  # ← fixed SQL is returned correctly

        except ProgrammingError as e2:
            return (
                f"I couldn't generate a working query for that question. "
                f"The database returned: {str(e2)[:200]}. "
                "Try rephrasing or asking about a broader geographic area.",
                [],
                sql,
            )
        except Exception as e2:
            return (f"An unexpected error occurred: {str(e2)[:200]}", [], sql)

    except TimeoutError as e:
        return (str(e), [], sql)

    except RuntimeError:
        return ("Database connection error. Please try again in a moment.", [], sql)


# ---------------------------------------------------------------------------
# Public Phase 1: SQL generation + query execution
# ---------------------------------------------------------------------------

def run_query_phase(
    question: str,
    conversation_history: list[dict],
) -> tuple[_QueryPhase | None, AgentResponse | None]:
    """
    Phase 1 of the agent pipeline: validate → generate SQL → execute.

    Returns:
        (_QueryPhase, None)      on success — caller should stream the answer
        (None, AgentResponse)    on error or refusal
    """
    start_ms = int(time.time() * 1000)
    _log(f"▶ QUESTION: {question[:120]}")

    # Step 1: Guardrail
    allowed, reason = guardrails.classify_topic(question)
    if not allowed:
        _log(f"  ✗ GUARDRAIL BLOCKED: {reason[:80]}")
        return None, AgentResponse(
            answer=reason, refusal=True,
            elapsed_ms=int(time.time() * 1000) - start_ms,
        )
    _log("  ✓ Guardrail passed")

    # Step 2: Schema selection
    schema_context = get_relevant_schema(question)
    city_note = _build_city_note(question)
    trimmed_history = trim_conversation_history(conversation_history, max_turns=8)
    history_text = format_conversation_for_prompt(trimmed_history)

    sql_user_prompt = USER_PROMPT_SQL_TEMPLATE.format(
        schema_context=schema_context,
        geographic_notes=GEOGRAPHIC_NOTES,
        city_note=city_note,
        history=history_text,
        question=question,
    )

    # Step 3: SQL generation
    _log("  → Claude #1: generating SQL...")
    client = _get_anthropic_client()
    try:
        sql_response = _call_claude(client, SYSTEM_PROMPT_SQL, sql_user_prompt, MAX_TOKENS_SQL, 0)
    except anthropic.APIError:
        return None, AgentResponse(
            answer="The AI service is temporarily unavailable. Please try again.",
            error=True, elapsed_ms=int(time.time() * 1000) - start_ms,
        )

    # Check if Claude refused
    is_ref, ref_msg = guardrails.is_refusal(sql_response)
    if is_ref:
        return None, AgentResponse(
            answer=ref_msg or "I can only answer questions about US Census demographic data.",
            refusal=True, elapsed_ms=int(time.time() * 1000) - start_ms,
        )

    sql = guardrails.extract_sql(sql_response)
    if not sql:
        return None, AgentResponse(
            answer=(
                "I wasn't able to generate a valid query for that question. "
                "Try rephrasing it — for example, specify a state, county, or topic "
                "like income, age, race, or housing."
            ),
            error=True, elapsed_ms=int(time.time() * 1000) - start_ms,
        )

    # Step 4: SQL safety validation
    valid, validation_error = guardrails.validate_sql(sql)
    if not valid:
        return None, AgentResponse(
            answer=f"The generated query didn't pass safety checks ({validation_error}). Please try a different question.",
            sql=sql, error=True, elapsed_ms=int(time.time() * 1000) - start_ms,
        )

    sql = guardrails.enforce_limit(sql)
    _log(f"  ✓ SQL generated:\n{sql}\n")

    # Step 5: Execute (with one retry on SQL error)
    _log("  → Snowflake: executing query...")
    rows_or_err, columns, final_sql = _execute_with_retry(
        client, sql, question, schema_context, city_note, history_text
    )

    if isinstance(rows_or_err, str):
        _log(f"  ✗ Snowflake error: {rows_or_err[:120]}")
        return None, AgentResponse(
            answer=rows_or_err, sql=final_sql, error=True,
            elapsed_ms=int(time.time() * 1000) - start_ms,
        )

    rows: list[dict] = rows_or_err
    _log(f"  ✓ Snowflake returned {len(rows)} row(s), columns: {columns}")

    # Pre-build the answer synthesis prompt
    if not rows:
        answer_prompt = USER_PROMPT_ANSWER_EMPTY_TEMPLATE.format(
            question=question, sql=final_sql
        )
    else:
        results_table = results_to_markdown_table(rows, columns, max_rows=50)
        answer_prompt = USER_PROMPT_ANSWER_TEMPLATE.format(
            question=question, sql=final_sql,
            row_count=len(rows), results_table=results_table,
        )

    return _QueryPhase(
        client=client,
        question=question,
        sql=final_sql,
        rows=rows,
        columns=columns,
        answer_user_prompt=answer_prompt,
        start_ms=start_ms,
    ), None


# ---------------------------------------------------------------------------
# Public Phase 2: Streaming answer synthesis
# ---------------------------------------------------------------------------

def stream_answer(phase: _QueryPhase) -> Generator[str, None, None]:
    """
    Phase 2: stream the natural language answer from Claude.
    Yields text chunks suitable for st.write_stream().
    """
    try:
        with phase.client.messages.stream(
            model=MODEL,
            max_tokens=MAX_TOKENS_ANSWER,
            temperature=0.3,
            system=SYSTEM_PROMPT_ANSWER,
            messages=[{"role": "user", "content": phase.answer_user_prompt}],
        ) as stream:
            yield from stream.text_stream
    except anthropic.APIError:
        yield "I retrieved the data but encountered an error generating the summary. Please try again."


# ---------------------------------------------------------------------------
# Follow-up question suggestions
# ---------------------------------------------------------------------------

def get_followup_suggestions(question: str) -> list[str]:
    """
    Return 3 short follow-up question suggestions related to the answered question.
    Uses claude-haiku for speed/cost. Returns [] on any error.
    """
    import json
    try:
        client = _get_anthropic_client()
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            temperature=0.7,
            system=SYSTEM_PROMPT_FOLLOWUP,
            messages=[{
                "role": "user",
                "content": USER_PROMPT_FOLLOWUP_TEMPLATE.format(question=question),
            }],
        )
        text = response.content[0].text.strip()
        suggestions = json.loads(text)
        if isinstance(suggestions, list):
            return [s for s in suggestions if isinstance(s, str)][:3]
    except Exception:
        pass
    return []


# ---------------------------------------------------------------------------
# Non-streaming convenience wrapper (for testing / fallback)
# ---------------------------------------------------------------------------

def run_agent(
    question: str,
    conversation_history: list[dict],
) -> AgentResponse:
    """Full non-streaming pipeline. Used for fallback or unit testing."""
    phase, err = run_query_phase(question, conversation_history)
    if err:
        return err

    chunks = list(stream_answer(phase))
    answer = "".join(chunks)

    elapsed = int(time.time() * 1000) - phase.start_ms
    return AgentResponse(
        answer=answer,
        sql=phase.sql,
        row_count=len(phase.rows),
        rows=phase.rows,
        columns=phase.columns,
        elapsed_ms=elapsed,
        error=False,
    )
