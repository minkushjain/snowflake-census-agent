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

# Sonnet 4.6 chosen for its strong instruction-following and SQL accuracy.
# Swapping to a different model only requires changing this constant.
MODEL = "claude-sonnet-4-6"

# 1024 tokens is enough for even complex multi-table SQL; overshooting wastes latency.
MAX_TOKENS_SQL = 1024
# 1536 gives Claude room for a thorough 3-4 paragraph answer without padding.
MAX_TOKENS_ANSWER = 1536


# ---------------------------------------------------------------------------
# Response dataclasses
# ---------------------------------------------------------------------------

@dataclass
class AgentResponse:
    """Final response payload returned to the Streamlit UI layer."""
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
    """Initialize Anthropic client from Streamlit secrets (never at module import).

    Intentionally not cached — each call creates a fresh client, which is cheap
    (no network call). Avoiding module-level init ensures secrets are available
    when the function is first called, not when the module is imported.
    """
    return anthropic.Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])


def _call_claude(
    client: anthropic.Anthropic,
    system: str,
    user_message: str,
    max_tokens: int,
    temperature: float,
) -> str:
    """Call Claude synchronously and return the response text.

    Used for SQL generation (temperature=0, deterministic) and the retry path.
    Answer synthesis uses the streaming variant instead — see stream_answer().
    History is intentionally not passed here; the caller embeds it in user_message.
    """
    response = client.messages.create(
        model=MODEL,
        max_tokens=max_tokens,
        temperature=temperature,
        system=system,
        messages=[{"role": "user", "content": user_message}],
    )
    return response.content[0].text


def _build_city_note(question: str) -> str:
    """If question mentions a known city, return a FIPS hint for SQL generation.

    The ACS dataset has no city column — only 12-digit Census Block Group FIPS codes.
    Cities are approximated by filtering to their primary county's 5-digit FIPS prefix.
    This note is injected into the SQL prompt so Claude generates the correct WHERE clause
    instead of hallucinating a non-existent city filter.
    """
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
        # ProgrammingError means the SQL is syntactically/semantically wrong
        # (e.g., wrong column name, bad quoting). Send it back to Claude with the
        # error message so it can self-correct. Only retry once to bound latency.
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

    # Step 2: Schema selection — keyword scoring picks the 1-2 most relevant ACS
    # tables out of 8 and returns only their columns (~800 tokens vs 7,500+ for the
    # full schema). This is the primary cost and latency optimization in the pipeline.
    schema_context = get_relevant_schema(question)
    city_note = _build_city_note(question)
    # Keep last 8 turns to give Claude follow-up context without blowing up the prompt.
    trimmed_history = trim_conversation_history(conversation_history, max_turns=8)
    history_text = format_conversation_for_prompt(trimmed_history)

    sql_user_prompt = USER_PROMPT_SQL_TEMPLATE.format(
        schema_context=schema_context,
        geographic_notes=GEOGRAPHIC_NOTES,
        city_note=city_note,
        history=history_text,
        question=question,
    )

    # Step 3: SQL generation — temperature=0 makes output deterministic so the same
    # question always produces the same SQL. This also improves caching hit rates.
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

    # Pre-build the answer prompt here (Phase 1) so stream_answer() (Phase 2) needs
    # no extra arguments — the _QueryPhase object carries everything it needs.
    # max_rows=50 caps the markdown table size sent to Claude; beyond that we show
    # a truncation note. Full results are still stored in phase.rows for the UI.
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

    Split from Phase 1 so the UI can show a progress indicator during SQL execution,
    then immediately begin streaming the answer token-by-token once data arrives.
    temperature=0.3 allows natural-sounding prose while staying factually grounded.
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
        # Data was fetched successfully — only the summary generation failed.
        # Yield a recoverable message so the user knows what happened.
        yield "I retrieved the data but encountered an error generating the summary. Please try again."


# ---------------------------------------------------------------------------
# Follow-up question suggestions
# ---------------------------------------------------------------------------

def get_followup_suggestions(question: str) -> list[str]:
    """
    Return 3 short follow-up question suggestions related to the answered question.
    Uses a lightweight Claude model when available and falls back gracefully.
    """
    import json
    import re

    def _default_suggestions() -> list[str]:
        """Safe fallback shown when model output is unavailable/unparseable."""
        return [
            "How does this compare to the national average?",
            "Can you break this down by state or county?",
            "How has this changed over time?",
        ]

    def _normalize_suggestions(items: object) -> list[str]:
        """Clean model output into short, unique strings for UI buttons."""
        if not isinstance(items, list):
            return []
        cleaned: list[str] = []
        for item in items:
            if not isinstance(item, str):
                continue
            suggestion = " ".join(item.split()).strip()
            if suggestion:
                cleaned.append(suggestion[:140])
        # De-dupe while preserving order
        return list(dict.fromkeys(cleaned))[:3]

    def _parse_suggestions(raw_text: str) -> list[str]:
        """Parse strict JSON first, then recover JSON arrays from wrapped text."""
        text = raw_text.strip()
        try:
            return _normalize_suggestions(json.loads(text))
        except Exception:
            # Handle fenced markdown / extra explanatory text around the array.
            if text.startswith("```"):
                text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
                text = re.sub(r"\s*```$", "", text)
            match = re.search(r"\[[\s\S]*\]", text)
            if not match:
                return []
            try:
                return _normalize_suggestions(json.loads(match.group(0)))
            except Exception:
                return []

    client = _get_anthropic_client()
    user_prompt = USER_PROMPT_FOLLOWUP_TEMPLATE.format(question=question)
    # Haiku is significantly cheaper and faster than Sonnet for this low-stakes task.
    # Falling back to MODEL ensures suggestions still appear even if Haiku is unavailable.
    model_candidates = ("claude-haiku-4-5-20251001", MODEL)

    for model_name in model_candidates:
        try:
            response = client.messages.create(
                model=model_name,
                max_tokens=200,
                temperature=0.7,
                system=SYSTEM_PROMPT_FOLLOWUP,
                messages=[{"role": "user", "content": user_prompt}],
            )
            text = response.content[0].text
            suggestions = _parse_suggestions(text)
            if suggestions:
                return suggestions
        except Exception as e:
            _log(f"  ⚠ Follow-up suggestion generation failed with {model_name}: {str(e)[:120]}")

    return _default_suggestions()


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
