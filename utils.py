"""
Shared utility functions: result formatting, history trimming, FIPS lookups, number formatting.
"""

from typing import Any
# Shared state lookup table lives in schema metadata.
from schema_metadata import STATE_FIPS


def results_to_markdown_table(
    rows: list[dict[str, Any]],
    columns: list[str],
    max_rows: int = 50,
) -> str:
    """
    Convert query results to a compact markdown table for Claude's answer prompt.

    Caps at max_rows to control token usage — sending all 220K CBG rows to Claude
    would be extremely expensive and unhelpful. The truncation note tells Claude
    that the full dataset was larger, so it doesn't undercount in its answer.
    The full result set is still stored in _QueryPhase.rows for UI rendering.
    """
    if not rows or not columns:
        return "(no rows returned)"

    truncated = len(rows) > max_rows
    display_rows = rows[:max_rows]

    header = "| " + " | ".join(str(c) for c in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"

    data_lines = []
    for row in display_rows:
        cells = []
        for col in columns:
            val = row.get(col)
            if val is None:
                cells.append("N/A")
            elif isinstance(val, float):
                # 2 decimal places for percentages/ratios; commas for readability
                cells.append(f"{val:,.2f}")
            elif isinstance(val, int):
                cells.append(f"{val:,}")
            else:
                cells.append(str(val))
        data_lines.append("| " + " | ".join(cells) + " |")

    table = "\n".join([header, separator] + data_lines)

    if truncated:
        table += f"\n\n_(showing first {max_rows} of {len(rows)} rows)_"

    return table


def trim_conversation_history(
    history: list[dict[str, str]],
    max_turns: int = 8,
) -> list[dict[str, str]]:
    """
    Trim conversation history to keep token usage manageable.

    Strategy: keep the first message (establishes the session's geographic/topic
    context, e.g. "Tell me about California") plus the most recent (max_turns - 1)
    messages. This preserves both the original context and the immediate thread,
    which handles the most common follow-up patterns without a sliding-window-only approach.
    """
    if len(history) <= max_turns:
        return history

    first = history[:1]
    recent = history[-(max_turns - 1):]

    # If history is short enough that the first message is already in `recent`,
    # returning both would duplicate it.
    if history[0] in recent:
        return recent

    return first + recent


def format_conversation_for_prompt(history: list[dict[str, str]]) -> str:
    """
    Convert conversation history list to a formatted string for injection into prompts.

    Only `role` and `content` are included — SQL queries, row data, timestamps, and
    other metadata stored on messages are intentionally stripped. This keeps the
    conversation context token-efficient and avoids leaking internal SQL to Claude's
    answer synthesis step. Individual messages are capped at 500 chars for the same reason.
    """
    if not history:
        return "(no prior conversation)"

    lines = []
    for msg in history:
        role = "User" if msg["role"] == "user" else "Assistant"
        content = msg.get("content", "")
        if len(content) > 500:
            content = content[:500] + "... [truncated]"
        lines.append(f"{role}: {content}")

    return "\n".join(lines)


def fips_to_state_name(fips_code: str) -> str:
    """Convert a 2-digit state FIPS code to the state name."""
    # zfill keeps behavior consistent for values like "6" -> "06".
    return STATE_FIPS.get(str(fips_code).zfill(2), f"FIPS {fips_code}")
