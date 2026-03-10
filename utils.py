"""
Shared utility functions: result formatting, history trimming, FIPS lookups, number formatting.
"""

from typing import Any
from schema_metadata import STATE_FIPS


def results_to_markdown_table(
    rows: list[dict[str, Any]],
    columns: list[str],
    max_rows: int = 50,
) -> str:
    """
    Convert query results to a compact markdown table for Claude.

    Caps at max_rows to stay within token limits. Shows a note if truncated.
    """
    if not rows or not columns:
        return "(no rows returned)"

    truncated = len(rows) > max_rows
    display_rows = rows[:max_rows]

    # Build header
    header = "| " + " | ".join(str(c) for c in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"

    # Build rows
    data_lines = []
    for row in display_rows:
        cells = []
        for col in columns:
            val = row.get(col)
            if val is None:
                cells.append("N/A")
            elif isinstance(val, float):
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

    Strategy: keep the first message (establishes geographic/topic context)
    plus the most recent (max_turns - 1) messages.
    """
    if len(history) <= max_turns:
        return history

    # Always keep the first exchange (user message at index 0)
    first = history[:1]
    recent = history[-(max_turns - 1):]

    # Avoid duplication if history is very short
    if history[0] in recent:
        return recent

    return first + recent


def format_conversation_for_prompt(history: list[dict[str, str]]) -> str:
    """
    Convert conversation history list to a formatted string for injection into prompts.
    Only includes role and content (not SQL or metadata).
    """
    if not history:
        return "(no prior conversation)"

    lines = []
    for msg in history:
        role = "User" if msg["role"] == "user" else "Assistant"
        content = msg.get("content", "")
        # Truncate very long messages to avoid token bloat
        if len(content) > 500:
            content = content[:500] + "... [truncated]"
        lines.append(f"{role}: {content}")

    return "\n".join(lines)


def fips_to_state_name(fips_code: str) -> str:
    """Convert a 2-digit state FIPS code to the state name."""
    return STATE_FIPS.get(str(fips_code).zfill(2), f"FIPS {fips_code}")
