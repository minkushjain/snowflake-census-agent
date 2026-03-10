"""
US Census Data Chat Agent — Streamlit web application.
Beautiful, interactive chat interface with streaming answers, auto-charts, and CSV export.
Features: dark mode, multi-session conversation history, follow-up suggestions.
"""

import time
import uuid
import pandas as pd
import plotly.express as px
import streamlit as st
from agent import run_query_phase, stream_answer, get_followup_suggestions
from utils import fips_to_state_name

# State FIPS → 2-letter abbreviation (for choropleth)
STATE_FIPS_TO_ABBR = {
    "01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA","08":"CO","09":"CT",
    "10":"DE","11":"DC","12":"FL","13":"GA","15":"HI","16":"ID","17":"IL",
    "18":"IN","19":"IA","20":"KS","21":"KY","22":"LA","23":"ME","24":"MD",
    "25":"MA","26":"MI","27":"MN","28":"MS","29":"MO","30":"MT","31":"NE",
    "32":"NV","33":"NH","34":"NJ","35":"NM","36":"NY","37":"NC","38":"ND",
    "39":"OH","40":"OK","41":"OR","42":"PA","44":"RI","45":"SC","46":"SD",
    "47":"TN","48":"TX","49":"UT","50":"VT","51":"VA","53":"WA","54":"WV",
    "55":"WI","56":"WY",
}

# ---------------------------------------------------------------------------
# Page config (must be first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="US Census AI Assistant",
    page_icon="🇺🇸",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------
def _new_session() -> dict:
    n = len(st.session_state.get("sessions", [])) + 1
    return {
        "id": str(uuid.uuid4())[:8],
        "name": f"Chat {n}",
        "messages": [],
        "query_count": 0,
        "created_at": time.time(),
        "generating": False,
    }

if "sessions" not in st.session_state:
    first = _new_session()
    st.session_state.sessions = [first]
    st.session_state.current_session_id = first["id"]

st.session_state.setdefault("show_sql", True)
st.session_state.setdefault("dark_mode", False)
st.session_state.setdefault("prefilled_question", None)


def _get_session(sid: str) -> dict:
    for s in st.session_state.sessions:
        if s["id"] == sid:
            return s
    return st.session_state.sessions[0]


def _current() -> dict:
    return _get_session(st.session_state.current_session_id)


# ---------------------------------------------------------------------------
# Theme CSS
# ---------------------------------------------------------------------------
LIGHT_CSS = """
<style>
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }
.stApp { background: #f8f9fc; }

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0a1628 0%, #1a2f52 100%);
}
[data-testid="stSidebar"] > div:first-child {
    overflow-y: auto;
    overflow-x: hidden;
    max-height: 100vh;
    padding-bottom: 2rem;
    scrollbar-width: thin;
    scrollbar-color: rgba(255,255,255,0.25) transparent;
}
[data-testid="stSidebar"] > div:first-child::-webkit-scrollbar {
    width: 4px;
}
[data-testid="stSidebar"] > div:first-child::-webkit-scrollbar-track {
    background: transparent;
}
[data-testid="stSidebar"] > div:first-child::-webkit-scrollbar-thumb {
    background: rgba(255,255,255,0.25);
    border-radius: 4px;
}
[data-testid="stSidebar"] > div:first-child::-webkit-scrollbar-thumb:hover {
    background: rgba(255,255,255,0.45);
}
[data-testid="stSidebar"] * { color: #e8edf5 !important; }
[data-testid="stSidebar"] .stButton > button {
    background: rgba(255,255,255,0.07) !important;
    border: 1px solid rgba(255,255,255,0.15) !important;
    color: #e8edf5 !important;
    border-radius: 8px !important;
    text-align: left !important;
    font-size: 0.8rem !important;
    padding: 5px 10px !important;
    margin: 2px 0 !important;
    transition: background 0.2s;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(0,104,201,0.4) !important;
    border-color: rgba(0,104,201,0.6) !important;
    color: #e8edf5 !important;
}
[data-testid="stSidebar"] .stButton > button:active,
[data-testid="stSidebar"] .stButton > button:focus,
[data-testid="stSidebar"] .stButton > button:focus-visible {
    background: rgba(0,104,201,0.5) !important;
    border-color: rgba(0,104,201,0.8) !important;
    color: #e8edf5 !important;
    outline: none !important;
    box-shadow: none !important;
}
[data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.12) !important; }
/* Fix expander header hover — prevent white flash */
[data-testid="stSidebar"] details summary {
    font-size: 0.82rem !important;
    color: rgba(255,255,255,0.8) !important;
}
[data-testid="stSidebar"] details summary:hover,
[data-testid="stSidebar"] details summary:hover * {
    background: rgba(0,104,201,0.25) !important;
    color: #e8edf5 !important;
}
[data-testid="stSidebar"] details summary svg { color: rgba(255,255,255,0.6) !important; }
[data-testid="stSidebar"] .stMetric label,
[data-testid="stSidebar"] .stMetric [data-testid="stMetricValue"] {
    font-size: 0.72rem !important;
}
[data-testid="stSidebar"] p { font-size: 0.8rem !important; }


.hero-banner {
    background: linear-gradient(135deg, #0a1628 0%, #0d2b5e 50%, #1a4a9a 100%);
    border-radius: 16px; padding: 28px 36px; margin-bottom: 24px;
    color: white; display: flex; align-items: center; gap: 20px;
    animation: heroFade 0.6s ease-in;
}
@keyframes heroFade { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: translateY(0); } }
.hero-title { font-size: 1.9rem; font-weight: 700; margin: 0; line-height: 1.2; color: white; }
.hero-subtitle { font-size: 0.9rem; color: rgba(255,255,255,0.75); margin: 6px 0 0 0; }
.hero-badge {
    display: inline-block; background: rgba(255,255,255,0.15);
    border: 1px solid rgba(255,255,255,0.25); border-radius: 20px;
    padding: 3px 10px; font-size: 0.75rem; color: rgba(255,255,255,0.9);
    margin-right: 6px; margin-top: 10px;
}

[data-testid="stChatMessage"] {
    background: white !important; border: 1px solid #e9ecef !important;
    border-radius: 14px !important; padding: 4px 8px !important;
    margin: 8px 0 !important; box-shadow: 0 1px 6px rgba(0,0,0,0.05) !important;
    animation: msgIn 0.25s ease-out;
}
@keyframes msgIn { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: translateY(0); } }

.metric-card {
    background: linear-gradient(135deg, #f0f7ff, #e8f0fe);
    border: 1px solid #c3d9ff; border-radius: 12px;
    padding: 14px 20px; text-align: center;
}
.metric-value { font-size: 1.6rem; font-weight: 700; color: #0d2b5e; }
.metric-label { font-size: 0.75rem; color: #4a6fa5; margin-top: 2px; }

[data-testid="stDownloadButton"] > button {
    background: #f0f7ff !important; color: #0068C9 !important;
    border: 1px solid #c3d9ff !important; border-radius: 8px !important;
    font-size: 0.82rem !important; padding: 4px 12px !important;
}
.timing-caption { font-size: 0.72rem; color: #a0aec0; margin-top: 6px; }
.session-active { background: rgba(0,104,201,0.35) !important; border-color: rgba(0,104,201,0.7) !important; }
.main .block-container { padding-top: 1rem !important; max-width: 1100px; }
</style>
"""

DARK_CSS = """
<style>
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }
.stApp { background: #0f172a !important; }
.main .block-container { background: #0f172a !important; }

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #050d1a 0%, #0a1628 100%);
}
[data-testid="stSidebar"] > div:first-child {
    overflow-y: auto;
    overflow-x: hidden;
    max-height: 100vh;
    padding-bottom: 2rem;
    scrollbar-width: thin;
    scrollbar-color: rgba(255,255,255,0.25) transparent;
}
[data-testid="stSidebar"] > div:first-child::-webkit-scrollbar {
    width: 4px;
}
[data-testid="stSidebar"] > div:first-child::-webkit-scrollbar-track {
    background: transparent;
}
[data-testid="stSidebar"] > div:first-child::-webkit-scrollbar-thumb {
    background: rgba(255,255,255,0.25);
    border-radius: 4px;
}
[data-testid="stSidebar"] > div:first-child::-webkit-scrollbar-thumb:hover {
    background: rgba(255,255,255,0.45);
}
[data-testid="stSidebar"] * { color: #e8edf5 !important; }
[data-testid="stSidebar"] .stButton > button {
    background: rgba(255,255,255,0.07) !important;
    border: 1px solid rgba(255,255,255,0.15) !important;
    color: #e8edf5 !important; border-radius: 8px !important;
    text-align: left !important; font-size: 0.74rem !important;
    padding: 4px 8px !important; margin: 1px 0 !important;
    transition: background 0.2s;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(0,104,201,0.4) !important;
    border-color: rgba(0,104,201,0.6) !important;
    color: #e8edf5 !important;
}
[data-testid="stSidebar"] .stButton > button:active,
[data-testid="stSidebar"] .stButton > button:focus,
[data-testid="stSidebar"] .stButton > button:focus-visible {
    background: rgba(0,104,201,0.5) !important;
    border-color: rgba(0,104,201,0.8) !important;
    color: #e8edf5 !important;
    outline: none !important;
    box-shadow: none !important;
}
[data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.12) !important; }
[data-testid="stSidebar"] details summary {
    font-size: 0.82rem !important;
    color: rgba(255,255,255,0.8) !important;
}
[data-testid="stSidebar"] details summary:hover,
[data-testid="stSidebar"] details summary:hover * {
    background: rgba(0,104,201,0.25) !important;
    color: #e8edf5 !important;
}
[data-testid="stSidebar"] details summary svg { color: rgba(255,255,255,0.6) !important; }
[data-testid="stSidebar"] .stMetric label,
[data-testid="stSidebar"] .stMetric [data-testid="stMetricValue"] {
    font-size: 0.72rem !important;
}
[data-testid="stSidebar"] p { font-size: 0.8rem !important; }


p, span, label, h1, h2, h3 { color: #e2e8f0 !important; }

.hero-banner {
    background: linear-gradient(135deg, #0a1628 0%, #0d2b5e 50%, #1a4a9a 100%);
    border-radius: 16px; padding: 28px 36px; margin-bottom: 24px;
    color: white; display: flex; align-items: center; gap: 20px;
    animation: heroFade 0.6s ease-in;
}
@keyframes heroFade { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: translateY(0); } }
.hero-title { font-size: 1.9rem; font-weight: 700; margin: 0; line-height: 1.2; color: white !important; }
.hero-subtitle { font-size: 0.9rem; color: rgba(255,255,255,0.75) !important; margin: 6px 0 0 0; }
.hero-badge {
    display: inline-block; background: rgba(255,255,255,0.15);
    border: 1px solid rgba(255,255,255,0.25); border-radius: 20px;
    padding: 3px 10px; font-size: 0.75rem; color: rgba(255,255,255,0.9) !important;
    margin-right: 6px; margin-top: 10px;
}

[data-testid="stChatMessage"] {
    background: #1e293b !important; border: 1px solid #334155 !important;
    border-radius: 14px !important; padding: 4px 8px !important;
    margin: 8px 0 !important; box-shadow: 0 1px 8px rgba(0,0,0,0.3) !important;
    animation: msgIn 0.25s ease-out;
}
[data-testid="stChatMessage"] * { color: #e2e8f0 !important; }
@keyframes msgIn { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: translateY(0); } }

.metric-card {
    background: linear-gradient(135deg, #1e3a5f, #1a2f52);
    border: 1px solid #2a4a7f; border-radius: 12px;
    padding: 14px 20px; text-align: center;
}
.metric-value { font-size: 1.6rem; font-weight: 700; color: #93c5fd !important; }
.metric-label { font-size: 0.75rem; color: #94a3b8 !important; margin-top: 2px; }

[data-testid="stDownloadButton"] > button {
    background: #1e3a5f !important; color: #93c5fd !important;
    border: 1px solid #2a4a7f !important; border-radius: 8px !important;
    font-size: 0.82rem !important; padding: 4px 12px !important;
}
[data-testid="stStatusWidget"] { background: #1e293b !important; border-color: #334155 !important; }
[data-testid="stExpander"] { background: #1e293b !important; border-color: #334155 !important; }
.stCodeBlock, pre, code { background: #0f172a !important; color: #93c5fd !important; }
.timing-caption { font-size: 0.72rem; color: #64748b !important; margin-top: 6px; }
.session-active { background: rgba(0,104,201,0.35) !important; border-color: rgba(0,104,201,0.7) !important; }
.main .block-container { padding-top: 1rem !important; max-width: 1100px; }

/* Welcome screen buttons */
[data-testid="stButton"] > button {
    background: #1e293b !important; border-color: #334155 !important;
    color: #e2e8f0 !important;
}
[data-testid="stButton"] > button:hover {
    background: #1e3a5f !important; border-color: #2a4a7f !important;
}

/* Dark mode chat input box */
[data-testid="stChatInputContainer"],
[data-testid="stChatInputContainer"] > div,
[data-testid="stChatInputContainer"] > div > div {
    background: #1e293b !important;
    border-color: #334155 !important;
}
[data-testid="stChatInputContainer"] textarea {
    background: #1e293b !important;
    color: #e2e8f0 !important;
    caret-color: #93c5fd !important;
}
[data-testid="stChatInputContainer"] textarea::placeholder {
    color: #64748b !important;
}
/* Bottom bar area background */
.stBottom, .stBottom > div {
    background: #0f172a !important;
    border-top-color: #1e293b !important;
}
</style>
"""

st.markdown(DARK_CSS if st.session_state.dark_mode else LIGHT_CSS, unsafe_allow_html=True)

# Hide the sidebar collapse button so users can't accidentally close it
st.markdown(
    """
    <style>
    [data-testid="stSidebarCollapseButton"] { display: none !important; }
    [data-testid="collapsedControl"] { display: none !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
EXAMPLE_QUESTIONS = {
    "👥 Population": [
        "What is the total population of the United States?",
        "What is the median age in Florida?",
        "Which state has the largest population?",
    ],
    "💰 Income & Poverty": [
        "Which state has the highest median household income?",
        "Compare the per capita income of Alabama vs Massachusetts.",
        "What are the top 10 counties by poverty rate?",
    ],
    "🏠 Housing": [
        "What percentage of housing in San Francisco is renter-occupied?",
        "Which state has the highest median home value?",
        "What is the vacancy rate in Detroit?",
    ],
    "🎓 Education": [
        "Which states have the highest percentage of college graduates?",
        "Compare education levels between Texas and Massachusetts.",
        "What percentage of adults in Mississippi have a high school diploma?",
    ],
    "🧬 Race & Ethnicity": [
        "What percentage of California's population is Hispanic?",
        "How does the racial composition differ between Texas and New York?",
        "Which states have the highest percentage of Asian residents?",
    ],
}

with st.sidebar:
    # Header
    st.markdown(
        """
        <div style="padding: 12px 0 8px 0;">
            <div style="font-size:1.3rem; font-weight:700; color:#e8edf5;">🇺🇸 Census AI</div>
            <div style="font-size:0.75rem; color:rgba(255,255,255,0.55); margin-top:2px;">
                2019 ACS · Snowflake · Claude Sonnet
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    # Dark mode toggle
    dm_label = "☀️ Light mode" if st.session_state.dark_mode else "🌙 Dark mode"
    if st.button(dm_label, use_container_width=True):
        st.session_state.dark_mode = not st.session_state.dark_mode
        st.rerun()

    st.divider()

    # Stats
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Queries", _current()["query_count"])
    with col2:
        st.metric("Messages", len(_current()["messages"]))

    st.divider()

    # Settings
    show_sql = st.toggle(
        "🔍 Show SQL queries",
        value=st.session_state.show_sql,
        help="Display the Snowflake SQL query used for each answer",
    )
    st.session_state.show_sql = show_sql

    st.divider()

    # Conversation history
    st.markdown(
        "<div style='font-size:0.8rem; font-weight:600; color:rgba(255,255,255,0.7); "
        "text-transform:uppercase; letter-spacing:0.05em; margin-bottom:8px;'>Conversations</div>",
        unsafe_allow_html=True,
    )

    if st.button("＋ New conversation", use_container_width=True):
        new_s = _new_session()
        st.session_state.sessions.append(new_s)
        st.session_state.current_session_id = new_s["id"]
        st.rerun()

    for s in reversed(st.session_state.sessions):
        is_active = s["id"] == st.session_state.current_session_id
        # Show message count as hint
        n_msgs = len(s["messages"])
        label = f"{'▶ ' if is_active else ''}{s['name']}  ({n_msgs // 2} Q)"
        btn_key = f"sess_{s['id']}"
        if st.button(label, key=btn_key, use_container_width=True):
            st.session_state.current_session_id = s["id"]
            st.rerun()

    st.divider()

    # Example questions
    st.markdown(
        "<div style='font-size:0.8rem; font-weight:600; color:rgba(255,255,255,0.7); "
        "text-transform:uppercase; letter-spacing:0.05em; margin-bottom:8px;'>Try asking</div>",
        unsafe_allow_html=True,
    )

    for category, questions in EXAMPLE_QUESTIONS.items():
        with st.expander(category, expanded=False):
            for q in questions:
                if st.button(q, key=f"ex_{hash(q)}", use_container_width=True):
                    st.session_state.prefilled_question = q
                    st.rerun()

    st.divider()

    # Export conversation as Markdown
    session_msgs = _current()["messages"]
    if session_msgs:
        export_lines = [f"# Census AI — {_current()['name']}\n\n"]
        for m in session_msgs:
            role = "**You**" if m["role"] == "user" else "**Assistant**"
            export_lines.append(f"### {role}\n{m['content']}\n")
            if m.get("sql"):
                export_lines.append(f"```sql\n{m['sql']}\n```\n")
        export_md = "\n".join(export_lines)
        st.download_button(
            "📥 Export conversation",
            data=export_md,
            file_name="census_conversation.md",
            mime="text/markdown",
            use_container_width=True,
            key="export_conv",
        )

    if st.button("🗑️ Clear this conversation", use_container_width=True, type="secondary"):
        _current()["messages"] = []
        _current()["query_count"] = 0
        st.rerun()

    st.divider()

    st.markdown(
        """
        <div style='font-size:0.72rem; color:rgba(255,255,255,0.4); line-height:1.6;'>
        <b style='color:rgba(255,255,255,0.6);'>Data:</b> SafeGraph US Open Census Data<br>
        <b style='color:rgba(255,255,255,0.6);'>Source:</b> Snowflake Marketplace<br>
        <b style='color:rgba(255,255,255,0.6);'>Vintage:</b> 2019 ACS 5-year estimates<br>
        <b style='color:rgba(255,255,255,0.6);'>Coverage:</b> ~220K Census Block Groups
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Hero banner
# ---------------------------------------------------------------------------
st.markdown(
    """
    <div class="hero-banner">
        <div style="font-size:3rem; line-height:1;">🇺🇸</div>
        <div>
            <p class="hero-title">US Census Data Assistant</p>
            <p class="hero-subtitle">
                Ask anything about US population, income, housing, education, race, or poverty.
            </p>
            <div style="margin-top:10px;">
                <span class="hero-badge">2019 ACS Data</span>
                <span class="hero-badge">220K+ Census Block Groups</span>
                <span class="hero-badge">Natural Language → SQL</span>
                <span class="hero-badge">Claude Sonnet + Snowflake</span>
            </div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Welcome screen (shown only when no messages in current session)
# ---------------------------------------------------------------------------
messages = _current()["messages"]

if not messages:
    st.markdown("### What would you like to know?")
    st.markdown(
        "<p style='color:#718096; margin-bottom:16px;'>"
        "Click a topic to start, or type your own question below.</p>",
        unsafe_allow_html=True,
    )

    welcome_questions = [
        ("🗺️", "Income Map", "Which states have the highest median household income?"),
        ("👥", "Population", "What are the top 15 states by total population?"),
        ("🎓", "Education", "Which states have the highest percentage of college graduates?"),
        ("📉", "Poverty", "Rank all states by poverty rate from highest to lowest"),
        ("🏠", "Housing", "What percentage of housing units are renter-occupied in each state?"),
        ("🧬", "Diversity", "What is the Hispanic population percentage in each state?"),
    ]

    cols = st.columns(3)
    for i, (icon, name, question) in enumerate(welcome_questions):
        with cols[i % 3]:
            if st.button(
                f"{icon} **{name}**\n\n_{question}_",
                key=f"welcome_{i}",
                use_container_width=True,
            ):
                st.session_state.prefilled_question = question
                st.rerun()

    st.divider()


# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------
def _enrich_with_state_names(rows: list[dict], columns: list[str]) -> tuple[list[dict], list[str]]:
    """Replace 2-digit FIPS codes with readable state names for chart labels."""
    if not rows:
        return rows, columns

    fips_col = None
    for col in columns:
        vals = [str(r.get(col, "")) for r in rows if r.get(col) is not None]
        if vals and all(v.isdigit() and len(v) <= 2 for v in vals[:5]):
            fips_col = col
            break

    if not fips_col:
        return rows, columns

    enriched = []
    for row in rows:
        new_row = dict(row)
        fips = str(new_row.get(fips_col, ""))
        state_name = fips_to_state_name(fips)
        if not state_name.startswith("FIPS"):
            new_row[fips_col] = state_name
        enriched.append(new_row)
    return enriched, columns


def _detect_label_value(df: pd.DataFrame, columns: list[str]) -> tuple[str | None, str | None]:
    """Find best label column (text) and value column (numeric) from a DataFrame."""
    label_col = None
    value_col = None
    for col in columns:
        if value_col is None:
            try:
                df[col] = pd.to_numeric(df[col])
                value_col = col
                continue
            except (ValueError, TypeError):
                pass
        if label_col is None and df[col].dtype == object:
            label_col = col
    return label_col, value_col


def _try_render_map(rows: list[dict], columns: list[str]) -> bool:
    """Render a US choropleth map when data covers 10+ states. Returns True if rendered."""
    if not rows or len(rows) < 10 or len(columns) < 2:
        return False

    df = pd.DataFrame(rows, columns=columns)
    label_col, value_col = _detect_label_value(df, list(columns))
    if not label_col or not value_col:
        return False

    # Check if label col contains state FIPS codes (2-digit numeric strings)
    sample = [str(v).strip().zfill(2) for v in df[label_col].dropna().head(10)]
    if not all(s in STATE_FIPS_TO_ABBR for s in sample):
        return False

    df["state_abbr"] = df[label_col].apply(lambda v: STATE_FIPS_TO_ABBR.get(str(v).strip().zfill(2), ""))
    df = df[df["state_abbr"] != ""]
    if len(df) < 10:
        return False

    col_label = value_col.replace("_", " ").title()
    fig = px.choropleth(
        df,
        locations="state_abbr",
        locationmode="USA-states",
        color=value_col,
        scope="usa",
        color_continuous_scale="Blues",
        labels={value_col: col_label},
        hover_name="state_abbr",
        hover_data={value_col: ":,.0f", "state_abbr": False},
        title=f"US Map — {col_label}",
    )
    fig.update_layout(
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        coloraxis_colorbar={"title": col_label, "thickness": 14},
        height=420,
    )
    st.plotly_chart(fig, use_container_width=True)
    return True


def _try_render_chart(rows: list[dict], columns: list[str]) -> bool:
    """Render interactive Plotly horizontal bar chart for ranked results. Returns True if rendered."""
    if not rows or len(rows) < 2 or len(rows) > 50 or len(columns) < 2:
        return False

    df = pd.DataFrame(rows, columns=columns)
    label_col, value_col = _detect_label_value(df, list(columns))

    if not label_col or not value_col:
        return False

    chart_df = (
        df[[label_col, value_col]]
        .dropna()
        .sort_values(value_col, ascending=False)
        .head(20)
    )
    if len(chart_df) < 2:
        return False

    col_label = value_col.replace("_", " ").title()
    n = len(chart_df)
    colors = [f"rgba(0, {int(68 + i * (180 / max(n - 1, 1)))}, {int(150 + i * (80 / max(n - 1, 1)))}, 0.85)" for i in range(n)]

    fig = px.bar(
        chart_df,
        x=value_col,
        y=label_col,
        orientation="h",
        labels={value_col: col_label, label_col: ""},
        title=col_label,
        color=value_col,
        color_continuous_scale="Blues",
    )
    fig.update_traces(
        hovertemplate=f"<b>%{{y}}</b><br>{col_label}: %{{x:,.0f}}<extra></extra>",
    )
    fig.update_layout(
        yaxis={"autorange": "reversed", "tickfont": {"size": 12}},
        xaxis={"tickformat": ","},
        margin={"l": 10, "r": 20, "t": 40, "b": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
        height=max(280, n * 30 + 80),
        showlegend=False,
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)")
    fig.update_yaxes(showgrid=False)
    st.plotly_chart(fig, use_container_width=True)
    return True


def _render_key_insights(rows: list[dict], columns: list[str]) -> None:
    """Render a Key Insights panel highlighting highest, lowest, and average values."""
    if not rows or len(rows) < 3 or len(columns) < 2:
        return

    df = pd.DataFrame(rows, columns=columns)
    label_col, value_col = _detect_label_value(df, list(columns))
    if not label_col or not value_col:
        return

    df = df[[label_col, value_col]].dropna()
    if len(df) < 3:
        return

    col_label = value_col.replace("_", " ").title()
    max_row = df.loc[df[value_col].idxmax()]
    min_row = df.loc[df[value_col].idxmin()]
    avg_val = df[value_col].mean()

    def _fmt(v):
        if v >= 1_000_000:
            return f"{v / 1_000_000:.1f}M"
        if v >= 1_000:
            return f"{v:,.0f}"
        return f"{v:,.2f}"

    st.markdown(
        f"""
        <div style="background:linear-gradient(135deg,#f0f7ff,#e8f0fe);border:1px solid #c3d9ff;
                    border-radius:12px;padding:14px 20px;margin:10px 0;">
            <div style="font-size:0.75rem;font-weight:700;color:#4a6fa5;
                        text-transform:uppercase;letter-spacing:0.05em;margin-bottom:10px;">
                📊 Key Insights — {col_label}
            </div>
            <div style="display:flex;gap:16px;flex-wrap:wrap;">
                <div style="flex:1;min-width:140px;">
                    <div style="font-size:0.7rem;color:#4a6fa5;">🏆 Highest</div>
                    <div style="font-size:1.1rem;font-weight:700;color:#0d2b5e;">{_fmt(max_row[value_col])}</div>
                    <div style="font-size:0.75rem;color:#718096;">{max_row[label_col]}</div>
                </div>
                <div style="flex:1;min-width:140px;">
                    <div style="font-size:0.7rem;color:#4a6fa5;">📉 Lowest</div>
                    <div style="font-size:1.1rem;font-weight:700;color:#0d2b5e;">{_fmt(min_row[value_col])}</div>
                    <div style="font-size:0.75rem;color:#718096;">{min_row[label_col]}</div>
                </div>
                <div style="flex:1;min-width:140px;">
                    <div style="font-size:0.7rem;color:#4a6fa5;">∅ Average</div>
                    <div style="font-size:1.1rem;font-weight:700;color:#0d2b5e;">{_fmt(avg_val)}</div>
                    <div style="font-size:0.75rem;color:#718096;">across {len(df):,} records</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _try_render_metrics(rows: list[dict], columns: list[str]) -> bool:
    """If result is a single row with numeric columns, render as metric cards. Returns True if rendered."""
    if len(rows) != 1 or not columns:
        return False

    row = rows[0]
    numeric_pairs = []
    for col in columns:
        val = row.get(col)
        if val is None:
            continue
        try:
            numeric_pairs.append((col, float(val)))
        except (TypeError, ValueError):
            pass

    if not numeric_pairs or len(numeric_pairs) > 6:
        return False

    cols = st.columns(len(numeric_pairs))
    for i, (label, val) in enumerate(numeric_pairs):
        with cols[i]:
            if val >= 1_000_000:
                display = f"{val/1_000_000:.1f}M"
            elif val >= 1_000:
                display = f"{val:,.0f}"
            else:
                display = f"{val:,.2f}"
            st.markdown(
                f"""<div class="metric-card">
                    <div class="metric-value">{display}</div>
                    <div class="metric-label">{label.replace("_", " ").title()}</div>
                </div>""",
                unsafe_allow_html=True,
            )
    return True


# ---------------------------------------------------------------------------
# Orphan cleanup — if a rerun interrupted generation mid-stream, the user
# message was appended but the assistant message was never saved. Remove it.
# ---------------------------------------------------------------------------
_sess = _current()
if _sess.get("generating") and _sess["messages"] and _sess["messages"][-1]["role"] == "user":
    _sess["messages"].pop()
    _sess["generating"] = False
    st.session_state.prefilled_question = None

# ---------------------------------------------------------------------------
# Render existing conversation
# ---------------------------------------------------------------------------
for msg in messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

        if msg["role"] == "assistant":
            rows = msg.get("rows", [])
            cols_stored = msg.get("columns", [])
            if rows:
                enriched_rows, enriched_cols = _enrich_with_state_names(rows, cols_stored)
                rendered = _try_render_metrics(enriched_rows, enriched_cols)
                if not rendered:
                    map_rendered = _try_render_map(enriched_rows, enriched_cols)
                    _try_render_chart(enriched_rows, enriched_cols)
                    if not map_rendered and len(rows) >= 3:
                        _render_key_insights(enriched_rows, enriched_cols)
                # Raw data table
                if len(rows) >= 2:
                    with st.expander("📋 View raw data table", expanded=False):
                        st.dataframe(
                            pd.DataFrame(rows, columns=cols_stored),
                            use_container_width=True,
                        )

            if st.session_state.show_sql and msg.get("sql"):
                with st.expander("🔍 View SQL query", expanded=False):
                    st.code(msg["sql"], language="sql")
                    if msg.get("row_count"):
                        st.caption(f"Returned {msg['row_count']:,} row(s)")

            if rows and cols_stored:
                csv_df = pd.DataFrame(rows, columns=cols_stored)
                st.download_button(
                    "⬇️ Download data as CSV",
                    csv_df.to_csv(index=False),
                    file_name="census_data.csv",
                    mime="text/csv",
                    key=f"dl_{msg.get('timestamp', id(msg))}",
                )

            if msg.get("elapsed_ms"):
                st.markdown(
                    f"<div class='timing-caption'>⏱ {msg['elapsed_ms']/1000:.1f}s</div>",
                    unsafe_allow_html=True,
                )

# ---------------------------------------------------------------------------
# Handle input
# ---------------------------------------------------------------------------
user_input: str | None = st.chat_input("Ask anything about US Census data…")

if st.session_state.prefilled_question and not user_input:
    user_input = st.session_state.prefilled_question
    st.session_state.prefilled_question = None

if user_input:
    user_input = user_input.strip()
    if not user_input:
        st.stop()

    session = _current()

    with st.chat_message("user"):
        st.markdown(user_input)

    session["generating"] = True
    session["messages"].append(
        {"role": "user", "content": user_input, "timestamp": time.time()}
    )

    # Auto-name the session from first question
    if session["query_count"] == 0:
        session["name"] = user_input[:30] + ("…" if len(user_input) > 30 else "")

    agent_history = [
        {"role": m["role"], "content": m["content"]}
        for m in session["messages"][:-1]
        if m["role"] in ("user", "assistant")
    ]

    generation_complete = False
    try:
        with st.chat_message("assistant"):
            start_ts = time.time()

            with st.status("Analyzing question…", expanded=True) as status:
                st.write("🔍 Understanding your question…")
                phase, err_response = run_query_phase(user_input, agent_history)

                if phase is not None:
                    rows_info = f"{len(phase.rows):,} row(s)" if phase.rows else "aggregated result"
                    st.write(f"⚡ Snowflake query executed — {rows_info}")
                    st.write("✍️ Generating answer…")
                    status.update(label="Generating answer…", state="running", expanded=False)
                else:
                    status.update(label="Done", state="complete", expanded=False)

            if err_response is not None:
                if err_response.refusal:
                    st.info(f"🚫 {err_response.answer}")
                else:
                    st.error(err_response.answer)
                answer = err_response.answer
                final_sql = err_response.sql
                final_rows: list[dict] = []
                final_columns: list[str] = []
                row_count = 0

            else:
                answer = st.write_stream(stream_answer(phase))
                final_sql = phase.sql
                final_rows = phase.rows
                final_columns = phase.columns
                row_count = len(phase.rows)

                if final_rows:
                    enriched_rows, enriched_cols = _enrich_with_state_names(final_rows, final_columns)
                    rendered_metric = _try_render_metrics(enriched_rows, enriched_cols)
                    if not rendered_metric:
                        map_rendered = _try_render_map(enriched_rows, enriched_cols)
                        _try_render_chart(enriched_rows, enriched_cols)
                        if not map_rendered and len(final_rows) >= 3:
                            _render_key_insights(enriched_rows, enriched_cols)
                    # Raw data table
                    if len(final_rows) >= 2:
                        with st.expander("📋 View raw data table", expanded=False):
                            st.dataframe(
                                pd.DataFrame(final_rows, columns=final_columns),
                                use_container_width=True,
                            )

                if st.session_state.show_sql and final_sql:
                    with st.expander("🔍 View SQL query", expanded=False):
                        st.code(final_sql, language="sql")
                        st.caption(f"Returned {row_count:,} row(s)")

                if final_rows:
                    csv_df = pd.DataFrame(final_rows, columns=final_columns)
                    st.download_button(
                        "⬇️ Download data as CSV",
                        csv_df.to_csv(index=False),
                        file_name="census_data.csv",
                        mime="text/csv",
                        key=f"dl_new_{time.time()}",
                    )

                # Follow-up suggestions
                suggestions = get_followup_suggestions(user_input)
                if suggestions:
                    st.markdown(
                        "<div style='font-size:0.8rem; color:#718096; margin-top:12px; margin-bottom:6px;'>"
                        "💡 <b>You might also ask:</b></div>",
                        unsafe_allow_html=True,
                    )
                    sug_cols = st.columns(len(suggestions))
                    for i, suggestion in enumerate(suggestions):
                        with sug_cols[i]:
                            if st.button(suggestion, key=f"sug_{time.time()}_{i}", use_container_width=True):
                                st.session_state.prefilled_question = suggestion
                                st.rerun()

            elapsed_ms = int((time.time() - start_ts) * 1000)
            st.markdown(
                f"<div class='timing-caption'>⏱ {elapsed_ms/1000:.1f}s</div>",
                unsafe_allow_html=True,
            )

        session["query_count"] += 1
        session["messages"].append(
            {
                "role": "assistant",
                "content": answer,
                "sql": final_sql,
                "rows": final_rows,
                "columns": final_columns,
                "row_count": row_count,
                "elapsed_ms": elapsed_ms,
                "timestamp": time.time(),
            }
        )
        generation_complete = True

    finally:
        session["generating"] = False
        # If generation was interrupted before the assistant message was saved,
        # remove the orphaned user message so the user can retry cleanly.
        if not generation_complete and session["messages"] and session["messages"][-1]["role"] == "user":
            session["messages"].pop()
