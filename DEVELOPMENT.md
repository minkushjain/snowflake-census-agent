# Development Process & Future Improvements

## Development Process

### Approach

The central challenge was bridging two very different systems: a natural language conversational interface and a structured relational database with 7,500+ columns spread across ACS demographic tables.

I chose a **text-to-SQL architecture** rather than pre-computing embeddings or using vector search, because:
1. The questions are factual and structured — SQL is the right query language
2. Snowflake already has the data; no ETL or data movement needed
3. Claude Sonnet 4.6 has excellent SQL generation capability, especially with good prompt engineering

### Architecture

```
User question
    ↓
Guardrail layer (NSFW blocklist + topic classifier)
    ↓
Schema selection (keyword scoring → 1-2 relevant tables, ~800 tokens)
    ↓
Query result cache check (SHA-256 key, 1-hour TTL)
    ↓  [cache miss only]
Claude #1: NL → SQL (temperature=0, deterministic)
    ↓
SQL safety validation (regex for DML/DDL, table whitelist)
    ↓
Snowflake execution (with one auto-retry on SQL error)
    ↓
Result cached for future identical queries
    ↓
Claude #2: results → streamed NL answer (temperature=0.3)
    ↓
Post-processing:
  - State name enrichment (FIPS → readable names)
  - US choropleth map (for 10+ state results with FIPS codes)
  - Interactive Plotly bar chart (horizontal, gradient, hover tooltips)
  - Key Insights panel (highest / lowest / average)
  - Raw data table (sortable, filterable st.dataframe)
  - CSV download button
    ↓
Follow-up question suggestions (claude-haiku, after main answer)
```

### Key Design Decisions

**Pre-compiled schema metadata (`schema_metadata.py`)**
The biggest risk was overwhelming Claude with 7,500 column names in every prompt. The solution was a hand-curated metadata dictionary mapping semantic concepts (keywords) to the 15-30 most useful columns per table. A fast keyword-scoring function selects the 1-2 most relevant tables per question with zero LLM calls, zero latency. This keeps schema context under ~800 tokens while covering ~95% of realistic questions.

**Two-phase Claude pipeline with streaming**
SQL generation runs synchronously (temperature=0, deterministic) — this must complete before executing the query. The answer synthesis phase streams tokens via `client.messages.stream()`, rendered live in the UI with `st.write_stream()`. This gives near-instant visual feedback once the Snowflake query returns.

**Guardrails layering (4 layers)**
1. NSFW keyword blocklist — instant, before any API call
2. Topic keyword scoring — fast, covers ~99% of off-topic cases
3. SQL validation regex — prevents DML/DDL injection, enforces table whitelist
4. System prompt instructions to Claude — catches subtle off-topic phrasing

**City-level query handling**
The ACS data has no city column — only Census Block Groups. A pre-built `MAJOR_CITY_TO_COUNTY_FIPS` dict with 70+ major US cities maps to their primary county FIPS. When a city name is detected in the question, the SQL prompt gets a FIPS hint so Claude generates the correct `WHERE LEFT(census_block_group, 5) = 'XXXXX'` filter.

**Snowflake connection resilience**
`@st.cache_resource` caches the connection across Streamlit sessions. A liveness ping (`SELECT 1`) before every query detects stale connections; on failure the cache is cleared and a fresh connection is established automatically.

**Query result caching**
An in-process dict cache (`_QUERY_CACHE`) keyed on SHA-256 of the SQL string stores successful query results for 1 hour. Repeated identical questions (e.g. "what is total US population") return in milliseconds with zero Snowflake credits consumed. Only successful results are cached; errors always hit Snowflake fresh.

**State name enrichment**
Snowflake query results for state-level aggregations return 2-digit FIPS codes (e.g., "06"). A post-processing step maps these to readable state names ("California"), making bar charts and answers far more readable.

**Auto-visualization pipeline**
After each query, the result is examined for chartability in priority order:
1. Single-row numeric → metric cards
2. 10+ rows with state FIPS codes → US choropleth map (`px.choropleth`)
3. 2-50 rows with label+numeric → interactive horizontal bar chart (`px.bar`)
4. 3+ rows → Key Insights panel (highest / lowest / average)
5. 2+ rows → raw data table in expander

**Multi-session conversation history**
Sessions are stored in `st.session_state` as a list of dicts. Each session has its own message list, query count, and auto-generated name (taken from the first user question). The sidebar lists all sessions with a one-click switcher.

### Challenges Encountered

- **Table name quoting**: Snowflake table names starting with digits (e.g., `2019_CBG_B01`) must be double-quoted. Addressed with explicit examples in the system prompt.

- **Column name case sensitivity**: ACS column names like `B19013e1` have a lowercase `e`. Without double-quotes, Snowflake auto-uppercases to `B19013E1` which doesn't exist. Fixed by instructing Claude to always double-quote column names AND showing pre-quoted column names in the schema context.

- **`census_block_group` quoting exception**: Unlike ACS B-series columns, `census_block_group` is stored in uppercase and must NOT be double-quoted. This required an explicit exception in both the system prompt and schema metadata after hitting `invalid identifier '"census_block_group"'` errors.

- **Column name hallucination**: Early iterations had Claude inventing plausible column names (e.g., `B01001_total`). Fixed by restricting schema context to explicitly listed columns with the instruction "NEVER invent column names."

- **Result set size**: Without limits, a query like "show all CBGs" returns 220,000 rows. The `enforce_limit()` function auto-appends `LIMIT 100` to non-aggregation queries.

---

## Things I Would Improve With More Time

1. **Semantic schema selection with embeddings**
   Replace keyword scoring with embedding-based similarity so synonyms like "affluent" → income table, "schooling" → education table work naturally without maintaining keyword lists.

2. **Multi-year comparison**
   The dataset has ACS releases for 2016-2020. Add year selection to enable trend questions like "How did median income change from 2016 to 2019?" and render line charts showing change over time.

3. **More ACS tables**
   Currently covers 8 tables. The dataset has 50+ B-series tables covering commute patterns, language spoken at home, citizenship, disability, veteran status, and more.

4. **Expanded city coverage**
   Replace the 70-city county approximation with a full CBG-to-Census Place mapping using TIGER/Line shapefiles for accurate city boundaries (e.g., "San Jose" vs "Silicon Valley").

5. **Snowflake Cortex integration**
   Explore Snowflake Cortex's native LLM capabilities (`CORTEX.COMPLETE`) as an alternative to the external Claude API, keeping everything within the Snowflake platform for tighter data governance.

6. **Automated regression testing**
   A test suite of question/expected-SQL pairs to catch prompt regressions when iterating on the system prompt. Run on every commit via GitHub Actions.

7. **County-level choropleth maps**
   Extend the map visualization to county level (using FIPS-to-GeoJSON lookup) for finer-grained geographic insights beyond state-level aggregations.
