# US Census Data Chat Agent

An interactive, chat-based AI agent that answers natural language questions about US population demographics using the SafeGraph US Open Census Data on Snowflake.

## Live Demo

**URL:** [https://census-agent.streamlit.app/](https://census-agent.streamlit.app/)

**Demo credentials:** None — the app is publicly accessible.

---

## Development Process & Future Improvements

See [DEVELOPMENT.md](DEVELOPMENT.md) for:
- Architecture decisions and design rationale
- Key challenges encountered and how they were solved
- A list of things I would improve with more time

---

## What It Does

- Ask plain-English questions about US demographics, income, housing, race, education, poverty, and more
- Get accurate answers backed by 2019 ACS 5-year Census data (220,000+ Census Block Groups)
- Supports national, state, county, and city-level queries
- Multi-turn conversation — ask follow-up questions and the agent preserves context
- Auto-generated follow-up question suggestions after each answer
- Multi-session conversation history — manage multiple independent chats from the sidebar
- **Interactive Plotly bar charts** with hover tooltips for ranked results
- **US Choropleth map** automatically rendered for state-level comparisons (10+ states)
- **Key Insights panel** — highlights highest, lowest, and average values at a glance
- **Raw data table** — interactive, sortable/filterable table in an expander below charts
- **Session export** — download the full conversation as a Markdown file
- Toggle to reveal the exact SQL query used for each answer
- Download any query result as CSV
- Dark mode toggle
- Built-in guardrails reject off-topic and inappropriate questions
- Query result caching — repeated questions return instantly without hitting Snowflake

---

## Tech Stack

| Component | Technology |
|---|---|
| AI Model | Claude Sonnet 4.6 (Anthropic API) |
| Data | Snowflake — SafeGraph US Open Census Data (Marketplace) |
| Frontend | Streamlit |
| Charts | Plotly Express |
| Deployment | Streamlit Community Cloud |

---

## Local Setup

### Prerequisites

- Python 3.11+
- A Snowflake account with the [US Open Census Data](https://app.snowflake.com/marketplace/listing/GZSNZ2UNN0) marketplace dataset activated
- An [Anthropic API key](https://console.anthropic.com/)

### Installation

```bash
git clone https://github.com/YOUR_USERNAME/census-agent.git
cd census-agent

python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Configure Secrets

Create `.streamlit/secrets.toml` with your credentials:

```toml
ANTHROPIC_API_KEY = "sk-ant-..."

[connections.snowflake]
account = "xy12345.us-east-1"   # Your Snowflake account identifier
user = "your_username"
password = "your_password"
warehouse = "COMPUTE_WH"
role = "SYSADMIN"
```

**Finding your Snowflake account identifier:**
Your Snowflake login URL looks like `https://xy12345.us-east-1.snowflakecomputing.com`.
The account identifier is everything before `.snowflakecomputing.com` → `xy12345.us-east-1`.

### Run Locally

```bash
streamlit run app.py
```

Open `http://localhost:8501` in your browser.

---

## Snowflake Setup

1. Log in to your Snowflake account
2. Go to **Data** → **Marketplace**
3. Search for "US Open Census Data Neighborhood Insights" (by SafeGraph)
4. Click **Get** to add it to your account (free)
5. The database `US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET` will appear in your data catalog
6. Ensure your user/role has `USAGE` on the database and `SELECT` on `PUBLIC.*`

---

## Deployment (Streamlit Community Cloud)

1. Push this repository to GitHub (**private repo is supported**)
2. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with GitHub
3. Click **New app**
4. Grant Streamlit access to your repo when prompted (one-time OAuth)
5. Select your repository, branch `main`, and main file `app.py`
6. Click **Advanced settings** → paste the contents of your `secrets.toml`
7. Click **Deploy**

The app will be live at `https://your-app-name.streamlit.app` within 2-5 minutes.

### Deployment Auth

`externalbrowser` auth requires a browser popup — it won't work on Streamlit Cloud servers. For deployment, create a dedicated Snowflake service account:

```sql
-- Run this in your Snowflake worksheet
CREATE USER CENSUS_APP_USER
  PASSWORD = 'choose-a-strong-password'
  DEFAULT_ROLE = ACCOUNTADMIN
  DEFAULT_WAREHOUSE = COMPUTE_WH;

GRANT ROLE ACCOUNTADMIN TO USER CENSUS_APP_USER;
```

Then in Streamlit Cloud's secrets editor, use:
```toml
[connections.snowflake]
account = "YOUR-ACCOUNT-IDENTIFIER"   # e.g. xy12345.us-east-1
user = "CENSUS_APP_USER"
password = "choose-a-strong-password"
warehouse = "COMPUTE_WH"
role = "ACCOUNTADMIN"
database = "US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET"
schema = "PUBLIC"
```

---

## Example Questions

**Try these to see all features in action:**

- "Which states have the highest median household income?" — renders a choropleth map + bar chart + Key Insights
- "What are the top 15 states by total population?" — interactive bar chart with hover
- "What is the total population of the United States?" — metric card display
- "What are the top 10 counties by poverty rate?" — bar chart + raw data table
- "What percentage of California's population is Hispanic?" — single-value answer
- "How does the racial composition differ between Texas and New York?" — comparison answer
- "What is the median age in Florida?" — quick fact
- "Which states have the highest percentage of college graduates?" — ranked bar chart
- "What percentage of housing in San Francisco is renter-occupied?" — city-level (county approx.)
- "Compare the per capita income of Alabama vs Massachusetts." — side-by-side metric

---

## Data Coverage

| Category | Tables Used |
|---|---|
| Population & Age | `2019_CBG_B01` |
| Race | `2019_CBG_B02` |
| Hispanic/Latino Origin | `2019_CBG_B03` |
| Households | `2019_CBG_B11` |
| Education | `2019_CBG_B15` |
| Poverty | `2019_CBG_B17` |
| Income | `2019_CBG_B19` |
| Housing | `2019_CBG_B25` |

All data is from the **2019 American Community Survey 5-year estimates** at the **Census Block Group** level (~220,000 block groups nationwide).

---

## Project Structure

```
app.py              # Streamlit UI — chat interface, charts, session management
agent.py            # Core NL→SQL→NL orchestration pipeline
schema_metadata.py  # Table/column metadata and keyword-based schema selection
snowflake_client.py # Snowflake connection, query execution, result caching
prompts.py          # Claude prompt templates
guardrails.py       # Topic classifier, SQL validator, safety checks
utils.py            # Formatting helpers, FIPS lookups
requirements.txt    # Python dependencies
.streamlit/
  config.toml       # Streamlit theme configuration
  secrets.toml      # Local credentials (never committed)
```
