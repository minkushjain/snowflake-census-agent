"""
Pre-compiled schema metadata for the SafeGraph US Open Census Data.

This module maps semantic concepts to specific Snowflake tables and columns,
avoiding the need to dump 7,500+ ACS columns into every Claude prompt.
"""

# ---------------------------------------------------------------------------
# Table Metadata
# Each entry: description, key_concepts (for keyword scoring), core_columns
# ---------------------------------------------------------------------------
TABLES = {
    "2019_CBG_B01": {
        "description": "Sex by Age — total population counts broken down by age bracket and sex. Use for total population, population by age group, median age, male/female split.",
        "key_concepts": [
            "population", "total population", "people", "residents", "inhabitants",
            "age", "median age", "elderly", "senior", "children", "youth", "young",
            "adults", "male", "female", "sex", "gender", "men", "women", "boys", "girls",
            "demographic", "how many people", "how large", "size",
        ],
        "core_columns": {
            "B01001e1": "Total population",
            "B01001e2": "Total male population",
            "B01001e26": "Total female population",
            "B01001e3": "Male: Under 5 years",
            "B01001e4": "Male: 5 to 9 years",
            "B01001e5": "Male: 10 to 14 years",
            "B01001e6": "Male: 15 to 17 years",
            "B01001e7": "Male: 18 and 19 years",
            "B01001e8": "Male: 20 years",
            "B01001e9": "Male: 21 years",
            "B01001e10": "Male: 22 to 24 years",
            "B01001e11": "Male: 25 to 29 years",
            "B01001e12": "Male: 30 to 34 years",
            "B01001e13": "Male: 35 to 39 years",
            "B01001e14": "Male: 40 to 44 years",
            "B01001e15": "Male: 45 to 49 years",
            "B01001e16": "Male: 50 to 54 years",
            "B01001e17": "Male: 55 to 59 years",
            "B01001e18": "Male: 60 and 61 years",
            "B01001e19": "Male: 62 to 64 years",
            "B01001e20": "Male: 65 and 66 years",
            "B01001e21": "Male: 67 to 69 years",
            "B01001e22": "Male: 70 to 74 years",
            "B01001e23": "Male: 75 to 79 years",
            "B01001e24": "Male: 80 to 84 years",
            "B01001e25": "Male: 85 years and over",
            "B01001e27": "Female: Under 5 years",
            "B01001e28": "Female: 5 to 9 years",
            "B01001e29": "Female: 10 to 14 years",
            "B01001e30": "Female: 15 to 17 years",
            "B01001e31": "Female: 18 and 19 years",
            "B01001e49": "Female: 85 years and over",
            "B01002e1": "Median age (overall)",
            "B01002e2": "Median age (male)",
            "B01002e3": "Median age (female)",
        },
    },
    "2019_CBG_B02": {
        "description": "Race — population counts by racial category. Use for racial composition, percentage white/Black/Asian/etc., racial diversity.",
        "key_concepts": [
            "race", "racial", "white", "black", "african american", "asian",
            "native american", "american indian", "alaska native", "pacific islander",
            "native hawaiian", "multiracial", "two or more races", "other race",
            "diversity", "racial composition", "racial breakdown", "ethnicity",
        ],
        "core_columns": {
            "B02001e1": "Total population (race universe)",
            "B02001e2": "White alone",
            "B02001e3": "Black or African American alone",
            "B02001e4": "American Indian and Alaska Native alone",
            "B02001e5": "Asian alone",
            "B02001e6": "Native Hawaiian and Other Pacific Islander alone",
            "B02001e7": "Some other race alone",
            "B02001e8": "Two or more races",
        },
    },
    "2019_CBG_B03": {
        "description": "Hispanic or Latino Origin — population counts by Hispanic/Latino status and race. Use for Hispanic population, Latino demographics.",
        "key_concepts": [
            "hispanic", "latino", "latina", "latinx", "mexican", "puerto rican",
            "cuban", "central american", "south american", "spanish origin",
            "non-hispanic", "hispanic origin",
        ],
        "core_columns": {
            "B03002e1": "Total population (Hispanic origin universe)",
            "B03002e2": "Not Hispanic or Latino",
            "B03002e3": "Not Hispanic or Latino: White alone",
            "B03002e4": "Not Hispanic or Latino: Black or African American alone",
            "B03002e5": "Not Hispanic or Latino: American Indian and Alaska Native alone",
            "B03002e6": "Not Hispanic or Latino: Asian alone",
            "B03002e12": "Hispanic or Latino",
            "B03002e13": "Hispanic or Latino: White alone",
            "B03002e14": "Hispanic or Latino: Black or African American alone",
        },
    },
    "2019_CBG_B11": {
        "description": "Household Type — household counts by type (family, non-family, married couple, single-person, etc.).",
        "key_concepts": [
            "household", "family", "married", "couple", "single", "alone",
            "living alone", "roommate", "nonfamily", "non-family",
            "household type", "household size", "home", "dwelling",
        ],
        "core_columns": {
            "B11001e1": "Total households",
            "B11001e2": "Family households",
            "B11001e3": "Family households: Married-couple family",
            "B11001e4": "Family households: Other family",
            "B11001e5": "Family households: Other family: Male householder, no wife present",
            "B11001e6": "Family households: Other family: Female householder, no husband present",
            "B11001e7": "Nonfamily households",
            "B11001e8": "Nonfamily households: Householder living alone",
            "B11001e9": "Nonfamily households: Householder not living alone",
        },
    },
    "2019_CBG_B15": {
        "description": "Educational Attainment — population 25+ by highest education level completed.",
        "key_concepts": [
            "education", "educated", "college", "university", "degree", "bachelor",
            "graduate", "masters", "phd", "doctorate", "high school", "diploma",
            "ged", "dropout", "school", "academic", "literacy", "attainment",
            "less than high school", "some college", "associate degree",
        ],
        "core_columns": {
            "B15003e1": "Population 25 years and over",
            "B15003e2": "No schooling completed",
            "B15003e17": "Regular high school diploma",
            "B15003e18": "GED or alternative credential",
            "B15003e19": "Some college, less than 1 year",
            "B15003e20": "Some college, 1 or more years, no degree",
            "B15003e21": "Associate's degree",
            "B15003e22": "Bachelor's degree",
            "B15003e23": "Master's degree",
            "B15003e24": "Professional school degree",
            "B15003e25": "Doctorate degree",
        },
    },
    "2019_CBG_B17": {
        "description": "Poverty Status — population, family, and household counts above and below the federal poverty line. Use B17021 columns for individual poverty rate, B17010 for family poverty, B17017 for household poverty.",
        "key_concepts": [
            "poverty", "poor", "low income", "below poverty", "poverty line",
            "poverty rate", "poverty level", "impoverished", "food stamps",
            "welfare", "public assistance", "economic hardship",
        ],
        "core_columns": {
            "B17021e1": "Total population (poverty status determined)",
            "B17021e2": "Income below poverty level — individuals",
            "B17021e3": "Below poverty: In family households",
            "B17021e10": "Income at or above poverty level — individuals",
            "B17010e1": "Total families",
            "B17010e2": "Families below poverty level",
            "B17010e22": "Families at or above poverty level",
            "B17017e1": "Total households",
            "B17017e2": "Households below poverty level",
        },
    },
    "2019_CBG_B19": {
        "description": "Income — median household income, per capita income, aggregate income. Use for wealth, earnings, wages, economic status.",
        "key_concepts": [
            "income", "earnings", "wages", "salary", "pay", "wealth",
            "median income", "household income", "per capita income",
            "average income", "rich", "wealthy", "affluent", "economic",
            "money", "financial", "earnings", "compensation", "gdp",
            "aggregate income", "highest income", "lowest income",
        ],
        "core_columns": {
            "B19013e1": "Median household income (dollars)",
            "B19025e1": "Aggregate household income (dollars)",
            "B19301e1": "Per capita income (dollars)",
            "B19083e1": "Gini index of income inequality",
        },
    },
    "2019_CBG_B25": {
        "description": "Housing — housing units, occupancy, tenure (own vs rent), housing costs.",
        "key_concepts": [
            "housing", "house", "home", "apartment", "rent", "renter", "owner",
            "homeowner", "mortgage", "vacancy", "vacant", "occupied", "occupancy",
            "housing unit", "dwelling", "property", "real estate",
            "own", "ownership", "tenure", "rental",
        ],
        "core_columns": {
            "B25001e1": "Total housing units",
            "B25002e1": "Occupancy status: Total",
            "B25002e2": "Occupied housing units",
            "B25002e3": "Vacant housing units",
            "B25003e1": "Tenure: Total occupied housing units",
            "B25003e2": "Owner-occupied housing units",
            "B25003e3": "Renter-occupied housing units",
            "B25077e1": "Median value of owner-occupied housing units (dollars)",
            "B25064e1": "Median gross rent (dollars)",
            "B25071e1": "Median gross rent as a percentage of household income",
        },
    },
}

# ---------------------------------------------------------------------------
# Geographic Reference Data
# ---------------------------------------------------------------------------

GEOGRAPHIC_NOTES = """
GEOGRAPHIC DATA NOTES:
- The census_block_group column is a 12-digit FIPS string, e.g. '010010201001'
- State FIPS (2-digit) = LEFT(census_block_group, 2)
- County FIPS (5-digit) = LEFT(census_block_group, 5)
- To aggregate to STATE level: GROUP BY LEFT(census_block_group, 2)
- To aggregate to COUNTY level: GROUP BY LEFT(census_block_group, 5)
- All tables share census_block_group as the primary join key
- Column names ending in 'e' or 'E' (e.g. "B01001e1", "B17001E2") are ESTIMATES — always use these (double-quoted with EXACT case from schema!)
- Column names ending in 'm' or 'M' are MARGINS OF ERROR — do NOT use unless specifically asked
- IMPORTANT: All ACS estimate columns use lowercase 'e', e.g. "B17021e2", "B01001e1", "B19013e1". Always match the schema exactly — never invent column names.
- There is NO built-in city column; approximate city queries using county FIPS
- Data source: American Community Survey (ACS) 5-year estimates, 2019 vintage
- CRITICAL: Only double-quote ACS column names as shown in the schema. Do NOT double-quote census_block_group.
"""

STATE_FIPS = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut", "10": "Delaware",
    "11": "District of Columbia", "12": "Florida", "13": "Georgia", "15": "Hawaii",
    "16": "Idaho", "17": "Illinois", "18": "Indiana", "19": "Iowa",
    "20": "Kansas", "21": "Kentucky", "22": "Louisiana", "23": "Maine",
    "24": "Maryland", "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
    "28": "Mississippi", "29": "Missouri", "30": "Montana", "31": "Nebraska",
    "32": "Nevada", "33": "New Hampshire", "34": "New Jersey", "35": "New Mexico",
    "36": "New York", "37": "North Carolina", "38": "North Dakota", "39": "Ohio",
    "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania", "44": "Rhode Island",
    "45": "South Carolina", "46": "South Dakota", "47": "Tennessee", "48": "Texas",
    "49": "Utah", "50": "Vermont", "51": "Virginia", "53": "Washington",
    "54": "West Virginia", "55": "Wisconsin", "56": "Wyoming",
    "72": "Puerto Rico",
}

STATE_NAME_TO_FIPS = {v.lower(): k for k, v in STATE_FIPS.items()}
# Add common abbreviations
STATE_ABBR_TO_FIPS = {
    "al": "01", "ak": "02", "az": "04", "ar": "05", "ca": "06", "co": "08",
    "ct": "09", "de": "10", "dc": "11", "fl": "12", "ga": "13", "hi": "15",
    "id": "16", "il": "17", "in": "18", "ia": "19", "ks": "20", "ky": "21",
    "la": "22", "me": "23", "md": "24", "ma": "25", "mi": "26", "mn": "27",
    "ms": "28", "mo": "29", "mt": "30", "ne": "31", "nv": "32", "nh": "33",
    "nj": "34", "nm": "35", "ny": "36", "nc": "37", "nd": "38", "oh": "39",
    "ok": "40", "or": "41", "pa": "42", "ri": "44", "sc": "45", "sd": "46",
    "tn": "47", "tx": "48", "ut": "49", "vt": "50", "va": "51", "wa": "53",
    "wv": "54", "wi": "55", "wy": "56", "pr": "72",
}

# City → county FIPS mapping (approximate — cities may span multiple counties;
# this maps to the primary/most populous county for that city)
MAJOR_CITY_TO_COUNTY_FIPS = {
    "new york city": "36061",     # New York County (Manhattan)
    "new york": "36061",
    "nyc": "36061",
    "los angeles": "06037",       # Los Angeles County
    "la": "06037",
    "chicago": "17031",           # Cook County
    "houston": "48201",           # Harris County
    "phoenix": "04013",           # Maricopa County
    "philadelphia": "42101",      # Philadelphia County
    "philly": "42101",
    "san antonio": "48029",       # Bexar County
    "san diego": "06073",         # San Diego County
    "dallas": "48113",            # Dallas County
    "san jose": "06085",          # Santa Clara County
    "austin": "48453",            # Travis County
    "jacksonville": "12031",      # Duval County
    "fort worth": "48439",        # Tarrant County
    "columbus": "39049",          # Franklin County (Ohio)
    "charlotte": "37119",         # Mecklenburg County
    "san francisco": "06075",     # San Francisco County
    "sf": "06075",
    "indianapolis": "18097",      # Marion County
    "seattle": "53033",           # King County
    "denver": "08031",            # Denver County
    "washington dc": "11001",     # District of Columbia
    "washington": "11001",
    "dc": "11001",
    "nashville": "47037",         # Davidson County
    "oklahoma city": "40109",     # Oklahoma County
    "el paso": "48141",           # El Paso County
    "las vegas": "32003",         # Clark County
    "louisville": "21111",        # Jefferson County (Kentucky)
    "memphis": "47157",           # Shelby County
    "portland": "41051",          # Multnomah County
    "baltimore": "24510",         # Baltimore City
    "milwaukee": "55079",         # Milwaukee County
    "albuquerque": "35001",       # Bernalillo County
    "tucson": "04019",            # Pima County
    "fresno": "06019",            # Fresno County
    "sacramento": "06067",        # Sacramento County
    "mesa": "04013",              # Maricopa County
    "kansas city": "29095",       # Jackson County (Missouri)
    "atlanta": "13121",           # Fulton County
    "omaha": "31055",             # Douglas County
    "colorado springs": "08041",  # El Paso County (Colorado)
    "raleigh": "37183",           # Wake County
    "long beach": "06037",        # Los Angeles County
    "virginia beach": "51810",    # Virginia Beach City
    "minneapolis": "27053",       # Hennepin County
    "tampa": "12057",             # Hillsborough County
    "new orleans": "22071",       # Orleans Parish
    "honolulu": "15003",          # Honolulu County
    "anaheim": "06059",           # Orange County
    "aurora": "08005",            # Arapahoe County (Colorado)
    "santa ana": "06059",         # Orange County
    "corpus christi": "48355",    # Nueces County
    "riverside": "06065",         # Riverside County
    "lexington": "21067",         # Fayette County (Kentucky)
    "st. louis": "29189",         # St. Louis City
    "saint louis": "29189",
    "pittsburgh": "42003",        # Allegheny County
    "cleveland": "39035",         # Cuyahoga County
    "cincinnati": "39061",        # Hamilton County
    "miami": "12086",             # Miami-Dade County
    "detroit": "26163",           # Wayne County
    "boston": "25025",            # Suffolk County
    "brooklyn": "36047",          # Kings County
    "queens": "36081",            # Queens County
    "bronx": "36005",             # Bronx County
    "staten island": "36085",     # Richmond County
    "manhattan": "36061",         # New York County
}


# ---------------------------------------------------------------------------
# Schema Selection Function
# ---------------------------------------------------------------------------

def get_relevant_schema(question: str) -> str:
    """
    Score each table by how many of its key_concepts appear in the question.
    Return a formatted schema string for the top 1-2 most relevant tables.
    Falls back to all table descriptions if no match found.
    """
    q = question.lower()
    scores: dict[str, int] = {}

    for table_name, meta in TABLES.items():
        score = 0
        for concept in meta["key_concepts"]:
            if concept in q:
                score += 1
        scores[table_name] = score

    # Sort by score descending
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    # Select top tables with score > 0, capped at 2
    top_tables = [t for t, s in ranked if s > 0][:2]

    # If nothing matched, return all table-level descriptions for Claude to choose
    if not top_tables:
        lines = ["Available tables (pick the most relevant one for the question):"]
        for table_name, meta in TABLES.items():
            lines.append(f'\n- Table: "{table_name}"\n  Description: {meta["description"]}')
        return "\n".join(lines)

    # Build detailed schema for selected tables
    lines = []
    for table_name in top_tables:
        meta = TABLES[table_name]
        lines.append(f'Table: "US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET"."PUBLIC"."{table_name}"')
        lines.append(f'Description: {meta["description"]}')
        lines.append('Key columns — ALWAYS double-quote every column name in SQL (case-sensitive!):')
        lines.append('  census_block_group  VARCHAR  -- 12-digit FIPS identifier (always present, do NOT double-quote)')
        for col, desc in meta["core_columns"].items():
            lines.append(f'  "{col}"  NUMBER  -- {desc}')
        lines.append("")

    return "\n".join(lines)


def get_city_county_fips(question: str) -> str | None:
    """
    Check if the question mentions a known city; return its county FIPS if so.
    Returns None if no city match found.
    """
    q = question.lower()
    for city, fips in MAJOR_CITY_TO_COUNTY_FIPS.items():
        if city in q:
            return fips
    return None


def get_state_fips(question: str) -> str | None:
    """
    Check if the question mentions a US state by name or abbreviation.
    Returns the 2-digit FIPS code if found, else None.
    """
    q = question.lower()
    for state_name, fips in STATE_NAME_TO_FIPS.items():
        if state_name in q:
            return fips
    for abbr, fips in STATE_ABBR_TO_FIPS.items():
        # Only match abbreviations as whole words to avoid false positives
        import re
        if re.search(r'\b' + abbr + r'\b', q):
            return fips
    return None
