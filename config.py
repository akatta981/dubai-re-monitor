"""
config.py — Central configuration for Dubai RE Monitor.
All thresholds, area mappings, and settings live here.
Edit this file to tune buy signal sensitivity.
"""

from __future__ import annotations

# ─── Areas to Monitor ──────────────────────────────────────────────────────────
# Keys = display names; values = substrings to match in DLD 'AREA_EN' column
# DLD data uses varying spellings — we match on substrings (case-insensitive)
MONITORED_AREAS: dict[str, list[str]] = {
    "Downtown Dubai": ["downtown"],
    "Palm Jumeirah": ["palm jumeirah"],
    "Dubai Marina": ["dubai marina", "marina"],
    "JVC/JVT": ["jumeirah village circle", "jumeirah village triangle", "jvc", "jvt"],
    "Business Bay": ["business bay"],
    "Arabian Ranches": ["arabian ranches"],
    "Dubai Hills": ["dubai hills"],
    "Dubai Investment Park 1": ["dubai investment park 1", "dubai investment park first", "investment park first"],
    "Dubai Investment Park 2": ["dubai investment park 2", "dubai investment park second", "investment park second"],
    "Dubai Maritime City": ["dubai maritime city", "maritime city"],
}

# ─── Investment Filter ─────────────────────────────────────────────────────────
MAX_PRICE_AED: int = 3_000_000          # Only flag signals for properties <= this
MIN_PRICE_AED: int = 300_000            # Filter out obvious data errors
PROPERTY_TYPES: list[str] = ["apartment", "villa", "townhouse"]  # lowercase match

# ─── Anomaly Thresholds ────────────────────────────────────────────────────────
# Volume: alert if daily count < (VOLUME_DROP_THRESHOLD × 7-day rolling MA)
VOLUME_DROP_THRESHOLD: float = 0.80    # 20% drop triggers alert

# Price: alert if today's avg price/sqm < (PRICE_DIP_THRESHOLD × 30-day MA)
PRICE_DIP_THRESHOLD: float = 0.95      # 5% dip triggers alert

# Supply: alert if Bayut listing count increased by more than this % vs 7-day MA
SUPPLY_SURGE_THRESHOLD: float = 0.10   # 10% listing surge triggers alert

# Combined signal: flag as STRONG BUY if both volume drop AND price dip fire
STRONG_BUY_REQUIRES_BOTH: bool = True

# ─── Rolling Window Sizes ──────────────────────────────────────────────────────
VOLUME_MA_DAYS: int = 7
PRICE_MA_DAYS: int = 30
SUPPLY_MA_DAYS: int = 7
LOOKBACK_DAYS: int = 90               # How far back to pull DLD data

# ─── DLD Data Source ──────────────────────────────────────────────────────────
# Open data portal — check this URL if fetching breaks
DLD_BASE_URL: str = "https://www.dubailand.gov.ae/en/open-data/real-estate-data/"

# DLD CSV column names (update here if DLD changes their schema)
DLD_COLUMNS = {
    "date": "Transaction Date",         # format: DD/MM/YYYY
    "worth": "actual_worth",            # total transaction value AED
    "area_sqm": "procedure_area",       # area in sqm
    "trans_group": "trans_group_en",    # e.g. "Sales"
    "usage": "property_usage_en",       # e.g. "Residential"
    "location": "AREA_EN",              # area name
    "prop_type": "type_en",             # e.g. "Apartment", "Villa"
    "transaction_id": "trans_group_id", # for deduplication
    "building":       "building_name_en",   # individual tower / block name
    "project":        "project_name_en",    # sub-development / project name
    "master_project": "master_project_en",  # parent community / master plan
}

# ─── Scheduler ─────────────────────────────────────────────────────────────────
FETCH_INTERVAL_MINUTES: int = 15       # How often to refresh data
ALERT_COOLDOWN_HOURS: int = 6          # Don't re-alert same signal within this window

# ─── Bayut Scraper ─────────────────────────────────────────────────────────────
BAYUT_BASE_URL: str = "https://www.bayut.com/to-rent/property/dubai/"
BAYUT_REQUEST_DELAY_MIN: float = 2.0   # Min seconds between requests
BAYUT_REQUEST_DELAY_MAX: float = 5.0   # Max seconds between requests
BAYUT_MAX_RETRIES: int = 3
SCRAPER_USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ─── Dashboard ─────────────────────────────────────────────────────────────────
DASHBOARD_TITLE: str = "🏙️ Dubai RE Anomaly Monitor"
DASHBOARD_REFRESH_SECONDS: int = 900   # 15 min auto-refresh
CHART_THEME: str = "plotly_dark"
