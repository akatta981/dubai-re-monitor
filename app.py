"""
app.py — Dubai RE Anomaly Monitor — Streamlit Dashboard.
Run: streamlit run app.py
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

import config
from db import AnomalyLog, DailyMetric, FetchLog, SessionLocal, Transaction, init_db
from market_intelligence import get_buy_recommendation, MACRO_SUMMARY, AREA_INTEL

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Page Config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dubai RE Monitor",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');

    /*
     * TEXT HIERARCHY (on #050505 background):
     *   Primary   #F0F0F0  — headings, key numbers         contrast ~18:1
     *   Secondary #BBBBBB  — body text, descriptions        contrast ~11:1
     *   Tertiary  #888888  — labels, captions               contrast  ~6:1
     *   Muted     #666666  — timestamps, footnotes only     contrast  ~4:1
     */

    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        color: #BBBBBB !important;
    }

    /* ── Global background ─────────────────────────────────────── */
    .stApp { background: #050505 !important; }
    section[data-testid="stSidebar"] {
        background: #0A0A0A !important;
        border-right: 1px solid #2A2A2A !important;
    }

    /* ── Scrollbar ─────────────────────────────────────────────── */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #0A0A0A; }
    ::-webkit-scrollbar-thumb { background: #FF5500; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #FF7733; }

    /* ── Selection ─────────────────────────────────────────────── */
    ::selection { background: #FF550033; color: #FF5500; }

    /* ── All Streamlit text ─────────────────────────────────────── */
    p, li, span, label, div { color: #BBBBBB; }

    /* Headings */
    h1, h2, h3, h4, [data-testid="stHeading"] h1,
    [data-testid="stHeading"] h2,
    [data-testid="stHeading"] h3 {
        color: #F0F0F0 !important;
        font-weight: 700 !important;
    }

    /* Captions — tertiary level */
    [data-testid="stCaptionText"],
    .stCaption p {
        color: #888 !important;
        font-size: 0.82rem !important;
    }

    /* Markdown body text */
    [data-testid="stMarkdown"] p { color: #BBBBBB !important; }
    [data-testid="stMarkdown"] li { color: #BBBBBB !important; }

    /* Sidebar widget labels */
    label[data-testid="stWidgetLabel"],
    .stSidebar label,
    [data-testid="stSidebar"] label { color: #CCC !important; font-weight: 500 !important; }

    /* Sidebar subheaders + expander headers */
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] .stSubheader { color: #EEE !important; }

    /* Expander label */
    [data-testid="stExpander"] summary p { color: #BBB !important; }

    /* Toggle labels */
    [data-testid="stToggle"] label p { color: #CCC !important; }

    /* Slider label */
    [data-testid="stSlider"] label { color: #CCC !important; }
    [data-testid="stSlider"] [data-testid="stTickBarMin"],
    [data-testid="stSlider"] [data-testid="stTickBarMax"] { color: #888 !important; }

    /* Multiselect label */
    [data-testid="stMultiSelect"] label { color: #CCC !important; }

    /* Selectbox label */
    [data-testid="stSelectbox"] label { color: #CCC !important; }

    /* Info / warning / success / error boxes — improve text */
    [data-testid="stAlert"] p { color: #EEE !important; font-size: 0.9rem !important; }

    /* ── Metric containers ─────────────────────────────────────── */
    div[data-testid="stMetricValue"] {
        font-size: 1.35rem;
        font-weight: 700;
        font-family: 'JetBrains Mono', monospace !important;
        color: #F0F0F0 !important;
    }
    div[data-testid="stMetricLabel"] {
        font-size: 0.82rem !important;
        color: #AAA !important;
        font-weight: 500 !important;
    }
    div[data-testid="stMetricDelta"] {
        font-size: 0.8rem !important;
    }
    div[data-testid="metric-container"] {
        background: #0D0D0D !important;
        border: 1px solid #2A2A2A !important;
        border-radius: 8px !important;
        padding: 14px 16px !important;
        transition: border-color 0.2s ease;
    }
    div[data-testid="metric-container"]:hover { border-color: #FF5500 !important; }

    /* ── Tabs ───────────────────────────────────────────────────── */
    button[data-baseweb="tab"] {
        background: transparent !important;
        color: #888 !important;
        border-bottom: 2px solid transparent !important;
        font-size: 0.88rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.3px !important;
        transition: color 0.2s ease !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        color: #FF5500 !important;
        border-bottom: 2px solid #FF5500 !important;
    }
    button[data-baseweb="tab"]:hover { color: #FF7733 !important; }

    /* ── Buttons ────────────────────────────────────────────────── */
    .stButton > button {
        background: #111 !important;
        border: 1px solid #333 !important;
        color: #CCC !important;
        border-radius: 6px !important;
        font-size: 0.85rem !important;
        font-weight: 600 !important;
        transition: all 0.2s ease !important;
    }
    .stButton > button:hover {
        border-color: #FF5500 !important;
        color: #FF5500 !important;
        background: #1A0800 !important;
    }

    /* ── Dividers ───────────────────────────────────────────────── */
    hr { border-color: #2A2A2A !important; }

    /* ── Footer text ────────────────────────────────────────────── */
    .last-refresh { color: #666; font-size: 0.78rem; font-family: 'JetBrains Mono', monospace; }

    /* ── Tables (dataframe / st.table) ──────────────────────────── */
    [data-testid="stDataFrame"] { border: 1px solid #2A2A2A !important; border-radius: 8px; }
    [data-testid="stDataFrame"] td, [data-testid="stDataFrame"] th {
        color: #CCC !important;
        font-size: 0.85rem !important;
    }

    /* ── Multiselect — dropdown menu ────────────────────────────── */
    [data-baseweb="menu"] {
        background: #111 !important;
        border: 1px solid #2A2A2A !important;
        border-radius: 8px !important;
    }
    [role="option"] {
        background: #111 !important;
        color: #DDD !important;
        font-size: 0.88rem !important;
        padding: 10px 14px !important;
    }
    [role="option"]:hover {
        background: #1A0800 !important;
        color: #FF7733 !important;
    }

    /* ── Multiselect — selected area chips ──────────────────────── */
    [data-baseweb="tag"] {
        background: #1A0800 !important;
        border: 1px solid #FF550066 !important;
        border-radius: 6px !important;
        margin: 2px !important;
    }
    [data-baseweb="tag"] span[role="presentation"] {
        color: #FFAA77 !important;
        font-size: 0.82rem !important;
        font-weight: 600 !important;
    }
    [data-baseweb="tag"] [role="button"] { color: #FF7733 !important; }

    /* ── Multiselect — input box ─────────────────────────────────── */
    [data-baseweb="select"] > div:first-child {
        background: #0D0D0D !important;
        border: 1px solid #2A2A2A !important;
        border-radius: 8px !important;
    }
    [data-baseweb="select"] input { color: #CCC !important; }
    [data-baseweb="select"] input::placeholder { color: #666 !important; }

    /* ── Legacy signal card classes ─────────────────────────────── */
    .signal-card { border-radius: 8px; padding: 18px 20px; margin: 8px 0; }
    .tag {
        display: inline-block;
        padding: 3px 11px;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: 600;
        margin-right: 6px;
        background: #1A1A1A;
        color: #AAA;
    }

    /* ── Sidebar area toggle buttons ────────────────────────────── */
    section[data-testid="stSidebar"] .stButton > button[kind="secondary"] {
        background: #0D0D0D !important;
        border: 1px solid #222 !important;
        color: #666 !important;
        text-align: left !important;
        justify-content: flex-start !important;
        font-size: 0.82rem !important;
        padding: 7px 12px !important;
        border-radius: 6px !important;
        letter-spacing: 0.2px !important;
        width: 100% !important;
        margin-bottom: 2px !important;
        font-weight: 500 !important;
    }
    section[data-testid="stSidebar"] .stButton > button[kind="secondary"]:hover {
        border-color: #FF5500 !important;
        color: #FF7733 !important;
        background: #0D0500 !important;
    }
    section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
        background: #1A0800 !important;
        border: 1px solid #FF5500 !important;
        color: #FF7733 !important;
        text-align: left !important;
        justify-content: flex-start !important;
        font-size: 0.82rem !important;
        padding: 7px 12px !important;
        border-radius: 6px !important;
        letter-spacing: 0.2px !important;
        width: 100% !important;
        margin-bottom: 2px !important;
        font-weight: 600 !important;
    }
    section[data-testid="stSidebar"] .stButton > button[kind="primary"]:hover {
        background: #240B00 !important;
        border-color: #FF7733 !important;
    }

    /* ── Status bar ─────────────────────────────────────────────── */
    .status-bar { padding: 4px 0 2px; }
    .status-row { display:flex; align-items:center; gap:8px; padding:5px 0; }
    .sdot { width:7px; height:7px; border-radius:50%; flex-shrink:0; display:inline-block; }
    .sdot-green { background:#22c55e; box-shadow:0 0 5px #22c55e88; }
    .sdot-amber { background:#f59e0b; box-shadow:0 0 5px #f59e0b88; }
    .slabel-green { font-size:0.72rem; font-weight:700; letter-spacing:0.9px;
                    text-transform:uppercase; color:#22c55e; }
    .slabel-amber { font-size:0.72rem; font-weight:700; letter-spacing:0.9px;
                    text-transform:uppercase; color:#f59e0b; }

    /* ── Section header label ────────────────────────────────────── */
    .sec-hdr {
        color: #555 !important;
        font-size: 0.7rem !important;
        font-weight: 700 !important;
        text-transform: uppercase !important;
        letter-spacing: 1.3px !important;
        padding: 0 0 6px 0 !important;
    }

    /* ── Simulation expander ────────────────────────────────────── */
    [data-testid="stExpander"] {
        background: #0A0A0A !important;
        border: 1px solid #1A1A1A !important;
        border-radius: 8px !important;
    }

    /* ── Snapshot area card (Select All / Clear buttons) ─────────── */
    .ctrl-btn > button {
        font-size: 0.78rem !important;
        padding: 5px 8px !important;
        color: #888 !important;
    }

    /* ── Pill navigation (st.radio used as tab nav) ────────────────── */
    div[data-testid="stRadio"] > div[role="radiogroup"] {
        display: flex !important;
        gap: 4px !important;
        justify-content: flex-end !important;
        flex-wrap: nowrap !important;
        background: transparent !important;
        padding: 4px 0 !important;
    }
    div[data-testid="stRadio"] label {
        background: #0D0D0D !important;
        border: 1px solid #222 !important;
        border-radius: 20px !important;
        padding: 7px 18px !important;
        color: #666 !important;
        font-size: 0.8rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.3px !important;
        cursor: pointer !important;
        transition: all 0.15s ease !important;
        white-space: nowrap !important;
        margin: 0 !important;
    }
    div[data-testid="stRadio"] label:hover {
        border-color: #FF5500 !important;
        color: #FF7733 !important;
    }
    div[data-testid="stRadio"] label:has(input:checked) {
        background: #FF5500 !important;
        border-color: #FF5500 !important;
        color: #000 !important;
        font-weight: 700 !important;
    }
    div[data-testid="stRadio"] input[type="radio"] {
        position: absolute !important;
        opacity: 0 !important;
        pointer-events: none !important;
        width: 0 !important;
        height: 0 !important;
    }
    div[data-testid="stRadio"] > label { display: none !important; }
    /* Hide the radio indicator dot (the 16×16 div before the text) */
    div[role="radiogroup"] label > div:first-child { display: none !important; }

    /* ══════════════════════════════════════════════════════════════════
     * MOBILE RESPONSIVE — @media ≤ 480px
     * Keeps desktop layout untouched; only overrides on small screens.
     * ══════════════════════════════════════════════════════════════════ */
    @media (max-width: 480px) {
        /* ── Global: prevent horizontal overflow ─────────────────── */
        .stApp, [data-testid="stMain"], [data-testid="stMainBlockContainer"],
        section.main .block-container {
            max-width: 100vw !important;
            overflow-x: hidden !important;
            padding-left: 0.6rem !important;
            padding-right: 0.6rem !important;
        }

        /* ── Force Streamlit columns to stack vertically ─────────── */
        [data-testid="stHorizontalBlock"] {
            flex-direction: column !important;
            gap: 8px !important;
        }
        [data-testid="stHorizontalBlock"] > [data-testid="stColumn"] {
            width: 100% !important;
            flex: 1 1 100% !important;
            min-width: 0 !important;
        }

        /* ── Header title: smaller on mobile ─────────────────────── */
        h1 { font-size: 1.4rem !important; }
        h2 { font-size: 1.15rem !important; }
        h3, h4 { font-size: 0.95rem !important; }

        /* ── Nav pills: wrap + center ────────────────────────────── */
        div[data-testid="stRadio"] > div[role="radiogroup"] {
            flex-wrap: wrap !important;
            justify-content: center !important;
            gap: 6px !important;
        }
        div[data-testid="stRadio"] label {
            padding: 6px 14px !important;
            font-size: 0.75rem !important;
        }

        /* ── Metric cards: compact ───────────────────────────────── */
        div[data-testid="stMetricValue"] { font-size: 1.1rem !important; }
        div[data-testid="stMetricLabel"] { font-size: 0.75rem !important; }
        div[data-testid="metric-container"] { padding: 10px 12px !important; }

        /* ── DataFrames: horizontal scroll ───────────────────────── */
        [data-testid="stDataFrame"] {
            overflow-x: auto !important;
            -webkit-overflow-scrolling: touch;
        }

        /* ── Expander: full width ────────────────────────────────── */
        [data-testid="stExpander"] { margin-left: 0 !important; margin-right: 0 !important; }

        /* ── Footer ──────────────────────────────────────────────── */
        .last-refresh { font-size: 0.65rem !important; }
    }
</style>
""", unsafe_allow_html=True)

# ─── Signal display metadata ──────────────────────────────────────────────────
SIGNAL_LABELS = {
    "STRONG_BUY":   "🚨 Strong Buy",
    "VOLUME_DROP":  "📉 Volume Drop",
    "PRICE_DIP":    "💰 Price Dip",
    "SUPPLY_SURGE": "🏗️ Supply Surge",
}

SIGNAL_COLORS = {
    "STRONG_BUY":   "#FF5500",
    "VOLUME_DROP":  "#FF7733",
    "PRICE_DIP":    "#FF9955",
    "SUPPLY_SURGE": "#FFAA77",
}

SIGNAL_BG = {
    "STRONG_BUY":   "#1A0800",
    "VOLUME_DROP":  "#150A00",
    "PRICE_DIP":    "#120C00",
    "SUPPLY_SURGE": "#100E00",
}

SIGNAL_ICONS = {
    "STRONG_BUY":   "🚨",
    "VOLUME_DROP":  "📉",
    "PRICE_DIP":    "💰",
    "SUPPLY_SURGE": "🏗️",
}

SIGNAL_EXPLAIN = {
    "VOLUME_DROP": (
        "Far fewer property transactions than usual are happening in this area. "
        "When activity slows sharply, sellers may become more willing to negotiate — "
        "this often precedes a price softening and is typically a good buying window."
    ),
    "PRICE_DIP": (
        "The average price per m² has dropped below its 30-day trend. "
        "Properties in this area may be available at a relative discount compared to recent weeks."
    ),
    "SUPPLY_SURGE": (
        "More listings than usual are available on Bayut. "
        "Higher supply without matching buyer demand tends to soften prices — "
        "watch for motivated sellers willing to deal below asking."
    ),
    "STRONG_BUY": (
        "Multiple signals are aligned at once — volume is down AND prices are dipping. "
        "Historically this combination represents the strongest entry windows for buyers."
    ),
}


# ─── Data Loaders (cached) ────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_daily_metrics(days: int = 90) -> pd.DataFrame:
    """Load daily metrics from DB as DataFrame."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).date()
    session = SessionLocal()
    try:
        rows = (
            session.query(DailyMetric)
            .filter(DailyMetric.metric_date >= cutoff)
            .order_by(DailyMetric.area_canonical, DailyMetric.metric_date)
            .all()
        )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([{
            "date":           r.metric_date,
            "area":           r.area_canonical,
            "volume":         r.transaction_volume,
            "price_sqm":      r.avg_price_sqm,
            "total_worth":    r.total_worth,
            "median_price":   r.median_price,
            "volume_ma7":     r.volume_ma7,
            "price_sqm_ma30": r.price_sqm_ma30,
            "listings":       r.bayut_listing_count,
            "supply_ma7":     r.supply_ma7,
        } for r in rows])
    finally:
        session.close()


@st.cache_data(ttl=300)
def load_anomaly_log(limit: int = 100) -> pd.DataFrame:
    """Load recent anomaly signals."""
    session = SessionLocal()
    try:
        rows = (
            session.query(AnomalyLog)
            .order_by(AnomalyLog.detected_at.desc())
            .limit(limit)
            .all()
        )
        if not rows:
            return pd.DataFrame(columns=[
                "detected_at", "area", "signal_type", "deviation_pct",
                "signal_value", "baseline_value", "alert_sent", "notes"
            ])
        return pd.DataFrame([{
            "detected_at":    r.detected_at,
            "area":           r.area_canonical,
            "signal_type":    r.signal_type,
            "deviation_pct":  r.deviation_pct,
            "signal_value":   r.signal_value,
            "baseline_value": r.baseline_value,
            "alert_sent":     r.alert_sent,
            "notes":          r.notes or "",
        } for r in rows])
    finally:
        session.close()


@st.cache_data(ttl=3600)
def load_2025_avg_price() -> dict[str, float]:
    """Load full-year 2025 average DLD sale price per m² per area (for YoY comparison)."""
    from datetime import date
    from sqlalchemy import func
    session = SessionLocal()
    try:
        rows = (
            session.query(
                Transaction.area_canonical,
                func.avg(Transaction.price_per_sqm).label("avg_price"),
            )
            .filter(
                Transaction.transaction_date >= date(2025, 1, 1),
                Transaction.transaction_date <= date(2025, 12, 31),
                Transaction.price_per_sqm.isnot(None),
            )
            .group_by(Transaction.area_canonical)
            .all()
        )
        return {r.area_canonical: float(r.avg_price) for r in rows}
    finally:
        session.close()


@st.cache_data(ttl=300)
def load_project_names(areas: list[str]) -> dict[str, list[str]]:
    """Return {area: [project_name, ...]} for the given areas (sorted, deduped)."""
    if not areas:
        return {}
    session = SessionLocal()
    try:
        rows = (
            session.query(Transaction.area_canonical, Transaction.project_name)
            .filter(
                Transaction.area_canonical.in_(areas),
                Transaction.project_name.isnot(None),
            )
            .distinct()
            .all()
        )
        result: dict[str, list[str]] = {a: [] for a in areas}
        for r in rows:
            if r.project_name and r.project_name not in result[r.area_canonical]:
                result[r.area_canonical].append(r.project_name)
        for a in result:
            result[a].sort()
        return result
    finally:
        session.close()


@st.cache_data(ttl=300)
def load_project_transactions(project: str, days: int = 90) -> pd.DataFrame:
    """Load raw transactions for a specific project_name from the DB."""
    from datetime import date as _date
    cutoff = (datetime.utcnow() - timedelta(days=days)).date()
    session = SessionLocal()
    try:
        rows = (
            session.query(Transaction)
            .filter(
                Transaction.project_name == project,
                Transaction.transaction_date >= cutoff,
            )
            .order_by(Transaction.transaction_date)
            .all()
        )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([{
            "date":          r.transaction_date,
            "price_sqm":     r.price_per_sqm,
            "actual_worth":  r.actual_worth,
            "area_sqm":      r.procedure_area,
            "prop_type":     r.prop_type,
            "building_name": r.building_name,
            "area":          r.area_canonical,
        } for r in rows])
    finally:
        session.close()


@st.cache_data(ttl=300)
def load_fetch_status() -> dict:
    """Get latest fetch status for each source."""
    session = SessionLocal()
    try:
        result = {}
        for source in ("dld", "bayut"):
            row = (
                session.query(FetchLog)
                .filter_by(source=source)
                .order_by(FetchLog.fetched_at.desc())
                .first()
            )
            result[source] = {
                "last_fetch": row.fetched_at.strftime("%d %b %Y, %H:%M") if row else "Never",
                "status":     row.status if row else "unknown",
                "rows":       row.rows_upserted if row else 0,
            } if row else {"last_fetch": "Never", "status": "unknown", "rows": 0}
        return result
    finally:
        session.close()


# ─── Helper: area signal status ───────────────────────────────────────────────
def area_signal_status(area: str, anomaly_df: pd.DataFrame) -> str:
    """Returns 'strong', 'signal', or 'ok'."""
    if anomaly_df.empty:
        return "ok"
    area_df = anomaly_df[anomaly_df["area"] == area]
    if area_df.empty:
        return "ok"
    if "STRONG_BUY" in area_df["signal_type"].values:
        return "strong"
    if area_df["signal_type"].isin(["VOLUME_DROP", "PRICE_DIP", "SUPPLY_SURGE"]).any():
        return "signal"
    return "ok"


# ─── Auto-seed (cloud / first run) ───────────────────────────────────────────
def _ensure_data() -> None:
    """
    On Streamlit Cloud the SQLite DB starts empty (data/ is gitignored).
    Seed mock data automatically so the dashboard is usable on first load.
    """
    from db import get_session
    with get_session() as session:
        count = session.query(Transaction).count()
    if count == 0:
        with st.spinner("⏳ First launch — seeding demo data (about 30 s)…"):
            from seed_data import seed_database
            seed_database()
        st.cache_data.clear()       # flush any caches built on empty DB
        st.rerun()


# ─── Main App ─────────────────────────────────────────────────────────────────
def main() -> None:
    init_db()
    # _ensure_data()  # Disabled to prevent mock data injection

    # ── Session state: selected areas ─────────────────────────────────────────
    if "selected_areas" not in st.session_state:
        st.session_state.selected_areas = []

    st.markdown(
        f'<meta http-equiv="refresh" content="{config.DASHBOARD_REFRESH_SECONDS}">',
        unsafe_allow_html=True,
    )

    # ── Pre-load anomaly log (needed by sidebar signal icons) ─────────────────
    _anomaly_full = load_anomaly_log()

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        # Branding
        st.markdown(
            '<div style="font-family:\'Inter\',sans-serif;font-size:1.55rem;font-weight:800;'
            'letter-spacing:-0.5px;padding:6px 0 2px;line-height:1.1;">'
            '<span style="color:#F0F0F0;">DUBAI</span>'
            '<span style="color:#FF5500;">RE</span>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.caption("Personal investment research tool")
        st.divider()

        # ── MONITORED AREAS ───────────────────────────────────────────────────
        st.markdown('<div class="sec-hdr">MONITORED AREAS</div>', unsafe_allow_html=True)

        _sa_cols = st.columns(2)
        with _sa_cols[0]:
            if st.button("Select All", key="sel_all", use_container_width=True):
                st.session_state.selected_areas = list(config.MONITORED_AREAS.keys())
                st.rerun()
        with _sa_cols[1]:
            if st.button("Clear", key="sel_clear", use_container_width=True):
                st.session_state.selected_areas = []
                st.rerun()

        st.markdown('<div style="height:6px;"></div>', unsafe_allow_html=True)

        for _area in config.MONITORED_AREAS:
            _is_active  = _area in st.session_state.selected_areas
            _short      = _area.replace("Dubai ", "").replace("Arabian ", "")
            _sig_status = area_signal_status(_area, _anomaly_full)
            _dot = "🔴 " if _sig_status == "strong" else ("🟡 " if _sig_status == "signal" else "⚫ ")
            if st.button(
                f"{_dot}{_short}",
                key=f"area_{_area}",
                use_container_width=True,
                type="primary" if _is_active else "secondary",
            ):
                if _is_active:
                    st.session_state.selected_areas.remove(_area)
                else:
                    st.session_state.selected_areas.append(_area)
                st.rerun()

        st.divider()

        # ── TIME HORIZON ──────────────────────────────────────────────────────
        st.markdown('<div class="sec-hdr">TIME HORIZON</div>', unsafe_allow_html=True)
        lookback = st.slider("Days", 14, 90, 60, label_visibility="collapsed")

        # ── PROJECT FOCUS ──────────────────────────────────────────────────────
        selected_project: str | None = None
        _sel_areas = st.session_state.selected_areas
        if _sel_areas:
            _proj_map  = load_project_names(tuple(_sel_areas))  # type: ignore[arg-type]
            _all_projs = sorted({p for ps in _proj_map.values() for p in ps})
            if _all_projs:
                st.markdown('<div class="sec-hdr" style="margin-top:8px;">PROJECT FOCUS</div>', unsafe_allow_html=True)
                _proj_opts  = ["All projects"] + _all_projs
                _proj_choice = st.selectbox(
                    "Project", options=_proj_opts, index=0, label_visibility="collapsed",
                    help="Drill into a specific development.",
                )
                selected_project = None if _proj_choice == "All projects" else _proj_choice

        st.divider()

        # ── OFFICIAL DATA SYNC ───────────────────────────────────────────────
        with st.expander("🛠️ ADMIN: OFFICIAL DATA SYNC"):
            st.warning("This will PURGE all mock/local records and fetch the last 90 days of official DLD data.")
            if st.button("🔥 PURGE & SYNC OFFICIAL DATA", use_container_width=True):
                log_placeholder = st.empty()
                logs = []
                
                with st.spinner("Purging tables..."):
                    from db import purge_all_data
                    purge_all_data()
                    logs.append("✅ Database purged.")
                    log_placeholder.code("\n".join(logs))
                
                with st.spinner("Fetching official DLD data..."):
                    from data_fetcher import fetch_dld_transactions
                    # Fetch in smaller chunks or areas to show progress
                    n_new, err = fetch_dld_transactions(lookback_days=90)
                    if err:
                        logs.append(f"❌ DLD API Blocked: {err}")
                    else:
                        logs.append(f"✅ DLD sync complete: {n_new} official records.")
                    log_placeholder.code("\n".join(logs))
                
                with st.spinner("Recalculating signals..."):
                    from anomaly_detector import run_detection_pipeline
                    signals = run_detection_pipeline()
                    logs.append(f"✅ Anomaly engine ran: {len(signals)} signals.")
                    log_placeholder.code("\n".join(logs))
                
                st.cache_data.clear()
                st.success(f"Sync Complete!")
                st.rerun()

        # ── SIMULATION ────────────────────────────────────────────────────────
        with st.expander("⚡ RE-RUN PIPELINE"):
            st.caption("Scan the existing local dataset for new signals.")
            alert_email    = st.toggle("Email alerts",    value=True, key="sim_email")
            alert_whatsapp = st.toggle("WhatsApp alerts", value=True, key="sim_wa")
            if st.button("▶ Run detection", use_container_width=True):
                with st.spinner("Running…"):
                    from anomaly_detector import run_detection_pipeline
                    from alerts import send_alerts
                    signals = run_detection_pipeline()
                    if signals:
                        channels = (["email"] if alert_email else []) + (["whatsapp"] if alert_whatsapp else [])
                        send_alerts(signals, channels=channels)
                    st.cache_data.clear()
                st.success(f"Analysis complete · {len(signals)} signals")
            if st.button("📧 Test alert", use_container_width=True, key="sim_test"):
                from alerts import test_alerts
                with st.spinner("Sending…"):
                    test_alerts()
                st.success("Sent — check logs")

        st.divider()

        # ── DATA SOURCES STATUS BAR ───────────────────────────────────────────
        _fetch = load_fetch_status()
        _dld   = _fetch.get("dld",   {"status": "unknown", "last_fetch": "Never", "rows": 0})
        _bayt  = _fetch.get("bayut", {"status": "unknown", "last_fetch": "Never", "rows": 0})
        _dld_green  = _dld["status"]  == "success"
        _bayt_green = _bayt["status"] == "success"
        st.markdown(
            f'<div class="status-bar">'
            f'<div class="status-row" title="Last fetch: {_dld["last_fetch"]} ({_dld["rows"]} rows)">'
            f'  <span class="sdot {"sdot-green" if _dld_green else "sdot-amber"}"></span>'
            f'  <span class="{"slabel-green" if _dld_green else "slabel-amber"}">{"DLD CONNECTED" if _dld_green else "DLD DEMO MODE"} ({_dld["rows"]} rows)</span>'
            f'  <span style="color:#666;font-size:0.65rem;margin-left:4px;">{_dld["last_fetch"]}</span>'
            f'</div>'
            f'<div class="status-row" title="Last fetch: {_bayt["last_fetch"]} ({_bayt["rows"]} rows)">'
            f'  <span class="sdot {"sdot-green" if _bayt_green else "sdot-amber"}"></span>'
            f'  <span class="{"slabel-green" if _bayt_green else "slabel-amber"}">{"SCRAPER ACTIVE" if _bayt_green else "SCRAPER PENDING"}</span>'
            f'  <span style="color:#666;font-size:0.65rem;margin-left:4px;">{_bayt["last_fetch"]}</span>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        st.divider()
        st.caption("Personal research only. Not financial advice.")

    # ── Load data ─────────────────────────────────────────────────────────────
    selected_areas = st.session_state.selected_areas
    df             = load_daily_metrics(days=lookback)
    anomaly_df     = _anomaly_full

    if df.empty:
        st.warning("⚠️ No transactions recorded for the selected areas.")
        st.info("If this is a fresh install, please use the **ADMIN: OFFICIAL DATA SYNC** tool in the sidebar to fetch real-world data from the Dubai Land Department.")
        st.stop()


    if not selected_areas:
        st.markdown("## 👈 Select areas to get started")
        st.markdown(
            "Click an area in the sidebar list to activate it. "
            "Use **Select All** to see all areas at once."
        )
        col1, col2, col3 = st.columns(3)
        col1.info("📉 Buy Signals\nDetected when volume or price drops below trend")
        col2.info("🏗️ Supply Surge\nDetected when Bayut listings spike above normal")
        col3.info("🚨 Strong Buy\nVolume drop + price dip at the same time")
        st.stop()

    df["date"] = pd.to_datetime(df["date"])
    df         = df[df["area"].isin(selected_areas)]
    anomaly_df = anomaly_df[anomaly_df["area"].isin(selected_areas)]

    latest_by_area = df.sort_values("date").groupby("area").last().reset_index()
    latest_date    = df["date"].max().strftime("%d %B %Y") if not df.empty else "N/A"

    # ── Pre-compute area trends (used in Snapshot cards + Buy Signals tab) ────
    _p2025_data: dict[str, float] = load_2025_avg_price()
    area_trends: dict[str, dict] = {}
    for _area in df["area"].unique():
        _adf = df[df["area"] == _area].sort_values("date")
        _t: dict = {"price_7d_chg": None, "listings_7d_chg": None, "yoy_pct": None}
        if len(_adf) >= 2:
            _latest = _adf.iloc[-1]
            _max_dt = _adf["date"].max()
            _old    = _adf[_adf["date"] <= _max_dt - pd.Timedelta(days=7)]
            if not _old.empty:
                _old_r = _old.iloc[-1]
                if pd.notna(_latest["price_sqm"]) and pd.notna(_old_r["price_sqm"]) and _old_r["price_sqm"] > 0:
                    _t["price_7d_chg"] = (_latest["price_sqm"] - _old_r["price_sqm"]) / _old_r["price_sqm"] * 100
                if pd.notna(_latest["listings"]) and pd.notna(_old_r["listings"]) and _old_r["listings"] > 0:
                    _t["listings_7d_chg"] = (_latest["listings"] - _old_r["listings"]) / _old_r["listings"] * 100
            _p2025 = _p2025_data.get(_area)
            _ma30  = _latest["price_sqm_ma30"]
            if pd.notna(_ma30) and _p2025 and _p2025 > 0:
                _t["yoy_pct"] = (_ma30 - _p2025) / _p2025 * 100
        area_trends[_area] = _t

    # ── Page header + pill nav ───────────────────────────────────────────────────────────────────────
    _hdr_l, _hdr_r = st.columns([5, 4])
    with _hdr_l:
        _zone_count = len(selected_areas)
        _proj_sub   = f"  ·  🏢 {selected_project}" if selected_project else ""
        st.markdown(
            '<h1 style="font-family:Inter,sans-serif;font-size:2.0rem;font-weight:800;'
            'letter-spacing:-0.9px;margin:0 0 4px;line-height:1.05;color:#F0F0F0;">'
            'Market Intelligence'
            '</h1>'
            f'<p style="color:#555;font-size:0.84rem;font-weight:400;margin:0 0 2px;">'
            f'Real-time analysis of {_zone_count} active zone{"s" if _zone_count != 1 else ""}'
            f'{_proj_sub}'
            f'</p>',
            unsafe_allow_html=True,
        )
    with _hdr_r:
        _nav = st.radio(
            "nav", ["Snapshot", "Trends", "Signals", "Deep-dive"],
            horizontal=True, label_visibility="collapsed", key="main_nav",
        )

    # ── Active signals banner ───────────────────────────────────────────────────────────────────────────
    if not anomaly_df.empty:
        strong_areas = anomaly_df[anomaly_df["signal_type"] == "STRONG_BUY"]["area"].unique().tolist()
        other_areas  = anomaly_df[
            anomaly_df["signal_type"].isin(["VOLUME_DROP", "PRICE_DIP", "SUPPLY_SURGE"])
        ]["area"].unique().tolist()

        if strong_areas:
            st.error(
                f"🚨 **Strong Buy signal active** in **{', '.join(strong_areas)}** — "
                "open the Signals tab for details."
            )
        elif other_areas:
            st.warning(
                f"📊 **Buy signals detected** in {len(other_areas)} area(s): "
                f"**{', '.join(other_areas)}** — see the Signals tab."
            )
    else:
        st.success("✅ No active signals — market looks normal across all monitored areas.")

    st.divider()

    # ── Nav routing ──────────────────────────────────────────────────────────────────────────────────
    if _nav == "Snapshot":
        # ── Area Snapshot cards (stacked metric rows, mockup design) ──────────────────────
        areas_to_show = [a for a in selected_areas if a in latest_by_area["area"].values]

        def _snapshot_card_html(area: str) -> str:
            """Build HTML for a single area snapshot card — stacked metrics with separator lines."""
            _row     = latest_by_area[latest_by_area["area"] == area].iloc[0]
            _status  = area_signal_status(area, anomaly_df)
            _price   = _row["price_sqm"]
            _vol     = int(_row["volume"]) if pd.notna(_row["volume"]) else 0
            _vol_ma  = _row["volume_ma7"]
            _lists   = int(_row["listings"]) if pd.notna(_row["listings"]) else 0
            _trends  = area_trends.get(area, {})

            # Accent colour by signal status
            if _status == "strong":
                _accent = "#FF5500"
            elif _status == "signal":
                _accent = "#FF9955"
            else:
                _accent = "#3B82F6"

            # Volume delta badge
            if pd.notna(_vol_ma) and _vol_ma > 0:
                _vpct = (_vol - _vol_ma) / _vol_ma * 100
                _vup  = _vpct >= 0
                _vbg  = "#001A0A" if _vup else "#1A0500"
                _vc   = "#4ade80" if _vup else "#FF5555"
                _varr = "↗" if _vup else "↘"
                _vbadge = (
                    f'<span class="snap-badge" style="background:{_vbg};border:1px solid {_vc}33;color:{_vc};'
                    f'padding:4px 12px;border-radius:6px;font-size:0.76rem;font-weight:700;">'
                    f'{_varr} {abs(_vpct):.1f}%</span>'
                )
            else:
                _vbadge = '<span style="color:#333;font-size:0.76rem;">—</span>'

            # Price delta badge
            _p7d = _trends.get("price_7d_chg")
            if _p7d is not None:
                _pup  = _p7d >= 0
                _pbg  = "#001A0A" if _pup else "#1A0500"
                _pc   = "#4ade80" if _pup else "#FF5555"
                _parr = "↗" if _pup else "↘"
                _pbadge = (
                    f'<span class="snap-badge" style="background:{_pbg};border:1px solid {_pc}33;color:{_pc};'
                    f'padding:4px 12px;border-radius:6px;font-size:0.76rem;font-weight:700;">'
                    f'{_parr} {abs(_p7d):.1f}%</span>'
                )
            else:
                _pbadge = '<span style="color:#333;font-size:0.76rem;">—</span>'

            _short     = area.replace("Dubai ", "").replace("Arabian ", "")
            _price_str = f"{_price:,.0f}" if pd.notna(_price) else "—"
            _border    = f"1px solid {_accent}44"

            # Pulse / heartbeat SVG icon
            _pulse_svg = (
                f'<svg width="26" height="15" viewBox="0 0 52 22" fill="none" '
                f'xmlns="http://www.w3.org/2000/svg">'
                f'<polyline points="0,11 8,11 13,2 17,20 22,6 26,16 30,11 52,11" '
                f'stroke="{_accent}" stroke-width="2.5" fill="none" '
                f'stroke-linejoin="round" stroke-linecap="round"/>'
                f'</svg>'
            )

            return (
                f'<html><head><meta name="viewport" content="width=device-width,initial-scale=1">'
                f'<style>@import url(\'https://fonts.googleapis.com/css2?family=Inter:wght@400;700;800'
                f'&family=JetBrains+Mono:wght@700&display=swap\');'
                f'@media(max-width:420px){{.snap-val{{font-size:1.2rem!important;}}'
                f'.snap-card{{padding:14px 14px!important;}}'
                f'.snap-badge{{padding:3px 8px!important;font-size:0.7rem!important;}}'
                f'}}</style>'
                f'</head><body style="margin:0;padding:4px 2px;background:#050505;'
                f'font-family:\'Inter\',-apple-system,sans-serif;">'
                f'<div style="background:#0D0D0D;border-radius:12px;border:{_border};'
                f'overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.7);">'
                f'<div style="height:2px;background:linear-gradient(90deg,{_accent},{_accent}11);"></div>'
                f'<div class="snap-card" style="padding:18px 20px 18px;">'
                f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:18px;">'
                f'<span style="font-size:0.95rem;font-weight:700;color:#F0F0F0;letter-spacing:-0.1px;">{_short}</span>'
                f'{_pulse_svg}'
                f'</div>'
                f'<div style="margin-bottom:14px;">'
                f'<div style="color:#444;font-size:0.62rem;text-transform:uppercase;'
                f'letter-spacing:1.2px;font-weight:700;margin-bottom:7px;">VOLUME (24H)</div>'
                f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                f'<span class="snap-val" style="font-size:1.65rem;font-weight:800;color:#F0F0F0;'
                f'font-family:\'JetBrains Mono\'  ,monospace;line-height:1;">{_vol}</span>'
                f'{_vbadge}'
                f'</div></div>'
                f'<div style="border-top:1px solid #1A1A1A;margin-bottom:14px;"></div>'
                f'<div style="margin-bottom:14px;">'
                f'<div style="color:#444;font-size:0.62rem;text-transform:uppercase;'
                f'letter-spacing:1.2px;font-weight:700;margin-bottom:7px;">PRICE / M²</div>'
                f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                f'<span class="snap-val" style="font-size:1.4rem;font-weight:800;color:#F0F0F0;'
                f'font-family:\'JetBrains Mono\'  ,monospace;line-height:1;">{_price_str}</span>'
                f'{_pbadge}'
                f'</div></div>'
                f'<div style="border-top:1px solid #1A1A1A;margin-bottom:14px;"></div>'
                f'<div>'
                f'<div style="color:#444;font-size:0.62rem;text-transform:uppercase;'
                f'letter-spacing:1.2px;font-weight:700;margin-bottom:7px;">ACTIVE LISTINGS</div>'
                f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                f'<span class="snap-val" style="font-size:1.4rem;font-weight:800;color:#F0F0F0;'
                f'font-family:\'JetBrains Mono\'  ,monospace;line-height:1;">{_lists}</span>'
                f'<span style="background:#1A0000;border:1px solid #FF333344;color:#FF4444;'
                f'padding:4px 10px;border-radius:6px;font-size:0.62rem;font-weight:700;'
                f'letter-spacing:0.9px;">⬤ LIVE</span>'
                f'</div></div>'
                f'</div></div>'
                f'</body></html>'
            )

        for _row_start in range(0, len(areas_to_show), 2):
            _pair = areas_to_show[_row_start:_row_start + 2]
            _snap_cols = st.columns(len(_pair))
            for _sc, _area in zip(_snap_cols, _pair):
                with _sc:
                    components.html(_snapshot_card_html(_area), height=272, scrolling=False)

    elif _nav == "Trends":
        # Stress-period shading (used on both charts)
        stress_start = pd.Timestamp("2026-02-15")
        stress_end   = pd.Timestamp("2026-02-25")
        show_stress  = df["date"].min() <= stress_end and df["date"].max() >= stress_start

        col_l, col_r = st.columns(2)

        with col_l:
            st.subheader("Transaction Volume by Area")
            st.caption("Dotted lines = 7-day rolling average. A sharp drop below average is a buy signal.")
            fig_vol = go.Figure()
            for area in df["area"].unique():
                adf = df[df["area"] == area].sort_values("date")
                fig_vol.add_trace(go.Scatter(
                    x=adf["date"], y=adf["volume"],
                    name=area, mode="lines",
                    line=dict(width=1.8), opacity=0.9,
                ))
                if adf["volume_ma7"].notna().any():
                    fig_vol.add_trace(go.Scatter(
                        x=adf["date"], y=adf["volume_ma7"],
                        name=f"{area} avg", mode="lines",
                        line=dict(width=1, dash="dot"),
                        opacity=0.3, showlegend=False,
                    ))
            if show_stress:
                fig_vol.add_vrect(
                    x0=stress_start, x1=stress_end,
                    fillcolor="#FF5500", opacity=0.07, layer="below", line_width=0,
                    annotation_text="Market stress", annotation_position="top left",
                    annotation_font=dict(size=10, color="#FF5500"),
                )
            fig_vol.update_layout(
                template=config.CHART_THEME, height=370,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, font_size=11, font_color="#888"),
                xaxis_title=None, yaxis_title="Transactions / day",
                paper_bgcolor="#050505", plot_bgcolor="#0A0A0A",
                font=dict(color="#AAA", family="Inter, sans-serif"),
                xaxis=dict(gridcolor="#1A1A1A", linecolor="#222", zerolinecolor="#222"),
                yaxis=dict(gridcolor="#1A1A1A", linecolor="#222", zerolinecolor="#222"),
            )
            st.plotly_chart(fig_vol, use_container_width=True)

        with col_r:
            st.subheader("Average Price per m² (AED)")
            st.caption("Monthly average DLD sale price per m². Each point = one calendar month.")

            # Aggregate daily prices → monthly average per area
            price_monthly = (
                df.groupby(["area", pd.Grouper(key="date", freq="MS")])["price_sqm"]
                .mean()
                .reset_index()
                .rename(columns={"price_sqm": "price_monthly"})
            )

            fig_price = go.Figure()
            for area in price_monthly["area"].unique():
                adf = price_monthly[price_monthly["area"] == area].sort_values("date")
                fig_price.add_trace(go.Scatter(
                    x=adf["date"], y=adf["price_monthly"],
                    name=area, mode="lines+markers",
                    line=dict(width=2),
                    marker=dict(size=5),
                ))
            if show_stress:
                fig_price.add_vrect(
                    x0=stress_start, x1=stress_end,
                    fillcolor="#FF5500", opacity=0.07, layer="below", line_width=0,
                )
            fig_price.update_layout(
                template=config.CHART_THEME, height=370,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, font_size=11, font_color="#888"),
                xaxis_title=None, yaxis_title="AED / m²",
                paper_bgcolor="#050505", plot_bgcolor="#0A0A0A",
                font=dict(color="#AAA", family="Inter, sans-serif"),
                xaxis=dict(gridcolor="#1A1A1A", linecolor="#222", zerolinecolor="#222"),
                yaxis=dict(gridcolor="#1A1A1A", linecolor="#222", zerolinecolor="#222"),
            )
            st.plotly_chart(fig_price, use_container_width=True)

        if df["listings"].notna().any():
            st.subheader("Active Listings on Bayut")
            st.caption("Rising supply without rising demand softens prices — watch for motivated sellers.")
            fig_supply = go.Figure()
            for area in df["area"].unique():
                adf = df[(df["area"] == area) & df["listings"].notna()].sort_values("date")
                if not adf.empty:
                    fig_supply.add_trace(go.Bar(
                        x=adf["date"], y=adf["listings"],
                        name=area, opacity=0.75,
                    ))
            fig_supply.update_layout(
                template=config.CHART_THEME, height=250, barmode="stack",
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title=None, yaxis_title="Active listings",
                paper_bgcolor="#050505", plot_bgcolor="#0A0A0A",
                font=dict(color="#AAA", family="Inter, sans-serif"),
                xaxis=dict(gridcolor="#1A1A1A", linecolor="#222"),
                yaxis=dict(gridcolor="#1A1A1A", linecolor="#222"),
            )
            st.plotly_chart(fig_supply, use_container_width=True)

    elif _nav == "Signals":
        if anomaly_df.empty:
            st.info("No signals recorded yet. They'll appear here when the market shows anomalies.")
            st.markdown("""
**Signals fire when:**
- 📉 Transaction volume drops >20% below its 7-day average
- 💰 Average price/m² falls below its 30-day trend
- 🏗️ Bayut listings spike >10% above normal
- 🚨 Volume drop AND price dip happen at the same time *(Strong Buy)*
            """)
        else:
            # Summary metrics
            n_total  = len(anomaly_df)
            n_areas  = anomaly_df["area"].nunique()
            n_strong = int((anomaly_df["signal_type"] == "STRONG_BUY").sum())

            m1, m2, m3 = st.columns(3)
            m1.metric("Total Signals", str(n_total), help="All signals logged in the selected period")
            m2.metric("Areas with Signals", str(n_areas))
            m3.metric("Strong Buy Signals", str(n_strong))

            st.divider()

            # Filter by type
            sig_filter = st.multiselect(
                "Filter by signal type",
                options=["STRONG_BUY", "VOLUME_DROP", "PRICE_DIP", "SUPPLY_SURGE"],
                default=["STRONG_BUY", "VOLUME_DROP", "PRICE_DIP", "SUPPLY_SURGE"],
                format_func=lambda x: SIGNAL_LABELS.get(x, x),
            )
            filtered = anomaly_df[anomaly_df["signal_type"].isin(sig_filter)]

            if filtered.empty:
                st.info("No signals match the selected filters.")
            else:
                st.markdown(f"**{len(filtered)} signal(s)** across **{filtered['area'].nunique()} area(s)** — most recent first")

                _all_signal_cards: list[tuple[str, int]] = []
                for _, row in filtered.iterrows():
                    sig   = row["signal_type"]
                    color = SIGNAL_COLORS.get(sig, "#4a5568")
                    label = SIGNAL_LABELS.get(sig, sig)

                    area  = row["area"]
                    dev   = row["deviation_pct"]
                    sv    = row["signal_value"]
                    bv    = row["baseline_value"]

                    detected     = row["detected_at"]
                    detected_str = detected.strftime("%d %b %Y") if hasattr(detected, "strftime") else str(detected)
                    dev_str      = (f"{abs(dev):.1f}% below normal" if pd.notna(dev) and dev < 0 else
                                    f"{abs(dev):.1f}% above normal" if pd.notna(dev) else "")

                    # -- Explain text
                    if pd.notna(sv) and pd.notna(bv):
                        if sig == "VOLUME_DROP":
                            wk_norm = round(bv * 7)
                            wk_now  = round(sv * 7)
                            explain = (
                                f"This area has been averaging {bv:.1f} transactions/day (~{wk_norm}/week). "
                                f"The most recent day recorded just {sv:.0f} transaction(s), putting this week on "
                                f"pace for only ~{wk_now}. Fewer buyers means less competition — sellers become more open to negotiation."
                            )
                        elif sig == "PRICE_DIP":
                            explain = (
                                f"The 30-day average has been AED {bv:,.0f}/m². Most recent transactions are coming "
                                f"in at AED {sv:,.0f}/m² — {abs(dev):.0f}% below that trend. "
                                f"Properties may be available at a relative discount compared to recent weeks."
                            )
                        elif sig == "SUPPLY_SURGE":
                            explain = (
                                f"Bayut typically shows ~{bv:.0f} active listings here. Right now there are {sv:.0f} — "
                                f"{abs(dev):.0f}% above normal. More supply without matching demand softens prices "
                                f"and gives you more leverage as a buyer."
                            )
                        elif sig == "STRONG_BUY":
                            explain = (
                                f"Volume drop and price dip are firing together — a rare and historically strong "
                                f"entry signal. Transaction activity is sharply down and prices are trending below "
                                f"their 30-day baseline."
                            )
                        else:
                            explain = SIGNAL_EXPLAIN.get(sig, "")
                    else:
                        explain = SIGNAL_EXPLAIN.get(sig, "")

                    # -- Stats grid cells
                    cells: list[tuple[str, str, str]] = []
                    if pd.notna(sv) and pd.notna(bv):
                        if sig == "VOLUME_DROP":
                            wk_norm = round(bv * 7)
                            wk_now  = round(sv * 7)
                            cells = [
                                ("7-Day Average", f"{bv:.1f}/day",          f"~{wk_norm} per week"),
                                ("Latest Day",    f"{sv:.0f} txns",          f"~{wk_now}/week pace"),
                                ("Volume Drop",   f"&#8722;{abs(dev):.0f}%", "vs 7-day trend"),
                            ]
                        elif sig == "PRICE_DIP":
                            cells = [
                                ("30-Day Avg",    f"AED {bv:,.0f}",          "per m²"),
                                ("Current Price", f"AED {sv:,.0f}",          "per m²"),
                                ("Price Dip",     f"&#8722;{abs(dev):.0f}%", "vs 30-day avg"),
                            ]
                        elif sig == "SUPPLY_SURGE":
                            cells = [
                                ("Typical Supply", f"{bv:.0f}",            "7-day avg listings"),
                                ("Now on Bayut",   f"{sv:.0f}",            "active listings"),
                                ("Supply Surge",   f"+{abs(dev):.0f}%",   "above normal"),
                            ]
                        else:
                            cells = [("Deviation", f"{abs(dev):.0f}%", "from baseline")]

                    n_cols = max(1, len(cells))
                    stats_cells_html = "".join(
                        f'<div style="background:#111;padding:14px 16px;border-radius:6px;border:1px solid #2A2A2A;">'
                        f'<div style="color:#888;font-size:0.72rem;text-transform:uppercase;letter-spacing:0.9px;margin-bottom:8px;font-weight:600;font-family:\'JetBrains Mono\',monospace;">{cl}</div>'
                        f'<div style="color:{color};font-size:1.15rem;font-weight:700;line-height:1;font-family:\'JetBrains Mono\',monospace;">{cv}</div>'
                        f'<div style="color:#999;font-size:0.78rem;margin-top:6px;">{cs}</div>'
                        f'</div>'
                        for cl, cv, cs in cells
                    )

                    # -- Broker tip
                    if sig == "VOLUME_DROP" and pd.notna(sv) and pd.notna(bv):
                        wk_norm = round(bv * 7)
                        wk_now  = round(sv * 7)
                        broker_tip = (
                            f"&ldquo;DLD data shows {area} normally registers ~{wk_norm} sales/week, but the latest "
                            f"day shows only {sv:.0f} transactions — pace of ~{wk_now}/week. That&rsquo;s a "
                            f"{abs(dev):.0f}% drop in buyer activity. With fewer buyers competing, I want to go in "
                            f"8&ndash;12% below asking and ask the seller to cover the agency fee or service charges.&rdquo;"
                        )
                    elif sig == "PRICE_DIP" and pd.notna(sv) and pd.notna(bv):
                        broker_tip = (
                            f"&ldquo;Prices in {area} have dipped to AED {sv:,.0f}/m² — {abs(dev):.0f}% below "
                            f"the 30-day trend of AED {bv:,.0f}/m². Pull DLD records from the last 2 weeks; the data "
                            f"supports a lower offer. I want to move now while prices are off their recent trend.&rdquo;"
                        )
                    elif sig == "SUPPLY_SURGE" and pd.notna(sv) and pd.notna(bv):
                        broker_tip = (
                            f"&ldquo;There are {sv:.0f} active listings in {area} vs the usual ~{bv:.0f}. More supply "
                            f"means I have leverage. Shortlist 3&ndash;4 options so sellers know I&rsquo;m comparing — "
                            f"that alone gives me the power to push the price down.&rdquo;"
                        )
                    elif sig == "STRONG_BUY":
                        broker_tip = (
                            f"&ldquo;Multiple signals are aligned in {area} — volume is down AND prices have dipped. "
                            f"This combination is rare. I want to move quickly. Push for 10&ndash;15% below asking, "
                            f"and if price is firm, ask for extras: parking, storage unit, or furnished handover.&rdquo;"
                        )
                    else:
                        broker_tip = (
                            f"Discuss this signal with your broker and ask them to pull DLD transaction data for {area}."
                        )

                    # -- Footer pills
                    _trends = area_trends.get(area, {})
                    _yoy    = _trends.get("yoy_pct")
                    _p7d    = _trends.get("price_7d_chg")
                    _l7d    = _trends.get("listings_7d_chg")

                    pills: list[str] = []
                    if _yoy is not None:
                        _ya = "▲" if _yoy >= 0 else "▼"
                        _yc = "#FF7733" if _yoy >= 0 else "#FFAA77"
                        pills.append(
                            f'<span style="background:#1A1A1A;border:1px solid #333;color:{_yc};'
                            f'padding:5px 13px;border-radius:20px;font-size:0.78rem;font-weight:600;white-space:nowrap;">'
                            f'📈 {_ya} {abs(_yoy):.1f}% vs 2025</span>'
                        )
                    if _p7d is not None and _p7d < 0 and _l7d is not None and _l7d > 0:
                        pills.append(
                            f'<span style="background:#1A0800;border:1px solid #FF550066;color:#FFBB88;'
                            f'padding:5px 13px;border-radius:20px;font-size:0.78rem;font-weight:600;white-space:nowrap;">'
                            f'⚡ Prices &#8722;{abs(_p7d):.1f}% &amp; Listings +{_l7d:.1f}% this week</span>'
                        )
                    _ac = "#FFAA77" if row["alert_sent"] else "#666"
                    _at = "✅ Alert sent" if row["alert_sent"] else "📭 No alert"
                    pills.append(f'<span style="color:{_ac};font-size:0.78rem;font-weight:500;">{_at} &nbsp;·&nbsp; {detected_str}</span>')
                    footer_html = " ".join(pills)

                    # -- Card height estimate (mobile wraps more → add buffer)
                    tip_rows = max(2, len(broker_tip) // 72)
                    card_h   = 330 + tip_rows * 21 + (38 if _yoy is not None else 0)

                    card_html = f"""<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  * {{ box-sizing: border-box; }}
  .stats-grid {{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:6px;padding:0 22px 16px;}}
  @media(max-width:420px){{
    .card-header{{flex-direction:column!important;}}
    .area-title{{font-size:1.05rem!important;}}
    .broker-box{{padding:11px 12px!important;margin:0 12px 14px!important;}}
    .stats-grid{{padding:0 12px 12px!important;grid-template-columns:1fr!important;gap:5px!important;}}
    .stats-grid > div{{padding:10px 12px!important;}}
    .card-header-wrap{{padding:14px 14px 10px!important;}}
    .explain-wrap{{padding:4px 14px 14px!important;}}
    .footer-wrap{{padding:0 14px 14px!important;}}
    .footer-wrap span{{font-size:0.7rem!important;}}
  }}
</style></head><body style="margin:0;padding:6px 2px;background:#050505;font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<div style="background:#0D0D0D;border-radius:10px;overflow:hidden;border:1px solid #2A2A2A;box-shadow:0 4px 32px rgba(0,0,0,0.8);">
  <div style="height:3px;background:linear-gradient(90deg,{color},{color}44);"></div>
  <div class="card-header card-header-wrap" style="padding:20px 24px 14px;display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;">
    <div>
      <div class="area-title" style="font-size:1.3rem;font-weight:800;color:#F0F0F0;letter-spacing:-0.4px;font-family:'Inter',sans-serif;">{area}</div>
      <div style="font-size:0.82rem;color:#888;margin-top:5px;font-weight:400;">{detected_str}{(' &nbsp;·&nbsp; ' + dev_str) if dev_str else ''}</div>
    </div>
    <div style="background:{color}18;border:1.5px solid {color}66;color:{color};padding:6px 16px;border-radius:20px;font-size:0.76rem;font-weight:700;letter-spacing:0.6px;white-space:nowrap;margin-top:2px;text-transform:uppercase;">
      {label}
    </div>
  </div>
  <div class="stats-grid">
    {stats_cells_html}
  </div>
  <div class="explain-wrap" style="padding:4px 24px 18px;">
    <div style="color:#BBB;font-size:0.88rem;line-height:1.85;">{explain}</div>
  </div>
  <div class="broker-box" style="margin:0 24px 18px;background:#110900;border-left:3px solid #FF5500;border-radius:0 8px 8px 0;padding:14px 18px;">
    <div style="color:#FF5500;font-size:0.72rem;font-weight:700;text-transform:uppercase;letter-spacing:1.3px;margin-bottom:10px;font-family:'Inter',sans-serif;">💬 YOUR BROKER SCRIPT</div>
    <div style="color:#FFBB88;font-size:0.88rem;line-height:1.85;font-style:italic;">{broker_tip}</div>
  </div>
  <div class="footer-wrap" style="padding:0 24px 20px;display:flex;flex-wrap:wrap;gap:8px;align-items:center;">
    {footer_html}
  </div>
</div></body></html>"""
                    _all_signal_cards.append((card_html, card_h))

                # Render cards in 2-column grid
                for _ci in range(0, len(_all_signal_cards), 2):
                    _pair = _all_signal_cards[_ci:_ci + 2]
                    _sig_cols = st.columns(len(_pair))
                    for _sc, (_chtml, _ch) in zip(_sig_cols, _pair):
                        with _sc:
                            components.html(_chtml, height=_ch, scrolling=False)

            st.divider()

            # -- Buying Window Calendar
            try:
                tdf = anomaly_df.copy()
                tdf["dt"] = pd.to_datetime(tdf["detected_at"])
                tdf["strength"] = tdf["signal_type"].map(
                    {"STRONG_BUY": 2, "VOLUME_DROP": 1, "PRICE_DIP": 1, "SUPPLY_SURGE": 1}
                ).fillna(1)

                end_dt      = pd.Timestamp(datetime.utcnow().date())
                start_dt    = end_dt - pd.Timedelta(days=lookback)
                weeks       = pd.period_range(start=start_dt, end=end_dt, freq="W")
                week_labels = [w.start_time.strftime("%d %b") for w in weeks]
                all_areas   = sorted(anomaly_df["area"].unique())

                matrix = pd.DataFrame(0, index=all_areas, columns=week_labels)
                hover  = pd.DataFrame("No signal", index=all_areas, columns=week_labels)

                for _, r in tdf.iterrows():
                    wk = r["dt"].to_period("W").start_time.strftime("%d %b")
                    ar = r["area"]
                    if ar in matrix.index and wk in matrix.columns:
                        if r["strength"] > matrix.loc[ar, wk]:
                            matrix.loc[ar, wk] = r["strength"]
                            hover.loc[ar, wk]  = SIGNAL_LABELS.get(r["signal_type"], r["signal_type"])

                st.subheader("📅 Signal History by Week")
                st.caption("Amber = signal active that week · Red = Strong Buy · Grey = no signal")

                fig_cal = go.Figure(data=go.Heatmap(
                    z=matrix.values,
                    x=matrix.columns.tolist(),
                    y=matrix.index.tolist(),
                    text=hover.values,
                    hovertemplate="<b>%{y}</b><br>Week of %{x}<br>%{text}<extra></extra>",
                    colorscale=[
                        [0.0,  "#1e293b"],
                        [0.01, "#1e293b"],
                        [0.5,  "#d97706"],
                        [1.0,  "#ef4444"],
                    ],
                    zmin=0, zmax=2,
                    showscale=False,
                    xgap=3, ygap=3,
                ))
                fig_cal.update_layout(
                    template=config.CHART_THEME,
                    height=max(160, len(all_areas) * 44 + 60),
                    margin=dict(l=0, r=0, t=10, b=0),
                    xaxis=dict(tickangle=-45, tickfont=dict(size=10), side="bottom"),
                    yaxis=dict(tickfont=dict(size=11), autorange="reversed"),
                )
                st.plotly_chart(fig_cal, use_container_width=True)

            except Exception as e:
                import traceback
                logger.warning("Timeline error: %s\n%s", e, traceback.format_exc())

            # Export
            export = anomaly_df.copy()
            export["signal_type"] = export["signal_type"].map(SIGNAL_LABELS).fillna(export["signal_type"])
            export.columns = [c.replace("_", " ").title() for c in export.columns]
            st.download_button(
                "📥 Export signals to CSV",
                data=export.to_csv(index=False).encode(),
                file_name=f"dubai_signals_{datetime.utcnow().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

    elif _nav == "Deep-dive":
        # ── Orange filled price-over-time area chart ──────────────────────────
        st.markdown(
            '<h4 style="font-size:0.88rem;font-weight:700;color:#888;text-transform:uppercase;'
            'letter-spacing:1px;margin:0 0 4px;">PRICE TREND — AED/M²</h4>',
            unsafe_allow_html=True,
        )
        # (r,g,b) tuples for palette — used for rgba fillcolor
        _AREA_PALETTE_RGB = [
            (255, 85,  0),  (255, 119, 51), (255, 153, 85), (255, 170, 119), (255, 187, 153),
            (204, 68,  0),  (255, 102, 34), (255, 136, 68), (255, 160, 102), (255, 181, 136),
        ]
        fig_trend = go.Figure()
        for _i, _area in enumerate(sorted(df["area"].unique())):
            _adf = df[df["area"] == _area].sort_values("date")
            _r, _g, _b = _AREA_PALETTE_RGB[_i % len(_AREA_PALETTE_RGB)]
            _clr  = f"rgb({_r},{_g},{_b})"
            _fill = f"rgba({_r},{_g},{_b},0.06)"
            fig_trend.add_trace(go.Scatter(
                x=_adf["date"], y=_adf["price_sqm"],
                name=_area.replace("Dubai ", "").replace("Arabian ", ""),
                mode="lines",
                line=dict(color=_clr, width=1.8),
                fill="tozeroy",
                fillcolor=_fill,
                hovertemplate=f"<b>{_area}</b><br>%{{x|%d %b %Y}}<br>AED %{{y:,.0f}}/m²<extra></extra>",
            ))
        fig_trend.update_layout(
            template=config.CHART_THEME,
            height=320,
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, font_size=10, font_color="#777"),
            xaxis_title=None, yaxis_title="AED / m²",
            paper_bgcolor="#050505", plot_bgcolor="#0A0A0A",
            font=dict(color="#AAA", family="Inter, sans-serif"),
            xaxis=dict(gridcolor="#1A1A1A", linecolor="#222", zerolinecolor="#222"),
            yaxis=dict(gridcolor="#1A1A1A", linecolor="#222", zerolinecolor="#222"),
        )
        st.plotly_chart(fig_trend, use_container_width=True)

        st.divider()
        st.subheader("Full Area Summary")
        st.caption(
            f"Snapshot as of **{latest_date}** · "
            "All prices = DLD-registered sale price per m² from completed transactions (not asking/listing price) · "
            "30d avg = rolling 30-day average from DLD · "
            "Bayut Listings = active **for-sale** supply on Bayut (not purchase count)"
        )
        price_2025 = load_2025_avg_price()
        src = latest_by_area[["area", "date", "volume", "price_sqm", "price_sqm_ma30", "listings"]].copy()
        # Compute raw 2025 and YoY values before any formatting
        src["p2025"]   = src["area"].map(price_2025)
        src["yoy_pct"] = (src["price_sqm_ma30"] - src["p2025"]) / src["p2025"] * 100

        vol_col = f"Sales Txns ({latest_date})"
        out = pd.DataFrame({
            "Area":                            src["area"],
            "Data Date":                       src["date"].apply(lambda x: x.strftime("%d %b %Y") if hasattr(x, "strftime") else str(x)),
            vol_col:                           src["volume"].apply(lambda x: int(x) if pd.notna(x) else 0),
            "Sale Price / m² (today, DLD)":    src["price_sqm"].apply(lambda x: f"AED {x:,.0f}" if pd.notna(x) else "—"),
            "Avg Sale Price / m² (30d, DLD)":  src["price_sqm_ma30"].apply(lambda x: f"AED {x:,.0f}" if pd.notna(x) else "—"),
            "Bayut For-Sale Listings":         src["listings"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "—"),
            "2025 Avg Sale Price / m² (DLD)":  src["p2025"].apply(lambda x: f"AED {x:,.0f}" if pd.notna(x) else "— (no data)"),
            "YoY vs 2025 (30d avg)":           src["yoy_pct"].apply(
                lambda x: f"{'▲' if x >= 0 else '▼'} {abs(x):.1f}%" if pd.notna(x) else "—"
            ),
        })
        st.dataframe(out, use_container_width=True, hide_index=True)

        # ── Project Deep-Dive ─────────────────────────────────────────────────
        st.divider()
        st.subheader("🏢 Project Deep-Dive")
        st.caption(
            "Select a specific development to see its individual DLD transaction history. "
            "Use the **Focus on a project** filter in the sidebar, or pick below."
        )

        proj_map_t3  = load_project_names(tuple(selected_areas))  # type: ignore[arg-type]
        all_projs_t3 = sorted({p for ps in proj_map_t3.values() for p in ps})

        if not all_projs_t3:
            st.info("No project data in the database yet. Re-seed or fetch live DLD data.")
        else:
            # Default to sidebar selection if one is active
            default_idx = 0
            if selected_project and selected_project in all_projs_t3:
                default_idx = all_projs_t3.index(selected_project)

            chosen_project = st.selectbox(
                "Select project",
                options=all_projs_t3,
                index=default_idx,
                key="t3_project_select",
            )

            proj_df = load_project_transactions(chosen_project, days=lookback)

            if proj_df.empty:
                st.info(f"No transactions found for **{chosen_project}** in the last {lookback} days.")
            else:
                proj_df["date"] = pd.to_datetime(proj_df["date"])
                n_tx   = len(proj_df)
                avg_px = proj_df["price_sqm"].mean()
                min_px = proj_df["price_sqm"].min()
                max_px = proj_df["price_sqm"].max()
                avg_sz = proj_df["area_sqm"].mean()

                # ── 2-col layout: left = chart/stats, right = buy target ─────
                _dd_left, _dd_right = st.columns([3, 2])

                with _dd_left:
                    pm1, pm2, pm3, pm4 = st.columns(4)
                    pm1.metric("Transactions", f"{n_tx}", help=f"Last {lookback} days")
                    pm2.metric("Avg Price / m²", f"AED {avg_px:,.0f}" if pd.notna(avg_px) else "—")
                    pm3.metric("Price Range / m²",
                               f"AED {min_px:,.0f} – {max_px:,.0f}" if pd.notna(min_px) else "—")
                    pm4.metric("Avg Unit Size", f"{avg_sz:,.0f} m²" if pd.notna(avg_sz) else "—")

                # ── Expert Buy Target Recommendation (multi-factor) ───────────
                # 30-day rolling avg from project's own DLD transactions
                _cutoff_30d = proj_df["date"].max() - pd.Timedelta(days=30)
                _recent_30d = proj_df[proj_df["date"] >= _cutoff_30d]["price_sqm"].dropna()
                _avg_30d    = _recent_30d.mean() if not _recent_30d.empty else avg_px

                # Project's area
                _proj_area = proj_df["area"].iloc[0]

                # Strongest active signal for this area
                _area_sigs  = anomaly_df[anomaly_df["area"] == _proj_area]["signal_type"].tolist()
                _active_sig = next(
                    (s for s in ["STRONG_BUY", "PRICE_DIP", "VOLUME_DROP", "SUPPLY_SURGE"]
                     if s in _area_sigs),
                    None,
                )

                # YoY% for this area (from area_trends computed in tab2, or recompute here)
                _p2025_map  = load_2025_avg_price()
                _p2025      = _p2025_map.get(_proj_area)
                _yoy        = ((_avg_30d - _p2025) / _p2025 * 100) if _p2025 and _p2025 > 0 else None

                # Supply/demand ratio: Bayut listings ÷ avg daily transaction volume
                _area_row   = latest_by_area[latest_by_area["area"] == _proj_area]
                _listings   = float(_area_row["listings"].iloc[0]) if not _area_row.empty and pd.notna(_area_row["listings"].iloc[0]) else None
                _daily_vol  = float(_area_row["volume_ma7"].iloc[0]) if not _area_row.empty and pd.notna(_area_row["volume_ma7"].iloc[0]) else None
                _sdr        = (_listings / _daily_vol) if (_listings and _daily_vol and _daily_vol > 0) else None

                # Call the intelligence engine
                _rec = get_buy_recommendation(
                    project               = chosen_project,
                    area                  = _proj_area,
                    avg_30d               = _avg_30d,
                    yoy_pct               = _yoy,
                    listings_per_daily_vol= _sdr,
                    active_signal         = _active_sig,
                    n_transactions        = len(_recent_30d),
                )
                _tgt     = _rec["target_price_sqm"]
                _tot_pct = _rec["total_discount_pct"]

                # Accent colour by signal
                _ACCENT_MAP = {
                    "STRONG_BUY":   "#FF5500",
                    "VOLUME_DROP":  "#FF7733",
                    "PRICE_DIP":    "#FF9955",
                    "SUPPLY_SURGE": "#FFAA77",
                    None:           "#FF5500",
                }
                _accent = _ACCENT_MAP.get(_active_sig, "#6366f1")

                # ── Factor bar chart rows (name + mini bar + % — no paragraph text) ──
                _max_factor = max((abs(v) for v, _ in _rec["breakdown"].values()), default=1.0) or 1.0
                _factor_bars_html = ""
                for _fname, (_fadj, _flabel) in _rec["breakdown"].items():
                    _bar_pct   = int(abs(_fadj) / _max_factor * 100)
                    _sign      = "+" if _fadj > 0 else ("−" if _fadj < 0 else "")
                    _val_color = "#4ade80" if _fadj > 0 else ("#f59e0b" if _fadj < 0 else "#555")
                    _bar_color = "rgba(74,222,128,0.5)" if _fadj > 0 else "rgba(245,158,11,0.6)"
                    _factor_bars_html += (
                        f'<div style="margin-bottom:12px;">'
                        f'  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:5px;">'
                        f'    <span style="color:#888;font-size:0.78rem;font-weight:500;">{_fname}</span>'
                        f'    <span style="color:{_val_color};font-size:0.84rem;font-weight:700;font-family:\'JetBrains Mono\',monospace;">{_sign}{abs(_fadj):.1f}%</span>'
                        f'  </div>'
                        f'  <div style="height:3px;background:#1A1A1A;border-radius:2px;overflow:hidden;">'
                        f'    <div style="height:100%;width:{_bar_pct}%;background:{_bar_color};border-radius:2px;"></div>'
                        f'  </div>'
                        f'</div>'
                    )

                # ── Budget grid ────────────────────────────────────────────────
                _unit_sizes = [("Studio", 42), ("1 BR", 65), ("2 BR", 95), ("3 BR", 130)]
                _size_rows  = "".join(
                    f'<div style="display:flex;justify-content:space-between;align-items:center;'
                    f'padding:8px 0;border-bottom:1px solid #1A1A1A;">'
                    f'<div>'
                    f'<span style="color:#888;font-size:0.8rem;font-weight:500;">{_ul}</span>'
                    f'<span style="color:#444;font-size:0.7rem;margin-left:5px;">~{_usqm}m²</span>'
                    f'</div>'
                    f'<span style="color:#F0F0F0;font-size:0.88rem;font-weight:700;'
                    f'font-family:\'JetBrains Mono\',monospace;">AED {_tgt * _usqm:,.0f}</span>'
                    f'</div>'
                    for _ul, _usqm in _unit_sizes
                )

                # ── Catalyst callout ───────────────────────────────────────────
                _yield_note = (
                    f'<div style="background:#0A1500;border-left:2px solid #4ade8088;'
                    f'border-radius:0 6px 6px 0;padding:10px 14px;margin-top:16px;">'
                    f'<div style="color:#4ade80;font-size:0.65rem;font-weight:700;text-transform:uppercase;'
                    f'letter-spacing:1.1px;margin-bottom:5px;">📈 AREA CATALYST</div>'
                    f'<div style="color:#88BB88;font-size:0.78rem;line-height:1.6;">{_rec["catalyst_note"]}</div>'
                    f'</div>'
                    if _rec["catalyst_note"] else ""
                )

                # ── Outlook note ───────────────────────────────────────────────
                _outlook_note = (
                    f'<div style="color:#555;font-size:0.74rem;line-height:1.6;margin-top:12px;padding-top:12px;border-top:1px solid #1A1A1A;">'
                    f'<span style="color:#444;font-weight:700;text-transform:uppercase;letter-spacing:0.6px;font-size:0.66rem;">5-YR: </span>{_rec["five_yr_outlook"]}</div>'
                    if _rec["five_yr_outlook"] else ""
                )

                # ── Badge chips ────────────────────────────────────────────────
                _rental_line = (
                    f'<span style="background:#0A1500;border:1px solid #4ade8044;color:#4ade80;'
                    f'padding:3px 10px;border-radius:20px;font-size:0.72rem;font-weight:600;">'
                    f'~{_rec["rental_yield_pct"]:.1f}% yield</span>'
                ) if _rec["rental_yield_pct"] else ""

                _conf_line = (
                    f'<span style="background:#111;border:1px solid #2A2A2A;color:{_rec["conf_color"]};'
                    f'padding:3px 10px;border-radius:20px;font-size:0.72rem;font-weight:600;">'
                    f'{_rec["confidence"]} · {_rec["n_transactions"]} txns</span>'
                )

                # ── Full card ──────────────────────────────────────────────────
                _target_html = f"""<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600;700&display=swap');
  * {{ box-sizing: border-box; }}
  @media(max-width:420px){{
    .tgt-card{{padding:14px 14px 14px!important;}}
    .tgt-hero{{font-size:1.8rem!important;}}
  }}
</style></head>
<body style="margin:0;padding:4px 2px 8px;background:#050505;font-family:'Inter',-apple-system,sans-serif;">
<div style="background:#0D0D0D;border-radius:12px;overflow:hidden;border:1px solid #1E1E1E;box-shadow:0 8px 40px rgba(0,0,0,0.9);">
  <div style="height:2px;background:linear-gradient(90deg,{_accent} 0%,{_accent}66 60%,transparent 100%);"></div>
  <div class="tgt-card" style="padding:20px 20px 18px;">

    <!-- Section tag -->
    <div style="color:#444;font-size:0.63rem;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:14px;">MARKET INTELLIGENCE</div>

    <!-- Project + Price hero -->
    <div style="color:#666;font-size:0.72rem;font-weight:500;letter-spacing:0.2px;margin-bottom:6px;">{chosen_project}</div>
    <div class="tgt-hero" style="color:{_accent};font-size:2.5rem;font-weight:800;letter-spacing:-1.5px;line-height:0.9;font-family:'JetBrains Mono',monospace;">AED {_tgt:,.0f}</div>
    <div style="color:#555;font-size:0.75rem;margin-top:6px;font-family:'JetBrains Mono',monospace;">/m²&nbsp;&nbsp;·&nbsp;&nbsp;&#8722;{_tot_pct:.1f}% vs 30d avg</div>

    <!-- Badges -->
    <div style="display:flex;flex-wrap:wrap;gap:6px;margin:14px 0 20px;">
      {_rental_line}
      {_conf_line}
    </div>

    <!-- Divider -->
    <div style="border-top:1px solid #1A1A1A;margin-bottom:16px;"></div>

    <!-- Factor breakdown header -->
    <div style="color:#444;font-size:0.63rem;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:14px;">PRICING FACTORS</div>

    <!-- Factor bar rows -->
    {_factor_bars_html}

    <!-- Total row -->
    <div style="display:flex;justify-content:space-between;align-items:center;border-top:1px solid #222;padding-top:12px;margin-top:4px;margin-bottom:20px;">
      <span style="color:#888;font-size:0.75rem;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;">Target discount</span>
      <span style="color:{_accent};font-size:1.2rem;font-weight:800;font-family:'JetBrains Mono',monospace;">&#8722;{_tot_pct:.1f}%</span>
    </div>

    <!-- Budget estimates -->
    <div style="color:#444;font-size:0.63rem;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;">BUDGET ESTIMATE</div>
    {_size_rows}

    <!-- Catalyst -->
    {_yield_note}

    <!-- Outlook -->
    {_outlook_note}

    <!-- Footnote -->
    <div style="color:#333;font-size:0.65rem;margin-top:14px;font-family:'JetBrains Mono',monospace;">
      {_rec["n_transactions"]} DLD txns · last 30d · ±5% for floor/view/fit-out
    </div>
  </div>
</div>
</body></html>"""

                with _dd_right:
                    components.html(_target_html, height=640, scrolling=True)

                with _dd_left:
                    # Price over time chart (orange filled)
                    daily_proj = (
                        proj_df.groupby("date")["price_sqm"].mean().reset_index()
                        .rename(columns={"price_sqm": "avg_price"})
                        .sort_values("date")
                    )
                    fig_proj = go.Figure()
                    fig_proj.add_trace(go.Scatter(
                        x=daily_proj["date"], y=daily_proj["avg_price"],
                        name="Avg Price/m²", mode="lines",
                        line=dict(color="rgb(255,85,0)", width=2),
                        fill="tozeroy",
                        fillcolor="rgba(255,85,0,0.08)",
                        hovertemplate="<b>%{x|%d %b %Y}</b><br>AED %{y:,.0f}/m²<extra></extra>",
                    ))
                    if len(daily_proj) >= 7:
                        daily_proj["ma7"] = daily_proj["avg_price"].rolling(7, min_periods=3).mean()
                        fig_proj.add_trace(go.Scatter(
                            x=daily_proj["date"], y=daily_proj["ma7"],
                            name="7d MA", mode="lines",
                            line=dict(color="#FF9955", width=1.5, dash="dot"),
                            opacity=0.8,
                        ))
                    fig_proj.update_layout(
                        template=config.CHART_THEME,
                        height=280,
                        title=dict(text=f"{chosen_project} — Price/m² (AED)", font=dict(color="#777", size=12)),
                        margin=dict(l=0, r=0, t=36, b=0),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, font_size=10, font_color="#777"),
                        xaxis_title=None,
                        yaxis_title="AED / m²",
                        paper_bgcolor="#050505", plot_bgcolor="#0A0A0A",
                        font=dict(color="#AAA", family="Inter, sans-serif"),
                        xaxis=dict(gridcolor="#1A1A1A", linecolor="#222", zerolinecolor="#222"),
                        yaxis=dict(gridcolor="#1A1A1A", linecolor="#222", zerolinecolor="#222"),
                    )
                    st.plotly_chart(fig_proj, use_container_width=True)

                    # Building breakdown
                    bldg_counts = proj_df["building_name"].value_counts().reset_index()
                    bldg_counts.columns = ["Building", "Transactions"]
                    if len(bldg_counts) > 1:
                        st.caption("Transactions by building:")
                        st.dataframe(bldg_counts, use_container_width=True, hide_index=True)

                    # Raw transactions
                    with st.expander(f"📋 Raw DLD transactions ({n_tx} rows)"):
                        display_df = proj_df[["date", "building_name", "prop_type", "area_sqm", "price_sqm", "actual_worth"]].copy()
                        display_df.columns = ["Date", "Building", "Type", "Size (m²)", "Price/m² (AED)", "Total (AED)"]
                        display_df["Date"]           = display_df["Date"].dt.strftime("%d %b %Y")
                        display_df["Price/m² (AED)"] = display_df["Price/m² (AED)"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
                        display_df["Total (AED)"]    = display_df["Total (AED)"].apply(lambda x: f"AED {x:,.0f}" if pd.notna(x) else "—")
                        display_df["Size (m²)"]      = display_df["Size (m²)"].apply(lambda x: f"{x:.0f}" if pd.notna(x) else "—")
                        st.dataframe(display_df, use_container_width=True, hide_index=True)

    # ── Footer ────────────────────────────────────────────────────────────────
    st.divider()
    st.markdown(
        f'<p class="last-refresh">'
        f'Last rendered: {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")} · '
        f'Auto-refreshes every {config.DASHBOARD_REFRESH_SECONDS // 60} min · '
        f'Personal research only — not financial advice'
        f'</p>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
