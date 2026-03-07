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
    div[data-testid="stMetricValue"] { font-size: 1.3rem; font-weight: 700; }
    div[data-testid="stMetricLabel"] { font-size: 0.8rem; }
    .last-refresh { color: #718096; font-size: 12px; }
    .signal-card {
        border-radius: 10px;
        padding: 18px 20px;
        margin: 8px 0;
    }
    .tag {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 0.78rem;
        font-weight: 600;
        margin-right: 6px;
        background: #2d3748;
        color: #cbd5e0;
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
    "STRONG_BUY":   "#fc8181",
    "VOLUME_DROP":  "#f6ad55",
    "PRICE_DIP":    "#68d391",
    "SUPPLY_SURGE": "#76e4f7",
}

SIGNAL_BG = {
    "STRONG_BUY":   "#2d1515",
    "VOLUME_DROP":  "#2d2310",
    "PRICE_DIP":    "#132d1e",
    "SUPPLY_SURGE": "#0d2535",
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
            return pd.DataFrame()
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


# ─── Main App ─────────────────────────────────────────────────────────────────
def main() -> None:
    init_db()

    st.markdown(
        f'<meta http-equiv="refresh" content="{config.DASHBOARD_REFRESH_SECONDS}">',
        unsafe_allow_html=True,
    )

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.title("🏙️ Dubai RE Monitor")
        st.caption("Personal investment research tool")
        st.divider()

        st.subheader("🔍 Filters")
        selected_areas = st.multiselect(
            "Areas to watch",
            options=list(config.MONITORED_AREAS.keys()),
            default=[],
            placeholder="Select one or more areas…",
        )

        # Project filter — populated once areas are chosen
        selected_project: str | None = None
        if selected_areas:
            proj_map   = load_project_names(tuple(selected_areas))  # type: ignore[arg-type]
            all_projs  = sorted({p for ps in proj_map.values() for p in ps})
            if all_projs:
                proj_options = ["All projects"] + all_projs
                proj_choice  = st.selectbox(
                    "Focus on a project",
                    options=proj_options,
                    index=0,
                    help="Drill into a specific development. Choose 'All projects' for area-level view.",
                )
                selected_project = None if proj_choice == "All projects" else proj_choice

        lookback = st.slider("Show last N days", 14, 90, 60)

        st.divider()

        with st.expander("ℹ️ How it works"):
            st.markdown("""
**What this monitors:**
- **DLD** — Dubai Land Department publishes every property transaction. We watch for sudden drops in volume or price.
- **Bayut** — We count active listings as a supply proxy.

**Signal types:**
- 📉 **Volume Drop** — transactions fell >20% below their 7-day average
- 💰 **Price Dip** — price/m² dropped below its 30-day trend
- 🏗️ **Supply Surge** — listings spiked >10% above normal
- 🚨 **Strong Buy** — volume drop AND price dip at the same time

**Your target:** Under AED 2M · Residential · All 7 areas
            """)

        st.divider()
        st.subheader("📡 Data Sources")
        fetch_status = load_fetch_status()
        for source, info in fetch_status.items():
            icon  = "✅" if info["status"] == "success" else "⚠️"
            label = "DLD Transactions" if source == "dld" else "Bayut Listings"
            st.markdown(f"**{label}** {icon}")
            st.caption(f"Last updated: {info['last_fetch']}")

        st.divider()
        st.subheader("🔔 Alerts")
        alert_email    = st.toggle("Email alerts",    value=True)
        alert_whatsapp = st.toggle("WhatsApp alerts", value=True)

        col_b1, col_b2 = st.columns(2)
        with col_b1:
            if st.button("🔄 Refresh now", use_container_width=True):
                with st.spinner("Running pipeline…"):
                    from data_fetcher import fetch_dld_transactions
                    from anomaly_detector import run_detection_pipeline
                    from alerts import send_alerts
                    n       = fetch_dld_transactions()
                    signals = run_detection_pipeline()
                    if signals:
                        channels = (["email"] if alert_email else []) + (["whatsapp"] if alert_whatsapp else [])
                        send_alerts(signals, channels=channels)
                    st.cache_data.clear()
                st.success(f"{n} new rows · {len(signals)} signals")
        with col_b2:
            if st.button("📧 Test alert", use_container_width=True):
                from alerts import test_alerts
                with st.spinner("Sending…"):
                    test_alerts()
                st.success("Sent — check logs")

        st.divider()
        st.caption("⚠️ Personal research only. Not financial advice.")

    # ── Load data ─────────────────────────────────────────────────────────────
    df         = load_daily_metrics(days=lookback)
    anomaly_df = load_anomaly_log()

    if df.empty:
        st.warning("⚠️ No data in database yet.")
        st.info("Run `python seed_data.py` in your terminal to load mock data.")
        return

    if not selected_areas:
        st.markdown("## 👈 Select areas to get started")
        st.markdown(
            "Use the **Areas to watch** filter in the sidebar to choose one or more areas. "
            "Charts and signals will appear once you make a selection."
        )
        col1, col2, col3 = st.columns(3)
        col1.info("📉 Buy Signals\nDetected when volume or price drops below trend")
        col2.info("🏗️ Supply Surge\nDetected when Bayut listings spike above normal")
        col3.info("🚨 Strong Buy\nVolume drop + price dip at the same time")
        st.stop()

    df["date"] = pd.to_datetime(df["date"])
    if selected_areas:
        df         = df[df["area"].isin(selected_areas)]
        anomaly_df = anomaly_df[anomaly_df["area"].isin(selected_areas)]

    latest_by_area = df.sort_values("date").groupby("area").last().reset_index()
    latest_date    = df["date"].max().strftime("%d %B %Y")

    # ── Page header ───────────────────────────────────────────────────────────
    st.title("🏙️ Dubai Real Estate Monitor")
    _proj_label = f" · 🏢 Focused on **{selected_project}**" if selected_project else ""
    st.caption(f"Data through **{latest_date}** · {len(selected_areas)} areas monitored · Under AED 2M residential{_proj_label}")

    # ── Active signals banner ─────────────────────────────────────────────────
    if not anomaly_df.empty:
        strong_areas = anomaly_df[anomaly_df["signal_type"] == "STRONG_BUY"]["area"].unique().tolist()
        other_areas  = anomaly_df[
            anomaly_df["signal_type"].isin(["VOLUME_DROP", "PRICE_DIP", "SUPPLY_SURGE"])
        ]["area"].unique().tolist()

        if strong_areas:
            st.error(
                f"🚨 **Strong Buy signal active** in **{', '.join(strong_areas)}** — "
                "open the Buy Signals tab for details."
            )
        elif other_areas:
            st.warning(
                f"📊 **Buy signals detected** in {len(other_areas)} area(s): "
                f"**{', '.join(other_areas)}** — see the Buy Signals tab."
            )
    else:
        st.success("✅ No active signals — market looks normal across all monitored areas.")

    st.divider()

    # ── Area status cards ─────────────────────────────────────────────────────
    st.markdown("### Area Snapshot")
    st.caption("Each card shows the most recent day's data. Colour indicates signal status.")

    areas_to_show = [a for a in selected_areas if a in latest_by_area["area"].values]
    CARDS_PER_ROW = 4

    for row_start in range(0, len(areas_to_show), CARDS_PER_ROW):
        row_areas = areas_to_show[row_start:row_start + CARDS_PER_ROW]
        cols = st.columns(len(row_areas))

        for col, area in zip(cols, row_areas):
            row      = latest_by_area[latest_by_area["area"] == area].iloc[0]
            status   = area_signal_status(area, anomaly_df)
            price    = f"AED {row['price_sqm']:,.0f}/m²" if pd.notna(row["price_sqm"]) else "—"
            vol      = int(row["volume"]) if pd.notna(row["volume"]) else 0
            vol_ma   = row["volume_ma7"]

            if pd.notna(vol_ma) and vol_ma > 0:
                pct      = (vol - vol_ma) / vol_ma * 100
                delta    = f"{pct:+.0f}% vs 7d avg  ({vol} txns today)"
                d_color  = "normal"
            else:
                delta    = f"{vol} txns today"
                d_color  = "off"

            if status == "strong":
                icon, help_text = "🚨", "Strong Buy signal — see Buy Signals tab"
            elif status == "signal":
                icon, help_text = "📉", "Signal detected — see Buy Signals tab"
            else:
                icon, help_text = "✅", "No active signals"

            short = area.replace("Dubai ", "").replace("Arabian ", "")
            with col:
                st.metric(
                    label       = f"{icon} {short}",
                    value       = price,
                    delta       = delta,
                    delta_color = d_color,
                    help        = help_text,
                )

    st.divider()

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Market Trends",
        "🎯 Buy Signals",
        "🏘️ Areas Deep-Dive",
        "⚙️ Settings",
    ])

    # ── Tab 1: Market Trends ──────────────────────────────────────────────────
    with tab1:
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
                    fillcolor="#fc8181", opacity=0.07, layer="below", line_width=0,
                    annotation_text="Market stress", annotation_position="top left",
                    annotation_font=dict(size=10, color="#fc8181"),
                )
            fig_vol.update_layout(
                template=config.CHART_THEME, height=370,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, font_size=11),
                xaxis_title=None, yaxis_title="Transactions / day",
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
                    fillcolor="#fc8181", opacity=0.07, layer="below", line_width=0,
                )
            fig_price.update_layout(
                template=config.CHART_THEME, height=370,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, font_size=11),
                xaxis_title=None, yaxis_title="AED / m²",
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
            )
            st.plotly_chart(fig_supply, use_container_width=True)

    # ── Tab 2: Buy Signals ────────────────────────────────────────────────────
    with tab2:
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

            # ── Per-area 7-day trends + YoY (used in signal + summary cards) ──
            _p2025_data = load_2025_avg_price()
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

            if filtered.empty:
                st.info("No signals match the selected filters.")
            else:
                st.markdown(f"**{len(filtered)} signal(s)** across **{filtered['area'].nunique()} area(s)** — most recent first")

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
                        f'<div style="background:#1a2035;padding:13px 15px;border-radius:8px;">'
                        f'<div style="color:#64748b;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:7px;font-weight:600;">{cl}</div>'
                        f'<div style="color:{color};font-size:1.1rem;font-weight:700;line-height:1;">{cv}</div>'
                        f'<div style="color:#475569;font-size:0.73rem;margin-top:5px;">{cs}</div>'
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
                        _yc = "#f6ad55" if _yoy >= 0 else "#68d391"
                        pills.append(
                            f'<span style="background:#1e293b;border:1px solid #334155;color:{_yc};'
                            f'padding:4px 12px;border-radius:20px;font-size:0.73rem;font-weight:600;white-space:nowrap;">'
                            f'📈 {_ya} {abs(_yoy):.1f}% vs 2025</span>'
                        )
                    if _p7d is not None and _p7d < 0 and _l7d is not None and _l7d > 0:
                        pills.append(
                            f'<span style="background:#0f2318;border:1px solid #166534;color:#86efac;'
                            f'padding:4px 12px;border-radius:20px;font-size:0.73rem;font-weight:600;white-space:nowrap;">'
                            f'⚡ Prices &#8722;{abs(_p7d):.1f}% &amp; Listings +{_l7d:.1f}% this week</span>'
                        )
                    _ac = "#4ade80" if row["alert_sent"] else "#64748b"
                    _at = "✅ Alert sent" if row["alert_sent"] else "📭 No alert"
                    pills.append(f'<span style="color:{_ac};font-size:0.73rem;">{_at} &nbsp;·&nbsp; {detected_str}</span>')
                    footer_html = " ".join(pills)

                    # -- Card height estimate
                    tip_rows = max(2, len(broker_tip) // 88)
                    card_h   = 305 + tip_rows * 21 + (38 if _yoy is not None else 0)

                    card_html = f"""<html><body style="margin:0;padding:6px 2px;background:#0e1117;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<div style="background:#0f172a;border-radius:14px;overflow:hidden;border:1px solid #1e293b;box-shadow:0 4px 24px rgba(0,0,0,0.6);">
  <div style="height:4px;background:linear-gradient(90deg,{color},{color}66);"></div>
  <div style="padding:18px 22px 14px;display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;">
    <div>
      <div style="font-size:1.25rem;font-weight:800;color:#f1f5f9;letter-spacing:-0.4px;">{area}</div>
      <div style="font-size:0.77rem;color:#64748b;margin-top:4px;font-weight:500;">{detected_str}{(' &nbsp;·&nbsp; ' + dev_str) if dev_str else ''}</div>
    </div>
    <div style="background:{color}18;border:1.5px solid {color}44;color:{color};padding:5px 15px;border-radius:20px;font-size:0.76rem;font-weight:700;letter-spacing:0.3px;white-space:nowrap;margin-top:2px;">
      {label}
    </div>
  </div>
  <div style="display:grid;grid-template-columns:repeat({n_cols},1fr);gap:6px;padding:0 22px 16px;">
    {stats_cells_html}
  </div>
  <div style="padding:0 22px 16px;">
    <div style="color:#94a3b8;font-size:0.86rem;line-height:1.8;">{explain}</div>
  </div>
  <div style="margin:0 22px 16px;background:#0c1c33;border-left:3px solid #3b82f6;border-radius:0 10px 10px 0;padding:13px 16px;">
    <div style="color:#60a5fa;font-size:0.67rem;font-weight:700;text-transform:uppercase;letter-spacing:1.1px;margin-bottom:9px;">💬 Your Broker Script</div>
    <div style="color:#93c5fd;font-size:0.84rem;line-height:1.8;font-style:italic;">{broker_tip}</div>
  </div>
  <div style="padding:0 22px 18px;display:flex;flex-wrap:wrap;gap:8px;align-items:center;">
    {footer_html}
  </div>
</div></body></html>"""
                    components.html(card_html, height=card_h, scrolling=False)

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

    # ── Tab 3: Areas Deep-Dive ────────────────────────────────────────────────
    with tab3:
        col_a, col_b = st.columns(2)

        with col_a:
            fig_heat = px.bar(
                latest_by_area.sort_values("price_sqm", ascending=True),
                x="price_sqm", y="area", orientation="h",
                color="price_sqm", color_continuous_scale="Blues",
                labels={"price_sqm": "AED / m²", "area": ""},
                title="Price per m² — Most Recent Day",
                template=config.CHART_THEME,
            )
            fig_heat.update_layout(height=360, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_heat, use_container_width=True)

        with col_b:
            fig_vol_bar = px.bar(
                latest_by_area.sort_values("volume", ascending=True),
                x="volume", y="area", orientation="h",
                color="volume", color_continuous_scale="Greens",
                labels={"volume": "Transactions", "area": ""},
                title="Transaction Volume — Most Recent Day",
                template=config.CHART_THEME,
            )
            fig_vol_bar.update_layout(height=360, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_vol_bar, use_container_width=True)

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
                    "STRONG_BUY":   "#fc8181",
                    "VOLUME_DROP":  "#f6ad55",
                    "PRICE_DIP":    "#68d391",
                    "SUPPLY_SURGE": "#76e4f7",
                    None:           "#6366f1",
                }
                _accent = _ACCENT_MAP.get(_active_sig, "#6366f1")

                # Factor breakdown rows
                _factor_rows = ""
                for _fname, (_fadj, _flabel) in _rec["breakdown"].items():
                    _sign  = "+" if _fadj > 0 else ("−" if _fadj < 0 else "")
                    _fcolor = "#68d391" if _fadj > 0 else ("#f6ad55" if _fadj < 0 else "#64748b")
                    _factor_rows += (
                        f'<tr>'
                        f'<td style="color:#94a3b8;font-size:0.78rem;padding:5px 8px 5px 0;white-space:nowrap;">{_fname}</td>'
                        f'<td style="color:{_fcolor};font-size:0.78rem;font-weight:700;padding:5px 12px 5px 0;white-space:nowrap;">'
                        f'{_sign}{abs(_fadj):.1f}%</td>'
                        f'<td style="color:#64748b;font-size:0.74rem;line-height:1.4;">{_flabel}</td>'
                        f'</tr>'
                    )

                # Unit size estimates
                _unit_sizes = [("Studio", 42), ("1 BR", 65), ("2 BR", 95), ("3 BR", 130)]
                _size_rows  = "".join(
                    f'<div style="display:flex;justify-content:space-between;align-items:center;'
                    f'padding:7px 0;border-bottom:1px solid #1e293b;">'
                    f'<span style="color:#94a3b8;font-size:0.82rem;">{_ul} (~{_usqm} m²)</span>'
                    f'<span style="color:#f1f5f9;font-size:0.85rem;font-weight:700;">AED {_tgt * _usqm:,.0f}</span>'
                    f'</div>'
                    for _ul, _usqm in _unit_sizes
                )

                _yield_note = (
                    f'<div style="background:#0c1c33;border-left:3px solid #3b82f6;border-radius:0 8px 8px 0;'
                    f'padding:10px 14px;margin-top:12px;">'
                    f'<span style="color:#60a5fa;font-size:0.72rem;font-weight:700;text-transform:uppercase;'
                    f'letter-spacing:0.8px;">📈 Area catalyst</span>'
                    f'<div style="color:#93c5fd;font-size:0.80rem;line-height:1.7;margin-top:6px;">'
                    f'{_rec["catalyst_note"]}</div>'
                    f'</div>'
                    if _rec["catalyst_note"] else ""
                )

                _outlook_note = (
                    f'<div style="color:#475569;font-size:0.76rem;line-height:1.6;margin-top:10px;">'
                    f'<span style="color:#64748b;font-weight:600;">5-year outlook: </span>{_rec["five_yr_outlook"]}</div>'
                    if _rec["five_yr_outlook"] else ""
                )

                _rental_line = (
                    f'<span style="background:#1e293b;border:1px solid #334155;color:#a78bfa;'
                    f'padding:3px 10px;border-radius:12px;font-size:0.72rem;font-weight:600;margin-right:6px;">'
                    f'🏠 ~{_rec["rental_yield_pct"]:.1f}% gross yield</span>'
                ) if _rec["rental_yield_pct"] else ""

                _conf_line = (
                    f'<span style="background:#1e293b;border:1px solid #334155;color:{_rec["conf_color"]};'
                    f'padding:3px 10px;border-radius:12px;font-size:0.72rem;font-weight:600;">'
                    f'Data confidence: {_rec["confidence"]} ({_rec["n_transactions"]} txns)</span>'
                )

                _target_html = f"""<html><body style="margin:0;padding:6px 2px;background:#0e1117;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<div style="background:#0f172a;border-radius:14px;overflow:hidden;border:1px solid #1e293b;box-shadow:0 4px 24px rgba(0,0,0,0.5);">
  <div style="height:4px;background:linear-gradient(90deg,{_accent},{_accent}55);"></div>
  <div style="padding:18px 22px 14px;">

    <!-- Header row -->
    <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;margin-bottom:14px;">
      <div>
        <div style="color:#64748b;font-size:0.67rem;text-transform:uppercase;letter-spacing:1px;font-weight:600;margin-bottom:6px;">💡 Expert Buy Target — {chosen_project}</div>
        <div style="color:{_accent};font-size:2.1rem;font-weight:800;letter-spacing:-0.5px;line-height:1;">AED {_tgt:,.0f}<span style="font-size:0.9rem;font-weight:400;color:#64748b;"> /m²</span></div>
        <div style="color:#64748b;font-size:0.76rem;margin-top:5px;">&#8722;{_tot_pct:.1f}% vs 30-day DLD avg of AED {_avg_30d:,.0f}/m²</div>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:4px;">
        {_rental_line}
        {_conf_line}
      </div>
    </div>

    <!-- Factor breakdown table -->
    <div style="color:#475569;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.8px;font-weight:600;margin-bottom:6px;">How this target was calculated</div>
    <div style="background:#0a0f1e;border-radius:8px;padding:10px 14px;margin-bottom:14px;overflow-x:auto;">
      <table style="width:100%;border-collapse:collapse;">
        {_factor_rows}
        <tr style="border-top:1px solid #1e293b;">
          <td style="color:#f1f5f9;font-size:0.78rem;font-weight:700;padding:8px 8px 4px 0;">Total discount</td>
          <td style="color:{_accent};font-size:0.85rem;font-weight:800;padding:8px 12px 4px 0;">&#8722;{_tot_pct:.1f}%</td>
          <td style="color:#64748b;font-size:0.74rem;">Clamped to 2–20% range</td>
        </tr>
      </table>
    </div>

    <!-- Unit size estimates -->
    <div style="color:#475569;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.8px;font-weight:600;margin-bottom:6px;">Total budget estimate by unit type</div>
    <div style="margin-bottom:12px;">{_size_rows}</div>

    <!-- Catalyst note -->
    {_yield_note}

    <!-- 5-year outlook -->
    {_outlook_note}

    <div style="color:#334155;font-size:0.71rem;margin-top:10px;">
      Based on {_rec["n_transactions"]} DLD transactions in last 30 days.
      Adjust offer ±5% for floor, view, fit-out quality, and payment terms.
    </div>
  </div>
</div></body></html>"""
                components.html(_target_html, height=620, scrolling=False)

                # Price over time chart
                daily_proj = (
                    proj_df.groupby("date")["price_sqm"].mean().reset_index()
                    .rename(columns={"price_sqm": "avg_price"})
                    .sort_values("date")
                )
                fig_proj = go.Figure()
                fig_proj.add_trace(go.Scatter(
                    x=daily_proj["date"], y=daily_proj["avg_price"],
                    name="Avg Price/m²", mode="lines+markers",
                    line=dict(color="#60a5fa", width=2),
                    marker=dict(size=6),
                ))
                # Add 7-day MA
                if len(daily_proj) >= 7:
                    daily_proj["ma7"] = daily_proj["avg_price"].rolling(7, min_periods=3).mean()
                    fig_proj.add_trace(go.Scatter(
                        x=daily_proj["date"], y=daily_proj["ma7"],
                        name="7-day MA", mode="lines",
                        line=dict(color="#f6ad55", width=1.5, dash="dot"),
                        opacity=0.7,
                    ))
                fig_proj.update_layout(
                    template=config.CHART_THEME,
                    height=300,
                    title=f"{chosen_project} — Price per m² (AED)",
                    margin=dict(l=0, r=0, t=40, b=0),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, font_size=11),
                    xaxis_title=None,
                    yaxis_title="AED / m²",
                )
                st.plotly_chart(fig_proj, use_container_width=True)

                # Building breakdown (if multiple buildings in same project)
                bldg_counts = proj_df["building_name"].value_counts().reset_index()
                bldg_counts.columns = ["Building", "Transactions"]
                if len(bldg_counts) > 1:
                    st.caption("Transactions by building within this project:")
                    st.dataframe(bldg_counts, use_container_width=True, hide_index=True)

                # Raw transactions table
                with st.expander(f"📋 Raw DLD transactions ({n_tx} rows)"):
                    display_df = proj_df[["date", "building_name", "prop_type", "area_sqm", "price_sqm", "actual_worth"]].copy()
                    display_df.columns = ["Date", "Building", "Type", "Size (m²)", "Price/m² (AED)", "Total (AED)"]
                    display_df["Date"]          = display_df["Date"].dt.strftime("%d %b %Y")
                    display_df["Price/m² (AED)"] = display_df["Price/m² (AED)"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
                    display_df["Total (AED)"]   = display_df["Total (AED)"].apply(lambda x: f"AED {x:,.0f}" if pd.notna(x) else "—")
                    display_df["Size (m²)"]     = display_df["Size (m²)"].apply(lambda x: f"{x:.0f}" if pd.notna(x) else "—")
                    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # ── Tab 4: Settings ───────────────────────────────────────────────────────
    with tab4:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Signal Thresholds")
            st.caption("These are the conditions that trigger a buy signal. Edit `config.py` to change them.")
            st.markdown(f"""
| Signal | Triggers when… |
|--------|---------------|
| 📉 Volume Drop | Volume falls below **{config.VOLUME_DROP_THRESHOLD*100:.0f}%** of its 7-day average |
| 💰 Price Dip | Price/m² falls below **{config.PRICE_DIP_THRESHOLD*100:.0f}%** of its 30-day average |
| 🏗️ Supply Surge | Listings exceed **{(1 + config.SUPPLY_SURGE_THRESHOLD)*100:.0f}%** of their 7-day average |
| 🚨 Strong Buy | Volume Drop **and** Price Dip fire at the same time |
| Max property price | AED **{config.MAX_PRICE_AED:,}** |
| Data lookback | **{config.LOOKBACK_DAYS}** days |
| Alert cooldown | **{config.ALERT_COOLDOWN_HOURS}** hours between repeat alerts |
            """)

        with col2:
            st.subheader("Monitored Areas")
            for area in config.MONITORED_AREAS:
                st.markdown(f"• {area}")

            st.divider()
            st.subheader("Alert Credentials")
            st.caption("Set these in your `.env` file. Never commit `.env` to git.")
            env_vars = {
                "SMTP_USER":             "Gmail sender address",
                "SMTP_PASS":             "Gmail app password",
                "ALERT_EMAIL_TO":        "Your email address",
                "TWILIO_ACCOUNT_SID":    "Twilio account SID",
                "TWILIO_AUTH_TOKEN":     "Twilio auth token",
                "TWILIO_WHATSAPP_FROM":  "WhatsApp from number",
                "TWILIO_WHATSAPP_TO":    "Your WhatsApp number",
            }
            for var, desc in env_vars.items():
                status = "✅ Set" if os.getenv(var) else "❌ Not set"
                st.markdown(f"**{desc}** — {status}  \n`{var}`")

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
