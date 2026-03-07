"""
anomaly_detector.py — Computes rolling moving averages and detects buy signals.

Signal types:
  VOLUME_DROP   — daily transaction count < 80% of 7-day MA
  PRICE_DIP     — avg price/sqm < 95% of 30-day MA
  SUPPLY_SURGE  — Bayut listing count > 10% above 7-day MA
  STRONG_BUY    — VOLUME_DROP + PRICE_DIP both fire for same area+date

Run standalone: python anomaly_detector.py
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import text

import config
from db import AnomalyLog, DailyMetric, Transaction, get_session, init_db, upsert_daily_metric
from data_fetcher import fetch_bayut_supply

logger = logging.getLogger(__name__)


# ─── Step 1: Aggregate raw transactions → daily metrics ──────────────────────

def aggregate_daily_metrics() -> None:
    """
    Read raw transactions from DB, group by (date, area),
    compute volume + avg price/sqm, and upsert into daily_metrics.
    """
    cutoff = datetime.utcnow().date() - timedelta(days=config.LOOKBACK_DAYS)

    with get_session() as session:
        rows = (
            session.query(Transaction)
            .filter(Transaction.transaction_date >= cutoff)
            .filter(Transaction.area_canonical.isnot(None))
            .all()
        )

        if not rows:
            logger.warning("No transactions found in DB for aggregation")
            return

        # Build DataFrame from ORM objects while session is still open
        df = pd.DataFrame([{
            "date": r.transaction_date,
            "area": r.area_canonical,
            "worth": r.actual_worth,
            "area_sqm": r.procedure_area,
            "price_sqm": r.price_per_sqm,
            "prop_type": r.prop_type,
        } for r in rows])

    df["date"] = pd.to_datetime(df["date"])

    # Aggregate per area per day
    grouped = df.groupby(["date", "area"]).agg(
        transaction_volume=("worth", "count"),
        avg_price_sqm=("price_sqm", "mean"),
        total_worth=("worth", "sum"),
        median_price=("worth", "median"),
    ).reset_index()

    with get_session() as session:
        for _, row in grouped.iterrows():
            upsert_daily_metric(session, {
                "metric_date": row["date"].date(),
                "area_canonical": row["area"],
                "transaction_volume": int(row["transaction_volume"]),
                "avg_price_sqm": float(row["avg_price_sqm"]) if pd.notna(row["avg_price_sqm"]) else None,
                "total_worth": float(row["total_worth"]) if pd.notna(row["total_worth"]) else None,
                "median_price": float(row["median_price"]) if pd.notna(row["median_price"]) else None,
            })

    logger.info("Aggregated %d area-day metric rows", len(grouped))


# ─── Step 2: Compute rolling MAs and write back ───────────────────────────────

def compute_rolling_mas() -> None:
    """
    For each monitored area, compute rolling MAs over daily_metrics
    and write volume_ma7, price_sqm_ma30 back to DB.
    """
    cutoff = datetime.utcnow().date() - timedelta(days=config.LOOKBACK_DAYS)

    with get_session() as session:
        rows = (
            session.query(DailyMetric)
            .filter(DailyMetric.metric_date >= cutoff)
            .order_by(DailyMetric.area_canonical, DailyMetric.metric_date)
            .all()
        )

        if not rows:
            return

        df = pd.DataFrame([{
            "id": r.id,
            "date": r.metric_date,
            "area": r.area_canonical,
            "volume": r.transaction_volume,
            "price_sqm": r.avg_price_sqm,
            "listings": r.bayut_listing_count,
        } for r in rows])

    df["date"] = pd.to_datetime(df["date"])
    updates = []

    for area, grp in df.groupby("area"):
        grp = grp.sort_values("date").copy()
        grp["volume_ma7"] = grp["volume"].rolling(config.VOLUME_MA_DAYS, min_periods=1).mean()
        grp["price_sqm_ma30"] = grp["price_sqm"].rolling(config.PRICE_MA_DAYS, min_periods=1).mean()
        grp["supply_ma7"] = grp["listings"].rolling(config.SUPPLY_MA_DAYS, min_periods=1).mean()
        updates.append(grp)

    updates_df = pd.concat(updates)

    with get_session() as session:
        for _, row in updates_df.iterrows():
            obj = session.get(DailyMetric, int(row["id"]))
            if obj:
                obj.volume_ma7 = float(row["volume_ma7"]) if pd.notna(row["volume_ma7"]) else None
                obj.price_sqm_ma30 = float(row["price_sqm_ma30"]) if pd.notna(row["price_sqm_ma30"]) else None
                obj.supply_ma7 = float(row["supply_ma7"]) if pd.notna(row["supply_ma7"]) else None

    logger.info("Rolling MAs updated for %d area-day rows", len(updates_df))


# ─── Step 3: Refresh Bayut listing counts ────────────────────────────────────

def refresh_supply_data() -> None:
    """Fetch Bayut listing counts and write into today's daily_metrics rows."""
    supply = fetch_bayut_supply()
    today = datetime.utcnow().date()

    with get_session() as session:
        for area, count in supply.items():
            if count is None:
                continue
            existing = session.query(DailyMetric).filter_by(
                metric_date=today,
                area_canonical=area,
            ).first()
            if existing:
                existing.bayut_listing_count = count
            else:
                session.add(DailyMetric(
                    metric_date=today,
                    area_canonical=area,
                    bayut_listing_count=count,
                    transaction_volume=0,
                ))

    logger.info("Bayut supply counts updated for %d areas", sum(1 for v in supply.values() if v))


# ─── Step 4: Detect anomalies ─────────────────────────────────────────────────

def _already_alerted(area: str, signal_type: str, within_hours: int = config.ALERT_COOLDOWN_HOURS) -> bool:
    """Check if this signal already fired recently (dedup guard)."""
    cutoff = datetime.utcnow() - timedelta(hours=within_hours)
    with get_session() as session:
        count = (
            session.query(AnomalyLog)
            .filter(
                AnomalyLog.area_canonical == area,
                AnomalyLog.signal_type == signal_type,
                AnomalyLog.detected_at >= cutoff,
            )
            .count()
        )
    return count > 0


def detect_anomalies() -> list[dict]:
    """
    Scan the most recent daily_metrics for each area and detect buy signals.
    Logs new signals to anomaly_log table.
    Returns list of new signal dicts for immediate alerting.
    """
    today = datetime.utcnow().date()
    cutoff = today - timedelta(days=7)  # Need at least 7 days of data
    new_signals: list[dict] = []

    with get_session() as session:
        latest_rows = (
            session.query(DailyMetric)
            .filter(DailyMetric.metric_date >= cutoff)
            .order_by(DailyMetric.area_canonical, DailyMetric.metric_date.desc())
            .all()
        )

        if not latest_rows:
            logger.info("No recent metrics to analyse")
            return []

        df = pd.DataFrame([{
            "date": r.metric_date,
            "area": r.area_canonical,
            "volume": r.transaction_volume,
            "price_sqm": r.avg_price_sqm,
            "volume_ma7": r.volume_ma7,
            "price_sqm_ma30": r.price_sqm_ma30,
            "listings": r.bayut_listing_count,
            "supply_ma7": r.supply_ma7,
        } for r in latest_rows])

    # Analyse the latest data point per area
    for area in df["area"].unique():
        area_df = df[df["area"] == area].sort_values("date", ascending=False)
        if area_df.empty:
            continue
        latest = area_df.iloc[0]
        signal_date = latest["date"]
        volume_signals: list[str] = []
        price_signals: list[str] = []

        # ── Volume Drop Check ─────────────────────────────────────────────────
        vol = latest["volume"]
        vol_ma = latest["volume_ma7"]
        if (
            pd.notna(vol) and pd.notna(vol_ma) and vol_ma > 0
            and vol < config.VOLUME_DROP_THRESHOLD * vol_ma
        ):
            deviation = (vol - vol_ma) / vol_ma * 100
            signal = {
                "area": area,
                "signal_type": "VOLUME_DROP",
                "signal_date": signal_date,
                "signal_value": vol,
                "baseline_value": vol_ma,
                "deviation_pct": deviation,
                "notes": f"Volume {vol:.0f} is {abs(deviation):.1f}% below 7-day MA of {vol_ma:.1f}",
            }
            if not _already_alerted(area, "VOLUME_DROP"):
                volume_signals.append("VOLUME_DROP")
                new_signals.append(signal)
                _log_anomaly(signal)

        # ── Price Dip Check ───────────────────────────────────────────────────
        price = latest["price_sqm"]
        price_ma = latest["price_sqm_ma30"]
        if (
            pd.notna(price) and pd.notna(price_ma) and price_ma > 0
            and price < config.PRICE_DIP_THRESHOLD * price_ma
        ):
            deviation = (price - price_ma) / price_ma * 100
            signal = {
                "area": area,
                "signal_type": "PRICE_DIP",
                "signal_date": signal_date,
                "signal_value": price,
                "baseline_value": price_ma,
                "deviation_pct": deviation,
                "notes": f"Avg AED/sqm {price:.0f} is {abs(deviation):.1f}% below 30-day MA of {price_ma:.0f}",
            }
            if not _already_alerted(area, "PRICE_DIP"):
                price_signals.append("PRICE_DIP")
                new_signals.append(signal)
                _log_anomaly(signal)

        # ── Supply Surge Check ────────────────────────────────────────────────
        listings = latest["listings"]
        supply_ma = latest["supply_ma7"]
        if (
            pd.notna(listings) and pd.notna(supply_ma) and supply_ma > 0
            and listings > (1 + config.SUPPLY_SURGE_THRESHOLD) * supply_ma
        ):
            deviation = (listings - supply_ma) / supply_ma * 100
            signal = {
                "area": area,
                "signal_type": "SUPPLY_SURGE",
                "signal_date": signal_date,
                "signal_value": listings,
                "baseline_value": supply_ma,
                "deviation_pct": deviation,
                "notes": f"Bayut listings {listings} is {deviation:.1f}% above 7-day MA of {supply_ma:.1f}",
            }
            if not _already_alerted(area, "SUPPLY_SURGE"):
                new_signals.append(signal)
                _log_anomaly(signal)

        # ── Strong Buy Check ──────────────────────────────────────────────────
        if volume_signals and price_signals:
            signal = {
                "area": area,
                "signal_type": "STRONG_BUY",
                "signal_date": signal_date,
                "signal_value": None,
                "baseline_value": None,
                "deviation_pct": None,
                "notes": f"STRONG BUY: Both volume drop AND price dip detected in {area}",
            }
            if not _already_alerted(area, "STRONG_BUY"):
                new_signals.append(signal)
                _log_anomaly(signal)

    if new_signals:
        logger.info("Detected %d new signals: %s",
                    len(new_signals),
                    [(s["area"], s["signal_type"]) for s in new_signals])
    else:
        logger.info("No new anomalies detected")

    return new_signals


def _log_anomaly(signal: dict) -> None:
    """Persist a detected signal to anomaly_log."""
    with get_session() as session:
        session.add(AnomalyLog(
            signal_date=signal["signal_date"],
            area_canonical=signal["area"],
            signal_type=signal["signal_type"],
            signal_value=signal.get("signal_value"),
            baseline_value=signal.get("baseline_value"),
            deviation_pct=signal.get("deviation_pct"),
            notes=signal.get("notes"),
        ))


# ─── Full Pipeline ─────────────────────────────────────────────────────────────

def run_detection_pipeline() -> list[dict]:
    """
    Run the full detection pipeline in order:
    1. Aggregate raw transactions to daily metrics
    2. Refresh Bayut supply counts
    3. Compute rolling MAs
    4. Detect anomalies
    Returns list of new signals.
    """
    logger.info("=== Starting anomaly detection pipeline ===")
    aggregate_daily_metrics()
    refresh_supply_data()
    compute_rolling_mas()
    signals = detect_anomalies()
    logger.info("=== Pipeline complete — %d new signals ===", len(signals))
    return signals


# ─── Entry Point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    init_db()
    signals = run_detection_pipeline()
    if signals:
        print(f"\n{'='*50}")
        print(f"  🚨 {len(signals)} BUY SIGNAL(S) DETECTED")
        print(f"{'='*50}")
        for s in signals:
            print(f"  [{s['signal_type']}] {s['area']} — {s['notes']}")
    else:
        print("\n  ✅ No anomalies detected — market looks normal")
