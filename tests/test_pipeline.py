"""
tests/test_pipeline.py — Tests for the detection pipeline against a throwaway
in-memory database (your real data/dubai_re.db is never touched).
Run: pytest tests/ -v
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db


@pytest.fixture()
def memory_db(monkeypatch):
    """Point db.get_session() at a fresh in-memory SQLite database."""
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    db.Base.metadata.create_all(engine)
    monkeypatch.setattr(db, "SessionLocal", sessionmaker(bind=engine, autocommit=False, autoflush=False))
    return engine


def _add_dld_day(day: date, area: str, volume: int, price_sqm: float, listings: int | None = None) -> None:
    with db.get_session() as s:
        s.add(db.DailyMetric(
            metric_date=day, area_canonical=area,
            transaction_volume=volume, avg_price_sqm=price_sqm,
            total_worth=volume * price_sqm * 80, median_price=price_sqm * 80,
            bayut_listing_count=listings,
        ))


def test_bayut_only_row_does_not_trigger_volume_drop(memory_db):
    """
    The scheduler stores today's Bayut count before DLD publishes today's sales.
    That row has no sales data and must NOT be read as a 'zero sales' day.
    """
    from anomaly_detector import compute_rolling_mas, detect_anomalies

    today = date.today()
    area = "Dubai Marina"
    for i in range(10, 0, -1):                       # 10 normal days up to yesterday
        _add_dld_day(today - timedelta(days=i), area, volume=20, price_sqm=15_000, listings=2_000)
    with db.get_session() as s:                       # today: Bayut-only row
        s.add(db.DailyMetric(metric_date=today, area_canonical=area,
                             transaction_volume=None, bayut_listing_count=2_010))

    compute_rolling_mas()
    signals = detect_anomalies()

    assert [s for s in signals if s["signal_type"] == "VOLUME_DROP"] == []
    assert signals == []


def test_real_volume_drop_still_detected(memory_db):
    """A genuine drop in DLD sales on the latest DLD day must still fire."""
    from anomaly_detector import compute_rolling_mas, detect_anomalies

    today = date.today()
    area = "Business Bay"
    for i in range(10, 1, -1):
        _add_dld_day(today - timedelta(days=i), area, volume=20, price_sqm=16_000, listings=2_000)
    _add_dld_day(today - timedelta(days=1), area, volume=5, price_sqm=14_000, listings=2_000)
    with db.get_session() as s:                       # today: Bayut-only row
        s.add(db.DailyMetric(metric_date=today, area_canonical=area,
                             transaction_volume=None, bayut_listing_count=2_600))

    compute_rolling_mas()
    types = {s["signal_type"] for s in detect_anomalies()}

    assert {"VOLUME_DROP", "PRICE_DIP", "STRONG_BUY", "SUPPLY_SURGE"} <= types


def test_volume_ma_ignores_bayut_only_rows(memory_db):
    from anomaly_detector import compute_rolling_mas

    today = date.today()
    area = "JVC/JVT"
    for i in range(5, 0, -1):
        _add_dld_day(today - timedelta(days=i), area, volume=30, price_sqm=9_000)
    with db.get_session() as s:
        s.add(db.DailyMetric(metric_date=today, area_canonical=area,
                             transaction_volume=0, bayut_listing_count=5_000))  # legacy 0-row

    compute_rolling_mas()
    with db.get_session() as s:
        row = s.query(db.DailyMetric).filter_by(metric_date=today, area_canonical=area).one()
        assert row.volume_ma7 == pytest.approx(30.0)


# ─── Mock data date windows ───────────────────────────────────────────────────

@pytest.mark.parametrize("today", [date(2026, 11, 5), date(2027, 2, 10), date(2026, 3, 7)])
def test_mock_windows_end_today_and_fit_lookback(today):
    import config
    from seed_data import STRESS_DAYS, mock_date_windows

    w = mock_date_windows(today)
    assert w["end"] == today
    assert w["stress_end"] == today
    assert (w["stress_end"] - w["stress_start"]).days == STRESS_DAYS - 1
    # Current period covers the dashboard's full lookback window
    assert (today - w["start"]).days >= config.LOOKBACK_DAYS
    # Previous-year data never overlaps the current period
    assert w["prev_start"] == date(today.year - 1, 1, 1)
    assert w["prev_end"] < w["start"]


# ─── Mock vs real data ────────────────────────────────────────────────────────

def test_purge_mock_data_keeps_real_transactions(memory_db):
    from data_fetcher import purge_mock_data
    from seed_data import db_is_mock_only

    today = date.today()
    with db.get_session() as s:
        db.upsert_transaction(s, {"transaction_id": "MOCK-1", "transaction_date": today,
                                  "actual_worth": 1e6, "procedure_area": 80.0, "area_canonical": "Dubai Marina"})
        s.add(db.DailyMetric(metric_date=today, area_canonical="Dubai Marina", transaction_volume=1))
        s.add(db.AnomalyLog(signal_date=today, area_canonical="Dubai Marina", signal_type="PRICE_DIP"))
    assert db_is_mock_only()

    with db.get_session() as s:
        db.upsert_transaction(s, {"transaction_id": "123456", "transaction_date": today,
                                  "actual_worth": 1.2e6, "procedure_area": 90.0, "area_canonical": "Dubai Marina"})
    assert not db_is_mock_only()

    assert purge_mock_data() == 1
    with db.get_session() as s:
        assert [t.transaction_id for t in s.query(db.Transaction).all()] == ["123456"]
        assert s.query(db.DailyMetric).count() == 0
        assert s.query(db.AnomalyLog).count() == 0


# ─── DLD JSON API fetcher (no network: responses are simulated) ──────────────

def _api_row(**over):
    row = {
        "TRANSACTION_NUMBER": "1-2-2026", "INSTANCE_DATE": date.today().isoformat() + "T10:00:00",
        "TRANS_VALUE": 1_500_000, "PROCEDURE_AREA": 90, "GROUP_EN": "Sales",
        "USAGE_EN": "Residential", "AREA_EN": "Marsa Dubai (Dubai Marina)",
        "PROP_TYPE_EN": "Unit", "PROJECT_EN": "Marina Gate", "MASTER_PROJECT_EN": "Dubai Marina",
    }
    row.update(over)
    return row


def test_api_row_filters_sales_residential_and_price_cap():
    import config
    from data_fetcher import _api_row_to_record

    assert _api_row_to_record(_api_row())["area_canonical"] == "Dubai Marina"
    assert _api_row_to_record(_api_row(GROUP_EN="Mortgage")) is None
    assert _api_row_to_record(_api_row(USAGE_EN="Commercial")) is None
    assert _api_row_to_record(_api_row(TRANS_VALUE=config.MAX_PRICE_AED + 1)) is None
    assert _api_row_to_record(_api_row(TRANS_VALUE=config.MIN_PRICE_AED - 1)) is None
    assert _api_row_to_record(_api_row(AREA_EN="Al Barsha")) is None


def test_paginate_area_respects_page_cap(monkeypatch):
    import config
    import data_fetcher

    calls = []

    def fake_page(http, from_date, to_date, area_id="", skip=0):
        calls.append(skip)
        return [_api_row(TRANSACTION_NUMBER=f"T{skip + i}") for i in range(200)], 100_000

    monkeypatch.setattr(data_fetcher, "_fetch_dld_page", fake_page)
    monkeypatch.setattr(config, "DLD_API_MAX_PAGES_PER_AREA", 3)
    rows = data_fetcher._paginate_area(None, "a", "b", "A-1")
    assert len(calls) == 3 and len(rows) == 600


def test_paginate_area_stops_when_all_rows_fetched(monkeypatch):
    import data_fetcher

    def fake_page(http, from_date, to_date, area_id="", skip=0):
        return ([_api_row()] * 200, 300) if skip == 0 else ([_api_row()] * 100, 300)

    monkeypatch.setattr(data_fetcher, "_fetch_dld_page", fake_page)
    assert len(data_fetcher._paginate_area(None, "a", "b", "A-1")) == 300


@pytest.mark.parametrize("dld_name,expected", [
    ("Marsa Dubai", "Dubai Marina"),
    ("Al Barsha South Fourth", "JVC/JVT"),
    ("Hadaeq Sheikh Mohammed Bin Rashid", "Dubai Hills"),
    ("Burj Khalifa", "Downtown Dubai"),
    ("Madinat Al Mataar", "Dubai South"),
    ("Al Barsha First", None),
])
def test_official_dld_area_names(dld_name, expected):
    from data_fetcher import canonicalise_area
    assert canonicalise_area(dld_name) == expected
