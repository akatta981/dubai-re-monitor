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


def test_real_volume_drop_still_detected(memory_db, monkeypatch):
    """A genuine drop in DLD sales on the latest DLD day must still fire."""
    import config
    from anomaly_detector import compute_rolling_mas, detect_anomalies
    monkeypatch.setattr(config, "SIGNAL_SKIP_WEEKEND_VOLUME", False)   # test must not depend on weekday

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


def test_api_row_filters_sales_residential_and_price_floor():
    import config
    from data_fetcher import _api_row_to_record

    assert _api_row_to_record(_api_row())["area_canonical"] == "Dubai Marina"
    assert _api_row_to_record(_api_row(GROUP_EN="Mortgage")) is None
    assert _api_row_to_record(_api_row(USAGE_EN="Commercial")) is None
    # Above the cap is stored (the cap is applied at analysis time) …
    assert _api_row_to_record(_api_row(TRANS_VALUE=config.MAX_PRICE_AED + 1)) is not None
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



# ─── History download / analysis cap / off-plan ──────────────────────────────

def test_date_chunks_cover_range_without_gaps():
    from data_fetcher import _date_chunks
    chunks = _date_chunks(date(2025, 1, 1), date(2025, 3, 1), 15)
    assert chunks[0][0] == date(2025, 1, 1) and chunks[-1][1] == date(2025, 3, 1)
    for (a_start, a_end), (b_start, _) in zip(chunks, chunks[1:]):
        assert b_start == a_end + timedelta(days=1)


@pytest.mark.parametrize("raw,label", [
    ({"IS_OFFPLAN": 1}, "Off-plan"), ({"IS_OFFPLAN": "0"}, "Ready"),
    ({"IS_OFFPLAN_EN": "Off-Plan"}, "Off-plan"), ({"IS_OFFPLAN_EN": "Ready"}, "Ready"), ({}, None),
])
def test_offplan_label(raw, label):
    from data_fetcher import _offplan_label
    assert _offplan_label(raw) == label


def test_history_download_uses_date_slices(memory_db, monkeypatch):
    import data_fetcher
    seen_ranges = []
    monkeypatch.setattr(data_fetcher, "_resolve_area_ids", lambda http: {"Dubai Marina": ["A-1"]})

    def fake_paginate(http, f, t, area_id):
        seen_ranges.append((f, t))
        return [_api_row(TRANSACTION_NUMBER=f"T-{f}", INSTANCE_DATE=f"{f[6:]}-{f[:2]}-{f[3:5]}T10:00:00",
                         IS_OFFPLAN="1")]
    monkeypatch.setattr(data_fetcher, "_paginate_area", fake_paginate)

    n, err = data_fetcher.fetch_dld_transactions(start=date(2025, 1, 1), end=date(2025, 1, 31), chunk_days=7)
    assert err == "" and n == 5 and len(seen_ranges) == 5
    assert seen_ranges[0] == ("01/01/2025", "01/08/2025")      # one day overlap
    with db.get_session() as s:
        assert {t.is_offplan for t in s.query(db.Transaction).all()} == {"Off-plan"}


def test_redownload_fills_missing_offplan(memory_db):
    with db.get_session() as s:
        db.upsert_transaction(s, {"transaction_id": "X1", "transaction_date": date.today(),
                                  "actual_worth": 1e6, "procedure_area": 80.0})
    with db.get_session() as s:
        assert db.upsert_transaction(s, {"transaction_id": "X1", "transaction_date": date.today(),
                                         "actual_worth": 1e6, "procedure_area": 80.0,
                                         "is_offplan": "Ready"}) is False
    with db.get_session() as s:
        assert s.query(db.Transaction).one().is_offplan == "Ready"


def test_analysis_excludes_sales_above_cap(memory_db):
    import config
    from anomaly_detector import aggregate_daily_metrics
    with db.get_session() as s:
        for i, worth in enumerate([1_000_000, 1_500_000, config.MAX_PRICE_AED + 500_000]):
            db.upsert_transaction(s, {"transaction_id": f"C{i}", "transaction_date": date.today(),
                                      "actual_worth": float(worth), "procedure_area": 100.0,
                                      "area_canonical": "Dubai Marina"})
    aggregate_daily_metrics()
    with db.get_session() as s:
        m = s.query(db.DailyMetric).one()
        assert m.transaction_volume == 2 and m.avg_price_sqm == pytest.approx(12_500)


# ─── Analysis clean-up and sub-area mapping ──────────────────────────────────

def test_analysis_leaves_out_land_and_bad_values(memory_db):
    from anomaly_detector import aggregate_daily_metrics
    good = {"actual_worth": 1_200_000.0, "procedure_area": 100.0, "prop_type": "Unit"}
    variants = [good,
                {**good, "prop_type": "Land"},                      # land plot
                {**good, "procedure_area": 10.0},                   # size < 20 m²
                {**good, "actual_worth": 1_000_000.0, "procedure_area": 900.0}]  # < AED 2,000/m²
    with db.get_session() as s:
        for i, v in enumerate(variants):
            db.upsert_transaction(s, {"transaction_id": f"L{i}", "transaction_date": date.today(),
                                      "area_canonical": "Dubai Marina", **v})
    aggregate_daily_metrics()
    with db.get_session() as s:
        assert s.query(db.DailyMetric).one().transaction_volume == 1


def test_damac_riverside_mapped_inside_dip2():
    from data_fetcher import _api_row_to_record
    rec = _api_row_to_record(_api_row(AREA_EN="Dubai Investment Park Second",
                                      PROJECT_EN="DAMAC Riverside Views - Marine 1"))
    assert rec["area_canonical"] == "DAMAC Riverside"
    rec = _api_row_to_record(_api_row(AREA_EN="Dubai Investment Park Second", PROJECT_EN="Expo Village"))
    assert rec["area_canonical"] == "Dubai Investment Park 2"
    # "riverside" outside DIP 2 must not be re-assigned
    rec = _api_row_to_record(_api_row(AREA_EN="Marsa Dubai", PROJECT_EN="Riverside Tower"))
    assert rec["area_canonical"] == "Dubai Marina"


def test_redownload_refreshes_area(memory_db):
    base = {"transaction_id": "Z1", "transaction_date": date.today(), "actual_worth": 1e6, "procedure_area": 80.0}
    with db.get_session() as s:
        db.upsert_transaction(s, {**base, "area_canonical": "Dubai Investment Park 2"})
    with db.get_session() as s:
        db.upsert_transaction(s, {**base, "area_canonical": "DAMAC Riverside"})
    with db.get_session() as s:
        assert s.query(db.Transaction).one().area_canonical == "DAMAC Riverside"
