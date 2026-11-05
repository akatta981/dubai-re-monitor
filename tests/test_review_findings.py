"""
tests/test_review_findings.py — Regression tests for issues found in an
independent adversarial review. Uses an in-memory DB (real data untouched).
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
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    db.Base.metadata.create_all(engine)
    monkeypatch.setattr(db, "SessionLocal", sessionmaker(bind=engine, autocommit=False, autoflush=False))
    return engine


def _tx(tx_id, day=None, area="Dubai Marina"):
    return {"transaction_id": tx_id, "transaction_date": day or date.today(),
            "actual_worth": 1_000_000.0, "procedure_area": 80.0, "area_canonical": area}


def _signal(area="Dubai Marina", signal_type="PRICE_DIP", day=None):
    return {"area": area, "signal_type": signal_type, "signal_date": day or date.today(),
            "signal_value": 1.0, "baseline_value": 2.0, "deviation_pct": -50.0, "notes": "test"}


# 1. No real alerts may be sent while the DB only holds demo data
def test_no_alerts_sent_for_demo_data(memory_db, monkeypatch):
    import alerts
    sent = []
    monkeypatch.setattr(alerts, "send_email_alert", lambda s: sent.append("email") or True)
    monkeypatch.setattr(alerts, "send_whatsapp_alert", lambda s: sent.append("wa") or True)
    with db.get_session() as s:
        db.upsert_transaction(s, _tx("MOCK-1"))
    assert alerts.send_alerts([_signal()]) == {}
    assert sent == []


def test_alerts_sent_for_real_data(memory_db, monkeypatch):
    import alerts
    monkeypatch.setattr(alerts, "send_email_alert", lambda s: True)
    monkeypatch.setattr(alerts, "send_whatsapp_alert", lambda s: True)
    with db.get_session() as s:
        db.upsert_transaction(s, _tx("12345"))
    assert alerts.send_alerts([_signal()]) == {"email": True, "whatsapp": True}


def test_test_alert_still_works_on_demo_data(memory_db, monkeypatch):
    import alerts
    monkeypatch.setattr(alerts, "send_email_alert", lambda s: True)
    with db.get_session() as s:
        db.upsert_transaction(s, _tx("MOCK-1"))
    assert alerts.test_alerts(channels=["email"]) == {"email": True}


# 15. No channels selected -> nothing recorded as "failed"
def test_no_channels_records_nothing(memory_db):
    import alerts
    with db.get_session() as s:
        db.upsert_transaction(s, _tx("12345"))
        s.add(db.AnomalyLog(signal_date=date.today(), area_canonical="Dubai Marina", signal_type="PRICE_DIP"))
    assert alerts.send_alerts([_signal()], channels=[]) == {}
    with db.get_session() as s:
        assert s.query(db.AnomalyLog).one().alert_channel is None


# 2. A sync that fails part-way must still remove mock data
def test_partial_sync_purges_mock_data(memory_db, monkeypatch):
    import data_fetcher
    with db.get_session() as s:
        db.upsert_transaction(s, _tx("MOCK-1"))
    monkeypatch.setattr(data_fetcher, "_resolve_area_ids", lambda http: {"Dubai Marina": ["A-1"], "Business Bay": ["A-2"]})

    def fake_paginate(http, f, t, area_id):
        if area_id == "A-2":
            raise TimeoutError("read timed out")
        return [{"TRANSACTION_NUMBER": "R-1", "INSTANCE_DATE": date.today().isoformat() + "T10:00:00",
                 "TRANS_VALUE": 1_500_000, "PROCEDURE_AREA": 90, "GROUP_EN": "Sales",
                 "USAGE_EN": "Residential", "AREA_EN": "Marsa Dubai"}]
    monkeypatch.setattr(data_fetcher, "_paginate_area", fake_paginate)

    n, err = data_fetcher.fetch_dld_transactions()
    assert n == 1 and "timed out" in err
    with db.get_session() as s:
        assert [t.transaction_id for t in s.query(db.Transaction).all()] == ["R-1"]


# 3. A Bayut-only row must store "unknown" sales, not 0
def test_bayut_only_row_stores_null_volume(memory_db, monkeypatch):
    import anomaly_detector
    monkeypatch.setattr(anomaly_detector, "fetch_bayut_supply", lambda: {"Dubai Marina": 3000})
    anomaly_detector.refresh_supply_data()
    with db.get_session() as s:
        assert s.query(db.DailyMetric).one().transaction_volume is None


# 5. Demo data must end on the day it is generated, not the day the module was imported
def test_seed_uses_current_date(memory_db, monkeypatch):
    import seed_data
    from datetime import date as real_date

    class FakeDate(real_date):
        @classmethod
        def today(cls):
            return real_date.today() + timedelta(days=10)

    orig = seed_data.mock_date_windows
    later = FakeDate.today()
    monkeypatch.setattr(seed_data, "date", FakeDate)
    monkeypatch.setattr(seed_data, "seed_2025_transactions", lambda: 0)   # keep the test fast
    monkeypatch.setattr(seed_data, "mock_date_windows",
                        lambda today=None: {**orig(later), "start": later - timedelta(days=20)})
    seed_data.seed_database()
    with db.get_session() as s:
        latest = max(t.transaction_date for t in s.query(db.Transaction).all())
    assert latest == later


# 6. Pagination: string totals and missing totals
def test_paginate_string_total(monkeypatch):
    import data_fetcher
    monkeypatch.setattr(data_fetcher, "_fetch_dld_page",
                        lambda h, f, t, area_id="", skip=0: ([{"x": 1}] * 200, "500") if skip < 400 else ([{"x": 1}] * 100, "500"))
    assert len(data_fetcher._paginate_area(None, "a", "b", "A-1")) == 500


def test_paginate_missing_total_keeps_going_while_pages_full(monkeypatch):
    import data_fetcher
    monkeypatch.setattr(data_fetcher, "_fetch_dld_page",
                        lambda h, f, t, area_id="", skip=0: ([{"x": 1}] * 200, 0) if skip < 600 else ([{"x": 1}] * 50, 0))
    assert len(data_fetcher._paginate_area(None, "a", "b", "A-1")) == 650


# 8. Real rows without a transaction ID are still real data
def test_null_id_rows_count_as_real(memory_db):
    from seed_data import db_is_mock_only
    with db.get_session() as s:
        db.upsert_transaction(s, _tx(None))
    assert db_is_mock_only() is False


# 14. The same DLD day must not re-alert every 6 hours
def test_same_signal_date_does_not_realert(memory_db, monkeypatch):
    import anomaly_detector
    from datetime import datetime
    day = date.today() - timedelta(days=2)
    with db.get_session() as s:
        s.add(db.AnomalyLog(signal_date=day, area_canonical="Dubai Marina", signal_type="PRICE_DIP",
                            detected_at=datetime.utcnow() - timedelta(hours=12)))
    assert anomaly_detector._already_alerted("Dubai Marina", "PRICE_DIP", signal_date=day)
    assert not anomaly_detector._already_alerted("Dubai Marina", "PRICE_DIP", signal_date=date.today())


# 10. Alternative DLD spelling "Al Barshaa"
def test_barshaa_spelling():
    from data_fetcher import canonicalise_area
    assert canonicalise_area("Al Barshaa South Fourth") == "JVC/JVT"
