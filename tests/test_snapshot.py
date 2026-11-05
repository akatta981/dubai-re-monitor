"""tests/test_snapshot.py — export → load round trip (in-memory DB, no network)."""

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


def _fresh_db(monkeypatch):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    db.Base.metadata.create_all(engine)
    monkeypatch.setattr(db, "SessionLocal", sessionmaker(bind=engine, autocommit=False, autoflush=False))


def _fill(n_days=20):
    with db.get_session() as s:
        for d in range(n_days):
            for k in range(6):
                db.upsert_transaction(s, {
                    "transaction_id": f"{d}-{k}", "transaction_date": date.today() - timedelta(days=d + 1),
                    "actual_worth": 1_200_000.0, "procedure_area": 90.0, "area_canonical": "JVC/JVT",
                    "prop_type": "Unit", "is_offplan": "Ready" if k % 2 else "Off-plan",
                    "project_name": "Bloom Heights"})
        db.upsert_transaction(s, {"transaction_id": "MOCK-x", "transaction_date": date.today(),
                                  "actual_worth": 1e6, "procedure_area": 80.0, "area_canonical": "JVC/JVT"})


def test_export_then_load_into_empty_db(tmp_path, monkeypatch):
    import snapshot
    path = tmp_path / "snap.csv.gz"
    monkeypatch.setattr(snapshot, "SNAPSHOT_PATH", path)
    monkeypatch.setattr(snapshot.config, "SNAPSHOT_URL", "")          # no network in tests

    _fresh_db(monkeypatch)
    _fill()
    assert snapshot.export_snapshot(path) == 120                      # demo row not exported

    _fresh_db(monkeypatch)                                            # "website after restart"
    msg = snapshot.ensure_data_loaded()
    assert msg and "120" in msg
    with db.get_session() as s:
        assert s.query(db.Transaction).count() == 120
        t = s.query(db.Transaction).filter_by(transaction_id="3-1").one()
        assert t.price_per_sqm == pytest.approx(1_200_000 / 90) and t.is_offplan == "Ready"
        assert s.query(db.DailyMetric).count() > 0                     # metrics rebuilt
    assert snapshot.ensure_data_loaded() is None                       # not empty → no reload


def test_load_skips_rows_already_present(tmp_path, monkeypatch):
    import snapshot
    path = tmp_path / "snap.csv.gz"
    monkeypatch.setattr(snapshot, "SNAPSHOT_PATH", path)
    monkeypatch.setattr(snapshot.config, "SNAPSHOT_URL", "")
    _fresh_db(monkeypatch)
    _fill()
    snapshot.export_snapshot(path)
    added, _ = snapshot.load_latest()
    assert added == 0


def test_url_failure_falls_back_to_bundled_file(tmp_path, monkeypatch):
    import snapshot
    path = tmp_path / "snap.csv.gz"
    monkeypatch.setattr(snapshot, "SNAPSHOT_PATH", path)
    monkeypatch.setattr(snapshot.config, "SNAPSHOT_URL", "https://example.invalid/snap.csv.gz")

    def boom(*a, **k):
        raise ConnectionError("offline")
    monkeypatch.setattr(snapshot.requests, "get", boom)
    _fresh_db(monkeypatch)
    _fill(2)
    snapshot.export_snapshot(path)
    df, source = snapshot.read_snapshot()
    assert source == "bundled file" and len(df) == 12


def test_no_snapshot_anywhere(tmp_path, monkeypatch):
    import snapshot
    monkeypatch.setattr(snapshot, "SNAPSHOT_PATH", tmp_path / "missing.csv.gz")
    monkeypatch.setattr(snapshot.config, "SNAPSHOT_URL", "")
    _fresh_db(monkeypatch)
    assert snapshot.ensure_data_loaded() is None
