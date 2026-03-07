"""
db.py — SQLite database layer using SQLAlchemy ORM.
Three tables: transactions (raw DLD), daily_metrics (aggregated), anomaly_log (alerts).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator

from sqlalchemy import (
    Boolean, Column, Date, DateTime, Float,
    Integer, String, Text, UniqueConstraint,
    create_engine, event, inspect, text,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "data" / "dubai_re.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False},
    echo=False,
)


@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_conn, _connection_record) -> None:
    """Enable WAL mode so Streamlit reads don't block scheduler writes."""
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


class Base(DeclarativeBase):
    pass


# ─── Models ───────────────────────────────────────────────────────────────────

class Transaction(Base):
    """Raw DLD transaction record. Deduplicated on transaction_id."""
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String(64), unique=True, nullable=True)
    transaction_date = Column(Date, nullable=False, index=True)
    actual_worth = Column(Float, nullable=True)
    procedure_area = Column(Float, nullable=True)
    price_per_sqm = Column(Float, nullable=True)
    trans_group = Column(String(64), nullable=True)
    property_usage = Column(String(64), nullable=True)
    prop_type = Column(String(64), nullable=True)
    area_name = Column(String(128), nullable=True, index=True)
    area_canonical = Column(String(64), nullable=True, index=True)
    building_name = Column(String(256), nullable=True)
    project_name = Column(String(256), nullable=True, index=True)
    master_project = Column(String(256), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class DailyMetric(Base):
    """Aggregated per area per day with pre-calculated rolling MAs."""
    __tablename__ = "daily_metrics"
    __table_args__ = (
        UniqueConstraint("metric_date", "area_canonical", name="uq_date_area"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    metric_date = Column(Date, nullable=False, index=True)
    area_canonical = Column(String(64), nullable=False, index=True)
    transaction_volume = Column(Integer, default=0)
    avg_price_sqm = Column(Float, nullable=True)
    total_worth = Column(Float, nullable=True)
    median_price = Column(Float, nullable=True)
    volume_ma7 = Column(Float, nullable=True)
    price_sqm_ma30 = Column(Float, nullable=True)
    bayut_listing_count = Column(Integer, nullable=True)
    supply_ma7 = Column(Float, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AnomalyLog(Base):
    """Every triggered buy signal recorded for history and alert deduplication."""
    __tablename__ = "anomaly_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    detected_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    signal_date = Column(Date, nullable=False)
    area_canonical = Column(String(64), nullable=False, index=True)
    signal_type = Column(String(32), nullable=False)
    signal_value = Column(Float, nullable=True)
    baseline_value = Column(Float, nullable=True)
    deviation_pct = Column(Float, nullable=True)
    alert_sent = Column(Boolean, default=False)
    alert_channel = Column(String(32), nullable=True)
    notes = Column(Text, nullable=True)


class FetchLog(Base):
    """Tracks each data fetch attempt for debugging."""
    __tablename__ = "fetch_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fetched_at = Column(DateTime, default=datetime.utcnow)
    source = Column(String(32), nullable=False)
    status = Column(String(16), nullable=False)
    rows_upserted = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def init_db() -> None:
    """Create all tables if they don't exist. Safe to call repeatedly."""
    Base.metadata.create_all(engine)
    _migrate_transactions()
    logger.info("Database initialised at %s", DB_PATH)


def _migrate_transactions() -> None:
    """
    Add new columns to the transactions table if they don't already exist.
    Handles upgrades from older DB versions gracefully.
    """
    inspector = inspect(engine)
    try:
        existing = {col["name"] for col in inspector.get_columns("transactions")}
    except Exception:
        return  # table doesn't exist yet — create_all will handle it

    new_cols: dict[str, str] = {
        "building_name":  "VARCHAR(256)",
        "project_name":   "VARCHAR(256)",
        "master_project": "VARCHAR(256)",
    }
    with engine.connect() as conn:
        for col_name, col_type in new_cols.items():
            if col_name not in existing:
                conn.execute(text(f"ALTER TABLE transactions ADD COLUMN {col_name} {col_type}"))
                conn.commit()
                logger.info("Migrated: added column '%s' to transactions", col_name)


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Context manager for safe session lifecycle."""
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def upsert_transaction(session: Session, record: dict) -> bool:
    """
    Insert transaction or skip if already exists.
    Returns True if inserted, False if duplicate.
    """
    tx_id = record.get("transaction_id")
    if tx_id:
        existing = session.query(Transaction).filter_by(transaction_id=tx_id).first()
        if existing:
            return False

    worth = record.get("actual_worth")
    area = record.get("procedure_area")
    record["price_per_sqm"] = (worth / area) if (worth and area and area > 0) else None

    session.add(Transaction(**record))
    return True


def upsert_daily_metric(session: Session, record: dict) -> None:
    """Insert or update daily metric row keyed on date + area."""
    existing = session.query(DailyMetric).filter_by(
        metric_date=record["metric_date"],
        area_canonical=record["area_canonical"],
    ).first()
    if existing:
        for k, v in record.items():
            setattr(existing, k, v)
        existing.updated_at = datetime.utcnow()
    else:
        session.add(DailyMetric(**record))
