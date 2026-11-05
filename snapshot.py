"""
snapshot.py — Share your DLD sales data with the website (and other PCs).

Streamlit Cloud wipes the website's database whenever the app sleeps or
restarts. A "snapshot" is a compressed copy of your real sales stored in the
GitHub repo; the website loads it automatically whenever its database is empty.

On your PC (after a sync / history download):
    python snapshot.py publish     # export + upload to GitHub (main branch)
    python snapshot.py export      # only write snapshot/transactions.csv.gz

Anywhere:
    python snapshot.py load        # load the newest snapshot into this database

Only public DLD transaction data is exported — no settings, passwords or alerts.
"""

from __future__ import annotations

import io
import logging
import subprocess
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

import config
from db import MOCK_ID_PREFIX, FetchLog, Transaction, get_session, init_db

logger = logging.getLogger(__name__)

SNAPSHOT_PATH = Path(__file__).parent / "snapshot" / "transactions.csv.gz"

COLUMNS = [
    "transaction_id", "transaction_date", "actual_worth", "procedure_area",
    "trans_group", "property_usage", "prop_type", "area_name", "area_canonical",
    "building_name", "project_name", "master_project", "is_offplan",
]


# ─── Export ─────────────────────────────────────────────────────────────────

def export_snapshot(path: Path = SNAPSHOT_PATH) -> int:
    """Write all real (non-demo) transactions to a gzip CSV. Returns row count."""
    with get_session() as s:
        rows = (
            s.query(*[getattr(Transaction, c) for c in COLUMNS])
            .filter(~Transaction.transaction_id.like(f"{MOCK_ID_PREFIX}%")
                    | Transaction.transaction_id.is_(None))
            .order_by(Transaction.transaction_date)
            .all()
        )
    df = pd.DataFrame(rows, columns=COLUMNS)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, compression={"method": "gzip", "mtime": 0})
    return len(df)


# ─── Read ───────────────────────────────────────────────────────────────────

def _parse(raw: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(raw), compression="gzip", dtype={"transaction_id": str})
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"snapshot is missing columns: {missing}")
    return df


def read_snapshot(prefer_url: bool = True) -> tuple[Optional[pd.DataFrame], str]:
    """
    Return (dataframe, source). Tries the GitHub copy first (always the newest
    published snapshot, even if this app was deployed from another branch),
    then the file bundled with the code. (None, reason) if neither works.
    """
    errors = []
    sources = (["url", "file"] if prefer_url else ["file", "url"])
    for src in sources:
        try:
            if src == "url":
                if not config.SNAPSHOT_URL:
                    continue
                resp = requests.get(config.SNAPSHOT_URL, timeout=60)
                resp.raise_for_status()
                return _parse(resp.content), "GitHub"
            if SNAPSHOT_PATH.exists():
                return _parse(SNAPSHOT_PATH.read_bytes()), "bundled file"
        except Exception as e:   # network down, 404, corrupt file …
            errors.append(f"{src}: {e}")
    return None, "; ".join(errors) or "no snapshot published yet"


# ─── Import ─────────────────────────────────────────────────────────────────

def import_snapshot(df: pd.DataFrame, rebuild: bool = True) -> int:
    """Insert snapshot rows not already in the DB, then rebuild metrics. Returns rows added."""
    df = df.copy()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date
    df = df.astype(object).where(pd.notna(df), None)

    with get_session() as s:
        existing = {t for (t,) in s.query(Transaction.transaction_id).all() if t}
    records = []
    for rec in df.to_dict("records"):
        if rec["transaction_id"] and rec["transaction_id"] in existing:
            continue
        worth, area = rec.get("actual_worth"), rec.get("procedure_area")
        rec["price_per_sqm"] = (worth / area) if (worth and area and area > 0) else None
        records.append(rec)

    for i in range(0, len(records), 5_000):          # commit in batches
        with get_session() as s:
            s.bulk_insert_mappings(Transaction, records[i:i + 5_000])

    if records:
        with get_session() as s:
            s.add(FetchLog(source="dld", status="success", rows_upserted=len(records),
                           error_message="Loaded from data snapshot"))
    if records and rebuild:
        from anomaly_detector import aggregate_daily_metrics, compute_rolling_mas, detect_anomalies
        aggregate_daily_metrics()
        compute_rolling_mas()
        detect_anomalies()        # logs signals only — never sends alerts
    return len(records)


def load_latest() -> tuple[int, str]:
    """Download/read the newest snapshot and add any new rows. Returns (rows added, message)."""
    df, source = read_snapshot()
    if df is None:
        return 0, f"No snapshot available ({source})"
    added = import_snapshot(df)
    newest = pd.to_datetime(df["transaction_date"]).max()
    return added, (f"Loaded {added:,} new sales from the {source} snapshot "
                   f"({len(df):,} sales, up to {newest:%d %b %Y}).")


def ensure_data_loaded() -> Optional[str]:
    """If the database has no sales at all, load the snapshot. Returns a message or None."""
    with get_session() as s:
        if s.query(Transaction.id).first() is not None:
            return None
    added, msg = load_latest()
    return msg if added else None


# ─── Publish (export + git push) ────────────────────────────────────────────

def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=Path(__file__).parent,
                          capture_output=True, text=True)


def publish() -> int:
    n = export_snapshot()
    print(f"✅ Exported {n:,} sales to {SNAPSHOT_PATH.relative_to(Path(__file__).parent)}")
    rel = str(SNAPSHOT_PATH.relative_to(Path(__file__).parent))
    _git("add", rel)
    if _git("diff", "--cached", "--quiet").returncode == 0:
        print("ℹ️  Snapshot unchanged since last publish — nothing to upload.")
        return 0
    c = _git("commit", "-m", f"Update data snapshot ({n:,} sales)")
    if c.returncode != 0:
        print("❌ Could not save the snapshot in git:\n" + c.stdout + c.stderr)
        return 1
    print("⬇️  Getting the latest code from GitHub first…")
    p = _git("pull", "--no-edit", "origin", "main")
    if p.returncode != 0:
        print("❌ git pull failed:\n" + p.stdout + p.stderr)
        return 1
    print("⬆️  Uploading to GitHub… (a GitHub sign-in window may open the first time)")
    r = _git("push", "origin", "HEAD:main")
    if r.returncode != 0:
        print("❌ Upload failed:\n" + r.stdout + r.stderr)
        return 1
    print("✅ Published. The website loads it next time it starts, or click "
          "'📥 Load latest data snapshot' in its ADMIN panel.")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    init_db()
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "export":
        print(f"Exported {export_snapshot():,} sales to {SNAPSHOT_PATH}")
    elif cmd == "publish":
        sys.exit(publish())
    elif cmd == "load":
        print(load_latest()[1])
    else:
        print(__doc__)
