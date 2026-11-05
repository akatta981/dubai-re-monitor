"""
check_data.py — Data health check + price summary. Read-only: never changes the database.

Prints a short report:
  1. Overview   — how many sales, which dates, real vs demo, off-plan vs ready
  2. Coverage   — per area: date range and sales per day (spots cut-short downloads)
  3. Summary    — per area: 2025 vs since 1 March 2026 (median price per m²)
  4. Monthly    — month-by-month median price per m² from 1 March 2026
  5. Segments   — whole market vs under-cap, ready vs off-plan (explains "mix" effects)
  6. Oddities   — suspicious sizes/prices that can distort averages

Uses the MEDIAN price per m² (the middle sale) — one unusual sale can't swing it
the way it swings an average.

Run:  python check_data.py
      python check_data.py --baseline-year 2025 --trend-start 2026-03-01
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

import pandas as pd

import config
from db import MOCK_ID_PREFIX, Transaction, get_session, init_db

logging.disable(logging.CRITICAL)


def load() -> pd.DataFrame:
    with get_session() as s:
        rows = s.query(
            Transaction.transaction_id, Transaction.transaction_date, Transaction.actual_worth,
            Transaction.procedure_area, Transaction.price_per_sqm, Transaction.area_canonical,
            Transaction.prop_type, Transaction.trans_group, Transaction.property_usage,
            Transaction.is_offplan,
        ).all()
    df = pd.DataFrame(rows, columns=["id", "date", "worth", "sqm", "psqm", "area", "type",
                                     "group", "usage", "offplan"])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df["is_mock"] = df["id"].fillna("").str.startswith(MOCK_ID_PREFIX)
        df["offplan"] = df["offplan"].fillna("Unknown")
    return df


def _pct(new: float, old: float) -> str:
    if pd.isna(new) or pd.isna(old) or not old:
        return "     —"
    return f"{(new / old - 1) * 100:+6.1f}%"


def _num(v: float) -> str:
    return "     —" if pd.isna(v) else f"{v:>6,.0f}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline-year", type=int, default=2025)
    ap.add_argument("--trend-start", default="2026-03-01")
    args = ap.parse_args()
    base_year = args.baseline_year
    t0 = pd.Timestamp(args.trend_start)

    init_db()
    df = load()
    print("=" * 96)
    print(" DUBAI RE MONITOR — DATA CHECK & PRICE SUMMARY  (read-only)")
    print("=" * 96)
    if df.empty:
        print("Database is empty — run PURGE & SYNC (or: python data_fetcher.py --history) first.")
        return

    # ── 1. Overview ──────────────────────────────────────────────────────────
    real = df[~df["is_mock"]]
    print("\n1) OVERVIEW")
    print(f"   Sales stored: {len(df):,}   real DLD: {len(real):,}   demo: {int(df['is_mock'].sum()):,}"
          + ("   ⚠ DEMO DATA PRESENT — numbers are not real" if df["is_mock"].any() else ""))
    print(f"   Dates: {df['date'].min():%d %b %Y} → {df['date'].max():%d %b %Y}   (today {date.today():%d %b %Y})")
    n_base = int((df["date"].dt.year == base_year).sum())
    n_trend = int((df["date"] >= t0).sum())
    print(f"   Sales in {base_year}: {n_base:,}   since {t0:%d %b %Y}: {n_trend:,}")
    if n_base == 0:
        print(f"   ⚠ No {base_year} sales stored — run:  python data_fetcher.py --history")
    if df["date"].min() > t0:
        print(f"   ⚠ Data starts after {t0:%d %b %Y} — run:  python data_fetcher.py --history")
    print(f"   Off-plan vs ready: {dict(df['offplan'].value_counts())}")
    print(f"   Property types   : {dict(df['type'].fillna('?').value_counts().head(6))}")
    print(f"   Price cap (analysis): AED {config.MIN_PRICE_AED:,}–{config.MAX_PRICE_AED:,}; "
          f"sales above cap stored but excluded: {int((df['worth'] > config.MAX_PRICE_AED).sum()):,}")

    cap = df[(df["worth"] >= config.MIN_PRICE_AED) & (df["worth"] <= config.MAX_PRICE_AED)
             & df["psqm"].notna()].copy()
    cap["month"] = cap["date"].dt.to_period("M")

    # ── 2. Coverage per area ─────────────────────────────────────────────────
    print(f"\n2) COVERAGE PER AREA  (under AED {config.MAX_PRICE_AED / 1e6:.0f}M)")
    cov = cap.groupby("area").agg(sales=("worth", "size"), first=("date", "min"),
                                  last=("date", "max"), days=("date", "nunique"))
    cov["per_day"] = (cov["sales"] / cov["days"]).round(1)
    cov["first"] = cov["first"].dt.strftime("%d %b %y")
    cov["last"] = cov["last"].dt.strftime("%d %b %y")
    print("   " + cov.to_string().replace("\n", "\n   "))
    missing = [a for a in config.MONITORED_AREAS if a not in cov.index]
    if missing:
        print(f"   ⚠ No sales at all for: {', '.join(missing)}  (area name not recognised?)")

    # ── 3. Summary per area ──────────────────────────────────────────────────
    last_full = (pd.Timestamp(date.today()).to_period("M") - 1)
    print(f"\n3) SUMMARY — median AED/m², under AED {config.MAX_PRICE_AED / 1e6:.0f}M")
    print(f"   {'Area':24} {str(base_year):>9} {'since '+t0.strftime('%d%b'):>12} "
          f"{t0.strftime('%b %y'):>8} {last_full.strftime('%b %y'):>8} {'vs '+str(base_year):>8} "
          f"{t0.strftime('%b')+'→'+last_full.strftime('%b'):>9}   sales")
    rows = []
    for area, g in cap.groupby("area"):
        b = g[g["date"].dt.year == base_year]["psqm"].median()
        since = g[g["date"] >= t0]["psqm"].median()
        first_m = g[g["month"] == t0.to_period("M")]["psqm"].median()
        last_m = g[g["month"] == last_full]["psqm"].median()
        rows.append((area, b, since, first_m, last_m, len(g[g["date"] >= t0])))
    allb = cap[cap["date"].dt.year == base_year]["psqm"].median()
    rows.append(("ALL AREAS", allb, cap[cap["date"] >= t0]["psqm"].median(),
                 cap[cap["month"] == t0.to_period("M")]["psqm"].median(),
                 cap[cap["month"] == last_full]["psqm"].median(), int((cap["date"] >= t0).sum())))
    for area, b, since, fm, lm, n in rows:
        print(f"   {area[:24]:24} {_num(b):>9} {_num(since):>12} {_num(fm):>8} {_num(lm):>8} "
              f"{_pct(since, b):>8} {_pct(lm, fm):>9}   {n:,}")
    print(f"   (vs {base_year} = since-{t0:%d %b} median vs {base_year} median;  "
          f"{t0:%b}→{last_full.strftime('%b')} = last full month vs {t0:%B})")

    # ── 4. Monthly from trend start ──────────────────────────────────────────
    m = cap[cap["date"] >= t0]
    if not m.empty:
        print(f"\n4) MONTHLY MEDIAN AED/m² FROM {t0:%d %b %Y}  (under AED {config.MAX_PRICE_AED / 1e6:.0f}M; "
              f"current month is partial)")
        tab = m.groupby(["area", "month"])["psqm"].median().unstack()
        tab.loc["ALL AREAS"] = m.groupby("month")["psqm"].median()
        cols = list(tab.columns)
        print(f"   {'Area':24}" + "".join(f"{c.strftime('%b%y'):>8}" for c in cols))
        for area, r in tab.iterrows():
            print(f"   {area[:24]:24}" + "".join(f"{_num(v):>8}" for v in r))
        cnt = m.groupby("month").size()
        print(f"   {'sales/month':24}" + "".join(f"{int(cnt.get(c, 0)):>8}" for c in cols))

    # ── 5. Segments (mix effects) ────────────────────────────────────────────
    allp = df[(df["worth"] >= config.MIN_PRICE_AED) & df["psqm"].notna()].copy()
    allp["month"] = allp["date"].dt.to_period("M")
    seg = allp[allp["date"] >= t0]
    if not seg.empty:
        print(f"\n5) SEGMENTS — monthly median AED/m², all areas combined, from {t0:%b %Y}")
        print("   A rising average can hide falling prices when the MIX changes (e.g. more")
        print("   pricey off-plan launches, or the price cap letting in different units).")
        cols = sorted(seg["month"].unique())
        print(f"   {'Segment':28}" + "".join(f"{c.strftime('%b%y'):>8}" for c in cols)
              + f"{'  '+str(base_year):>9}")

        def line(label: str, d: pd.DataFrame, base: pd.DataFrame) -> None:
            med = d.groupby("month")["psqm"].median()
            print(f"   {label:28}" + "".join(f"{_num(med.get(c)):>8}" for c in cols)
                  + f"{_num(base['psqm'].median()):>9}")

        b_all = allp[allp["date"].dt.year == base_year]
        in_cap = lambda d: d[d["worth"] <= config.MAX_PRICE_AED]
        line("All prices (whole market)", seg, b_all)
        line(f"Under AED {config.MAX_PRICE_AED / 1e6:.0f}M", in_cap(seg), in_cap(b_all))
        for lab in ("Ready", "Off-plan"):
            line(f"Under {config.MAX_PRICE_AED / 1e6:.0f}M — {lab}",
                 in_cap(seg[seg["offplan"] == lab]), in_cap(b_all[b_all["offplan"] == lab]))
        share = in_cap(seg).groupby("month")["offplan"].apply(lambda s: (s == "Off-plan").mean() * 100)
        print(f"   {'Off-plan share % (<cap)':28}" + "".join(f"{share.get(c, float('nan')):>7.0f}%" for c in cols))
        if (seg["offplan"] == "Unknown").all():
            print("   (off-plan/ready unknown for these sales — run: python data_fetcher.py --history)")

    # ── 6. Oddities ──────────────────────────────────────────────────────────
    print("\n6) ODDITIES THAT CAN DISTORT AVERAGES")
    print(f"   price/m² < AED 2,000: {int((cap['psqm'] < 2_000).sum())}   "
          f"> AED 80,000: {int((cap['psqm'] > 80_000).sum())}   "
          f"size < 20 m²: {int((cap['sqm'].fillna(0) < 20).sum())}   "
          f"land plots: {int(cap['type'].fillna('').str.lower().str.contains('land').sum())}")
    print("\nDone. Copy everything above and paste it to Claude.")


if __name__ == "__main__":
    main()
