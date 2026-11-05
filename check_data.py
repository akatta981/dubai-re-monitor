"""
check_data.py — Data health check + price summary. Read-only: never changes the database.

Prints a short report:
  1. Overview   — how many sales, which dates, real vs demo, off-plan vs ready
  2. Coverage   — per area: date range and sales per day
  3. Summary    — per area: baseline vs since trend start (median price per m²)
  4. Resale     — the same for READY (resale) homes only — off-plan launch prices
                  can hide what the resale market is doing
  5. Monthly    — month-by-month median price per m² from trend start
  6. Segments   — whole market vs under-cap, ready vs off-plan, off-plan share
  7. Clean-up   — sales left out of the analysis (land, bad sizes/prices)
  8. Scorecard  — buy-now evidence per area (see decision.py)

Uses the MEDIAN price per m² (the middle sale) — one unusual sale can't swing it.
Baseline = the baseline year (default 2025) if it is in the database; otherwise
1 January of this year up to the day before the trend start.

Run:  python check_data.py
      python check_data.py --baseline-year 2025 --trend-start 2026-03-01
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

import pandas as pd

import config
from db import MOCK_ID_PREFIX, Transaction, get_session, init_db

logging.disable(logging.CRITICAL)


def load() -> pd.DataFrame:
    with get_session() as s:
        rows = s.query(
            Transaction.transaction_id, Transaction.transaction_date, Transaction.actual_worth,
            Transaction.procedure_area, Transaction.price_per_sqm, Transaction.area_canonical,
            Transaction.prop_type, Transaction.is_offplan,
        ).all()
    df = pd.DataFrame(rows, columns=["id", "date", "worth", "sqm", "psqm", "area", "type", "offplan"])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df["is_mock"] = df["id"].fillna("").str.startswith(MOCK_ID_PREFIX)
        df["offplan"] = df["offplan"].fillna("Unknown")
        df["month"] = df["date"].dt.to_period("M")
    return df


def clean_mask(df: pd.DataFrame, apply_cap: bool = True) -> pd.Series:
    """Same rules as db.analysis_conditions(): plausible sizes/prices, no land plots."""
    m = (
        (df["worth"] >= config.MIN_PRICE_AED)
        & df["psqm"].between(config.MIN_PRICE_SQM, config.MAX_PRICE_SQM)
        & (df["sqm"].fillna(0) >= config.MIN_SIZE_SQM)
        & ~df["type"].fillna("").str.lower().isin([t.lower() for t in config.EXCLUDED_PROP_TYPES])
    )
    if apply_cap:
        m &= df["worth"] <= config.MAX_PRICE_AED
    return m


def _pct(new: float, old: float) -> str:
    if pd.isna(new) or pd.isna(old) or not old:
        return "—"
    return f"{(new / old - 1) * 100:+.1f}%"


def _num(v: float) -> str:
    return "—" if v is None or pd.isna(v) else f"{v:,.0f}"


def area_table(d: pd.DataFrame, is_base: pd.Series, t0: pd.Timestamp, last_full: pd.Period,
               base_label: str) -> None:
    """Per-area: baseline | since t0 | t0 month | last full month | changes | sales."""
    print(f"   {'Area':24} {base_label:>10} {'since ' + t0.strftime('%d %b'):>12} "
          f"{t0.strftime('%b %y'):>8} {last_full.strftime('%b %y'):>8} "
          f"{'vs base':>8} {t0.strftime('%b') + '→' + last_full.strftime('%b'):>9} {'sales':>7}")
    groups = list(d.groupby("area")) + [("ALL AREAS", d)]
    for area, g in groups:
        gb = g[is_base.reindex(g.index, fill_value=False)]
        since = g[g["date"] >= t0]
        b = gb["psqm"].median()
        s = since["psqm"].median()
        fm = g[g["month"] == t0.to_period("M")]["psqm"].median()
        lm = g[g["month"] == last_full]["psqm"].median()
        print(f"   {str(area)[:24]:24} {_num(b):>10} {_num(s):>12} {_num(fm):>8} {_num(lm):>8} "
              f"{_pct(s, b):>8} {_pct(lm, fm):>9} {len(since):>7,}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline-year", type=int, default=2025)
    ap.add_argument("--trend-start", default="2026-03-01")
    args = ap.parse_args()
    t0 = pd.Timestamp(args.trend_start)
    last_full = pd.Timestamp(date.today()).to_period("M") - 1

    init_db()
    df = load()
    print("=" * 100)
    print(" DUBAI RE MONITOR — DATA CHECK & PRICE SUMMARY  (read-only)")
    print("=" * 100)
    if df.empty:
        print("Database is empty — run PURGE & SYNC (or: python data_fetcher.py --history) first.")
        return

    # Baseline period
    if (df["date"].dt.year == args.baseline_year).any():
        is_base = df["date"].dt.year == args.baseline_year
        base_label = str(args.baseline_year)
    else:
        b_start = pd.Timestamp(t0.year, 1, 1)
        is_base = (df["date"] >= b_start) & (df["date"] < t0)
        base_label = f"{b_start:%b}–{(t0 - pd.Timedelta(days=1)):%b %y}"

    # ── 1. Overview ──────────────────────────────────────────────────────────
    print("\n1) OVERVIEW")
    print(f"   Sales stored: {len(df):,}   real DLD: {int((~df['is_mock']).sum()):,}   "
          f"demo: {int(df['is_mock'].sum()):,}"
          + ("   ⚠ DEMO DATA PRESENT — numbers are not real" if df["is_mock"].any() else ""))
    print(f"   Dates: {df['date'].min():%d %b %Y} → {df['date'].max():%d %b %Y}   (today {date.today():%d %b %Y})")
    if base_label != str(args.baseline_year):
        print(f"   No {args.baseline_year} sales available → baseline = {base_label} "
              f"({int(is_base.sum()):,} sales)")
    print(f"   Off-plan vs ready: {dict(df['offplan'].value_counts())}")
    print(f"   Property types   : {dict(df['type'].fillna('?').value_counts().head(6))}")

    cap_m = clean_mask(df)
    d = df[cap_m]
    print(f"   Analysed (clean, AED {config.MIN_PRICE_AED / 1e3:.0f}k–{config.MAX_PRICE_AED / 1e6:.0f}M): "
          f"{len(d):,}   above cap: {int((df['worth'] > config.MAX_PRICE_AED).sum()):,}   "
          f"cleaned out: {int((~clean_mask(df, apply_cap=False)).sum()):,}")

    # ── 2. Coverage ──────────────────────────────────────────────────────────
    print(f"\n2) COVERAGE PER AREA  (analysed sales)")
    cov = d.groupby("area").agg(sales=("worth", "size"), first=("date", "min"),
                                last=("date", "max"), days=("date", "nunique"))
    cov["per_day"] = (cov["sales"] / cov["days"]).round(1)
    cov["first"] = cov["first"].dt.strftime("%d %b %y")
    cov["last"] = cov["last"].dt.strftime("%d %b %y")
    print("   " + cov.to_string().replace("\n", "\n   "))
    missing = [a for a in config.MONITORED_AREAS if a not in cov.index]
    if missing:
        print(f"   ⚠ No sales for: {', '.join(missing)}  (DLD uses a different area name?)")

    # ── 3. Summary — all sales ───────────────────────────────────────────────
    print(f"\n3) ALL SALES — median AED/m², under AED {config.MAX_PRICE_AED / 1e6:.0f}M")
    area_table(d, is_base, t0, last_full, base_label)

    # ── 4. Summary — ready (resale) only ─────────────────────────────────────
    ready = d[d["offplan"] == "Ready"]
    print(f"\n4) READY / RESALE ONLY — median AED/m², under AED {config.MAX_PRICE_AED / 1e6:.0f}M")
    if ready.empty:
        print("   (no sales marked Ready — run: python data_fetcher.py --history)")
    else:
        area_table(ready, is_base, t0, last_full, base_label)
    print(f"   (vs base = since {t0:%d %b} vs {base_label};  "
          f"{t0:%b}→{last_full.strftime('%b')} = last full month vs {t0:%B}; "
          "areas with few sales jump around)")

    # ── 5. Monthly ───────────────────────────────────────────────────────────
    m = d[d["date"] >= t0]
    if not m.empty:
        print(f"\n5) MONTHLY MEDIAN AED/m² FROM {t0:%d %b %Y}  (all sales under cap; current month partial)")
        tab = m.groupby(["area", "month"])["psqm"].median().unstack()
        tab.loc["ALL AREAS"] = m.groupby("month")["psqm"].median()
        cols = list(tab.columns)
        print(f"   {'Area':24}" + "".join(f"{c.strftime('%b%y'):>8}" for c in cols))
        for area, r in tab.iterrows():
            print(f"   {str(area)[:24]:24}" + "".join(f"{_num(v):>8}" for v in r))
        cnt = m.groupby("month").size()
        print(f"   {'sales/month':24}" + "".join(f"{int(cnt.get(c, 0)):>8}" for c in cols))

    # ── 6. Segments ──────────────────────────────────────────────────────────
    allp = df[clean_mask(df, apply_cap=False)]
    seg = allp[allp["date"] >= t0]
    if not seg.empty:
        print(f"\n6) SEGMENTS — monthly median AED/m², all areas combined")
        print("   Off-plan prices are set by developers; READY prices show the resale market.")
        cols = sorted(seg["month"].unique())
        print(f"   {'Segment':28}{base_label:>10}" + "".join(f"{c.strftime('%b%y'):>8}" for c in cols))
        base_all = allp[is_base.reindex(allp.index, fill_value=False)]
        capped = lambda x: x[x["worth"] <= config.MAX_PRICE_AED]

        def line(label: str, cur: pd.DataFrame, base: pd.DataFrame) -> None:
            med = cur.groupby("month")["psqm"].median()
            print(f"   {label:28}{_num(base['psqm'].median()):>10}"
                  + "".join(f"{_num(med.get(c)):>8}" for c in cols))

        line("All prices (whole market)", seg, base_all)
        line(f"Under AED {config.MAX_PRICE_AED / 1e6:.0f}M", capped(seg), capped(base_all))
        for lab in ("Ready", "Off-plan"):
            line(f"Under {config.MAX_PRICE_AED / 1e6:.0f}M — {lab}",
                 capped(seg[seg["offplan"] == lab]), capped(base_all[base_all["offplan"] == lab]))
        known = capped(seg[seg["offplan"] != "Unknown"])
        share = known.groupby("month")["offplan"].apply(lambda s: (s == "Off-plan").mean() * 100)
        print(f"   {'Off-plan share (known)':28}{'':>10}"
              + "".join(f"{share.get(c):>7.0f}%" if pd.notna(share.get(c)) else f"{'—':>8}" for c in cols))
        unknown = capped(seg)[capped(seg)["offplan"] == "Unknown"]
        if len(unknown):
            print(f"   ({len(unknown):,} sales with unknown off-plan/ready — fill in with: "
                  f"python data_fetcher.py --since {unknown['date'].min():%Y-%m-%d})")

    # ── 7. Clean-up ──────────────────────────────────────────────────────────
    print("\n7) LEFT OUT OF THE ANALYSIS (still stored)")
    print(f"   land plots: {int(df['type'].fillna('').str.lower().isin(config.EXCLUDED_PROP_TYPES).sum())}   "
          f"price/m² < {config.MIN_PRICE_SQM:,}: {int((df['psqm'] < config.MIN_PRICE_SQM).sum())}   "
          f"price/m² > {config.MAX_PRICE_SQM:,}: {int((df['psqm'] > config.MAX_PRICE_SQM).sum())}   "
          f"size < {config.MIN_SIZE_SQM:.0f} m²: {int((df['sqm'].fillna(0) < config.MIN_SIZE_SQM).sum())}")
    # ── 8. Buy-decision scorecard ────────────────────────────────────────────
    print("\n8)", end="")
    from decision import print_scorecard
    print_scorecard()
    print("\nDone. Copy everything above and paste it to Claude.")


if __name__ == "__main__":
    main()
