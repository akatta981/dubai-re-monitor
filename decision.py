"""
decision.py — "Should I buy now?" evidence scorecard, per area.

Reads stored DLD sales (cleaned by db.analysis_conditions: under the AED cap,
no land plots or implausible records) and compares the LAST 3 FULL MONTHS
with the 3 MONTHS BEFORE:

  1. Resale price trend   — ready-home median AED/m²: falling, levelling, rising?
  2. Fall from the high   — resale now vs the highest month this year
  3. Buyer activity       — number of sales: buyers returning or leaving?
  4. Off-plan premium     — off-plan vs resale median AED/m² right now
  5. Supply ahead         — off-plan share of sales (future handovers)
  6. Confidence           — enough resale sales for the numbers to mean anything?

The "lean" is a summary of the evidence on TIMING only. It is not financial
advice and ignores personal factors (budget, financing, plans).
All thresholds live in config.py (DECISION_*).
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd

import config
from db import Transaction, analysis_conditions, get_session

ALL = "ALL AREAS"


def load_sales() -> pd.DataFrame:
    """Analysed (clean, under-cap) sales with date, area, price/m² and off-plan flag."""
    with get_session() as s:
        rows = (
            s.query(Transaction.transaction_date, Transaction.area_canonical,
                    Transaction.price_per_sqm, Transaction.is_offplan)
            .filter(Transaction.area_canonical.isnot(None), *analysis_conditions())
            .all()
        )
    df = pd.DataFrame(rows, columns=["date", "area", "psqm", "offplan"])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df["month"] = df["date"].dt.to_period("M")
        df["offplan"] = df["offplan"].fillna("Unknown")
    return df


def _pct(new: Optional[float], old: Optional[float]) -> Optional[float]:
    if new is None or old is None or pd.isna(new) or pd.isna(old) or not old:
        return None
    return (new / old - 1) * 100


def _median(d: pd.DataFrame) -> Optional[float]:
    return float(d["psqm"].median()) if len(d) else None


def _area_row(g: pd.DataFrame, recent: list, prior: list, prior2: list) -> dict:
    ready = g[g["offplan"] == "Ready"]
    off = g[g["offplan"] == "Off-plan"]
    in_ = lambda d, months: d[d["month"].isin(months)]

    r_recent, r_prior, r_prior2 = (_median(in_(ready, m)) for m in (recent, prior, prior2))
    resale_chg = _pct(r_recent, r_prior)
    resale_chg_before = _pct(r_prior, r_prior2)

    # Highest monthly resale median this year (months with at least 10 resale sales)
    year_ready = ready[ready["date"].dt.year == recent[-1].year]
    monthly = year_ready.groupby("month")["psqm"].agg(["median", "size"])
    monthly = monthly[monthly["size"] >= 10]
    peak = float(monthly["median"].max()) if len(monthly) else None
    from_peak = _pct(r_recent, peak)

    n_recent, n_prior = len(in_(g, recent)), len(in_(g, prior))
    activity_chg = _pct(n_recent, n_prior) if n_prior else None

    o_recent = _median(in_(off, recent))
    premium = _pct(o_recent, r_recent)
    known = in_(g, recent)
    known = known[known["offplan"] != "Unknown"]
    offplan_share = (known["offplan"] == "Off-plan").mean() * 100 if len(known) else None

    n_ready = len(in_(ready, recent))
    if n_ready >= config.DECISION_GOOD_READY_SALES:
        confidence = "High"
    elif n_ready >= config.DECISION_MIN_READY_SALES:
        confidence = "Medium"
    else:
        confidence = "Low"

    lean, reasons = _lean(resale_chg, resale_chg_before, from_peak, activity_chg,
                          premium, offplan_share, confidence)
    return {
        "resale_now": r_recent, "resale_3m_chg": resale_chg, "resale_prev_3m_chg": resale_chg_before,
        "from_peak": from_peak, "activity_chg": activity_chg, "sales_3m": n_recent,
        "offplan_now": o_recent, "offplan_premium": premium, "offplan_share": offplan_share,
        "ready_sales_3m": n_ready, "confidence": confidence, "lean": lean, "why": reasons,
    }


def _lean(chg, chg_before, from_peak, activity, premium, share, confidence) -> tuple[str, list[str]]:
    """Turn the indicators into a plain-language timing lean + reasons."""
    why: list[str] = []
    if confidence == "Low" or chg is None:
        return "⚪ Not enough data", ["Too few resale sales in the last 3 months to judge the trend."]

    falling = chg <= config.DECISION_FALLING_PCT
    rising = chg >= config.DECISION_RISING_PCT
    slowing = falling and chg_before is not None and chg_before < chg
    deep = from_peak is not None and from_peak <= config.DECISION_DEEP_DROP_PCT
    buyers_back = activity is not None and activity > 5

    if falling:
        why.append(f"Resale prices fell {abs(chg):.1f}% over the last 3 months.")
        if slowing:
            why.append(f"The fall is slowing (it was {chg_before:+.1f}% in the 3 months before).")
        if slowing and buyers_back:
            lean = "🟡 Window may be opening"
        else:
            lean = "🔴 Wait — still falling"
    elif rising:
        why.append(f"Resale prices rose {chg:.1f}% over the last 3 months.")
        lean = "🟢 Recovering from a dip" if deep else "🟠 Rising — little discount left"
    else:
        why.append(f"Resale prices are roughly flat ({chg:+.1f}% over 3 months).")
        lean = "🟢 Levelling off after a fall" if deep else "🟠 Stable — no timing edge"

    if from_peak is not None:
        why.append(f"Resale is {abs(from_peak):.1f}% {'below' if from_peak < 0 else 'above'} this year's highest month.")
    if activity is not None:
        why.append(f"Buyer activity {'up' if activity >= 0 else 'down'} {abs(activity):.0f}% vs the 3 months before.")
    if premium is not None and premium >= config.DECISION_HIGH_OFFPLAN_PREMIUM:
        why.append(f"Off-plan costs {premium:.0f}% more per m² than resale — ready homes look better value.")
    if share is not None and share >= config.DECISION_HIGH_OFFPLAN_SHARE:
        why.append(f"{share:.0f}% of sales are off-plan — many handovers ahead could weigh on resale prices.")
    return lean, why


def scorecard(df: Optional[pd.DataFrame] = None, today: Optional[date] = None) -> tuple[pd.DataFrame, dict]:
    """
    Return (table, periods). One row per area plus ALL AREAS.
    periods describes the months compared, for display.
    """
    df = load_sales() if df is None else df
    today = today or date.today()
    last_full = pd.Period(today, "M") - 1
    recent = [last_full - i for i in (2, 1, 0)]
    prior = [last_full - i for i in (5, 4, 3)]
    prior2 = [last_full - i for i in (8, 7, 6)]
    periods = {"recent": recent, "prior": prior,
               "label_recent": f"{recent[0].strftime('%b')}–{recent[-1].strftime('%b %Y')}",
               "label_prior": f"{prior[0].strftime('%b')}–{prior[-1].strftime('%b %Y')}"}
    if df.empty:
        return pd.DataFrame(), periods

    rows = []
    for area, g in list(df.groupby("area")) + [(ALL, df)]:
        rows.append({"area": area, **_area_row(g, recent, prior, prior2)})
    return pd.DataFrame(rows).set_index("area"), periods


def break_even_note(years: int = 3) -> str:
    c = config.DECISION_ROUND_TRIP_COST_PCT
    return (f"Buying and later selling costs roughly {c:.0f}% in fees (DLD 4% transfer fee, agents, "
            f"registration — verify current rates). Over ~{years} years, rent plus price growth "
            f"needs to beat that before you come out ahead.")


def print_scorecard() -> None:
    table, p = scorecard()
    print(f"\nBUY-DECISION SCORECARD — last 3 full months ({p['label_recent']}) vs ({p['label_prior']})")
    print("Evidence on TIMING only — not financial advice.\n")
    if table.empty:
        print("   No data.")
        return
    f = lambda v, pct=True: "—" if v is None or pd.isna(v) else (f"{v:+.1f}%" if pct else f"{v:,.0f}")
    print(f"   {'Area':24} {'Lean':34} {'Resale 3m':>9} {'vs high':>8} {'Buyers':>7} "
          f"{'Off-plan +':>10} {'Off-plan %':>10} {'Conf':>6}")
    for area, r in table.iterrows():
        share = "—" if r["offplan_share"] is None or pd.isna(r["offplan_share"]) else f"{r['offplan_share']:.0f}%"
        print(f"   {str(area)[:24]:24} {r['lean']:34} {f(r['resale_3m_chg']):>9} {f(r['from_peak']):>8} "
              f"{f(r['activity_chg']):>7} {f(r['offplan_premium']):>10} {share:>10} {r['confidence']:>6}")
    print("\n   Why (per area):")
    for area, r in table.iterrows():
        print(f"   • {area}: " + " ".join(r["why"]))
    print("\n   " + break_even_note())


if __name__ == "__main__":
    import logging
    logging.disable(logging.CRITICAL)
    print_scorecard()
