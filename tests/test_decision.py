"""tests/test_decision.py — the buy-decision scorecard gives the right lean for known scenarios."""

from __future__ import annotations

import os
import sys
from datetime import date

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from decision import ALL, scorecard

TODAY = date(2026, 11, 5)          # last full month = Oct 2026


def _sales(area: str, ready_by_month: dict, n_ready: int = 60, offplan_psqm: float | None = None,
           n_off: int = 60) -> list[dict]:
    rows = []
    for month, psqm in ready_by_month.items():
        for i in range(n_ready):
            rows.append({"date": pd.Timestamp(f"{month}-{(i % 27) + 1:02d}"), "area": area,
                         "psqm": psqm, "offplan": "Ready"})
        if offplan_psqm:
            for i in range(n_off):
                rows.append({"date": pd.Timestamp(f"{month}-{(i % 27) + 1:02d}"), "area": area,
                             "psqm": offplan_psqm, "offplan": "Off-plan"})
    return rows


def _frame(rows):
    df = pd.DataFrame(rows)
    df["month"] = df["date"].dt.to_period("M")
    return df


MONTHS = [f"2026-{m:02d}" for m in range(1, 11)]


def test_still_falling_is_wait():
    prices = dict(zip(MONTHS, [20000, 20000, 20000, 19800, 19600, 19400, 19000, 18400, 17800, 17200]))
    table, _ = scorecard(_frame(_sales("Business Bay", prices)), today=TODAY)
    assert table.loc["Business Bay", "lean"].startswith("🔴")
    assert table.loc["Business Bay", "resale_3m_chg"] < -3


def test_flat_after_deep_drop_is_levelling():
    prices = dict(zip(MONTHS, [20000, 20000, 20000, 19000, 18000, 17000, 17000, 17000, 17100, 17000]))
    table, _ = scorecard(_frame(_sales("JVC/JVT", prices)), today=TODAY)
    assert table.loc["JVC/JVT", "lean"].startswith("🟢")
    assert table.loc["JVC/JVT", "from_peak"] <= -8


def test_falling_but_slowing_with_buyers_back_is_window():
    # Feb–Apr 20,000 → May–Jul 18,000 (−10%) → Aug–Oct 17,280 (−4%): still falling, but slower
    prices = dict(zip(MONTHS, [20000, 20000, 20000, 20000, 18000, 18000, 18000, 17280, 17280, 17280]))
    rows = _sales("Dubai Marina", {m: p for m, p in prices.items() if m < "2026-08"}, n_ready=60)
    rows += _sales("Dubai Marina", {m: p for m, p in prices.items() if m >= "2026-08"}, n_ready=90)
    table, _ = scorecard(_frame(rows), today=TODAY)
    r = table.loc["Dubai Marina"]
    assert round(r["resale_3m_chg"]) == -4 and round(r["resale_prev_3m_chg"]) == -10
    assert r["activity_chg"] > 5
    assert r["lean"].startswith("🟡")


def test_low_data_is_not_enough():
    prices = dict(zip(MONTHS, [20000] * 10))
    table, _ = scorecard(_frame(_sales("Palm Jumeirah", prices, n_ready=5)), today=TODAY)
    assert table.loc["Palm Jumeirah", "lean"].startswith("⚪")


def test_offplan_premium_and_share_reported():
    prices = dict(zip(MONTHS, [15000] * 10))
    table, _ = scorecard(_frame(_sales("Dubai South", prices, offplan_psqm=18000, n_off=180)), today=TODAY)
    r = table.loc["Dubai South"]
    assert round(r["offplan_premium"]) == 20
    assert round(r["offplan_share"]) == 75
    assert any("better value" in w for w in r["why"])
    assert ALL in table.index
