"""
market_intelligence.py — Expert Dubai RE buy price recommendation engine.

Combines five independent factors into a single recommended buy price per m²:
  1. DLD signal discount        — active anomaly in this area?
  2. YoY price momentum         — are prices rising or softening vs 2025?
  3. Supply/demand pressure     — Bayut listings vs daily transaction velocity
  4. Area strategic premium     — long-term demand, supply pipeline, infrastructure catalysts
  5. Macro & geopolitical       — current Dubai market environment (March 2026)

Update MACRO_FACTORS and AREA_INTEL when conditions change materially.
All factor weights are designed to keep total discount in a realistic 3–20% range.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


# ─── 1. Macro & Geopolitical Context (March 2026) ────────────────────────────
# Each entry: (pct_points_effect_on_discount, reasoning)
# Positive  → MORE discount for buyer (buyer-friendly factor)
# Negative  → LESS discount for buyer (seller-friendly / price-supportive factor)

MACRO_FACTORS: dict[str, tuple[float, str]] = {
    "fed_rates_elevated": (
        +1.5,
        "US Fed funds rate ~4.5% → elevated global mortgage costs suppress leveraged buyers; "
        "cash buyers gain unusual negotiating power relative to 2021–22 era.",
    ),
    "global_trade_tensions": (
        +1.0,
        "Trump tariff cycle and US-China tensions creating hesitation in some corporate relocation "
        "pipelines; a subset of expat demand has been delayed into H2 2026.",
    ),
    "mena_risk_premium": (
        +0.5,
        "Post-ceasefire relative stability in MENA, but elevated risk premiums persist vs 2022; "
        "some HNWIs are pricing in tail risk when committing to illiquid assets.",
    ),
    "usd_peg_stability": (
        -1.0,
        "AED–USD peg eliminates currency risk for dollar-denominated buyers; "
        "this structural advantage supports a premium vs comparable EM markets.",
    ),
    "population_growth": (
        -0.5,
        "UAE population growing ~8% YoY driven by Golden Visa and talent visa uptake; "
        "structural demand outpacing new residential completions in premium segments.",
    ),
    "golden_visa_demand": (
        -0.5,
        "Golden Visa (property ≥ AED 2M) and retirement visa programmes driving "
        "long-horizon owner-occupier demand; these buyers are price-insensitive.",
    ),
    "cepa_expat_inflow": (
        -0.5,
        "UAE–India and UAE–UK Comprehensive Economic Partnership Agreements "
        "accelerating skilled professional inflow — key renter and buyer demographic.",
    ),
    "oil_above_80": (
        -0.5,
        "Brent crude above $80/barrel keeps Gulf sovereign wealth funds intact; "
        "regional HNWIs and family offices maintain Dubai real estate allocations.",
    ),
    "amia_expansion": (
        -0.5,
        "Al Maktoum International Airport (AMIA) mega-expansion approved and underway; "
        "will become world's largest airport. Structural long-term demand catalyst for "
        "Dubai South / DIP corridor.",
    ),
}

# Net macro effect (positive = buyer has macro tailwind)
MACRO_ADJ_PCT: float = sum(v for v, _ in MACRO_FACTORS.values())  # ≈ +0.5

MACRO_SUMMARY: str = (
    "Dubai's structural demand story (Golden Visa, population growth, USD-peg, Expo legacy, AMIA) "
    "remains intact and price-supportive. However, elevated global interest rates mean the pool "
    "of leveraged buyers has shrunk — giving cash buyers meaningful negotiating power they "
    "didn't have in 2021–22. Net: slight buyer's advantage in most segments, strongest in "
    "high-supply areas (JVC, Business Bay) and weakest in supply-constrained luxury (Palm, Downtown)."
)


# ─── 2. Area Strategic Intelligence ──────────────────────────────────────────

@dataclass
class AreaIntel:
    demand_score: int          # 1–10: structural long-term demand strength
    supply_score: int          # 1–10: 10 = tight supply (sellers hold firm), 1 = glut
    rental_yield_pct: float    # Typical gross rental yield %
    strategic_adj_pct: float   # ± pct points added to buyer's discount
                               # Positive = more leverage (supply/demand favours buyer)
                               # Negative = less leverage (sellers hold firm)
    catalyst_note: str         # Key near-term catalyst (positive or risk)
    five_yr_outlook: str       # Capital growth view


AREA_INTEL: dict[str, AreaIntel] = {
    "Downtown Dubai": AreaIntel(
        demand_score=9,
        supply_score=8,
        rental_yield_pct=5.5,
        strategic_adj_pct=-1.5,
        catalyst_note=(
            "Emaar has structurally undersupplied Downtown since 2020 — no major new residential "
            "tower launched since Address Residences Zabeel. Luxury HNWI demand from Indian, British, "
            "and Russian HNWIs is structural. Sellers here rarely accept >5% below asking on "
            "good-condition units. Best leverage point: units needing refurbishment."
        ),
        five_yr_outlook=(
            "Moderate capital growth (5–8% p.a.); primarily a wealth-preservation and "
            "yield play rather than high-growth. Burj Khalifa proximity commands permanent premium."
        ),
    ),
    "Palm Jumeirah": AreaIntel(
        demand_score=9,
        supply_score=9,
        rental_yield_pct=5.0,
        strategic_adj_pct=-2.0,
        catalyst_note=(
            "Supply is physically capped by island geography — no new land available. "
            "Ultra-HNWI demand from Russia, India, UK, and China remains resilient even in "
            "downturns. Royal Atlantis Residences and ORLA Dorchester are setting new price floors. "
            "Accept near-ask on desirable units — good ones sell in days. "
            "Best opportunity: aged units in older towers needing full fit-out."
        ),
        five_yr_outlook=(
            "Strongest long-term store of value in Dubai (8–12% p.a. capital growth). "
            "Limited supply + global brand = permanently elevated pricing."
        ),
    ),
    "Dubai Marina": AreaIntel(
        demand_score=7,
        supply_score=5,
        rental_yield_pct=6.5,
        strategic_adj_pct=+1.0,
        catalyst_note=(
            "Significant high-rise pipeline from Select Group, Cayan, and emerging developers "
            "adjacent to JBR continues to add supply pressure. Rental demand from expats "
            "in TECOM, Media City, and Internet City is strong, keeping yields healthy. "
            "Good negotiating leverage on interior-facing units and older towers; "
            "sea and canal views still command premiums where sellers hold firm."
        ),
        five_yr_outlook=(
            "Modest capital growth (3–5% p.a.); better as a rental income play. "
            "Yield advantage over Downtown/Palm makes up for slower price appreciation."
        ),
    ),
    "JVC/JVT": AreaIntel(
        demand_score=7,
        supply_score=3,
        rental_yield_pct=8.5,
        strategic_adj_pct=+2.5,
        catalyst_note=(
            "Highest new-supply pipeline in all of Dubai — Binghatti, Ellington, Samana, "
            "and dozens of boutique developers are all delivering 2025–2027. "
            "Market is dominated by yield-seeking investors, not owner-occupiers, "
            "so sellers know buyers have abundant choices. "
            "MAXIMUM negotiating leverage here — shortlist 5+ options and let sellers compete. "
            "Avoid studios in towers with high investor concentration."
        ),
        five_yr_outlook=(
            "Lower capital growth (2–4% p.a.) due to supply overhang, but exceptional "
            "rental yields (8–9%). Best for investors prioritising cash flow over appreciation."
        ),
    ),
    "Business Bay": AreaIntel(
        demand_score=7,
        supply_score=5,
        rental_yield_pct=7.0,
        strategic_adj_pct=+1.0,
        catalyst_note=(
            "Two-tier market: canal-facing towers (Aykon City, Canal Crown, Canal Heights) "
            "command strong premiums; interior towers face real supply pressure. "
            "Strong professional rental demand from DIFC and SZR proximity. "
            "Aggressive negotiating possible on non-waterfront units in the 2016–2020 vintage; "
            "less room on canal-facing units or Binghatti's newer branded stock."
        ),
        five_yr_outlook=(
            "Moderate capital growth (4–6% p.a.); strong rental yields in mid-tier. "
            "Canal-facing units outperform; interior units likely lag."
        ),
    ),
    "Arabian Ranches": AreaIntel(
        demand_score=8,
        supply_score=8,
        rental_yield_pct=5.5,
        strategic_adj_pct=-1.0,
        catalyst_note=(
            "Emaar's most family-oriented master community. Organic demand from families "
            "relocating from apartments; very low resale supply means sellers hold firm. "
            "Arabian Ranches III Phase 2 adds only limited new units. "
            "Best entry: direct developer for off-plan in Phase III phases; "
            "resale market has minimal negotiating room."
        ),
        five_yr_outlook=(
            "Good capital growth (6–8% p.a.). Undersupplied family villa/townhouse segment "
            "with no credible substitute in comparable price range."
        ),
    ),
    "Dubai Hills": AreaIntel(
        demand_score=8,
        supply_score=6,
        rental_yield_pct=6.0,
        strategic_adj_pct=0.0,
        catalyst_note=(
            "Dubai Hills Mall fully operational, driving footfall and residential desirability. "
            "Park Heights II handover in 2026 adds apartment supply — more negotiating room on "
            "apartments than villas. Golf course-facing units at premium with limited room to "
            "negotiate; inland apartments more flexible. Good school infrastructure "
            "(GEMS schools nearby) makes it appealing to family buyers."
        ),
        five_yr_outlook=(
            "Good growth (5–7% p.a.) for villas; apartments moderate (3–5% p.a.). "
            "Emaar brand quality provides downside protection."
        ),
    ),
    "Dubai Investment Park 1": AreaIntel(
        demand_score=6,
        supply_score=6,
        rental_yield_pct=7.5,
        strategic_adj_pct=+0.5,
        catalyst_note=(
            "Established residential community: Green Community villas, Ritaj apartments, "
            "The Sustainable City. Close to Ibn Battuta Mall, Jebel Ali Port employment hub, "
            "and Expo City Dubai (15 min). Al Maktoum International Airport (AMIA) expansion "
            "is the transformative catalyst — but timeline is 5–7 years. "
            "Buy now for long-term appreciation; expect modest near-term growth."
        ),
        five_yr_outlook=(
            "Significant upside (8–12% p.a.) if AMIA Phase 1 delivers on schedule by 2030. "
            "One of the best risk-adjusted bets for patient investors under AED 2M."
        ),
    ),
    "Dubai Investment Park 2": AreaIntel(
        demand_score=7,
        supply_score=5,
        rental_yield_pct=7.5,
        strategic_adj_pct=-0.5,
        catalyst_note=(
            "DAMAC Riverside is transforming DIP 2 into a premium branded community, "
            "bringing a new buyer demographic and raising area profile. "
            "Direct beneficiary of Al Maktoum International Airport expansion — "
            "world's largest airport when complete, ~10 min drive. "
            "Expo City Dubai is 10 min away, attracting commercial occupiers. "
            "DAMAC Riverside apartments (AED 900K–1.4M range) are well-priced off-plan; "
            "buy while pre-handover pricing holds."
        ),
        five_yr_outlook=(
            "Highest upside of all 10 monitored areas (10–15% p.a.) if AMIA timeline holds. "
            "Best risk-adjusted opportunity under AED 3M for the 2026–2031 horizon."
        ),
    ),
    "Dubai Maritime City": AreaIntel(
        demand_score=7,
        supply_score=4,
        rental_yield_pct=6.5,
        strategic_adj_pct=-1.0,
        catalyst_note=(
            "One of Dubai's last remaining prime waterfront development zones — "
            "only ~7% of Dubai coastline left for development. "
            "Chelsea Residences by DAMAC (Chelsea FC-branded, Gensler-designed) "
            "is driving global attention and brand premium to the area. "
            "Located between Port Rashid and Dubai Dry Docks; pedestrian waterfront "
            "masterplan underway. 22 min to Downtown, 15 min to City Walk. "
            "Early mover advantage — resale market is thin, creating potential "
            "for outsized appreciation once more units transfer on DLD."
        ),
        five_yr_outlook=(
            "High conviction long-term hold (8–12% p.a. appreciation potential). "
            "Supply is structurally capped by geography. Brand-name projects "
            "(Chelsea FC, Fairmont) attract HNWI buyers and support pricing floors. "
            "Watch for DLD resale data post-2027 handovers to confirm thesis."
        ),
    ),
}


# ─── 3. Signal Base Discounts ─────────────────────────────────────────────────

SIGNAL_BASE_DISCOUNT: dict[Optional[str], float] = {
    "STRONG_BUY":   15.0,  # Both volume AND price below trend simultaneously
    "VOLUME_DROP":  10.0,  # Far fewer buyers active; demand side thinning
    "PRICE_DIP":     8.0,  # DLD prices already tracking below 30d baseline
    "SUPPLY_SURGE":  8.0,  # Excess Bayut listings; seller competition high
    None:            5.0,  # Normal market; still room to negotiate
}

SIGNAL_RATIONALE: dict[Optional[str], str] = {
    "STRONG_BUY": (
        "🚨 DLD volume AND prices are simultaneously below trend — a rare convergence. "
        "This combination historically marks the best entry windows. Push 10–15% below asking "
        "and ask sellers to absorb agency fee or service charges."
    ),
    "VOLUME_DROP": (
        "📉 DLD transaction velocity is sharply below its 7-day average — "
        "far fewer competing buyers right now. Use that scarcity of competition "
        "to push 8–12% below asking price."
    ),
    "PRICE_DIP": (
        "💰 DLD-registered sale prices have dipped below their 30-day baseline. "
        "The data supports a lower offer — pull recent DLD comparables and "
        "show your broker the trend when negotiating."
    ),
    "SUPPLY_SURGE": (
        "🏗️ Bayut active listings have spiked above the 7-day moving average. "
        "More options = more leverage. Shortlist 3–4 properties in this area "
        "so sellers know you're comparing — that alone gives negotiating power."
    ),
    None: (
        "📊 No active market anomaly detected. Standard negotiating position: "
        "5% below asking is a reasonable opening position in a normal market."
    ),
}


# ─── 4. Core Recommendation Function ─────────────────────────────────────────

def get_buy_recommendation(
    project: str,
    area: str,
    avg_30d: float,
    yoy_pct: Optional[float],
    listings_per_daily_vol: Optional[float],
    active_signal: Optional[str],
    n_transactions: int,
) -> dict:
    """
    Compute an expert buy price recommendation for a specific project.

    Parameters
    ----------
    project                : project name (display only)
    area                   : canonical area name (must match AREA_INTEL key)
    avg_30d                : project's 30-day DLD average price per m²
    yoy_pct                : % change in area price vs 2025 full-year avg (None if unavailable)
    listings_per_daily_vol : Bayut active listings ÷ area's average daily transaction count
                             (proxy for supply pressure; higher = more buyer leverage)
    active_signal          : strongest active anomaly signal type, or None
    n_transactions         : number of DLD transactions underpinning the 30d avg

    Returns
    -------
    dict with target_price_sqm, total_discount_pct, breakdown, and narrative fields
    """
    intel = AREA_INTEL.get(area)

    # ── Factor 1: Signal discount ─────────────────────────────────────────────
    signal_disc = SIGNAL_BASE_DISCOUNT.get(active_signal, 5.0)

    # ── Factor 2: YoY momentum ───────────────────────────────────────────────
    yoy_adj, yoy_label = 0.0, "No 2025 data available"
    if yoy_pct is not None:
        if yoy_pct > 12:
            yoy_adj, yoy_label = -2.5, f"▲{yoy_pct:.1f}% YoY — hot market, sellers have pricing power"
        elif yoy_pct > 6:
            yoy_adj, yoy_label = -1.5, f"▲{yoy_pct:.1f}% YoY — rising market, moderate price support"
        elif yoy_pct > 0:
            yoy_adj, yoy_label = -0.5, f"▲{yoy_pct:.1f}% YoY — mild appreciation, roughly balanced"
        elif yoy_pct >= -5:
            yoy_adj, yoy_label = +0.5, f"{yoy_pct:+.1f}% YoY — flat/softening, marginal buyer advantage"
        else:
            yoy_adj, yoy_label = +2.0, f"▼{abs(yoy_pct):.1f}% YoY — price correction underway, significant leverage"

    # ── Factor 3: Supply/demand pressure ─────────────────────────────────────
    supply_adj, supply_label = 0.0, "Insufficient data"
    if listings_per_daily_vol is not None:
        if listings_per_daily_vol > 350:
            supply_adj   = +2.5
            supply_label = f"{listings_per_daily_vol:.0f}x ratio — extreme oversupply, maximum leverage"
        elif listings_per_daily_vol > 200:
            supply_adj   = +1.5
            supply_label = f"{listings_per_daily_vol:.0f}x ratio — high supply pressure, good leverage"
        elif listings_per_daily_vol > 100:
            supply_adj   = +0.5
            supply_label = f"{listings_per_daily_vol:.0f}x ratio — moderately elevated supply"
        elif listings_per_daily_vol > 60:
            supply_adj   = 0.0
            supply_label = f"{listings_per_daily_vol:.0f}x ratio — balanced supply/demand"
        else:
            supply_adj   = -1.0
            supply_label = f"{listings_per_daily_vol:.0f}x ratio — tight supply, sellers hold firm"

    # ── Factor 4: Area strategic adjustment ──────────────────────────────────
    area_adj   = intel.strategic_adj_pct if intel else 0.0
    area_label = (
        f"Demand {intel.demand_score}/10 · Supply tightness {intel.supply_score}/10"
        if intel else "Area data unavailable"
    )

    # ── Factor 5: Macro adjustment ────────────────────────────────────────────
    # Scale raw macro to ±1.5% max influence (it's background context, not the driver)
    macro_adj   = round(max(-1.5, min(1.5, MACRO_ADJ_PCT * 0.4)), 2)
    macro_label = f"Net macro {'tailwind' if MACRO_ADJ_PCT >= 0 else 'headwind'} ({MACRO_ADJ_PCT:+.1f}pts raw)"

    # ── Aggregate ─────────────────────────────────────────────────────────────
    total_disc  = signal_disc + yoy_adj + supply_adj + area_adj + macro_adj
    total_disc  = round(max(2.0, min(20.0, total_disc)), 1)   # clamp 2–20%
    target_px   = avg_30d * (1.0 - total_disc / 100.0)

    # ── Confidence ────────────────────────────────────────────────────────────
    if n_transactions >= 25:
        confidence, conf_color = "High", "#4ade80"
    elif n_transactions >= 10:
        confidence, conf_color = "Medium", "#f6ad55"
    else:
        confidence, conf_color = "Low — limited data", "#fc8181"

    return {
        "project":           project,
        "area":              area,
        "target_price_sqm":  round(target_px, 0),
        "avg_30d":           round(avg_30d, 0),
        "total_discount_pct": total_disc,
        "active_signal":     active_signal,
        "signal_rationale":  SIGNAL_RATIONALE.get(active_signal, ""),
        "breakdown": {
            "Signal discount":     (signal_disc,  SIGNAL_RATIONALE.get(active_signal, "No active signal")),
            "YoY momentum":        (yoy_adj,      yoy_label),
            "Supply pressure":     (supply_adj,   supply_label),
            "Area fundamentals":   (area_adj,     area_label),
            "Macro environment":   (macro_adj,    macro_label),
        },
        "catalyst_note":     intel.catalyst_note    if intel else "",
        "five_yr_outlook":   intel.five_yr_outlook  if intel else "",
        "rental_yield_pct":  intel.rental_yield_pct if intel else None,
        "confidence":        confidence,
        "conf_color":        conf_color,
        "n_transactions":    n_transactions,
        "macro_summary":     MACRO_SUMMARY,
    }
