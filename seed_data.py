"""
seed_data.py — Populates the database with realistic mock DLD transaction data.
Simulates Jan–Mar 2026 with a market dip in late February (buy signal trigger).
Run once before first launch: python seed_data.py
"""

from __future__ import annotations

import random
from datetime import date, timedelta

from db import init_db, get_session, upsert_transaction, upsert_daily_metric, AnomalyLog, DailyMetric
from config import MONITORED_AREAS

random.seed(42)  # Reproducible mock data

# Baseline price/sqm ranges per area (AED) — realistic 2025/26 Dubai market
AREA_BASELINES = {
    "Downtown Dubai":          {"price_sqm": (18_000, 24_000), "daily_vol": (8, 18)},
    "Palm Jumeirah":           {"price_sqm": (22_000, 35_000), "daily_vol": (4, 10)},
    "Dubai Marina":            {"price_sqm": (13_000, 18_000), "daily_vol": (10, 22)},
    "JVC/JVT":                 {"price_sqm": (8_000,  12_000), "daily_vol": (15, 35)},
    "Business Bay":            {"price_sqm": (14_000, 20_000), "daily_vol": (8, 20)},
    "Arabian Ranches":         {"price_sqm": (7_000,  11_000), "daily_vol": (5, 12)},
    "Dubai Hills":             {"price_sqm": (9_000,  15_000), "daily_vol": (6, 16)},
    "Dubai Investment Park 1": {"price_sqm": (6_000,  10_000), "daily_vol": (3,  9)},
    "Dubai Investment Park 2": {"price_sqm": (7_000,  11_500), "daily_vol": (2,  6)},
}

# Realistic Bayut for-sale listing counts (under AED 2M) per area
AREA_BAYUT_BASELINES = {
    "Downtown Dubai":          (800,  1_200),
    "Palm Jumeirah":           (1_500, 2_200),
    "Dubai Marina":            (2_000, 3_000),
    "JVC/JVT":                 (4_000, 6_000),
    "Business Bay":            (1_800, 2_500),
    "Arabian Ranches":         (300,   600),
    "Dubai Hills":             (500,   900),
    "Dubai Investment Park 1": (500,   1_000),
    "Dubai Investment Park 2": (300,   700),
}

PROPERTY_TYPES = ["Apartment", "Apartment", "Apartment", "Villa", "Townhouse"]  # weighted

# Realistic DLD-registered projects per area
# Tuple: (master_project, project_name, building_name)
AREA_PROJECTS: dict[str, list[tuple[str, str, str]]] = {
    "Downtown Dubai": [
        ("Downtown Dubai", "Burj Khalifa",      "Burj Khalifa"),
        ("Downtown Dubai", "Forte",             "Forte 1"),
        ("Downtown Dubai", "Forte",             "Forte 2"),
        ("Downtown Dubai", "Address Boulevard", "Address Boulevard"),
        ("Downtown Dubai", "Standpoint",        "Standpoint Tower A"),
        ("Downtown Dubai", "Boulevard Point",   "Boulevard Point"),
        ("Downtown Dubai", "Act One Act Two",   "Act One"),
        ("Downtown Dubai", "Act One Act Two",   "Act Two"),
        ("Downtown Dubai", "Emaar Grande",      "Emaar Grande"),
        ("Downtown Dubai", "Vida Residences",   "Vida Residences Downtown"),
    ],
    "Palm Jumeirah": [
        ("Palm Jumeirah", "Palm Beach Towers",      "Palm Beach Tower 1"),
        ("Palm Jumeirah", "Palm Beach Towers",      "Palm Beach Tower 2"),
        ("Palm Jumeirah", "Palm Beach Towers",      "Palm Beach Tower 3"),
        ("Palm Jumeirah", "Serenia Residences",     "Serenia Residences North"),
        ("Palm Jumeirah", "One Palm",               "One Palm"),
        ("Palm Jumeirah", "ORLA",                   "ORLA Dorchester Collection"),
        ("Palm Jumeirah", "Ellington Beach House",  "Ellington Beach House"),
        ("Palm Jumeirah", "Club Vista Mare",        "Club Vista Mare"),
    ],
    "Dubai Marina": [
        ("Dubai Marina", "Marina Gate",           "Marina Gate 1"),
        ("Dubai Marina", "Marina Gate",           "Marina Gate 2"),
        ("Dubai Marina", "1 JBR",                 "1 JBR"),
        ("Dubai Marina", "Address Dubai Marina",  "Address Dubai Marina"),
        ("Dubai Marina", "Cayan Tower",           "Cayan Tower"),
        ("Dubai Marina", "Princess Tower",        "Princess Tower"),
        ("Dubai Marina", "Marina Diamonds",       "Marina Diamonds 5"),
        ("Dubai Marina", "Torch Tower",           "Torch Tower"),
    ],
    "JVC/JVT": [
        ("Jumeirah Village Circle", "Belgravia",          "Belgravia 1"),
        ("Jumeirah Village Circle", "Belgravia Heights",  "Belgravia Heights 1"),
        ("Jumeirah Village Circle", "Belgravia Heights",  "Belgravia Heights 2"),
        ("Jumeirah Village Circle", "Oxford Terraces",    "Oxford Terraces"),
        ("Jumeirah Village Circle", "Bloom Heights",      "Bloom Heights Tower A"),
        ("Jumeirah Village Circle", "Bloom Heights",      "Bloom Heights Tower B"),
        ("Jumeirah Village Circle", "Binghatti Mirage",   "Binghatti Mirage"),
        ("Jumeirah Village Circle", "Westar Vista",       "Westar Vista"),
        ("Jumeirah Village Circle", "Wavez Residence",    "Wavez Residence"),
    ],
    "Business Bay": [
        ("Business Bay", "Aykon City",          "Aykon City Tower A"),
        ("Business Bay", "Aykon City",          "Aykon City Tower B"),
        ("Business Bay", "Canal Heights",       "Canal Heights"),
        ("Business Bay", "Regalia",             "Regalia by Deyaar"),
        ("Business Bay", "Executive Tower",     "Executive Tower B"),
        ("Business Bay", "Executive Tower",     "Executive Tower G"),
        ("Business Bay", "Binghatti Canal",     "Binghatti Canal"),
        ("Business Bay", "Vera Residences",     "Vera Residences"),
        ("Business Bay", "Damac Maison Prive",  "Damac Maison Prive"),
        ("Business Bay", "Canal Crown",         "Canal Crown"),
    ],
    "Arabian Ranches": [
        ("Arabian Ranches", "Arabian Ranches III", "Arabian Ranches III Joy"),
        ("Arabian Ranches", "Arabian Ranches III", "Arabian Ranches III Ruba"),
        ("Arabian Ranches", "Arabian Ranches II",  "Arabian Ranches II Rasha"),
        ("Arabian Ranches", "Arabian Ranches II",  "Arabian Ranches II Lila"),
        ("Arabian Ranches", "Mirador",             "Mirador La Coleccion"),
        ("Arabian Ranches", "Alvorada",            "Alvorada 1"),
        ("Arabian Ranches", "Saheel",              "Saheel 1"),
        ("Arabian Ranches", "Palma",               "Palma"),
    ],
    "Dubai Hills": [
        ("Dubai Hills Estate", "Maple",        "Maple 1"),
        ("Dubai Hills Estate", "Maple",        "Maple 2"),
        ("Dubai Hills Estate", "Maple",        "Maple 3"),
        ("Dubai Hills Estate", "Sidra Villas", "Sidra Villas I"),
        ("Dubai Hills Estate", "Sidra Villas", "Sidra Villas II"),
        ("Dubai Hills Estate", "Park Heights", "Park Heights 1"),
        ("Dubai Hills Estate", "Park Heights", "Park Heights 2"),
        ("Dubai Hills Estate", "Mulberry",     "Mulberry 1"),
        ("Dubai Hills Estate", "Acacia",       "Acacia A"),
        ("Dubai Hills Estate", "Golf Suites",  "Golf Suites"),
        ("Dubai Hills Estate", "Collective",   "Collective 2.0"),
    ],
    "Dubai Investment Park 1": [
        ("Dubai Investment Park 1", "Green Community",       "Green Community Village"),
        ("Dubai Investment Park 1", "Green Community",       "Green Community East"),
        ("Dubai Investment Park 1", "Green Community",       "Green Community West"),
        ("Dubai Investment Park 1", "Ritaj",                 "Ritaj D"),
        ("Dubai Investment Park 1", "Ritaj",                 "Ritaj E"),
        ("Dubai Investment Park 1", "Ritaj",                 "Ritaj G"),
        ("Dubai Investment Park 1", "Ritaj",                 "Ritaj H"),
        ("Dubai Investment Park 1", "Grand Paradise",        "Grand Paradise 1"),
        ("Dubai Investment Park 1", "Grand Paradise",        "Grand Paradise 2"),
        ("Dubai Investment Park 1", "The Sustainable City",  "The Sustainable City Cluster A"),
    ],
    "Dubai Investment Park 2": [
        ("Dubai Investment Park 2", "DAMAC Riverside",       "DAMAC Riverside Ivy"),
        ("Dubai Investment Park 2", "DAMAC Riverside",       "DAMAC Riverside Lush"),
        ("Dubai Investment Park 2", "DAMAC Riverside",       "DAMAC Riverside Olive"),
        ("Dubai Investment Park 2", "DAMAC Riverside Views", "DAMAC Riverside Views Marine 1"),
        ("Dubai Investment Park 2", "DAMAC Riverside Views", "DAMAC Riverside Views Marine 2"),
        ("Dubai Investment Park 2", "Expo Village Residences", "Expo Village Residence 1"),
        ("Dubai Investment Park 2", "Expo Village Residences", "Expo Village Residence 2"),
        ("Dubai Investment Park 2", "Lush at DAMAC Riverside", "Lush at DAMAC Riverside"),
    ],
}

# 2026 date range
START_DATE = date(2026, 1, 1)
END_DATE = date(2026, 3, 7)   # up to today

# Simulate a market stress period (Israel-Iran tension spike simulation)
STRESS_START = date(2026, 2, 15)
STRESS_END   = date(2026, 2, 25)

# 2025 full-year historical data — prices ~10-12% lower than 2026 (realistic YoY growth)
START_DATE_2025 = date(2025, 1, 1)
END_DATE_2025   = date(2025, 12, 31)
AREA_BASELINES_2025 = {
    "Downtown Dubai":          {"price_sqm": (16_000, 22_000), "daily_vol": (7, 16)},
    "Palm Jumeirah":           {"price_sqm": (19_500, 31_000), "daily_vol": (3,  9)},
    "Dubai Marina":            {"price_sqm": (11_500, 16_500), "daily_vol": (9, 20)},
    "JVC/JVT":                 {"price_sqm": (7_000,  10_500), "daily_vol": (13, 30)},
    "Business Bay":            {"price_sqm": (12_500, 18_000), "daily_vol": (7, 18)},
    "Arabian Ranches":         {"price_sqm": (6_200,   9_800), "daily_vol": (4, 10)},
    "Dubai Hills":             {"price_sqm": (8_000,  13_000), "daily_vol": (5, 14)},
    "Dubai Investment Park 1": {"price_sqm": (5_400,   9_000), "daily_vol": (2,  8)},
    "Dubai Investment Park 2": {"price_sqm": (6_300,  10_300), "daily_vol": (1,  5)},
}

tx_counter = 0


def generate_transactions_for_day(day: date, area: str) -> list[dict]:
    """Generate realistic transaction records for one area on one day."""
    global tx_counter
    baseline = AREA_BASELINES[area]
    price_range = baseline["price_sqm"]
    vol_range = baseline["daily_vol"]

    # Apply stress period: fewer transactions, lower prices
    is_stress = STRESS_START <= day <= STRESS_END
    stress_volume_factor = random.uniform(0.55, 0.72) if is_stress else 1.0
    stress_price_factor  = random.uniform(0.88, 0.94) if is_stress else 1.0

    # Weekends (Fri-Sat in Dubai) have lower volume
    is_weekend = day.weekday() in (4, 5)  # Friday = 4, Saturday = 5
    weekend_factor = 0.3 if is_weekend else 1.0

    num_transactions = max(1, int(
        random.randint(*vol_range) * stress_volume_factor * weekend_factor
    ))

    records = []
    for _ in range(num_transactions):
        tx_counter += 1
        price_sqm = random.uniform(*price_range) * stress_price_factor
        area_sqm = random.uniform(45, 180)  # sqm range for <2M AED properties

        # Keep price under 2M AED
        actual_worth = price_sqm * area_sqm
        if actual_worth > 1_950_000:
            area_sqm = 1_900_000 / price_sqm
            actual_worth = price_sqm * area_sqm

        projects = AREA_PROJECTS.get(area, [])
        master, proj, bldg = random.choice(projects) if projects else (area, None, None)

        records.append({
            "transaction_id":   f"MOCK-{day.isoformat()}-{area[:3].upper()}-{tx_counter:06d}",
            "transaction_date": day,
            "actual_worth":     round(actual_worth, 2),
            "procedure_area":   round(area_sqm, 2),
            "trans_group":      "Sales",
            "property_usage":   "Residential",
            "prop_type":        random.choice(PROPERTY_TYPES),
            "area_name":        area,
            "area_canonical":   area,
            "master_project":   master,
            "project_name":     proj,
            "building_name":    bldg,
        })
    return records


def seed_2025_transactions() -> int:
    """Seed full-year 2025 DLD transaction data for YoY price comparison."""
    total = 0
    counter = 500_000  # offset to avoid ID collision with 2026 records
    day = START_DATE_2025
    while day <= END_DATE_2025:
        with get_session() as session:
            for area, baseline in AREA_BASELINES_2025.items():
                is_weekend = day.weekday() in (4, 5)
                weekend_factor = 0.3 if is_weekend else 1.0
                num_tx = max(1, int(random.randint(*baseline["daily_vol"]) * weekend_factor))
                for _ in range(num_tx):
                    counter += 1
                    price_sqm = random.uniform(*baseline["price_sqm"])
                    area_sqm  = random.uniform(45, 180)
                    actual_worth = price_sqm * area_sqm
                    if actual_worth > 1_950_000:
                        area_sqm = 1_900_000 / price_sqm
                        actual_worth = price_sqm * area_sqm
                    projects = AREA_PROJECTS.get(area, [])
                    master, proj, bldg = random.choice(projects) if projects else (area, None, None)
                    r = {
                        "transaction_id":   f"MOCK-{day.isoformat()}-{area[:3].upper()}-{counter:07d}",
                        "transaction_date": day,
                        "actual_worth":     round(actual_worth, 2),
                        "procedure_area":   round(area_sqm, 2),
                        "trans_group":      "Sales",
                        "property_usage":   "Residential",
                        "prop_type":        random.choice(PROPERTY_TYPES),
                        "area_name":        area,
                        "area_canonical":   area,
                        "master_project":   master,
                        "project_name":     proj,
                        "building_name":    bldg,
                    }
                    if upsert_transaction(session, r):
                        total += 1
        day += timedelta(days=1)
    return total


def seed_bayut_listings() -> None:
    """
    Backfill mock Bayut listing counts into all daily_metrics rows.
    Must be called AFTER aggregate_daily_metrics() has created the rows.
    Simulates a supply surge during the stress period to trigger SUPPLY_SURGE signals.
    """
    with get_session() as session:
        rows = session.query(DailyMetric).all()
        for row in rows:
            area = row.area_canonical
            if area not in AREA_BAYUT_BASELINES:
                continue
            lo, hi = AREA_BAYUT_BASELINES[area]
            base = random.randint(lo, hi)

            # Stress period: listings surge 18-28% as sellers rush to exit
            is_stress = STRESS_START <= row.metric_date <= STRESS_END
            surge_factor = random.uniform(1.18, 1.28) if is_stress else 1.0

            # Add small day-to-day noise (±3%)
            noise = random.uniform(0.97, 1.03)
            row.bayut_listing_count = int(base * surge_factor * noise)

    print(f"  ✅ Bayut listing counts seeded for {len(rows)} area-day rows")


def seed_database() -> None:
    """Seed DB with mock data across all areas for the full date range."""
    init_db()
    print(f"Seeding mock data from {START_DATE} to {END_DATE}...")
    print(f"  Market stress period: {STRESS_START} → {STRESS_END} (simulated buy signal)\n")

    total_tx = 0
    day = START_DATE
    while day <= END_DATE:
        with get_session() as session:
            for area in AREA_BASELINES:
                records = generate_transactions_for_day(day, area)
                for r in records:
                    if upsert_transaction(session, r):
                        total_tx += 1
        day += timedelta(days=1)

    print(f"  ✅ Inserted {total_tx} mock 2026 transactions")
    print("  Seeding 2025 historical data for YoY comparison (this takes ~30s)...")
    tx_2025 = seed_2025_transactions()
    print(f"  ✅ Inserted {tx_2025} mock 2025 transactions")
    print("  Running aggregation pipeline...")

    # Run pipeline steps individually so we can inject mock Bayut data
    # between aggregation and MA calculation (skipping live Bayut scrape)
    from anomaly_detector import aggregate_daily_metrics, compute_rolling_mas, detect_anomalies
    aggregate_daily_metrics()        # creates daily_metrics rows from transactions
    seed_bayut_listings()            # inject mock listing counts into those rows
    compute_rolling_mas()            # compute MAs — now includes supply_ma7
    signals = detect_anomalies()     # detect VOLUME_DROP, PRICE_DIP, SUPPLY_SURGE

    print(f"  ✅ Metrics and rolling MAs computed")
    if signals:
        print(f"\n  🚨 {len(signals)} buy signal(s) detected in mock data:")
        for s in signals:
            print(f"     [{s['signal_type']}] {s['area']} — {s['notes']}")
    else:
        print("  📊 No signals in mock data (market looks stable)")

    print("\nDone! Run: streamlit run app.py")


if __name__ == "__main__":
    seed_database()
