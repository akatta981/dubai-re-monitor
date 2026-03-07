"""
data_fetcher.py — Fetches DLD transaction CSV and scrapes Bayut listing counts.

DLD open data: https://www.dubailand.gov.ae/en/open-data/real-estate-data/
Bayut: rate-limited polite scrape of public search result pages.

Run standalone:  python data_fetcher.py
"""

from __future__ import annotations

import io
import logging
import random
import re
import time
from datetime import date, datetime, timedelta
from typing import Optional
from urllib.parse import urlencode

import pandas as pd
import requests
from bs4 import BeautifulSoup

import config
from db import FetchLog, Transaction, get_session, init_db, upsert_transaction

logger = logging.getLogger(__name__)

# ─── Area Normalisation ───────────────────────────────────────────────────────

def canonicalise_area(raw_area: str) -> Optional[str]:
    """
    Map a raw DLD area string to one of our canonical area names.
    Returns None if the area is not in our watch list.
    """
    if not raw_area or not isinstance(raw_area, str):
        return None
    lower = raw_area.lower().strip()
    for canonical, keywords in config.MONITORED_AREAS.items():
        if any(kw in lower for kw in keywords):
            return canonical
    return None


# ─── DLD Fetcher ─────────────────────────────────────────────────────────────

def _find_dld_csv_url(session: requests.Session) -> Optional[str]:
    """
    Scrape the DLD open data page to find the direct CSV download link.
    DLD occasionally changes the filename — this makes it resilient.
    """
    try:
        resp = session.get(config.DLD_BASE_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # Look for links containing "TransactionDetails" or ending in .csv
        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            if "transaction" in href.lower() and href.lower().endswith(".csv"):
                if not href.startswith("http"):
                    href = "https://www.dubailand.gov.ae" + href
                logger.info("Found DLD CSV URL: %s", href)
                return href
        # Fallback: any .csv link on the page
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".csv"):
                if not href.startswith("http"):
                    href = "https://www.dubailand.gov.ae" + href
                return href
    except requests.RequestException as e:
        logger.error("Failed to scrape DLD page: %s", e)
    return None


def _parse_dld_csv(raw_bytes: bytes) -> pd.DataFrame:
    """
    Parse DLD CSV bytes into a clean DataFrame.
    Handles encoding issues and flexible column names.
    """
    # Try UTF-8 first, fall back to latin-1
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            df = pd.read_csv(io.BytesIO(raw_bytes), encoding=encoding, low_memory=False)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError("Could not decode DLD CSV with any supported encoding")

    # Normalise column names: strip whitespace
    df.columns = [c.strip() for c in df.columns]
    logger.info("DLD CSV loaded: %d rows, columns: %s", len(df), list(df.columns))
    return df


def fetch_dld_transactions(lookback_days: int = config.LOOKBACK_DAYS) -> int:
    """
    Download DLD CSV, filter to residential sales in monitored areas
    within the last `lookback_days`, upsert into DB.

    Returns number of new rows inserted.
    """
    http = requests.Session()
    http.headers.update({"User-Agent": config.SCRAPER_USER_AGENT})

    rows_inserted = 0
    error_msg = None

    try:
        # Step 1: Find CSV URL
        csv_url = _find_dld_csv_url(http)
        if not csv_url:
            raise ValueError("Could not locate DLD CSV download link on open data page")

        # Step 2: Download CSV
        logger.info("Downloading DLD CSV from %s ...", csv_url)
        resp = http.get(csv_url, timeout=120)
        resp.raise_for_status()

        # Step 3: Parse
        df = _parse_dld_csv(resp.content)
        col = config.DLD_COLUMNS

        # Step 4: Validate expected columns exist
        missing = [v for v in col.values() if v not in df.columns]
        if missing:
            # Try case-insensitive match
            col_lower = {c.lower(): c for c in df.columns}
            remapped = {}
            still_missing = []
            for k, v in col.items():
                if v.lower() in col_lower:
                    remapped[k] = col_lower[v.lower()]
                else:
                    still_missing.append(v)
            if still_missing:
                logger.warning("DLD CSV missing columns: %s — available: %s", still_missing, list(df.columns))
            col = {**col, **{k: remapped[k] for k in remapped}}

        # Step 5: Parse dates
        date_col = col.get("date", "Transaction Date")
        if date_col not in df.columns:
            raise ValueError(f"Date column '{date_col}' not found in CSV")

        df["_date"] = pd.to_datetime(df[date_col], format="%d/%m/%Y", errors="coerce")
        df = df.dropna(subset=["_date"])

        # Step 6: Filter by lookback window
        cutoff = datetime.utcnow().date() - timedelta(days=lookback_days)
        df = df[df["_date"].dt.date >= cutoff]

        # Step 7: Filter Sales + Residential
        trans_col = col.get("trans_group", "trans_group_en")
        usage_col = col.get("usage", "property_usage_en")
        if trans_col in df.columns:
            df = df[df[trans_col].str.strip().str.lower() == "sales"]
        if usage_col in df.columns:
            df = df[df[usage_col].str.strip().str.lower() == "residential"]

        # Step 8: Apply price filter (under AED 2M)
        worth_col = col.get("worth", "actual_worth")
        if worth_col in df.columns:
            df[worth_col] = pd.to_numeric(df[worth_col], errors="coerce")
            df = df[
                (df[worth_col] >= config.MIN_PRICE_AED) &
                (df[worth_col] <= config.MAX_PRICE_AED)
            ]

        # Step 9: Canonicalise areas + filter to monitored only
        loc_col = col.get("location", "AREA_EN")
        if loc_col in df.columns:
            df["_area_canonical"] = df[loc_col].apply(canonicalise_area)
            df = df[df["_area_canonical"].notna()]
        else:
            logger.warning("Location column '%s' not found — keeping all areas", loc_col)
            df["_area_canonical"] = "Unknown"

        logger.info("After filters: %d rows to upsert", len(df))

        # Step 10: Upsert into DB
        area_col     = col.get("location",       "AREA_EN")
        type_col     = col.get("prop_type",      "type_en")
        area_sqm_col = col.get("area_sqm",       "procedure_area")
        tx_id_col    = col.get("transaction_id", "trans_group_id")
        bldg_col     = col.get("building",       "building_name_en")
        proj_col     = col.get("project",        "project_name_en")
        master_col   = col.get("master_project", "master_project_en")

        with get_session() as session:
            for _, row in df.iterrows():
                def _str_or_none(c: str) -> str | None:
                    """Return stripped string value or None if column missing/NaN."""
                    if c not in df.columns:
                        return None
                    val = row.get(c)
                    return str(val).strip() or None if pd.notna(val) else None

                record = {
                    "transaction_id":   str(row.get(tx_id_col, "")) or None,
                    "transaction_date": row["_date"].date(),
                    "actual_worth":     float(row[worth_col]) if worth_col in df.columns and pd.notna(row.get(worth_col)) else None,
                    "procedure_area":   float(row[area_sqm_col]) if area_sqm_col in df.columns and pd.notna(row.get(area_sqm_col)) else None,
                    "trans_group":      _str_or_none(trans_col),
                    "property_usage":   _str_or_none(usage_col),
                    "prop_type":        _str_or_none(type_col),
                    "area_name":        _str_or_none(area_col),
                    "area_canonical":   row["_area_canonical"],
                    "building_name":    _str_or_none(bldg_col),
                    "project_name":     _str_or_none(proj_col),
                    "master_project":   _str_or_none(master_col),
                }
                if upsert_transaction(session, record):
                    rows_inserted += 1

        logger.info("DLD fetch complete: %d new transactions inserted", rows_inserted)

    except Exception as e:
        error_msg = str(e)
        logger.error("DLD fetch failed: %s", e, exc_info=True)

    # Log fetch attempt
    with get_session() as session:
        session.add(FetchLog(
            source="dld",
            status="success" if error_msg is None else "error",
            rows_upserted=rows_inserted,
            error_message=error_msg,
        ))

    return rows_inserted


# ─── Bayut Scraper ────────────────────────────────────────────────────────────

# Maps our canonical area names to Bayut URL slugs
BAYUT_AREA_SLUGS: dict[str, str] = {
    "Downtown Dubai": "downtown-dubai",
    "Palm Jumeirah": "palm-jumeirah",
    "Dubai Marina": "dubai-marina",
    "JVC/JVT": "jumeirah-village-circle",
    "Business Bay": "business-bay",
    "Arabian Ranches": "arabian-ranches",
    "Dubai Hills": "dubai-hills-estate",
}


def _scrape_bayut_listing_count(area_slug: str, http: requests.Session) -> Optional[int]:
    """
    Scrape Bayut sale listings page for a given area and extract the total count.
    Returns None on failure (caller handles gracefully).
    """
    url = f"https://www.bayut.com/for-sale/property/{area_slug}/"
    try:
        time.sleep(random.uniform(config.BAYUT_REQUEST_DELAY_MIN, config.BAYUT_REQUEST_DELAY_MAX))
        resp = http.get(url, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Bayut typically shows total count in a element like "1,234 results"
        # Multiple selectors tried for resilience against HTML changes
        patterns = [
            r"([\d,]+)\s+(?:properties|results|listings)",
            r"Showing.*?of\s+([\d,]+)",
        ]
        text_content = soup.get_text(" ", strip=True)
        for pattern in patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                count_str = match.group(1).replace(",", "")
                return int(count_str)

        # Fallback: count listing cards on the page (undercount but better than nothing)
        cards = soup.select("[class*='listing']") or soup.select("article")
        if cards:
            logger.debug("Bayut %s: fell back to card count = %d", area_slug, len(cards))
            return len(cards)

    except requests.RequestException as e:
        logger.warning("Bayut scrape failed for %s: %s", area_slug, e)
    except Exception as e:
        logger.warning("Bayut parse error for %s: %s", area_slug, e)
    return None


def fetch_bayut_supply() -> dict[str, Optional[int]]:
    """
    Scrape Bayut listing counts for all monitored areas.
    Returns dict: {area_canonical: listing_count or None}
    """
    http = requests.Session()
    http.headers.update({
        "User-Agent": config.SCRAPER_USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    results: dict[str, Optional[int]] = {}
    error_msg = None
    success_count = 0

    for canonical, slug in BAYUT_AREA_SLUGS.items():
        count = _scrape_bayut_listing_count(slug, http)
        results[canonical] = count
        if count is not None:
            success_count += 1
            logger.info("Bayut %s: %d listings", canonical, count)
        else:
            logger.warning("Bayut %s: no count retrieved", canonical)

    if success_count == 0:
        error_msg = "All Bayut scrapes failed — site may have changed or blocked"

    with get_session() as session:
        session.add(FetchLog(
            source="bayut",
            status="success" if error_msg is None else "error",
            rows_upserted=success_count,
            error_message=error_msg,
        ))

    return results


# ─── Entry Point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    init_db()
    print("Fetching DLD transactions...")
    n = fetch_dld_transactions()
    print(f"  → {n} new transactions inserted")

    print("Scraping Bayut supply data...")
    supply = fetch_bayut_supply()
    for area, count in supply.items():
        print(f"  → {area}: {count} listings")
