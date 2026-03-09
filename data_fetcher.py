"""
data_fetcher.py — Fetches DLD transactions via JSON API and scrapes Bayut listings.

DLD open data API: https://gateway.dubailand.gov.ae/open-data/transactions
Bayut: rate-limited polite scrape of public search result pages.

Run standalone:  python data_fetcher.py
"""

from __future__ import annotations

import logging
import random
import re
import time
from datetime import date, datetime, timedelta
from typing import Optional

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


# ─── DLD JSON API Fetcher ────────────────────────────────────────────────────

def _resolve_area_ids(http: requests.Session) -> dict[str, list[str]]:
    """
    Call the DLD carea-lookup endpoint and map each of our canonical
    area names to a list of DLD area IDs (both A-xxx and C-xxx variants).

    Returns:
        {canonical_area: [area_id, ...]}  — only monitored areas included.
    """
    url = f"{config.DLD_API_BASE}/carea-lookup"
    resp = http.post(url, json={}, timeout=30)
    resp.raise_for_status()

    areas = resp.json().get("response", {}).get("result", [])
    mapping: dict[str, list[str]] = {}

    for entry in areas:
        name_en = entry.get("NAME_EN", "")
        area_id = entry.get("AREA_ID", "")
        canonical = canonicalise_area(name_en)
        if canonical is not None and area_id:
            mapping.setdefault(canonical, []).append(area_id)

    logger.info("Resolved %d monitored areas to DLD IDs: %s",
                len(mapping), {k: v for k, v in mapping.items()})
    return mapping


def _fetch_dld_page(
    http: requests.Session,
    from_date: str,
    to_date: str,
    area_id: str = "",
    skip: int = 0,
) -> tuple[list[dict], int]:
    """
    Fetch one page of DLD transactions from the JSON API.

    Args:
        http: Requests session with User-Agent set.
        from_date: DD/MM/YYYY format start date.
        to_date: DD/MM/YYYY format end date.
        area_id: DLD area ID to filter server-side (empty = all areas).
        skip: Number of rows to skip (pagination offset).

    Returns:
        (rows, total) — list of row dicts + total matching records.
    """
    url = f"{config.DLD_API_BASE}/{config.DLD_API_COMMAND}"
    payload = {
        "P_FROM_DATE": from_date,
        "P_TO_DATE": to_date,
        "P_GROUP_ID": config.DLD_API_SALES_GROUP_ID,
        "P_IS_OFFPLAN": "",
        "P_IS_FREE_HOLD": "",
        "P_AREA_ID": area_id,
        "P_USAGE_ID": config.DLD_API_RESIDENTIAL_USAGE_ID,
        "P_PROP_TYPE_ID": "",
        "P_TAKE": str(config.DLD_API_PAGE_SIZE),
        "P_SKIP": str(skip),
        "P_SORT": "",
    }

    resp = http.post(url, json=payload, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    rows = data.get("response", {}).get("result", [])
    total = rows[0].get("TOTAL", 0) if rows else 0
    return rows, total


def _api_row_to_record(row: dict) -> Optional[dict]:
    """
    Convert a DLD API JSON row into the record dict expected by
    upsert_transaction(). Returns None if the row should be skipped
    (unmonitored area, price out of range, etc.).
    """
    col = config.DLD_API_COLUMNS

    # ── Parse date ───────────────────────────────────────────────────────
    raw_date = row.get(col["date"])
    if not raw_date:
        return None
    try:
        tx_date = datetime.fromisoformat(raw_date).date()
    except (ValueError, TypeError):
        return None

    # ── Canonicalise area ────────────────────────────────────────────────
    raw_area = row.get(col["location"], "")
    area_canonical = canonicalise_area(raw_area)
    if area_canonical is None:
        return None  # not in our watch list

    # ── Price filter ─────────────────────────────────────────────────────
    worth = row.get(col["worth"])
    if worth is None:
        return None
    try:
        worth = float(worth)
    except (ValueError, TypeError):
        return None
    if worth < config.MIN_PRICE_AED or worth > config.MAX_PRICE_AED:
        return None

    # ── Area in sqm ──────────────────────────────────────────────────────
    area_sqm = row.get(col["area_sqm"])
    try:
        area_sqm = float(area_sqm) if area_sqm is not None else None
    except (ValueError, TypeError):
        area_sqm = None

    # ── String fields (None-safe) ────────────────────────────────────────
    def _str(key: str) -> Optional[str]:
        val = row.get(col.get(key, ""))
        if val is None:
            return None
        s = str(val).strip()
        return s if s else None

    return {
        "transaction_id":   _str("transaction_id"),
        "transaction_date": tx_date,
        "actual_worth":     worth,
        "procedure_area":   area_sqm,
        "trans_group":      _str("trans_group"),
        "property_usage":   _str("usage"),
        "prop_type":        _str("prop_type"),
        "area_name":        (raw_area or "").strip() or None,
        "area_canonical":   area_canonical,
        "building_name":    None,  # API has no separate building column
        "project_name":     _str("project"),
        "master_project":   _str("master_project"),
    }


def _paginate_area(
    http: requests.Session,
    from_date: str,
    to_date: str,
    area_id: str,
) -> list[dict]:
    """Fetch all pages for a single DLD area ID. Returns list of API rows."""
    all_rows: list[dict] = []
    skip = 0
    total = None

    while True:
        page_rows, page_total = _fetch_dld_page(
            http, from_date, to_date, area_id=area_id, skip=skip,
        )
        if not page_rows:
            break
        if total is None:
            total = page_total
        all_rows.extend(page_rows)
        skip += len(page_rows)
        if skip >= total:
            break

    return all_rows


def fetch_dld_transactions(lookback_days: int = config.LOOKBACK_DAYS) -> int:
    """
    Fetch residential sales from DLD JSON API for the last `lookback_days`,
    filter to monitored areas + price range, and upsert into DB.

    Queries per-area (server-side filter) so we only download data for
    our monitored areas — much faster than pulling all of Dubai.

    Returns number of new rows inserted.
    """
    http = requests.Session()
    http.headers.update({
        "User-Agent": config.SCRAPER_USER_AGENT,
        "Content-Type": "application/json",
    })

    rows_inserted = 0
    error_msg = None

    try:
        # Build date range
        cutoff = datetime.utcnow().date() - timedelta(days=lookback_days)
        today = datetime.utcnow().date()
        from_date = cutoff.strftime("%d/%m/%Y")
        to_date = today.strftime("%d/%m/%Y")

        logger.info(
            "Fetching DLD transactions via API: %s to %s (lookback %d days)",
            from_date, to_date, lookback_days,
        )

        # Step 1: Resolve our canonical area names → DLD area IDs
        area_id_map = _resolve_area_ids(http)
        if not area_id_map:
            raise ValueError("carea-lookup returned no matching areas")

        # Step 2: Fetch transactions per area (server-side filter)
        total_downloaded = 0
        seen_tx_ids: set[str] = set()  # avoid dupes across overlapping area IDs

        for canonical, area_ids in area_id_map.items():
            for area_id in area_ids:
                area_rows = _paginate_area(http, from_date, to_date, area_id)
                total_downloaded += len(area_rows)

                if area_rows:
                    logger.info(
                        "  %s (ID %s): %d rows", canonical, area_id, len(area_rows),
                    )

                # Convert + filter + upsert (commit per area to avoid large txn)
                with get_session() as session:
                    for api_row in area_rows:
                        record = _api_row_to_record(api_row)
                        if record is None:
                            continue
                        # Skip if we already processed this tx_id in this run
                        tx_id = record.get("transaction_id")
                        if tx_id and tx_id in seen_tx_ids:
                            continue
                        if tx_id:
                            seen_tx_ids.add(tx_id)
                        if upsert_transaction(session, record):
                            rows_inserted += 1

        logger.info(
            "DLD fetch complete: downloaded %d rows, inserted %d new transactions",
            total_downloaded, rows_inserted,
        )

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
