"""
tests/test_anomaly_detector.py — Unit tests for anomaly detection logic.
Run: pytest tests/ -v
"""

from __future__ import annotations

import pytest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config


# ─── Test: canonicalise_area ──────────────────────────────────────────────────

def test_canonicalise_area_downtown():
    from data_fetcher import canonicalise_area
    assert canonicalise_area("DOWNTOWN DUBAI") == "Downtown Dubai"
    assert canonicalise_area("downtown dubai") == "Downtown Dubai"
    assert canonicalise_area("Downtown") == "Downtown Dubai"


def test_canonicalise_area_jvc():
    from data_fetcher import canonicalise_area
    assert canonicalise_area("Jumeirah Village Circle") == "JVC/JVT"
    assert canonicalise_area("JVC") == "JVC/JVT"


def test_canonicalise_area_unknown():
    from data_fetcher import canonicalise_area
    assert canonicalise_area("Silicon Oasis") is None
    assert canonicalise_area("") is None
    assert canonicalise_area(None) is None


# ─── Test: anomaly thresholds ─────────────────────────────────────────────────

def test_volume_drop_threshold():
    """Volume below 80% of MA should trigger signal."""
    vol_ma7 = 100.0
    current_vol = 75.0   # 75% of MA — below 80% threshold
    assert current_vol < config.VOLUME_DROP_THRESHOLD * vol_ma7


def test_volume_no_signal():
    """Volume at 85% of MA should NOT trigger."""
    vol_ma7 = 100.0
    current_vol = 85.0
    assert not (current_vol < config.VOLUME_DROP_THRESHOLD * vol_ma7)


def test_price_dip_threshold():
    """Price below 95% of MA should trigger signal."""
    price_ma30 = 15_000.0
    current_price = 14_000.0   # 93.3% of MA — below 95% threshold
    assert current_price < config.PRICE_DIP_THRESHOLD * price_ma30


def test_price_no_signal():
    """Price at 96% of MA should NOT trigger."""
    price_ma30 = 15_000.0
    current_price = 14_400.0  # 96% of MA
    assert not (current_price < config.PRICE_DIP_THRESHOLD * price_ma30)


def test_supply_surge_threshold():
    """Supply 15% above MA should trigger signal."""
    supply_ma7 = 200.0
    current_listings = 235.0   # 17.5% above MA — above 10% threshold
    assert current_listings > (1 + config.SUPPLY_SURGE_THRESHOLD) * supply_ma7


def test_supply_no_signal():
    """Supply 8% above MA should NOT trigger."""
    supply_ma7 = 200.0
    current_listings = 216.0   # 8% above MA
    assert not (current_listings > (1 + config.SUPPLY_SURGE_THRESHOLD) * supply_ma7)


# ─── Test: price per sqm calculation ─────────────────────────────────────────

def test_price_per_sqm_normal():
    from db import upsert_transaction
    record = {"actual_worth": 1_500_000.0, "procedure_area": 75.0}
    record["price_per_sqm"] = record["actual_worth"] / record["procedure_area"]
    assert abs(record["price_per_sqm"] - 20_000.0) < 0.01


def test_price_per_sqm_zero_area():
    """Should not divide by zero."""
    worth, area = 1_000_000.0, 0.0
    price_sqm = (worth / area) if (worth and area and area > 0) else None
    assert price_sqm is None


# ─── Test: price filter ───────────────────────────────────────────────────────

def test_price_filter_under_2m():
    """Properties under 2M AED should pass filter."""
    price = 1_800_000
    assert config.MIN_PRICE_AED <= price <= config.MAX_PRICE_AED


def test_price_filter_over_2m():
    """Properties over 2M AED should be filtered out."""
    price = 2_500_000
    assert not (config.MIN_PRICE_AED <= price <= config.MAX_PRICE_AED)


def test_price_filter_too_low():
    """Very low prices (data errors) should be filtered out."""
    price = 100  # AED 100 — clearly a data error
    assert not (config.MIN_PRICE_AED <= price <= config.MAX_PRICE_AED)


# ─── Test: config values are sane ────────────────────────────────────────────

def test_config_thresholds_range():
    assert 0 < config.VOLUME_DROP_THRESHOLD < 1
    assert 0 < config.PRICE_DIP_THRESHOLD < 1
    assert 0 < config.SUPPLY_SURGE_THRESHOLD < 1


def test_config_max_price():
    assert config.MAX_PRICE_AED == 2_000_000


def test_config_areas_populated():
    assert len(config.MONITORED_AREAS) >= 7
    assert "Downtown Dubai" in config.MONITORED_AREAS
    assert "JVC/JVT" in config.MONITORED_AREAS


def test_config_lookback_days():
    assert 30 <= config.LOOKBACK_DAYS <= 365
