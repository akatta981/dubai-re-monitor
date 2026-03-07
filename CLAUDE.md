# Dubai Real Estate Monitor — Claude Code Project Guide

## Project Purpose
Personal investment research tool for Abhi. Monitors Dubai real estate market
anomalies using DLD (Dubai Land Department) open transaction data + Bayut
supply-side scraping. Sends WhatsApp + Gmail alerts when buy signals trigger.

## Owner's Investment Profile
- **Target price**: Under AED 2,000,000
- **Property type**: Both apartments and villas/townhouses
- **Areas monitored**: Downtown Dubai, Palm Jumeirah, Dubai Marina, JVC/JVT,
  Business Bay, Arabian Ranches, Dubai Hills

## Architecture Overview
```
data_fetcher.py   →  db.py (SQLite)  →  anomaly_detector.py  →  alerts.py
                                               ↓
                                           app.py (Streamlit dashboard)
scheduler.py runs data_fetcher + anomaly_detector every 15 minutes
```

## Key Files
| File | Purpose |
|------|---------|
| `app.py` | Streamlit dashboard (main entry point) |
| `data_fetcher.py` | DLD CSV fetch + Bayut scraper |
| `anomaly_detector.py` | Rolling MA calculations + buy signal logic |
| `alerts.py` | Gmail SMTP + WhatsApp via Twilio |
| `db.py` | SQLite ORM (SQLAlchemy) |
| `scheduler.py` | APScheduler — 15-min refresh, Windows-compatible |
| `seed_data.py` | Populates DB with mock data for testing |

## Run Commands
```bash
# Install dependencies
pip install -r requirements.txt

# Seed mock data (first time setup)
python seed_data.py

# Run dashboard
streamlit run app.py

# Run scheduler (separate terminal)
python scheduler.py
```

## Environment Variables (copy .env.example → .env)
Never commit .env to git. All credentials via environment variables only.

## Database
SQLite at `data/dubai_re.db`. Three tables:
- `transactions` — raw DLD records
- `daily_metrics` — aggregated per area per day
- `anomaly_log` — triggered buy signals with timestamps

## Coding Standards
- Python 3.11+, PEP8, type hints on all functions
- Logging via Python `logging` module (structured, not print statements)
- All thresholds configurable via `config.py` — never hardcoded
- Scraper: polite delays (2–5s random), max 1 request/3s to Bayut
- Error handling: try/except on all network calls, fallback to cached DB data
