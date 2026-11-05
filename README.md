# 🏙️ Dubai Real Estate Anomaly Monitor

Personal investment research tool. Monitors DLD transaction data + Bayut supply listings
to detect buy signals for **under AED 2M residential properties** across 12 Dubai areas.

> ⚠️ **Personal research only. Not financial advice.**

---

## What It Does

| Signal | Trigger | Meaning |
|--------|---------|---------|
| 📉 Volume Drop | Daily transactions < 80% of 7-day MA | Buyer activity falling |
| 💰 Price Dip | Avg AED/sqm < 95% of 30-day MA | Prices softening |
| 🏗️ Supply Surge | Bayut listings > 10% above 7-day MA | Seller supply increasing |
| 🚨 Strong Buy | Volume drop + Price dip simultaneously | Optimal entry signal |

Alerts fire via **Gmail** and **WhatsApp** (Twilio).

---

## First-Time Setup (Windows + Claude Code)

### Step 1 — Prerequisites

Make sure you have these installed. Open **PowerShell** and check:

```powershell
python --version     # Need 3.11 or higher
pip --version
```

If Python isn't installed, download it from [python.org](https://www.python.org/downloads/).
During install, **tick "Add Python to PATH"**.

---

### Step 2 — Create a Project Folder

In PowerShell:

```powershell
# Navigate to wherever you keep projects (e.g. Desktop or Documents)
cd C:\Users\YourName\Documents

# Create and enter the project folder
mkdir dubai-re-monitor
cd dubai-re-monitor
```

---

### Step 3 — Open Claude Code Here

```powershell
claude
```

This opens Claude Code in your current folder. You're now in the Claude Code CLI.

---

### Step 4 — Copy All Project Files

Inside Claude Code, paste the project files into this folder. If you received a zip:

```powershell
# In a regular PowerShell (not Claude Code), extract the zip here
Expand-Archive dubai-re-monitor.zip -DestinationPath .
```

Or ask Claude Code to create each file for you (paste file contents one at a time).

---

### Step 5 — Install Dependencies

In **regular PowerShell** (not Claude Code):

```powershell
pip install -r requirements.txt
```

This installs: pandas, streamlit, plotly, sqlalchemy, requests, beautifulsoup4, apscheduler, twilio, python-dotenv.

---

### Step 6 — Configure Credentials

```powershell
# Copy the template
copy .env.example .env

# Open in Notepad to edit
notepad .env
```

Fill in your credentials:

**Gmail App Password** (required for email alerts):
1. Go to [myaccount.google.com/security](https://myaccount.google.com/security)
2. Enable 2-Step Verification if not already done
3. Search "App Passwords" → Create one named "Dubai RE Monitor"
4. Copy the 16-character password into `SMTP_PASS`

**Twilio WhatsApp** (required for WhatsApp alerts):
1. Sign up at [twilio.com](https://www.twilio.com) (free trial works)
2. Copy Account SID and Auth Token from Console
3. Go to Messaging → Try it out → Send a WhatsApp message
4. Follow sandbox setup (send the join code from your phone first)
5. Your `TWILIO_WHATSAPP_FROM` = `whatsapp:+14155238886` (Twilio sandbox)
6. Your `TWILIO_WHATSAPP_TO` = `whatsapp:+61XXXXXXXXX` (your number)

---

### Step 7 — Load Mock Data and Launch

```powershell
# Create database and populate with demo (mock) data that ends today
python seed_data.py

# Launch dashboard
streamlit run app.py
```

Your browser will open at **http://localhost:8501** automatically.

---

## Daily Use

### Terminal 1 — Dashboard
```powershell
streamlit run app.py
```

### Terminal 2 — Background Scheduler (fetches live data every 15 min)
```powershell
python scheduler.py
```

Leave both running. The scheduler fetches DLD data + Bayut supply, runs detection,
and sends alerts automatically. The dashboard refreshes every 15 minutes.

---

## Switching from Mock to Live Data

The mock data always ends **today**: it covers this year so far (at least the last
120 days) plus the whole previous year for the YoY comparison. The last 4 days are
a simulated "market stress" period, so you will see buy signals to test with.
You can also load it from the dashboard: sidebar → **🛠️ ADMIN: OFFICIAL DATA SYNC**
→ **🧪 Load demo data**. The status bar shows **DEMO DATA (not real)** while it is in use.
While only demo data is loaded, signals are detected but **no real email/WhatsApp
alerts are sent** (the "Test alert" button still works so you can check your setup).

To switch to live DLD data, click **🔥 PURGE & SYNC OFFICIAL DATA** in the same
sidebar panel, or from a terminal:

```powershell
python data_fetcher.py
```

This downloads the real DLD CSV and upserts new transactions. Run this once,
then let `scheduler.py` handle it automatically. The first time real DLD rows
arrive, the demo data is removed automatically so fake and real numbers are never
mixed. (`seed_data.py` refuses to run on a database with real data unless you add `--force`.)

---

## Price History & Data Check

The dashboard sync keeps the last 90 days. For year-on-year comparisons, download
the history once (takes 30+ minutes, run it in a terminal):

```powershell
python data_fetcher.py --history                 # from 1 Jan 2026 (DLD returned no 2025 sales)
python data_fetcher.py --since 2024-01-01        # or choose your own start date
```

Then print a summary (median price per m² per area, 2025 vs since 1 March 2026,
month by month, ready vs off-plan) and a data health check:

```powershell
python check_data.py
python check_data.py --baseline-year 2025 --trend-start 2026-03-01
```

If the 2025 baseline is not available, the report compares against Jan–Feb of
this year instead. It also shows READY (resale) homes separately: off-plan prices
are set by developers and can hide what the resale market is doing.

Land plots and implausible records (tiny sizes, impossible prices) are left out
of the analysis — see `EXCLUDED_PROP_TYPES`, `MIN_PRICE_SQM`, `MAX_PRICE_SQM`,
`MIN_SIZE_SQM` in `config.py`.

**Buy decision scorecard** — dashboard tab "Buy decision" (or `python decision.py`):
per area, last 3 full months vs the 3 before — resale price trend, fall from this
year's high, buyer activity, off-plan premium, off-plan share, confidence — and a
plain-language timing lean. Evidence only, not financial advice; thresholds are the
`DECISION_*` settings in `config.py`.

`check_data.py` only reads the database. All sales are stored; the AED price cap
in `config.py` (`MAX_PRICE_AED`) is applied when analysing, so you can change it
without re-downloading.

---

## Run with Docker (optional)

Requires Docker Desktop. Data is kept in a Docker volume between runs.

```powershell
docker build -t dubai-re-monitor .
docker run -p 8501:8501 -v dubai-re-data:/app/data --env-file .env dubai-re-monitor
```

Open http://localhost:8501. (Omit `--env-file .env` if you have no alert settings.)

---

## Run Tests

```powershell
pip install pytest
pytest tests/ -v
```

---

## Project Structure

```
dubai-re-monitor/
├── app.py                  Main Streamlit dashboard
├── data_fetcher.py         DLD CSV fetch + Bayut scraper
├── anomaly_detector.py     Rolling MA calculation + buy signal detection
├── alerts.py               Gmail SMTP + WhatsApp via Twilio
├── db.py                   SQLite ORM (SQLAlchemy)
├── scheduler.py            15-min background refresh (Windows-compatible)
├── seed_data.py            Populate DB with mock data for testing
├── config.py               All thresholds and settings (edit here to tune)
├── requirements.txt
├── .env.example            Credential template — copy to .env
├── CLAUDE.md               Claude Code project context
├── data/                   SQLite database lives here (auto-created)
│   └── dubai_re.db
└── tests/
    ├── test_anomaly_detector.py
    └── test_pipeline.py
```

---

## Tuning Buy Signals

All thresholds are in `config.py`. Key settings:

```python
VOLUME_DROP_THRESHOLD = 0.80   # Trigger if volume < 80% of 7d MA (increase to 0.85 for more sensitive)
PRICE_DIP_THRESHOLD   = 0.95   # Trigger if price < 95% of 30d MA
SUPPLY_SURGE_THRESHOLD = 0.10  # Trigger if listings > 110% of 7d MA
MAX_PRICE_AED = 2_000_000      # Only flag signals for properties under this
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `streamlit: command not found` | Run `pip install streamlit` again; restart PowerShell |
| Email not sending | Use App Password not account password; check Gmail allows less secure apps |
| WhatsApp not sending | Send the Twilio sandbox join message from your phone first |
| DLD data not downloading | The DLD open-data API may have changed — check `config.py` → `DLD_API_BASE` |
| Bayut returns no counts | Site may have changed HTML; check `data/scheduler.log` for details |
| No buy signals in mock data | Click **🧪 Load demo data** again (or run `python seed_data.py`) — the stress period is always the last 4 days |
| Dashboard says "No transactions recorded" | Use **PURGE & SYNC** for real DLD data, or **🧪 Load demo data** to test |
| Sync says "DLD fetch failed" | Your existing data is kept. Check internet access to `gateway.dubailand.gov.ae`; details in the terminal log |
| Busy areas (e.g. JVC) show too few sales | Raise `DLD_API_MAX_PAGES_PER_AREA` in `config.py` (each page = 200 sales) |

---

## Notes on Bayut Scraping

The Bayut scraper uses public listing pages with polite rate limiting (2–5s between requests).
It targets listing counts only — no personal data. If Bayut changes their HTML structure,
update the selectors in `data_fetcher.py` → `_scrape_bayut_listing_count()`.

---

*Built for personal investment research. All signals are statistical observations, not recommendations.*
