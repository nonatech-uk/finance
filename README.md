# Finance

Personal finance system replacing Bankivity (iBank). Ingests transactions from multiple sources into an immutable raw layer, deduplicates cross-source overlaps, normalises merchants, and categorises spending.

## Architecture

```
raw_transaction          immutable, append-only source of truth
    │
cleaned_transaction      rule-based merchant string cleaning
    │
canonical_merchant       virtual normalisation layer (query-time lookup)
    │
category                 hierarchical taxonomy (from iBank migration)
    │
economic_event           links related transactions (transfers, FX, fees)
```

All raw transactions are preserved exactly as received. Everything above is a derived projection that can be reprocessed from raw data at any time.

## Data Sources

| Source | Method | Transactions |
|--------|--------|-------------|
| Monzo | Direct API (OAuth + webhooks) | ~3,800 |
| Wise | Activities API + CSV | ~860 |
| First Direct | CSV export | ~4,500 |
| iBank (Bankivity) | Historical migration | ~17,500 |
| Amazon | Order history CSV matching | ~2,000 items |

## Deduplication

Three rules, run in order:

1. **`source_superseded`** — blanket suppression of an unreliable source for an account where another source is authoritative (e.g. iBank suppressed for First Direct accounts where CSV with running balances is gospel)
2. **`ibank_internal`** — same source, same (date, amount, currency, merchant)
3. **`cross_source_date_amount`** — different sources, same (institution, account_ref, date, amount, currency) with ROW_NUMBER positional matching

Source priority: monzo_api/wise_api (1) > first_direct_csv/wise_csv (2) > ibank (3)

The `active_transaction` view provides a deduplicated lens over raw data without modifying it.

## Stack

- **Python 3.12+**
- **PostgreSQL** (on NAS)
- **FastAPI** (REST API on :8000)
- **React 19 + Vite + TypeScript + Tailwind v4 + TanStack Query + Recharts** (UI on :5173)
- **psycopg2** (sync DB access with connection pool)
- **Podman** (daily sync container on NAS)

## Setup

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example config/.env  # fill in DB credentials and API tokens
```

## API

FastAPI on `:8000`, all endpoints under `/api/v1/`:

| Method | Path | Notes |
|--------|------|-------|
| GET | /transactions | Cursor-paginated, 12 filter params |
| GET | /transactions/{id} | Full detail + dedup group + economic event |
| GET | /accounts | Derived from active_transaction |
| GET | /accounts/{institution}/{account_ref} | Summary + recent txns |
| GET | /categories | Recursive tree |
| GET | /categories/spending | Aggregated with date range + currency filters |
| GET | /merchants | Cursor-paginated, search + unmapped filter |
| PUT | /merchants/{id}/mapping | Sets category_hint |
| GET | /stats/monthly | Income/expense by month |
| GET | /stats/overview | Dashboard summary stats |
| GET | /health | Pool status |

```bash
# Start API (from project root)
uvicorn src.api.app:app --host 0.0.0.0 --port 8000
```

## UI

React SPA with dark theme. Pages: Dashboard, Transactions, Accounts, AccountDetail, Categories, Merchants.

```bash
cd ui && npm run dev   # :5173, proxies /api -> :8000
```

## Scripts

```bash
# Data ingestion
python scripts/monzo_bulk_load.py           # Monzo API -> raw_transaction
python scripts/wise_bulk_load.py            # Wise Activities API -> raw_transaction
python scripts/wise_csv_load.py             # Wise CSV export -> raw_transaction
python scripts/fd_csv_load.py               # First Direct CSV -> raw_transaction
python scripts/load_ibank_transactions.py   # iBank/Bankivity migration

# Processing pipeline
python scripts/run_cleaning.py              # Apply merchant cleaning rules
python scripts/run_dedup.py                 # Cross-source deduplication
python scripts/run_dedup.py --stats         # View dedup statistics

# Daily sync (runs inside container)
python scripts/daily_sync.py                # Wise + Monzo sync, clean, dedup

# Ancillary
python scripts/amazon_load.py              # Amazon order history matching
python scripts/load_ibank_categories.py    # Category taxonomy from iBank
```

## Daily Sync Container

Podman container running on the NAS (192.168.128.9). Runs the Monzo auth server persistently on port 9876 and syncs Wise + Monzo transactions daily at 3am via systemd timer.

```bash
# Build and run
./deploy/run.sh

# Manual sync
podman exec finance-sync python scripts/daily_sync.py

# Install systemd timer
cp deploy/finance-sync.{service,timer} /etc/systemd/system/
systemctl enable --now finance-sync.timer
```

Monzo re-authentication available at `https://finance.mees.st/` from any LAN device.

## Project Structure

```
config/
    settings.py          # Pydantic settings from .env
src/
    ingestion/
        monzo.py         # Monzo OAuth + transaction fetcher
        monzo_auth.py    # Persistent Monzo auth server for container
        wise.py          # Wise API client (activities, card, transfer detail)
        wise_fx.py       # FX event builder for Wise statements
        writer.py        # Raw layer writer (idempotent)
    cleaning/
        rules.py         # Institution-specific cleaning rules
        processor.py     # Batch cleaning pipeline
        matcher.py       # Canonical merchant matching (exact/prefix/fuzzy)
    dedup/
        config.py        # Source priorities, supersession, cross-source pairs
        matcher.py       # Dedup matching engine
    api/
        app.py           # FastAPI app + lifespan + CORS
        deps.py          # Connection pool + get_conn dependency
        models.py        # Pydantic response models
        routers/         # transactions, accounts, categories, merchants, stats
scripts/                 # CLI entry points + daily_sync orchestrator
deploy/
    run.sh               # Podman build + run
    finance-sync.service # Systemd oneshot for daily sync
    finance-sync.timer   # 3am daily trigger
ui/                      # React + Vite + TypeScript
Containerfile            # Podman/Docker build
```

## License

MIT
