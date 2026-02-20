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
| Monzo | Direct API (OAuth + webhooks) | ~3,600 |
| Wise | Activities API + CSV | ~860 |
| First Direct | CSV export | ~3,300 |
| iBank (Bankivity) | Historical migration | ~17,500 |
| Amazon | Order history CSV matching | ~2,000 items |

## Deduplication

The same real transaction often appears in multiple sources (e.g. a Wise payment exists in wise_api, wise_csv, and ibank). The dedup pipeline groups these into `dedup_group` records without modifying raw data. The `active_transaction` view provides a deduplicated lens over the data.

## Stack

- **Python 3.12+**
- **PostgreSQL** (on NAS, via Docker)
- **FastAPI** (REST API, planned)
- **psycopg2** (sync DB access with connection pool)

## Setup

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill in DB credentials and API tokens
```

## Scripts

```bash
# Data ingestion
python scripts/monzo_bulk_load.py           # Monzo API → raw_transaction
python scripts/wise_bulk_load.py            # Wise Activities API → raw_transaction
python scripts/wise_csv_load.py             # Wise CSV export → raw_transaction
python scripts/fd_csv_load.py               # First Direct CSV → raw_transaction
python scripts/load_ibank_transactions.py   # iBank/Bankivity migration

# Processing pipeline
python scripts/run_cleaning.py              # Apply merchant cleaning rules
python scripts/run_dedup.py                 # Cross-source deduplication
python scripts/run_dedup.py --stats         # View dedup statistics

# Ancillary
python scripts/amazon_load.py              # Amazon order history matching
python scripts/load_ibank_categories.py    # Category taxonomy from iBank
```

## Project Structure

```
config/
    settings.py          # Pydantic settings from .env
src/
    ingestion/
        monzo.py         # Monzo OAuth + transaction fetcher
        wise.py          # Wise API client (activities, card, transfer detail)
        wise_fx.py       # FX event builder for Wise statements
        writer.py        # Raw layer writer (idempotent)
    cleaning/
        rules.py         # Institution-specific cleaning rules
        processor.py     # Batch cleaning pipeline
        matcher.py       # Canonical merchant matching (exact/prefix/fuzzy)
    dedup/
        config.py        # Source priorities, cross-source pair definitions
        matcher.py       # Dedup matching engine
scripts/                 # CLI entry points
```

## License

MIT
