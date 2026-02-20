# CLAUDE.md - Project State & Handoff

*Read this file first. Read DECISIONS.md and SCHEMA.md for architecture. This file captures current state for session continuity.*

---

## Quick Start

```bash
cd /Users/stu/Documents/Code/finance

# Activate venv (Python 3.12.12)
source .venv/bin/activate

# Start API (from project root, not ui/)
uvicorn src.api.app:app --host 0.0.0.0 --port 8000

# Start UI (separate terminal)
cd ui && npm run dev   # runs on :5173, proxies /api -> :8000
```

**DB**: PostgreSQL on `192.168.128.9:5432/finance` (credentials in `config/.env`)
**GitHub**: `git@github.com:nonatech-uk/finance.git`
**Owner**: Stuart Bevan

---

## What This Is

Personal finance system replacing Bankivity (iBank). Event-sourced: `raw_transaction` is immutable append-only source of truth. Everything above is a projection.

**Stack**: Python 3.12, FastAPI, psycopg2, PostgreSQL, React + Vite + TypeScript + Tailwind v4 + TanStack Query + Recharts.

---

## Current State (as of Feb 2026)

### Git History (3 commits on main, pushed)
1. `1d9adcf` - Initial commit: data pipeline + loaders (29 files, 4,244 lines)
2. `32a0938` - FastAPI REST API (12 files, 975 lines)
3. `71e42b3` - React UI (40 files, 6,060 lines)

### Database Counts
- `raw_transaction`: 25,519 rows
- `active_transaction` (view): 23,002 rows (2,517 removed by dedup)
- `dedup_group`: 2,420 groups (4,937 members)
- `cleaned_transaction`: 25,519 rows
- `canonical_merchant`: 3,891
- `merchant_raw_mapping`: 4,852
- `category`: 136
- `amazon_order_item`: 1,996

### Data Sources Loaded
| Source | Count | Notes |
|--------|-------|-------|
| ibank | 17,550 | Bankivity migration, `is_dirty=true`, covers 2014-2026 |
| monzo_api | 3,798 | Direct API, full history |
| first_direct_csv | 3,315 | Bank CSV export, covers 2020-02 to 2026-02 **with gap** |
| wise_api | 610 | Direct API |
| wise_csv | 246 | CSV supplement |

### Institutions (19)
aegon, cash, citi, computershare, fidelity, first_direct, goldman_sachs, hl, monzo, ns_and_i, octopus, other, property, puma_vct, scottish_widows, standard_life, swiss_bank, vehicle, wise

---

## ACTIVE BUG: Account 5682 Balance Incorrect (Dedup Gap)

**Problem**: The `fd_5682` (First Direct current account) shows balance of £68,634 in the UI. The CSV alone sums to £9,959. The inflated balance comes from un-deduped iBank transactions appearing alongside CSV transactions.

**Root Cause**: The First Direct CSV has a **12-month gap** (March 2024 to January 2025). During the overlap period where both CSV and iBank data exist, 7,234 iBank transactions were not matched by the dedup pipeline. Breakdown:

- **2014-2019**: iBank only (no CSV) — these are correct, no dedup needed
- **2020-02 to 2024-02**: CSV + iBank overlap — dedup matched ~1,582 pairs correctly
- **2024-03 to 2025-01**: CSV gap — iBank only, correct to include
- **2025-02 to 2026-02**: CSV + iBank overlap — dedup matched remaining pairs

The cross-source dedup matcher (`src/dedup/matcher.py`) uses `find_cross_source_duplicates()` which matches on `(posted_at, amount, currency)` with ROW_NUMBER positional matching. The filter on line 77-81 excludes records that are already non-preferred members of another group — **but it also excludes records that are already preferred members**. This means if an iBank txn was already marked as preferred in an iBank_internal dedup group, it won't be picked up by the cross-source matcher.

**Investigation so far** (done in this session):
- Confirmed the CSV covers 2020-02 to 2026-02 with the Mar2024-Jan2025 gap
- On dates where both sources exist (e.g., 2025-11-10), dedup works perfectly — all 15 pairs matched
- 7,234 iBank txns are unmatched and in the active view, inflating the balance by ~£58,675
- The unmatched iBank txns span all years 2014-2026 (not just the gap period)
- Years 2014-2019 unmatched iBank is EXPECTED (no CSV existed)
- Years 2020-2023 + 2025-2026 have varying overlap — need deeper investigation

**Next step**: Investigate why matched txns in the overlap period aren't all being deduped. Likely causes:
1. The `NOT EXISTS` filter in `find_cross_source_duplicates` may be too aggressive (excludes already-grouped preferred records)
2. The iBank internal dedup runs FIRST, grouping some iBank records, which then get skipped by cross-source matching
3. There may be genuinely different transactions in iBank not in CSV (account covers card transactions, standing orders etc. that CSV might miss)

**To reproduce**:
```python
# Run in project root with venv activated
import psycopg2
from config.settings import Settings
s = Settings()
conn = psycopg2.connect(host=s.db_host, port=s.db_port, dbname=s.db_name, user=s.db_user, password=s.db_password)
cur = conn.cursor()
cur.execute("SELECT source, COUNT(*), SUM(amount) FROM active_transaction WHERE account_ref = 'fd_5682' GROUP BY source")
print(cur.fetchall())
# Expected: CSV ~£9,959, iBank ~£58,675 (should be much less after proper dedup)
```

---

## Architecture Overview

### Data Pipeline
```
Bank APIs / CSVs / iBank export
        ↓
  scripts/*_load.py        → raw_transaction (immutable, append-only)
        ↓
  scripts/run_cleaning.py  → cleaned_transaction + merchant_raw_mapping + canonical_merchant
        ↓
  scripts/run_dedup.py     → dedup_group + dedup_group_member
        ↓
  active_transaction VIEW  → excludes non-preferred dedup group members
        ↓
  FastAPI REST API          → src/api/
        ↓
  React UI                  → ui/
```

### Key JOIN Chain (for displaying categorised transactions)
```sql
active_transaction
  → cleaned_transaction (ON id)
  → merchant_raw_mapping (ON cleaned_merchant)
  → canonical_merchant (ON canonical_merchant_id)
  → category (ON category_hint)
```

### Dedup Logic
- **Rule 1** `ibank_internal`: Same source, same (date, amount, currency, merchant) — runs first
- **Rule 2** `cross_source_date_amount`: Different sources, same (institution, account_ref, date, amount, currency) — uses ROW_NUMBER positional matching for same-day same-amount pairs
- Source priority: monzo_api/wise_api (1) > first_direct_csv/wise_csv (2) > ibank (3)
- Config in `src/dedup/config.py`, matcher in `src/dedup/matcher.py`

### active_transaction VIEW
```sql
SELECT * FROM raw_transaction rt
WHERE NOT EXISTS (
  SELECT 1 FROM dedup_group_member dgm
  WHERE dgm.raw_transaction_id = rt.id AND NOT dgm.is_preferred
);
```
Includes: all non-grouped txns + preferred members of each group. Excludes: non-preferred (duplicate) members.

---

## API Endpoints (FastAPI on :8000)

All under `/api/v1/`:

| Method | Path | Notes |
|--------|------|-------|
| GET | /transactions | Cursor-paginated (keyset on posted_at DESC, id DESC), 12 filter params |
| GET | /transactions/{id} | Full detail + dedup group + economic event |
| GET | /accounts | Derived from active_transaction GROUP BY institution/account_ref/currency |
| GET | /accounts/{institution}/{account_ref} | Summary + recent txns |
| GET | /categories | Recursive tree |
| GET | /categories/spending | Aggregated with date range + currency filters |
| GET | /merchants | Cursor-paginated, search + unmapped filter |
| PUT | /merchants/{id}/mapping | Only write endpoint — sets category_hint |
| GET | /stats/monthly | Income/expense by month |
| GET | /stats/overview | Dashboard summary stats |
| GET | /health | Pool status |

### API Notes
- Sync psycopg2 with `ThreadedConnectionPool` (not async)
- Pool managed via FastAPI lifespan context manager
- CORS allows all origins (personal tool)
- `src/api/app.py` adds project root to sys.path — must run from project root

---

## UI (React on :5173)

### Pages (all verified working with live data)
- **Dashboard**: 4 stat cards, 12-month income/expense BarChart, top spending categories
- **Transactions**: Debounced search, institution/currency/date filters, paginated table, click-to-open detail slide-over (merchant chain, dedup group, raw JSON)
- **Accounts**: Grouped by institution, balance cards, links to AccountDetail
- **AccountDetail**: Stats + recent transactions for one account
- **Categories**: Left=recursive tree with expand/collapse, Right=date range + spending chart + table
- **Merchants**: Search, "Unmapped only" checkbox, category assignment via `<select>`

### UI Stack
- Vite + React 19 + TypeScript
- Tailwind CSS v4 (CSS-first config, `@theme` block in index.css)
- TanStack Query (useInfiniteQuery for cursor pagination, useMutation for merchant mapping)
- Recharts for charts
- Dark theme with custom CSS vars (bg-primary: #0a0a0f etc.)
- Proxy: `/api` → `http://localhost:8000`

### Build
```bash
cd ui && npm run build  # 643kb JS bundle, zero TS errors
```

---

## File Layout

```
finance/
├── config/
│   ├── .env                    # DB credentials (gitignored)
│   ├── settings.py             # Pydantic Settings (db, api, pool)
│   └── cleaning_rules.json     # Merchant cleaning rules
├── scripts/
│   ├── wise_bulk_load.py       # Wise API → raw_transaction
│   ├── wise_csv_load.py        # Wise CSV → raw_transaction
│   ├── fd_csv_load.py          # First Direct CSV → raw_transaction
│   ├── monzo_bulk_load.py      # Monzo API → raw_transaction
│   ├── load_ibank_transactions.py  # iBank/Bankivity migration
│   ├── load_ibank_categories.py    # iBank category import
│   ├── amazon_load.py          # Amazon order history
│   ├── run_cleaning.py         # Merchant cleaning pipeline
│   └── run_dedup.py            # Deduplication pipeline
├── src/
│   ├── ingestion/
│   │   ├── writer.py           # Idempotent raw layer writer
│   │   ├── monzo.py            # Monzo OAuth + API client
│   │   ├── wise.py             # Wise API client
│   │   └── wise_fx.py          # Wise FX rate enrichment
│   ├── cleaning/
│   │   ├── processor.py        # Cleaning pipeline orchestrator
│   │   ├── rules.py            # Rule engine
│   │   └── matcher.py          # Fuzzy merchant matching
│   ├── dedup/
│   │   ├── config.py           # Source priorities + cross-source pairs
│   │   └── matcher.py          # Dedup matching + group creation
│   └── api/
│       ├── app.py              # FastAPI app + lifespan + CORS
│       ├── deps.py             # Connection pool + get_conn dependency
│       ├── models.py           # Pydantic response models
│       └── routers/
│           ├── transactions.py
│           ├── accounts.py
│           ├── categories.py
│           ├── merchants.py
│           └── stats.py
├── ui/                         # React + Vite + TypeScript
│   ├── vite.config.ts          # Proxy + Tailwind plugin
│   └── src/
│       ├── api/                # Types, client, domain modules
│       ├── hooks/              # TanStack Query hooks
│       ├── components/         # common/ (5) + layout/ (2)
│       └── pages/              # Dashboard, Transactions, Accounts,
│                               # AccountDetail, Categories, Merchants
├── .wise                       # Wise API token (gitignored)
├── DECISIONS.md                # Architecture & design decisions
├── SCHEMA.md                   # Full database schema
├── README.md                   # Project overview
└── LICENSE                     # MIT, Stuart Bevan
```

---

## Credentials & Secrets

- **DB**: See `config/.env` (gitignored)
- **Wise API token**: `.wise` file (gitignored)
- **Monzo client ID**: `oauth2client_0000B3VKN1klMnFavWSOCw` (in monzo.py)
- **GitHub**: `git@github.com:nonatech-uk/finance.git`

---

## What To Do Next

**Immediate**: Fix the dedup pipeline for `fd_5682` (see ACTIVE BUG section above). This is the highest priority data quality issue.

**Then**: General data cleanup phase:
1. Re-run dedup with improved matching (may need to `reset_groups()` and re-run)
2. Check other accounts for similar balance discrepancies
3. Review unmatched transactions to separate genuine uniques from missed duplicates
4. Consider adding merchant-name fuzzy matching as a secondary dedup signal

**Future** (from DECISIONS.md):
- Inter-account transfer matching (economic events)
- LLM-assisted categorisation
- Docker deployment to NAS
- Monzo webhook endpoint via Cloudflare Tunnel
- Recurring pattern detection
- Alerting & forecasting
