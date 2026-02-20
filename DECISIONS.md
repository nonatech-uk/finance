# Finance System - Architectural Decisions

*Handoff document for Claude Code. Read this and SCHEMA.md before touching any code.*

---

## What We're Building

A personal finance system to replace Bankivity (iBank). Motivations:
- Bankivity bank links partially broken
- Auto-categorisation is poor
- Performance struggles with 14 years of flat-file data
- Reporting is rigid
- No intelligent alerting or forecasting

Stack: Python, FastAPI, Postgres (on NAS), web UI. Developing locally at `/Users/stu/Documents/Code/finance`, deploying to NAS via Docker.

---

## The Single Most Important Decision

**The raw transaction layer is immutable and append-only. It is the source of truth. It is never modified after ingestion.**

Everything above it is a projection derived from raw data. If cleaning rules improve, reprocess from raw. If categorisation changes, reprocess from raw. A raw record, once written, is never touched - not even by a corrected feed from the bank.

This is event sourcing applied to personal finance. The raw transactions are the events. Current state is always a projection.

---

## Architecture - Modules

```
raw_transaction         immutable source of truth
    │
cleaned_transaction     rule-based merchant cleaning (reproducible)
    │
canonical_merchant      virtual normalisation layer (query-time lookup)
    │
economic_event          links related transactions (transfers, FX, fees)
    │
transaction_category    categorisation with provenance
    │
tag                     orthogonal reporting dimension
    │
recurring_pattern       detected regular payments
    │
alert                   anomaly detection and notifications
```

Plus infrastructure modules:
- **Import service**: Open Banking ingestion + file watcher for CSV/OFX
- **Categorisation engine**: LLM-assisted, with merchant memory table
- **Alerting/forecasting/budgeting**: Pattern learning, predictions
- **Reporting**: Flexible, tag and category aware
- **Export/integration**: Xero sync (future), HMRC MTD (future)

All modules talk to a **core data API**. The UI is just another client.

---

## Key Architectural Decisions

### 1. Merchant normalisation is a virtual layer

Raw merchant strings are never modified. The `merchant_raw_mapping` table maps cleaned strings to canonical names. Resolution happens at query/display time, not at ingestion time.

Consequence: fixing a merchant name mapping instantly fixes every historical transaction with that raw string. No data migration needed.

### 2. Two eras of data

**Pre-migration (Bankivity)**: 12 years of data from 2014-2026. Ingested with `source = 'bankivity_migration'` and `is_dirty = true`. Merchant strings are semi-canonical - Bankivity allowed editing them, so true raw strings are lost. Accept the limitation, don't try to re-derive originals.

**Post-migration (Open Banking)**: True raw strings from bank feeds. Full schema capabilities apply. `is_dirty = false`.

### 3. Economic events link what banks separate

A Wise FX transfer creates multiple raw transactions (debit one account, credit another, possibly a fee). These are economically one event. The `economic_event` table groups them.

Matching is semi-automated with a manual reconciliation queue. There aren't enough cross-account transactions to justify complex auto-matching - keep the algorithm simple and let the UI handle edge cases.

### 4. Fees belong to their parent event, not a standalone category

A Visa FX fee is a leg of the economic event (`leg_type = 'fx_fee'`), not a separately categorised transaction. This prevents double-counting while preserving fee visibility. The `fx_event` table captures achieved rate vs mid-market for FX cost reporting.

### 5. Tags are first-class citizens

Tags are orthogonal to categories. A transaction can be `Skiing:Eating Out` (category) and `Zermatt-2025-January` (tag) simultaneously. Reporting can pivot on either. Tags must be designed in from the start - they cannot be bolted on later without schema pain.

### 6. Categorisation engine is separate from ingestion

The import pipeline calls a categorisation service. This allows re-categorisation of historical data, batch jobs, and model swaps without touching ingestion logic.

Categorisation hierarchy:
1. Exact match in merchant override table → apply stored category
2. Fuzzy match against previously confirmed transactions
3. LLM categorisation with taxonomy as context
4. Flag low-confidence results for human review
5. Human correction writes back to merchant memory table

Method and confidence are stored on every categorisation. Override flag is the training signal.

### 7. Scheduled pulls, not always-on

Import service is cron-triggered, not a long-running daemon. Wakes up, does work, exits. Simpler to operate and monitor.

**Exception**: Monzo supports webhooks - real-time notification per transaction. Use both: webhooks for real-time alerting, scheduled reconciliation poll daily to catch anything missed.

### 8. Postgres, not SQLite

Data volumes are modest (~200k rows max) and SQLite would technically work. Postgres chosen for: JSONB support for raw_data column, proper TIMESTAMPTZ precision, better complex join handling, and zero additional operational burden running in Docker on NAS.

---

## Institutions

| Institution | Method | Notes |
|-------------|--------|-------|
| Monzo | Direct API + webhooks | Best API, developer-friendly, webhooks for real-time |
| Wise | Direct API | Use their API not an aggregator - gives structured FX data |
| First Direct | TrueLayer or Nordigen | Standard Open Banking |
| Hargreaves Lansdown | CSV export | No Open Banking for investment platforms |
| Fidelity | CSV export | Same |
| Computershare | CSV export | Same |
| TruePotential | CSV export | Same |

Investment platforms are not covered by PSD2. File-based import only.

---

## Cleaning Rules (Known)

Institution-specific prefix stripping before canonical lookup:

- **Wise**: Strip `OUT `, `IN `, `Transfer to `, `Transfer from `
- **Visa/Mastercard**: Strip trailing ` GUILDFORD GB`, ` LONDON GB` etc.
- **First Direct**: Normalise `BACS CREDIT` prefixes

Cleaning rules must be **configuration, not hardcoded logic**. New rules should be addable without code changes.

---

## Data Model for Raw Payload

Every raw transaction stores the complete original API response in `raw_data JSONB`. Nothing is discarded at ingestion time. If a field isn't understood yet, it's still there for later.

---

## Unix Philosophy Check

Design was validated against Unix Philosophy principles. Two areas needing active discipline:

1. **Mechanism vs policy**: Cleaning rules, alert thresholds, and categorisation rules must be configuration/data, not hardcoded. The engine is the mechanism; the rules are the policy.

2. **Failure transparency**: Open Banking OAuth token lifecycle fails silently without instrumentation. `ob_connection.error_count` and `last_error` are a start. Import pipeline must log failures clearly and surface them via the alert system.

---

## Future Extensions (Designed For, Not Yet Built)

- **Xero sync**: Push categorised transactions to Xero for business accounting. One-way only - this system is source of truth. Tag-driven (`xero-reimbursable` etc.)
- **HMRC Making Tax Digital**: Natural extension of Xero integration
- **Cloudflare Tunnel**: For Monzo webhook endpoint. Already planned.
- **Forecasting**: Statistical decomposition (fixed/variable/seasonal) for numerical prediction. LLM for narrative explanation of forecasts. 12 years of history is a significant asset here.

---

## Was created in Claude but not downloaded

- `scripts/create_schema.sql` - raw_transaction table with idempotency index
- `src/ingestion/monzo.py` - OAuth flow + bulk transaction fetcher
- `src/ingestion/writer.py` - raw layer writer, idempotent
- `scripts/monzo_bulk_load.py` - entry point, run this first
- `config/settings.py` - Pydantic settings from .env
- `SCHEMA.md` - full schema design with all tables

## Immediate Next Step

Run the Monzo bulk loader. Get real data into Postgres. Validate the schema against live data before building anything else.

```bash
python scripts/monzo_bulk_load.py
```

---

*See SCHEMA.md for the full data model with SQL and example queries.*
