"""Deduplication configuration.

Source priorities and cross-source pair definitions.
Rules-as-configuration, not hardcoded logic.
"""

# Lower number = higher priority = preferred in dedup groups
SOURCE_PRIORITY = {
    "monzo_api": 1,
    "wise_api": 1,
    "first_direct_bankivity": 1,
    "first_direct_csv": 2,
    "first_direct_pdf": 2,
    "wise_csv": 2,
    "marcus_csv": 2,
    "ibank": 3,
}


def get_priority(source: str) -> int:
    """Get source priority (lower = better). Unknown sources get 99."""
    return SOURCE_PRIORITY.get(source, 99)


# Source supersession: for these accounts, the superseded source is
# completely discarded — every transaction from it is marked non-preferred.
# Use when the preferred source is authoritative (e.g. bank CSV with
# running balances) and the superseded source is unreliable.
SOURCE_SUPERSEDED = [
    {
        "institution": "first_direct",
        "account_ref": "fd_5682",
        "superseded_source": "ibank",
    },
    {
        "institution": "first_direct",
        "account_ref": "fd_8897",
        "superseded_source": "ibank",
    },
    {
        "institution": "first_direct",
        "account_ref": "fd_8897",
        "superseded_source": "first_direct_csv",
    },
    {
        "institution": "monzo",
        "account_ref": "monzo_current",
        "superseded_source": "ibank",
    },
    {
        "institution": "monzo",
        "account_ref": "monzo_mees_pot",
        "superseded_source": "ibank",
    },
    {
        "institution": "goldman_sachs",
        "account_ref": "marcus",
        "superseded_source": "ibank",
    },
    # Wise — iBank superseded; API kept (daily sync source).
    # Where CSV and API overlap, cross_source_date_amount dedup handles it.
    {
        "institution": "wise",
        "account_ref": "wise_CHF",
        "superseded_source": "ibank",
    },
    {
        "institution": "wise",
        "account_ref": "wise_EUR",
        "superseded_source": "ibank",
    },
    {
        "institution": "wise",
        "account_ref": "wise_GBP",
        "superseded_source": "ibank",
    },
    {
        "institution": "wise",
        "account_ref": "wise_PLN",
        "superseded_source": "ibank",
    },
    {
        "institution": "wise",
        "account_ref": "wise_USD",
        "superseded_source": "ibank",
    },
]


# Cross-source pairs to check for each institution/account.
CROSS_SOURCE_PAIRS = [
    # Wise — API vs CSV overlap (CSV preferred via SOURCE_PRIORITY)
    {
        "institution": "wise",
        "account_ref": "wise_CHF",
        "pairs": [("wise_csv", "wise_api")],
    },
    {
        "institution": "wise",
        "account_ref": "wise_EUR",
        "pairs": [("wise_csv", "wise_api")],
    },
    {
        "institution": "wise",
        "account_ref": "wise_GBP",
        "pairs": [("wise_csv", "wise_api")],
    },
    {
        "institution": "wise",
        "account_ref": "wise_PLN",
        "pairs": [("wise_csv", "wise_api")],
    },
    {
        "institution": "wise",
        "account_ref": "wise_USD",
        "pairs": [("wise_csv", "wise_api")],
    },
    # First Direct — Bankivity (Salt Edge) vs existing sources
    {
        "institution": "first_direct",
        "account_ref": "fd_5682",
        "pairs": [
            ("first_direct_bankivity", "first_direct_csv"),
        ],
    },
    {
        "institution": "first_direct",
        "account_ref": "fd_8897",
        "pairs": [
            ("first_direct_bankivity", "first_direct_pdf"),
        ],
    },
]
