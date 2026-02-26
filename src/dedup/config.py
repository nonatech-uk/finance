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
    # Wise — CSV is authoritative; supersede both iBank and API
    # (iBank categories copied to matching CSV raw_data before supersession)
    {
        "institution": "wise",
        "account_ref": "wise_CHF",
        "superseded_source": "ibank",
    },
    {
        "institution": "wise",
        "account_ref": "wise_CHF",
        "superseded_source": "wise_api",
    },
    {
        "institution": "wise",
        "account_ref": "wise_EUR",
        "superseded_source": "ibank",
    },
    {
        "institution": "wise",
        "account_ref": "wise_EUR",
        "superseded_source": "wise_api",
    },
    {
        "institution": "wise",
        "account_ref": "wise_GBP",
        "superseded_source": "ibank",
    },
    {
        "institution": "wise",
        "account_ref": "wise_GBP",
        "superseded_source": "wise_api",
    },
    {
        "institution": "wise",
        "account_ref": "wise_PLN",
        "superseded_source": "ibank",
    },
    {
        "institution": "wise",
        "account_ref": "wise_PLN",
        "superseded_source": "wise_api",
    },
    {
        "institution": "wise",
        "account_ref": "wise_USD",
        "superseded_source": "ibank",
    },
    {
        "institution": "wise",
        "account_ref": "wise_USD",
        "superseded_source": "wise_api",
    },
    {
        "institution": "wise",
        "account_ref": "wise_NOK",
        "superseded_source": "wise_api",
    },
    {
        "institution": "wise",
        "account_ref": "wise_SEK",
        "superseded_source": "wise_api",
    },
]


# Cross-source pairs to check for each institution/account.
# All Wise sources handled by SOURCE_SUPERSEDED (CSV is sole authority).
CROSS_SOURCE_PAIRS = [
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
