"""Deduplication configuration.

Source priorities and cross-source pair definitions.
Rules-as-configuration, not hardcoded logic.
"""

# Lower number = higher priority = preferred in dedup groups
SOURCE_PRIORITY = {
    "monzo_api": 1,
    "wise_api": 1,
    "first_direct_csv": 2,
    "wise_csv": 2,
    "ibank": 3,
}


def get_priority(source: str) -> int:
    """Get source priority (lower = better). Unknown sources get 99."""
    return SOURCE_PRIORITY.get(source, 99)


# Cross-source pairs to check for each institution/account.
# Order matters for Wise: api↔csv first, then ibank against grouped records.
CROSS_SOURCE_PAIRS = [
    # First Direct
    {
        "institution": "first_direct",
        "account_ref": "fd_5682",
        "pairs": [("first_direct_csv", "ibank")],
    },
    {
        "institution": "first_direct",
        "account_ref": "fd_8897",
        "pairs": [("first_direct_csv", "ibank")],
    },
    # Monzo (iBank uses monzo_current alias)
    {
        "institution": "monzo",
        "account_ref": "acc_00009cSZpPQxiG2CFWlPjF",
        "pairs": [("monzo_api", "ibank")],
    },
    # Wise — pairwise processing: api↔csv first, then ibank
    {
        "institution": "wise",
        "account_ref": "wise_CHF",
        "pairs": [("wise_api", "wise_csv"), ("wise_api", "ibank"), ("wise_csv", "ibank")],
    },
    {
        "institution": "wise",
        "account_ref": "wise_EUR",
        "pairs": [("wise_api", "wise_csv"), ("wise_api", "ibank"), ("wise_csv", "ibank")],
    },
    {
        "institution": "wise",
        "account_ref": "wise_GBP",
        "pairs": [("wise_api", "wise_csv"), ("wise_api", "ibank"), ("wise_csv", "ibank")],
    },
    {
        "institution": "wise",
        "account_ref": "wise_USD",
        "pairs": [("wise_api", "wise_csv"), ("wise_api", "ibank"), ("wise_csv", "ibank")],
    },
    {
        "institution": "wise",
        "account_ref": "wise_PLN",
        "pairs": [("wise_api", "wise_csv"), ("wise_api", "ibank"), ("wise_csv", "ibank")],
    },
]
