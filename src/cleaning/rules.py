"""Merchant string cleaning rules.

Rules are configuration, not hardcoded logic. Each rule is applied in sequence.
New rules can be added without code changes — just extend the RULES list.
"""

import re
from typing import List, Tuple

# Cleaning rules applied in order. Each rule has:
#   institution: which institution this applies to ("*" = all)
#   type: the cleaning operation
#   + type-specific parameters
RULES = [
    # --- Phase 1: Institution-specific prefix strips ---

    # Monzo: strip pot references (pot_0000...)
    {"institution": "monzo", "type": "regex_replace",
     "pattern": r"^pot_[A-Za-z0-9]+$", "replacement": "Monzo Pot Transfer"},

    # Monzo: normalise internal transfers (MONZO-XXXXX, Monzo-XXXXX)
    {"institution": "monzo", "type": "regex_replace",
     "pattern": r"^(?:MONZO|Monzo)-[A-Z0-9]+$", "replacement": "Monzo Transfer"},

    # Wise: strip directional prefixes
    {"institution": "wise", "type": "prefix_strip",
     "prefixes": ["OUT ", "IN ", "Transfer to ", "Transfer from "]},

    # First Direct: strip BACS prefixes
    {"institution": "first_direct", "type": "prefix_strip",
     "prefixes": ["BACS CREDIT ", "BACS DEBIT ", "FASTER PAYMENTS RECEIPT ",
                   "FASTER PAYMENTS ", "STANDING ORDER "]},

    # First Direct: strip INTL CARD / INT'L + numeric reference prefix
    # e.g. "INTL CARD 05098008 ANC*ANCESTRY.CO.UK DUBLIN IE" -> "ANC*ANCESTRY.CO.UK DUBLIN IE"
    # e.g. "INT'L 1535859410 ANC*ANCESTRY.CO.UK800-404-9723" -> "ANC*ANCESTRY.CO.UK800-404-9723"
    {"institution": "first_direct", "type": "regex_strip",
     "pattern": r"^(?:INTL CARD|INT'L|NON-STERLING)\s+\d+\s+"},

    # --- Phase 2: Payment processor prefix strip (after institution prefixes removed) ---

    # Strip SumUp/iZettle payment processor prefix
    # e.g. "SumUp  *Billy Doran    Dublin        IRL" -> "Billy Doran    Dublin        IRL"
    # e.g. "SumUp *Albury MusGuildford" -> "Albury MusGuildford"
    {"institution": "*", "type": "regex_strip",
     "pattern": r"^(?:SumUp|SUMUP|iZ)\s*\*+\s*"},

    # --- Phase 3: Location suffix strips ---

    # Monzo: strip location suffix (2+ spaces followed by city/country)
    {"institution": "monzo", "type": "regex_strip", "pattern": r"\s{2,}.*$"},

    # First Direct Visa (CSV): strip location + country code suffix
    # e.g. "PAYPAL *OCADORETAIL    35314369001   GB" -> "PAYPAL *OCADORETAIL"
    # Matches: 2+ spaces, then anything, then space(s), then 2-letter country code at end
    {"institution": "first_direct", "type": "regex_strip",
     "pattern": r"\s{2,}.+\s[A-Z]{2}\s*$"},

    # First Direct Visa (Bankivity): strip trailing " <City> <CC>" with single spaces
    # e.g. "ANC*ANCESTRY.CO.UK DUBLIN IE" -> "ANC*ANCESTRY.CO.UK"
    # Only fires on card-descriptor strings (containing *) to avoid false positives
    # on strings like "Albury Village Store Guildford GB"
    {"institution": "first_direct", "type": "regex_strip",
     "pattern": r"(?<=\S)\s+[A-Z][a-zA-Z.-]+(?:\s+[A-Z][a-zA-Z.-]+)?\s+[A-Z]{2}\s*$",
     "guard": r"\*"},

    # General: collapse multiple spaces to single
    {"institution": "*", "type": "normalise_whitespace"},

    # General: strip leading/trailing whitespace
    {"institution": "*", "type": "strip"},
]

CLEANING_VERSION = "1.0"


def clean_merchant(raw_merchant: str, institution: str) -> Tuple[str, List[str]]:
    """Apply cleaning rules to a raw merchant string.

    Returns (cleaned_string, list_of_rule_names_applied).
    """
    if not raw_merchant:
        return ("", [])

    result = raw_merchant
    applied = []

    for rule in RULES:
        rule_inst = rule["institution"]
        if rule_inst != "*" and rule_inst != institution:
            continue

        before = result
        rule_type = rule["type"]

        if rule_type == "regex_strip":
            if "guard" in rule and not re.search(rule["guard"], result):
                continue
            result = re.sub(rule["pattern"], "", result)

        elif rule_type == "regex_replace":
            result = re.sub(rule["pattern"], rule["replacement"], result)

        elif rule_type == "prefix_strip":
            for prefix in rule["prefixes"]:
                if result.startswith(prefix):
                    result = result[len(prefix):]
                    break

        elif rule_type == "normalise_whitespace":
            result = re.sub(r"\s+", " ", result)

        elif rule_type == "strip":
            result = result.strip()

        if result != before:
            applied.append(f"{rule_type}:{rule_inst}")

    return (result, applied)
