"""Monzo CSV data export parser.

Parses the CSV export from Monzo (Settings > Data Export) into
normalised transaction dicts suitable for reconciliation or import.

CSV columns:
    Transaction ID, Date, Time, Type, Name, Emoji, Category, Amount,
    Currency, Local amount, Local currency, Notes and #tags, Address,
    Receipt, Description, Category split, Money Out, Money In
"""

import csv
import io
from decimal import Decimal, InvalidOperation


def parse_monzo_csv(file_bytes: bytes, account_ref: str) -> list[dict]:
    """Parse Monzo CSV export bytes into transaction dicts.

    Args:
        file_bytes: Raw CSV file content.
        account_ref: The Monzo account_ref (e.g. acc_0000...) to tag transactions with.

    Returns:
        List of dicts with keys matching the raw_transaction schema.
    """
    text = file_bytes.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))

    txns = []
    for row in reader:
        txn_id = row.get("Transaction ID", "").strip()
        if not txn_id:
            continue

        date_str = row.get("Date", "").strip()
        if not date_str:
            continue

        # Parse DD/MM/YYYY -> YYYY-MM-DD
        parts = date_str.split("/")
        if len(parts) == 3:
            posted_at = f"{parts[2]}-{parts[1]}-{parts[0]}"
        else:
            continue

        # Amount: prefer Money Out / Money In columns, fall back to Amount
        money_out = row.get("Money Out", "").strip()
        money_in = row.get("Money In", "").strip()
        try:
            if money_out:
                amount = -abs(Decimal(money_out))
            elif money_in:
                amount = abs(Decimal(money_in))
            else:
                amount = Decimal(row.get("Amount", "0").strip())
        except InvalidOperation:
            continue

        currency = row.get("Currency", "GBP").strip()
        name = row.get("Name", "").strip()
        description = row.get("Description", "").strip()
        merchant = name or description

        raw_data = {k: v for k, v in row.items()}

        txns.append({
            "source": "monzo_csv",
            "institution": "monzo",
            "account_ref": account_ref,
            "transaction_ref": txn_id,
            "posted_at": posted_at,
            "amount": amount,
            "currency": currency,
            "raw_merchant": merchant,
            "raw_memo": row.get("Notes and #tags") or None,
            "raw_data": raw_data,
        })

    return txns
