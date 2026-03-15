#!/usr/bin/env python3
"""First Direct bank account PDF statement parser & loader.

Parses current account, savings, and ISA PDF statements and loads into raw_transaction.
Handles 4 format eras spanning 1997-2018.

Supported account types:
  - Sole (Cheque/1st Account, 90245682)
  - Joint (1st Account, 62303469)
  - Vincent Square (Bank Account, 31097849/02621517)
  - ISA (Mini Cash ISA / Cash ISA, 64119061/34860489)
  - Savings (HISA, TESSA, e-Savings, Bonus Savings)

Format eras:
  Era 0: 1997 savings (Debit/Credit columns, MMMDD dates)
  Era 1: pre-2002 paper scans (PAID OUT/PAID IN/BALANCE, DDMMMYY dates)
  Era 2: 2002-2005 scanned (Paid out/Paid in/Balance, DD MMM YY dates)
  Era 3: 2007-2012 internet banking exports (lowercase headers, clean digital)
  Era 4: 2012-2018 modern printed (firstdirect.com header, clean digital)

Usage:
    python scripts/fd_bank_pdf_load.py --dry-run "/path/to/Sole/*.pdf"
    python scripts/fd_bank_pdf_load.py --load "/path/to/Sole/*.pdf" "/path/to/Joint/*.pdf"
    python scripts/fd_bank_pdf_load.py -o transactions.csv "/path/to/Sole/*.pdf"
"""

import argparse
import csv
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from glob import glob
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Account number -> account_ref mapping
ACCOUNT_MAP = {
    '90245682': 'fd_5682',   # Sole
    '62303469': 'fd_3469',   # Joint
    '31097849': 'fd_1517',   # Vincent Square (old number)
    '02621517': 'fd_1517',   # Vincent Square (new number)
    '34860489': 'fd_0489',   # Cash ISA
    '64119061': 'fd_0489',   # Cash ISA (old number)
    '30253200': 'fd_3200',   # HISA
    '12781913': 'fd_1913',   # TESSA / High Interest Savings
    '74272439': 'fd_2439',   # e-Savings
    '04453883': 'fd_3883',   # Bonus Savings
    '62589745': 'fd_9745',   # Direct Interest Savings
    '43804445': 'fd_4445',   # Bonus Savings (older number)
    '90245662': 'fd_5682',   # Sole (OCR variant of 90245682)
}

# Date patterns
ERA1_DATE_RE = re.compile(r'^(\d{2})([A-Z]{3})(\d{2})\b')
ERA234_DATE_RE = re.compile(
    r'^(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{2,4})\b',
    re.IGNORECASE,
)
SAVINGS97_DATE_RE = re.compile(r'^([A-Z]{3})(\d{2})\b')

# Amount in a column region
AMOUNT_RE = re.compile(r'(\d[\d ,]*\.\d{2})')
# European OCR variant: comma as decimal separator
EURO_AMOUNT_RE = re.compile(r'(\d[\d .]*,\d{2})')

# Known FD transaction type codes
TYPE_CODES = {'DD', 'SO', 'CR', 'DR', 'SWT', 'ATM', 'CIR', 'BP', 'TFR',
              'VIS', ')))', 'CHQ', 'CHO', 'CP', 'FPI', 'BAC'}


def has_type_code(text: str) -> bool:
    """Check if text starts with a known FD transaction type code."""
    stripped = text.strip()
    for code in TYPE_CODES:
        if stripped.startswith(code + ' ') or stripped == code:
            return True
    return False


@dataclass
class Transaction:
    posted_at: str       # YYYY-MM-DD
    description: str
    amount: Decimal
    is_credit: bool = False
    extra_details: list = field(default_factory=list)
    account_ref: str = ""
    filename: str = ""


# Lines to skip (checked case-insensitively)
SKIP_TEXTS = [
    'balance brought forward',
    'balance carried forward',
    'sub balance brought forward',
    'account summary',
    'opening balance',
    'closing balance',
    'payments in',
    'payments out',
    'overdraft limit',
    'please refer',
    'first direct is a division',
    'customer information',
    'important information',
    'annual subscription',
    'amount invested',
    'interest rate',
    'firstdirect.com',
    'internet banking - statement',
    'fi r s t d i r e c t',
    'fi r s t   direct',
    'wakefield road',
    'leeds ls98',
    'millshaw park',
    'mr s r bevan',
    'mr stuart',
    'miss frances',
    'ms h a',
    'mrs h',
    'account name',
    'sortcode',
    'sheet number',
    'payment type',
    'your cheque account',
    'your 1st account',
    'your bank account',
    'your mini cash',
    'your cash isa',
    'your cash esa',
    'your cash elsa',
    'your e-savings',
    'your bonus savings',
    'your high interest',
    'branch identifier',
    'international bank',
    'member hsbc',
    'midlgb',
    'hbukgb',
    'page ',
    'https://',
    'jsessionid',
    # Address lines are only skipped when they appear as standalone lines
    # (before transaction section). City names that might appear in merchant
    # descriptions (brighton, guildford, etc.) are NOT skipped here.
    '08 456',
    '03 456',
    '0345 100',
    'date details',
    'date payment',
    'date paym',
    'credit c',
    'summary of charges',
    'charges summary',
]


def should_skip_line(line: str) -> bool:
    """Check if line is boilerplate to skip."""
    stripped = line.strip()
    if not stripped:
        return True
    lower = stripped.lower()
    for text in SKIP_TEXTS:
        if text in lower:
            return True
    # Skip standalone amounts (not on a transaction line)
    if re.match(r'^£?\s*[\d, ]+\.\d{2}\s*[CD]?\s*$', stripped):
        return True
    # Skip standalone percentages
    if re.match(r'^\d+\.\d+%\s*$', stripped):
        return True
    # Skip sort code lines
    if re.match(r'^\s*40-47-\d{2}\s*$', stripped):
        return True
    # Skip lines that are just numbers/codes (barcode artifacts)
    if re.match(r'^[\d\s/]+$', stripped) and len(stripped) > 4:
        return True
    return False


def detect_era(text: str) -> int:
    """Detect format era from PDF text content."""
    # Era 0: 1997 savings (Debit/Credit/Balance with C/D suffix)
    if re.search(r'Credit\s*C.*Debit\s*D', text):
        return 0

    # Era 1: all-caps PAID OUT (pre-2002 paper scans)
    if 'PAID OUT' in text and 'PAID IN' in text:
        return 1

    # Era 3: internet banking export (has "internet banking" header)
    if 'internet banking' in text.lower():
        return 3

    # Era 4: modern (has "firstdirect.com" as header)
    if 'firstdirect.com' in text:
        return 4

    # Era 2: older scanned with title-case headers
    if re.search(r'Paid\s*out', text):
        return 2

    # Fallback: lowercase headers → Era 3
    if re.search(r'paid\s*out', text, re.IGNORECASE):
        return 3

    return 2  # default


def try_parse_column_header(line: str) -> Optional[dict]:
    """Try to parse column positions from a single header line."""
    lower = line.lower()

    # Check for "paid out" variants
    po_idx = -1
    for pat in ['paid out', 'paid  out']:
        idx = lower.find(pat)
        if idx != -1:
            po_idx = idx
            break

    if po_idx == -1:
        # Check for Debit/Credit format (1997 savings)
        deb_idx = lower.find('debit')
        if deb_idx != -1:
            rest = lower[deb_idx + 5:]
            ci = rest.find('credit')
            cred_idx = deb_idx + 5 + ci if ci != -1 else -1
            bal_idx = lower.find('balance')
            if cred_idx != -1 and bal_idx != -1 and deb_idx < cred_idx < bal_idx:
                return {'paid_out': deb_idx, 'paid_in': cred_idx, 'balance': bal_idx}
        return None

    # Find "balance" position
    bal_idx = -1
    for pat in ['balance', 'bolsnce', 'baiance']:
        idx = lower.find(pat)
        if idx != -1 and idx > po_idx:
            bal_idx = idx
            break

    if bal_idx == -1:
        return None

    # Find "paid in" position (various OCR variants)
    pi_idx = -1
    for pat in ['paid in', 'paid  in', 'paidin', 'paidtn', 'paidm',
                 'paiiiin', 'paidln', 'paid m', 'p s d i n',
                 'paid ln', 'paidlr']:
        idx = lower.find(pat)
        if idx != -1 and po_idx < idx < bal_idx:
            pi_idx = idx
            break

    # If paid_in not found, estimate from paid_out and balance
    if pi_idx == -1:
        pi_idx = (po_idx + bal_idx) // 2

    if po_idx < pi_idx < bal_idx:
        return {'paid_out': po_idx, 'paid_in': pi_idx, 'balance': bal_idx}

    return None


def find_column_positions(text: str) -> tuple[Optional[dict], bool]:
    """Find column positions from the first header line in the text.
    Returns (positions, is_fallback) tuple."""
    for line in text.split('\n'):
        result = try_parse_column_header(line)
        if result:
            return result, False

    # Fallback: detect balance column from "Balance brought/carried forward" lines
    for line in text.split('\n'):
        lower = line.lower()
        if ('balance' in lower and ('brought' in lower or 'carried' in lower
                                     or 'forward' in lower)):
            m = re.search(r'([\d,]+\.\d{2})\s*$', line)
            if m:
                bal = m.start(1)
                # Estimate paid_out and paid_in from balance position
                return {
                    'paid_out': int(bal * 0.45),
                    'paid_in': int(bal * 0.70),
                    'balance': bal,
                }, True
    return None, False


def extract_account_number(text: str) -> Optional[str]:
    """Extract 8-digit account number from statement text."""
    patterns = [
        # "ACCOUNT NO. 40-47-87 90245682"
        r'ACCOUNT\s+NO\.\s*\d{2}-\d{2}-\d{2}\s+(\d{8})',
        # "40-47-XX  NNNNNNNNN" (sort code + account)
        r'40-47-\d{2}\s+(\d{7,8})',
        # From IBAN: GB68MIDL40478790245682 or GB08HBUK40478790245682
        # Allow optional space between sort code digits and account number
        r'GB\w{2}(?:MIDL|HBUK)\d{6}\s*(\d{8})',
        # IBAN with spaces: G B 6 8 M I D L 4 0 4 7 8 7 9 0 2 4 5 6 8 2
        r'G\s*B\s*\w\s*\w\s*(?:M\s*I\s*D\s*L|H\s*B\s*U\s*K)\s*'
        r'(\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d)',
        # "Account No. 3 0 2 5 3 2 0 0" (OCR with spaces)
        r'Account\s+N[Oo0]\.\s*([\d\s]{10,})',
    ]
    first_unknown = None
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            digits = re.sub(r'\D', '', m.group(1))
            if len(digits) >= 8:
                # For IBAN pattern, last 8 digits are the account
                acct = digits[-8:] if len(digits) > 8 else digits[:8]
                if acct in ACCOUNT_MAP:
                    return acct
                # Try first 8 digits
                acct = digits[:8]
                if acct in ACCOUNT_MAP:
                    return acct
                # Remember first unknown, but try other patterns
                if first_unknown is None:
                    first_unknown = digits[:8]
    return first_unknown


def parse_era1_date(match) -> Optional[str]:
    """Parse DDMMMYY (e.g., 06AUG01) -> YYYY-MM-DD."""
    day = int(match.group(1))
    month_str = match.group(2).lower()[:3]
    year_str = match.group(3)

    # Handle OCR errors in year: OI -> 01, Ol -> 01
    year_str = year_str.replace('O', '0').replace('I', '1').replace('l', '1')
    try:
        year_raw = int(year_str)
    except ValueError:
        return None

    month = MONTH_MAP.get(month_str)
    if not month:
        return None

    year = 2000 + year_raw if year_raw < 50 else 1900 + year_raw
    try:
        return f"{year:04d}-{month:02d}-{day:02d}"
    except ValueError:
        return None


def parse_era234_date(match) -> Optional[str]:
    """Parse DD MMM YY (e.g., 19 Jan 02) -> YYYY-MM-DD."""
    day = int(match.group(1))
    month_str = match.group(2).lower()[:3]
    year_raw = int(match.group(3))

    month = MONTH_MAP.get(month_str)
    if not month:
        return None

    if year_raw < 100:
        year = 2000 + year_raw if year_raw < 50 else 1900 + year_raw
    else:
        year = year_raw

    try:
        return f"{year:04d}-{month:02d}-{day:02d}"
    except ValueError:
        return None


def parse_savings97_date(match, year: int) -> Optional[str]:
    """Parse MMMDD with known year -> YYYY-MM-DD."""
    month_str = match.group(1).lower()[:3]
    day = int(match.group(2))
    month = MONTH_MAP.get(month_str)
    if not month:
        return None
    try:
        return f"{year:04d}-{month:02d}-{day:02d}"
    except ValueError:
        return None


def extract_amount_from_region(line: str, start: int, end: int) -> Optional[Decimal]:
    """Extract a monetary amount from a specific column region of the line."""
    if start < 0:
        start = 0
    if end > len(line):
        end = len(line)
    region = line[start:end]

    # Standard format: 1,234.56 or 1 234.56
    m = AMOUNT_RE.search(region)
    if m:
        amount_str = m.group(1).replace(' ', '').replace(',', '')
        try:
            return Decimal(amount_str)
        except InvalidOperation:
            pass

    # European OCR variant: 1.234,56 or 1 234,56
    m = EURO_AMOUNT_RE.search(region)
    if m:
        amount_str = m.group(1).replace(' ', '').replace('.', '').replace(',', '.')
        try:
            return Decimal(amount_str)
        except InvalidOperation:
            pass

    return None


def parse_pdf(filepath: str) -> list[Transaction]:
    """Parse a single bank statement PDF. Returns list of transactions."""
    result = subprocess.run(
        ["pdftotext", "-layout", filepath, "-"],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"  ERROR: pdftotext failed for {filepath}: {result.stderr.strip()}")
        return []

    text = result.stdout
    fname = Path(filepath).name

    # Skip non-statement PDFs
    lower_name = fname.lower()
    if any(skip in lower_name for skip in ['charges', 'summary of charges',
                                            'maturity', 'annual statement']):
        return []

    era = detect_era(text)
    account_num = extract_account_number(text)
    account_ref = ACCOUNT_MAP.get(account_num, '') if account_num else ''

    # Initial column positions from first header found
    col_positions, used_fallback_cols = find_column_positions(text)
    if not col_positions:
        print(f"  WARNING: No column headers found in {fname}")
        return []

    transactions: list[Transaction] = []
    current_date: Optional[str] = None
    year_context: Optional[int] = None

    # For Era 0 (1997 savings), extract year(s) from header
    if era == 0:
        m = re.search(r'\b(19\d{2}|20\d{2})\b\s+Sheet', text)
        if m:
            year_context = int(m.group(1))

    # Eras with type codes use forward-buffering for multi-line descriptions
    era_has_type_codes = era in (1, 2, 4)
    pending_desc: list[str] = []  # Buffered description lines

    for line in text.split('\n'):
        stripped = line.strip()
        if not stripped:
            continue

        # --- Check for column header line (re-detect per page) ---
        new_cols = try_parse_column_header(line)
        if new_cols:
            col_positions = new_cols
            continue

        # --- Fallback: re-detect columns from "Balance brought forward" ---
        # Only when using fallback mode (no proper header found)
        if used_fallback_cols:
            lower_line = line.lower()
            if ('balance' in lower_line and ('brought' in lower_line
                                              or 'carried' in lower_line)):
                m_bal = re.search(r'([\d,]+\.\d{2})\s*$', line)
                if m_bal:
                    bal_pos = m_bal.start(1)
                    col_positions = {
                        'paid_out': int(bal_pos * 0.45),
                        'paid_in': int(bal_pos * 0.70),
                        'balance': bal_pos,
                    }

        # Compute column boundaries from current positions
        po = col_positions['paid_out']
        pi = col_positions['paid_in']
        bal = col_positions['balance']
        bound_po_pi = (po + pi) // 2
        bound_pi_bal = (pi + bal) // 2
        desc_end_col = po - 2

        if should_skip_line(line):
            pending_desc = []
            continue

        # --- Parse date ---
        date_parsed = None
        date_match = None

        if era == 0 and year_context:
            date_match = SAVINGS97_DATE_RE.match(stripped)
            if date_match:
                date_parsed = parse_savings97_date(date_match, year_context)
        elif era == 1:
            date_match = ERA1_DATE_RE.match(stripped)
            if date_match:
                date_parsed = parse_era1_date(date_match)
        else:
            date_match = ERA234_DATE_RE.match(stripped)
            if date_match:
                date_parsed = parse_era234_date(date_match)

        if date_parsed:
            current_date = date_parsed

        if not current_date:
            continue

        # --- Extract description text from this line ---
        desc_text = line[:desc_end_col].strip()
        if date_match:
            desc_text = re.sub(
                r'^\d{1,2}\s+\w{3}\s+\d{2,4}\s*', '', desc_text)
            desc_text = re.sub(
                r'^\d{2}[A-Z]{3}\d{2}\s*', '', desc_text)
            desc_text = re.sub(
                r'^[A-Z]{3}\d{2}\s*', '', desc_text)
        desc_text = ' '.join(desc_text.split())

        # --- Extract amounts from column regions ---
        paid_out = extract_amount_from_region(line, po - 5, bound_po_pi)
        paid_in = extract_amount_from_region(line, bound_po_pi, bound_pi_bal)
        has_txn_amount = paid_out is not None or paid_in is not None

        # --- Check for type code ---
        line_has_type_code = has_type_code(desc_text) if era_has_type_codes else False

        if has_txn_amount:
            # --- Line with an amount: create a transaction ---
            if pending_desc:
                if era_has_type_codes and not line_has_type_code:
                    # In type-code eras, pending lines are forward description
                    # for THIS transaction (type code started them, amount ends them)
                    full_desc = ' '.join(pending_desc + [desc_text])
                else:
                    # Pending lines are extra_details for PREVIOUS transaction
                    if transactions:
                        transactions[-1].extra_details.extend(pending_desc)
                    full_desc = desc_text
                pending_desc = []
            else:
                full_desc = desc_text

            if not full_desc:
                continue

            # Determine amount direction
            if paid_in is not None and paid_out is None:
                amount = paid_in
                is_credit = True
            elif paid_out is not None:
                amount = -paid_out
                is_credit = False
            else:
                continue

            txn = Transaction(
                posted_at=current_date,
                description=full_desc,
                amount=amount,
                is_credit=is_credit,
                account_ref=account_ref,
                filename=fname,
            )
            transactions.append(txn)

        elif line_has_type_code:
            # --- Type code without amount: start of multi-line transaction ---
            # Flush any pending desc to previous transaction as extra details
            if pending_desc and transactions:
                transactions[-1].extra_details.extend(pending_desc)
            pending_desc = [desc_text] if desc_text else []

        else:
            # --- Continuation line (no amount, no type code) ---
            if desc_text:
                pending_desc.append(desc_text)

    # Flush remaining pending descriptions
    if pending_desc and transactions:
        transactions[-1].extra_details.extend(pending_desc)

    return transactions


def make_transaction_ref(txn: Transaction, position: int = 0) -> str:
    """Generate a stable, unique reference for a transaction."""
    key = f"{txn.posted_at}|{txn.amount}|{txn.description}|{position}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def write_csv(transactions: list[Transaction], output_path: str):
    """Write transactions to CSV."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date", "description", "amount", "is_credit",
            "account_ref", "filename", "extra_details",
        ])
        for txn in transactions:
            writer.writerow([
                txn.posted_at,
                txn.description,
                str(txn.amount),
                txn.is_credit,
                txn.account_ref,
                txn.filename,
                "; ".join(txn.extra_details) if txn.extra_details else "",
            ])


def load_to_db(transactions: list[Transaction]):
    """Load transactions directly into raw_transaction."""
    import psycopg2
    from config.settings import settings

    conn = psycopg2.connect(settings.dsn)
    cur = conn.cursor()
    inserted = 0
    skipped = 0

    # Assign positional indices for disambiguation
    position_counter: Counter = Counter()
    for txn in transactions:
        key = (txn.posted_at, str(txn.amount), txn.description, txn.account_ref)
        txn._position = position_counter[key]
        position_counter[key] += 1

    for txn in transactions:
        ref = make_transaction_ref(txn, txn._position)
        raw_data = {
            "posted_at": txn.posted_at,
            "description": txn.description,
            "amount": str(txn.amount),
            "is_credit": txn.is_credit,
            "filename": txn.filename,
        }
        if txn.extra_details:
            raw_data["extra_details"] = txn.extra_details

        cur.execute("""
            INSERT INTO raw_transaction (
                source, institution, account_ref, transaction_ref,
                posted_at, amount, currency,
                raw_merchant, raw_memo, is_dirty, raw_data
            ) VALUES (
                'first_direct_pdf', 'first_direct', %s, %s,
                %s, %s, 'GBP',
                %s, %s, false, %s
            )
            ON CONFLICT (institution, account_ref, transaction_ref)
                WHERE transaction_ref IS NOT NULL
            DO NOTHING
            RETURNING id
        """, (
            txn.account_ref,
            ref,
            txn.posted_at,
            txn.amount,
            txn.description,
            "; ".join(txn.extra_details) if txn.extra_details else None,
            json.dumps(raw_data),
        ))

        result = cur.fetchone()
        if result:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    conn.close()
    return inserted, skipped


def main():
    parser = argparse.ArgumentParser(
        description="Parse First Direct bank statement PDFs"
    )
    parser.add_argument(
        "files", nargs="+",
        help="Path(s) to PDF files (supports glob patterns)"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output CSV path (default: print summary only)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and report only, don't write anything"
    )
    parser.add_argument(
        "--load", action="store_true",
        help="Load directly into database"
    )
    args = parser.parse_args()

    # Expand globs
    pdf_files = []
    for pattern in args.files:
        expanded = sorted(glob(pattern))
        if not expanded:
            print(f"WARNING: No files match {pattern}")
        pdf_files.extend(expanded)

    if not pdf_files:
        print("ERROR: No PDF files found")
        sys.exit(1)

    print(f"=== First Direct Bank Statement PDF Parser ===\n")
    print(f"  Found {len(pdf_files)} PDF files\n")

    all_transactions: list[Transaction] = []
    files_parsed = 0
    files_empty = 0
    no_account = set()

    for filepath in pdf_files:
        txns = parse_pdf(filepath)
        fname = Path(filepath).name

        if txns:
            dates = [t.posted_at for t in txns]
            debits = [t for t in txns if not t.is_credit]
            credits = [t for t in txns if t.is_credit]
            acct = txns[0].account_ref or "unknown"
            print(f"  {fname}: {len(txns)} txns "
                  f"({len(debits)} debits, {len(credits)} credits) "
                  f"[{min(dates)} to {max(dates)}] "
                  f"acct={acct}")
            if not txns[0].account_ref:
                no_account.add(fname)
            files_parsed += 1
        else:
            print(f"  {fname}: 0 txns")
            files_empty += 1

        all_transactions.extend(txns)

    # Sort by date
    all_transactions.sort(key=lambda t: (t.account_ref, t.posted_at))

    # Assign positional indices
    position_counter: Counter = Counter()
    for txn in all_transactions:
        key = (txn.posted_at, str(txn.amount), txn.description, txn.account_ref)
        txn._position = position_counter[key]
        position_counter[key] += 1

    # Summary by account
    by_account: dict[str, list[Transaction]] = {}
    for txn in all_transactions:
        by_account.setdefault(txn.account_ref or 'unknown', []).append(txn)

    print(f"\n  === Summary ===")
    print(f"  Files: {files_parsed} parsed, {files_empty} empty/skipped")
    print(f"  Total transactions: {len(all_transactions)}")

    for acct, txns in sorted(by_account.items()):
        dates = [t.posted_at for t in txns]
        total_debits = sum(t.amount for t in txns if not t.is_credit)
        total_credits = sum(t.amount for t in txns if t.is_credit)
        print(f"\n  {acct}: {len(txns)} txns [{min(dates)} to {max(dates)}]")
        print(f"    Debits: £{total_debits:,.2f}  Credits: £{total_credits:,.2f}")

    if no_account:
        print(f"\n  WARNING: {len(no_account)} files with unknown account:")
        for f in sorted(no_account):
            print(f"    {f}")

    if args.dry_run:
        print(f"\n  [DRY RUN] No output written.")
        return

    if args.load:
        # Filter out transactions without account_ref
        loadable = [t for t in all_transactions if t.account_ref]
        if not loadable:
            print("\n  ERROR: No transactions with known account_ref to load.")
            return
        inserted, skipped = load_to_db(loadable)
        print(f"\n  Database: {inserted} inserted, {skipped} skipped (duplicates)")
    elif args.output:
        write_csv(all_transactions, args.output)
        print(f"\n  Written to {args.output}")
        print(f"  {len(all_transactions)} rows")
    else:
        print("\n  Use --load to insert into DB, -o FILE for CSV, "
              "or --dry-run to preview.")


if __name__ == "__main__":
    main()
