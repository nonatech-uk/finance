#!/usr/bin/env python3
"""
Parser for Amex and Citibank Corporate/Personal Card PDF statements.
Handles multiple format variants across CS, GS, DB corporate cards,
personal Amex (UK/US), and Citibank commercial cards.
"""

import csv
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pdfplumber

# ── Source directories ───────────────────────────────────────────────────────

SOURCE_DIRS = [
    Path("/Users/stu/Documents/02 Archive/01 Finance/05 Corporate Cards"),
    Path("/Users/stu/Documents/02 Archive/01 Finance/01 Banking - Savings/87 American Express"),
]
OUTPUT_CSV = Path("/Users/stu/tmp/card_transactions.csv")

# ── helpers ──────────────────────────────────────────────────────────────────

MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9,
    'oct': 10, 'nov': 11, 'dec': 12,
}


def parse_amount(s):
    """Parse amount string, handling commas as thousands separators."""
    if not s:
        return None
    s = s.strip().replace(',', '')
    try:
        return float(s)
    except ValueError:
        return None


def parse_euro_amount(s):
    """Parse European format amount (period=thousands, comma=decimal): 1.234,56 → 1234.56"""
    if not s:
        return None
    s = s.strip()
    # Handle negative with leading dash or trailing -
    negative = False
    if s.startswith('-'):
        negative = True
        s = s[1:].strip()
    elif s.endswith('-'):
        negative = True
        s = s[:-1].strip()
    # Remove thousands separators (periods), replace decimal comma with period
    s = s.replace('.', '').replace(',', '.')
    try:
        val = float(s)
        return -val if negative else val
    except ValueError:
        return None


def detect_statement_date_from_filename(filename):
    """Extract YYYYMMDD from filename."""
    m = re.match(r'(\d{8})', os.path.basename(filename))
    if m:
        d = m.group(1)
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return None


def guess_year_from_statement_date(stmt_date_str):
    if stmt_date_str:
        return int(stmt_date_str[:4])
    return None


def resolve_date(day, month_name, year):
    """Resolve day + month name + year into YYYY-MM-DD."""
    month = MONTH_MAP.get(month_name.lower().strip())
    if month is None:
        return None
    try:
        return f"{year:04d}-{month:02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


def extract_card_number(full_text):
    """Extract card/membership/account number from statement text."""
    for pattern in [
        # Full numeric: 3742-924216-61001
        r'Card\s*(?:Number\s*)?(\d{4}[-\s]*\d{5,6}[-\s]*\d{5})',
        r'Membership\s*Number\s*(\d{4}[-\s]*\d{5,6}[-\s]*\d{5})',
        r'Account\s*Number\s*(\d{4}[-\s]*\d{5,6}[-\s]*\d{5})',
        # Masked with trailing digits: XXXX-XXXXX7-71000
        r'Card\s*(X{4}[-\s]*X{4,6}\d?[-\s]*\d{5})',
        r'Membership\s*Number\s*(X{4}[-\s]*X{4,6}\d?[-\s]*\d{5})',
        r'Account\s*Number\s*(X{4}[-\s]*X{4,6}\d?[-\s]*\d{5})',
        # Citibank: XXXX-XXXX-XX30-3908
        r'ACCOUNT\s*NUMBER:\s*(X{4}[-\s]*X{4}[-\s]*X{2}\d{2}[-\s]*\d{4})',
        # Fully masked with suffix: XXXX-XXXXXX-41006
        r'Card\s*(X{4}[-\s]*X{6}[-\s]*\d{5})',
        r'Membership\s*Number\s*(X{4}[-\s]*X{6}[-\s]*\d{5})',
    ]:
        m = re.search(pattern, full_text)
        if m:
            return re.sub(r'\s+', '', m.group(1))
    return ''


def detect_format(full_text):
    """Detect the statement format from the full text of the PDF."""
    if 'Citibank' in full_text or 'CITIBANK' in full_text:
        return 'CITIBANK'
    if 'Corporate Green Card' in full_text:
        return 'DB_UK'
    if 'CREDITSUISSE' in full_text or 'CREDIT SUISSE' in full_text:
        if 'Amount$' in full_text or 'Amount $' in full_text:
            return 'CS_US'
        return 'CS_UK'
    if 'CSFB' in full_text:
        return 'CS_UK'
    # UK Platinum personal - check BEFORE US_PERSONAL since UK statements
    # also have "Membership Rewards" on later pages
    if ('The Platinum Card' in full_text or 'ThePlatinumCard' in full_text):
        if 'BalanceDue£' in full_text or 'Balance Due £' in full_text:
            return 'UK_PERSONAL'
    # US Platinum personal (US dollar amounts, no GBP balance)
    if 'Platinum Card' in full_text and ('Amount$' in full_text or 'Amount $' in full_text or 'Balance$' in full_text or 'Balance $' in full_text):
        return 'US_PERSONAL'
    if 'Membership Rewards' in full_text or 'MembershipRewards' in full_text:
        if 'Amount$' in full_text or 'NewActivity$' in full_text or 'PreviousBalance$' in full_text:
            return 'US_PERSONAL'
    if 'BUSINESS ACCOUNT' in full_text:
        return 'GS_UK'
    if 'BalanceDue£' in full_text or 'Balance Due £' in full_text or 'Amount£' in full_text or 'Amount £' in full_text:
        return 'GS_UK'
    return 'UNKNOWN'


def make_txn(stmt_date, source_folder, fmt, currency, txn_date, description,
             amount, filename, card_number='', process_date=None, category='',
             foreign_amount=None, foreign_currency='', is_credit=False,
             fx_rate=None, fx_fee=None, cardholder='STUART BEVAN'):
    """Create a standardized transaction dict."""
    return {
        'statement_date': stmt_date,
        'source': source_folder,
        'format': fmt,
        'card_number': card_number,
        'cardholder': cardholder,
        'currency': currency,
        'transaction_date': txn_date,
        'process_date': process_date,
        'description': description,
        'category': category,
        'foreign_amount': foreign_amount,
        'foreign_currency': foreign_currency,
        'amount': amount,
        'is_credit': is_credit,
        'fx_rate': fx_rate,
        'fx_fee': fx_fee,
        'filename': os.path.basename(filename),
    }


# ── UK GS/Personal format parser ────────────────────────────────────────────

def parse_uk_amex(pdf, stmt_date, source_folder, filename, fmt='GS_UK'):
    """
    Parse UK Amex format statements (GS Business, UK Platinum Personal, CS/CSFB).
    Handles both simple early format and enhanced 2012+ format with FX details.
    """
    transactions = []
    full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    year = guess_year_from_statement_date(stmt_date)
    card_number = extract_card_number(full_text)

    # Detect if it's the enhanced format (has Transaction/Process date columns)
    has_process_date = bool(re.search(
        r'Transaction\s*Process\s*Transaction\s*Details', full_text, re.IGNORECASE))

    lines = full_text.split('\n')
    i = 0
    in_transactions = False
    current_txn = None

    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        # Start of transaction section
        if re.search(r'New\s*transactions\s*for\s*STUART', line, re.IGNORECASE):
            in_transactions = True
            i += 1
            if i < len(lines) and re.search(r'Card\s*[\dX]', lines[i].strip()):
                i += 1
            continue

        # Continuation header
        if re.search(r'New\s*transactions.*continued', line, re.IGNORECASE):
            in_transactions = True
            i += 1
            if i < len(lines) and re.search(r'Card\s*[\dX]', lines[i].strip()):
                i += 1
            continue

        # End of transactions
        if re.search(r'Total\s*of\s*new\s*transactions', line, re.IGNORECASE):
            if current_txn:
                transactions.append(current_txn)
                current_txn = None
            in_transactions = False
            i += 1
            continue

        # Skip payment lines
        if 'Paymentreceived' in line or 'Payment received' in line:
            i += 1
            continue

        if not in_transactions:
            i += 1
            continue

        # Try to parse transaction line
        if has_process_date:
            m = re.match(r'^(\d{1,2}\s*\w{3,9})\s+(\d{1,2}\s*\w{3,9})\s+(.+)', line)
        else:
            m = re.match(r'^(\d{1,2}\s*\w{3,9})\s+(.+)', line)

        if m:
            if has_process_date:
                txn_date_raw, proc_date_raw, rest = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
            else:
                txn_date_raw, proc_date_raw, rest = m.group(1).strip(), None, m.group(2).strip()

            # Parse date
            dm = re.match(r'(\d{1,2})\s*([A-Za-z]+)', txn_date_raw)
            if not dm or dm.group(2).lower() not in MONTH_MAP:
                i += 1
                continue

            if current_txn:
                transactions.append(current_txn)

            txn_date = resolve_date(dm.group(1), dm.group(2), year)
            proc_date = None
            if proc_date_raw:
                dm2 = re.match(r'(\d{1,2})\s*([A-Za-z]+)', proc_date_raw)
                if dm2 and dm2.group(2).lower() in MONTH_MAP:
                    proc_date = resolve_date(dm2.group(1), dm2.group(2), year)

            # Extract amounts from end
            amounts = re.findall(r'([\d,]+\.\d{2})', rest)
            description = rest
            foreign_amount = None
            local_amount = None

            if len(amounts) >= 2:
                foreign_amount = parse_amount(amounts[-2])
                local_amount = parse_amount(amounts[-1])
                last_idx = rest.rfind(amounts[-1])
                second_last_idx = rest.rfind(amounts[-2], 0, last_idx)
                description = rest[:second_last_idx].strip()
            elif len(amounts) == 1:
                local_amount = parse_amount(amounts[-1])
                last_idx = rest.rfind(amounts[-1])
                description = rest[:last_idx].strip()

            # Check for CR (credit)
            is_credit = bool(re.search(r'\bCR\b', rest))

            current_txn = make_txn(
                stmt_date, source_folder, fmt, 'GBP', txn_date, description,
                local_amount, filename, card_number, proc_date,
                foreign_amount=foreign_amount, is_credit=is_credit,
            )
            i += 1
            continue

        # Continuation lines for current transaction
        if current_txn:
            # Category line (may end with CR for credits)
            if re.match(r'^[A-Z][a-zA-Z\s\-/&]+(?:\s+CR)?$', line) and len(line) < 60 and not re.search(r'\d', line):
                if line.strip().endswith(' CR') or line.strip().endswith('CR'):
                    current_txn['is_credit'] = True
                    current_txn['category'] = re.sub(r'\s*CR$', '', line.strip())
                else:
                    current_txn['category'] = line.strip()
                i += 1
                continue

            # Reference line
            if re.match(r'^Reference\s*\d', line, re.IGNORECASE):
                i += 1
                continue

            # Currency line
            currency_m = re.match(
                r'^(US\s*Dollar|Euro|Japanese\s*Yen|Hong\s*Kong\s*Dollar|Singapore\s*Do[li]lar|Swiss\s*Francs?)',
                line, re.IGNORECASE)
            if currency_m:
                current_txn['foreign_currency'] = currency_m.group(1).strip()
                i += 1
                continue

            # FX rate + commission (enhanced format)
            fx_m = re.match(r'CurrencyConversionRate([\d.]+)\+CommissionAmount([\d.]+)', line)
            if fx_m:
                current_txn['fx_rate'] = float(fx_m.group(1))
                current_txn['fx_fee'] = float(fx_m.group(2))
                i += 1
                continue

            # Arrival/Departure
            if re.match(r'^Arrival\s+Departure', line, re.IGNORECASE):
                i += 1
                continue
            if re.match(r'^\d{2}/\d{2}/\d{2,4}\s+\d{2}/\d{2}/\d{2,4}', line):
                i += 1
                continue

            # Ticket/routing info - skip
            if 'Ticket number' in line or 'Routing' in line or 'From:' in line or 'Passenger' in line:
                i += 1
                continue

        i += 1

    if current_txn:
        transactions.append(current_txn)

    return transactions


# ── US CS/Personal format parser ─────────────────────────────────────────────

def parse_us_amex(pdf, stmt_date, source_folder, filename, fmt='CS_US'):
    """Parse US Amex statements (CS Corporate + Personal Platinum)."""
    transactions = []
    full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    year = guess_year_from_statement_date(stmt_date)
    card_number = extract_card_number(full_text)

    lines = full_text.split('\n')
    i = 0
    in_activity = False
    current_txn = None
    current_cardholder = 'STUART BEVAN'

    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        # Detect cardholder sections (personal card with supplementary members)
        ch_m = re.search(
            r'(?:Due\s*in\s*Full\s*(?:Activity|continued)\s*for)\s+([A-Z][A-Z\s]+?)(?:\s+Foreign|\s*$)',
            line)
        if ch_m:
            name = ch_m.group(1).strip()
            if name:
                if current_txn:
                    transactions.append(current_txn)
                    current_txn = None
                current_cardholder = name
            in_activity = True
            i += 1
            if i < len(lines) and re.search(r'Card\s*[\dX]', lines[i].strip()):
                i += 1
            continue

        # Activity start
        if re.search(r'^Activity\b', line) or re.search(r'^ActivityContinued', line):
            in_activity = True
            i += 1
            continue

        # Card number header
        if re.match(r'^Card\s*Number\s*[\dX]', line):
            i += 1
            continue

        # Total lines
        if re.search(r'Total\s*(?:of\s*)?(?:Due\s*in\s*Full\s*)?(?:Activity\s*)?for', line, re.IGNORECASE):
            if current_txn:
                transactions.append(current_txn)
                current_txn = None
            i += 1
            continue
        if re.search(r'^Total\s*Due\s*in\s*Full', line):
            if current_txn:
                transactions.append(current_txn)
                current_txn = None
            in_activity = False
            i += 1
            continue
        if re.search(r'Total\s*for\s*STUART', line, re.IGNORECASE):
            if current_txn:
                transactions.append(current_txn)
                current_txn = None
            in_activity = False
            i += 1
            continue

        # Skip non-activity content
        if re.search(r'Payment\s*Coupon', line) or re.search(r'Pleasefoldon', line):
            in_activity = False
            i += 1
            continue
        if re.search(r'Continued\s*on', line, re.IGNORECASE):
            i += 1
            continue
        if re.search(r'Important\s*Notice', line):
            in_activity = False
            i += 1
            continue

        if not in_activity:
            i += 1
            continue

        # Transaction: "MM/DD/YY[*] DESCRIPTION... AMOUNT"
        m = re.match(r'^(\d{2}/\d{2}/\d{2})\*?\s+(.+)', line)
        if m:
            if current_txn:
                transactions.append(current_txn)

            date_str = m.group(1)
            rest = m.group(2).strip()

            parts = date_str.split('/')
            yr = int(parts[2])
            if yr < 100:
                yr += 2000
            txn_date = f"{yr:04d}-{int(parts[0]):02d}-{int(parts[1]):02d}"

            # Check for payment
            if 'PAYMENT' in rest.upper() and ('RECEIVED' in rest.upper() or 'THANK' in rest.upper()):
                amount_m = re.search(r'(-?[\d,]+\.\d{2})\s*$', rest)
                amount = parse_amount(amount_m.group(1)) if amount_m else None
                current_txn = make_txn(
                    stmt_date, source_folder, fmt, 'USD', txn_date,
                    'PAYMENT RECEIVED', abs(amount) if amount else None,
                    filename, card_number, category='Payment',
                    is_credit=True, cardholder=current_cardholder,
                )
                i += 1
                continue

            # Extract amounts
            amounts = re.findall(r'(-?[\d,]+\.\d{2})', rest)
            description = rest
            usd_amount = None

            if amounts:
                usd_amount = parse_amount(amounts[-1])
                last_idx = rest.rfind(amounts[-1])
                description = rest[:last_idx].strip()

            # Clean up trailing reference codes
            description = re.sub(r'\s+\d{10,}\s*$', '', description).strip()

            is_credit = False
            if usd_amount and usd_amount < 0:
                is_credit = True
                usd_amount = abs(usd_amount)
            if re.search(r'\bCredit\b', rest):
                is_credit = True

            current_txn = make_txn(
                stmt_date, source_folder, fmt, 'USD', txn_date,
                description, usd_amount, filename, card_number,
                is_credit=is_credit, cardholder=current_cardholder,
            )
            i += 1
            continue

        # Continuation lines
        if current_txn:
            # Foreign spending: "90.00 **PoundsSterling"
            fx_m = re.match(r'^([\d,]+\.?\d*)\s+\*?\*?(Pounds?\s*Sterling|Swiss\s*Francs?)', line, re.IGNORECASE)
            if fx_m:
                current_txn['foreign_amount'] = parse_amount(fx_m.group(1))
                current_txn['foreign_currency'] = fx_m.group(2).strip()
                i += 1
                continue

            # Category
            cat_m = re.match(
                r'^(RESTAURANT|LODGING|GOODS|GOODS\s*AND/OR\s*SERVICES|BEAUTY/BARBER\s*SHOP|'
                r'LIQUOR\s*STORE|ELECTRONICS\s*STORE|BUSINESS\s*SERVICE|GROCERY|COSMETIC|'
                r'MERCHANDISE|GENERAL\s*MERCHANDISE|MISC)\s*$', line, re.IGNORECASE)
            if cat_m:
                current_txn['category'] = cat_m.group(1).strip()
                i += 1
                continue

        i += 1

    if current_txn:
        transactions.append(current_txn)

    return transactions


# ── UK DB format parser ──────────────────────────────────────────────────────

def parse_db_uk(pdf, stmt_date, source_folder, filename):
    """Parse UK Deutsche Bank Corporate Green Card statements (GBP)."""
    transactions = []
    full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    year = guess_year_from_statement_date(stmt_date)
    card_number = extract_card_number(full_text)

    lines = full_text.split('\n')
    i = 0
    in_transactions = False
    current_txn = None

    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        if re.search(r'New\s*Transactions\s*for\s*STUART', line, re.IGNORECASE):
            in_transactions = True
            i += 1
            if i < len(lines) and re.search(r'Card\s*X', lines[i].strip()):
                i += 1
            continue

        if re.search(r'continued', line, re.IGNORECASE) and re.search(r'STUART', line):
            in_transactions = True
            i += 1
            if i < len(lines) and re.search(r'Card\s*X', lines[i].strip()):
                i += 1
            continue

        if re.search(r'Total\s*of\s*New\s*Transactions', line, re.IGNORECASE):
            if current_txn:
                transactions.append(current_txn)
                current_txn = None
            in_transactions = False
            i += 1
            continue

        if re.search(r'Payment\s*Methods', line):
            if current_txn:
                transactions.append(current_txn)
                current_txn = None
            in_transactions = False
            i += 1
            continue

        if not in_transactions:
            i += 1
            continue

        # "DD Mon DD Mon DESCRIPTION AMOUNT"
        m = re.match(r'^(\d{1,2}\s+\w{3})\s+(\d{1,2}\s+\w{3})\s+(.+?)\s+([\d,]+\.\d{2})\s*$', line)
        if m:
            if current_txn:
                transactions.append(current_txn)

            dm1 = re.match(r'(\d{1,2})\s+(\w{3})', m.group(1))
            dm2 = re.match(r'(\d{1,2})\s+(\w{3})', m.group(2))
            txn_date = resolve_date(dm1.group(1), dm1.group(2), year) if dm1 else None
            proc_date = resolve_date(dm2.group(1), dm2.group(2), year) if dm2 else None

            current_txn = make_txn(
                stmt_date, source_folder, 'DB_UK', 'GBP', txn_date,
                m.group(3).strip(), parse_amount(m.group(4)),
                filename, card_number, proc_date,
            )
            i += 1
            continue

        if current_txn:
            fx_m = re.match(r'^Foreign\s+Spending\s+([\d,]+\.?\d*)\s+(.*)', line, re.IGNORECASE)
            if fx_m:
                current_txn['foreign_amount'] = parse_amount(fx_m.group(1))
                current_txn['foreign_currency'] = fx_m.group(2).strip()
                i += 1
                continue

            fee_m = re.match(r'^Non\s+GBP\s+Transaction\s+Fee\s+([\d.]+)', line, re.IGNORECASE)
            if fee_m:
                current_txn['fx_fee'] = float(fee_m.group(1))
                i += 1
                continue

            if re.match(r'^Exchange\s+Rate', line, re.IGNORECASE):
                rate_str = line.split()[-1]
                try:
                    current_txn['fx_rate'] = float(rate_str)
                except ValueError:
                    pass
                i += 1
                continue

            if re.match(r'^Arrival\s+Departure', line, re.IGNORECASE):
                i += 1
                continue
            if re.match(r'^\d{2}/\d{2}/\d{2,4}\s+\d{2}/\d{2}/\d{2,4}', line):
                i += 1
                continue

        i += 1

    if current_txn:
        transactions.append(current_txn)

    return transactions


# ── Citibank parser ──────────────────────────────────────────────────────────

def parse_citibank(pdf, stmt_date, source_folder, filename):
    """
    Parse Citibank Commercial Card statements.
    Uses European number format: period=thousands, comma=decimal (1.234,56).
    """
    transactions = []
    full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    year = guess_year_from_statement_date(stmt_date)
    card_number = extract_card_number(full_text)

    lines = full_text.split('\n')
    i = 0
    in_transactions = False
    current_txn = None

    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        # Start of transactions
        if re.search(r'TRANSACTION\s+DETAILS', line, re.IGNORECASE):
            in_transactions = True
            i += 1
            # Skip header line(s)
            while i < len(lines):
                l = lines[i].strip()
                if 'Transaction' in l and 'Amount' in l:
                    i += 1
                    continue
                if l.startswith('Date') or l.startswith('Previous Balance'):
                    i += 1
                    continue
                break
            continue

        # Skip Previous Balance line
        if line.startswith('Previous Balance'):
            i += 1
            continue

        # End markers
        if re.search(r'New Balance|TOTAL AMOUNT|This statement', line, re.IGNORECASE):
            if current_txn:
                transactions.append(current_txn)
                current_txn = None
            in_transactions = False
            i += 1
            continue

        if not in_transactions:
            i += 1
            continue

        # Transaction line: "DD/MM/YYYY REFERENCE DESCRIPTION AMOUNT BILLING_AMOUNT"
        # e.g., "12/06/2014 4000291928502 SINGAPOR LONDON GB GBR W4 5YS - GBP 999,90 - 999,90"
        m = re.match(r'^(\d{2}/\d{2}/\d{4})\s+(\d{5,})\s+(.+)', line)
        if m:
            if current_txn:
                transactions.append(current_txn)

            date_str = m.group(1)
            ref = m.group(2)
            rest = m.group(3).strip()

            # Parse date DD/MM/YYYY
            parts = date_str.split('/')
            txn_date = f"{parts[2]}-{parts[1]}-{parts[0]}"

            # Extract billing amount (last European-format number)
            # Pattern: optional "- " then digits with optional period thousands and comma decimal
            amounts = re.findall(r'(-?\s*[\d.]+,\d{2})', rest)

            description = rest
            billing_amount = None
            foreign_amount = None
            foreign_currency = ''
            is_credit = False

            if amounts:
                billing_str = amounts[-1]
                billing_amount = parse_euro_amount(billing_str)
                last_idx = rest.rfind(billing_str.strip())
                description = rest[:last_idx].strip()

                # Check for "- " prefix meaning credit/refund
                if billing_str.strip().startswith('-') or (billing_amount and billing_amount < 0):
                    is_credit = True
                    billing_amount = abs(billing_amount) if billing_amount else None

                # If there's a second amount, it's the foreign amount
                if len(amounts) >= 2:
                    foreign_str = amounts[-2]
                    foreign_amount = parse_euro_amount(foreign_str)
                    if foreign_amount and foreign_amount < 0:
                        foreign_amount = abs(foreign_amount)
                    # Extract currency from between amounts
                    second_last_idx = rest.rfind(foreign_str.strip(), 0, last_idx)
                    between = rest[second_last_idx + len(foreign_str.strip()):last_idx].strip()
                    description = rest[:second_last_idx].strip()
                    # Currency hint might be in description (e.g., "USD", "GBP")
                    cur_m = re.search(r'\b(USD|GBP|EUR|SGD|HKD|JPY|INR)\b', between + ' ' + description)
                    if cur_m:
                        foreign_currency = cur_m.group(1)

            # Clean trailing "- GBP" or currency codes from description
            description = re.sub(r'\s*-?\s*(GBP|USD|EUR)\s*$', '', description).strip()

            current_txn = make_txn(
                stmt_date, source_folder, 'CITIBANK', 'GBP', txn_date,
                description, billing_amount, filename, card_number,
                foreign_amount=foreign_amount, foreign_currency=foreign_currency,
                is_credit=is_credit,
            )
            i += 1
            continue

        # Continuation lines
        if current_txn:
            # Exchange Rate line
            rate_m = re.match(r'^Exchange\s+Rate:\s*(\w+)1\s*=\s*(\w+)([\d.]+)', line, re.IGNORECASE)
            if rate_m:
                try:
                    current_txn['fx_rate'] = float(rate_m.group(3))
                    current_txn['foreign_currency'] = rate_m.group(1).strip()
                except ValueError:
                    pass
                i += 1
                continue

            # Passenger Name, Ticket Number, Routing, Departure - store as extra info
            if re.match(r'^(Passenger Name|Ticket Number|Departure|Routing)\b', line):
                i += 1
                continue

        i += 1

    if current_txn:
        transactions.append(current_txn)

    return transactions


# ── Main processing ──────────────────────────────────────────────────────────

def process_all():
    """Process all PDF statements and output CSV."""
    all_transactions = []
    errors = []
    skipped = []

    # Collect all PDF files from all source directories
    pdf_files = []
    for base_dir in SOURCE_DIRS:
        for root, dirs, files in os.walk(base_dir):
            for f in sorted(files):
                if f.lower().endswith('.pdf'):
                    pdf_files.append((os.path.join(root, f), base_dir))

    print(f"Found {len(pdf_files)} PDF files to process\n")

    for filepath, base_dir in sorted(pdf_files):
        filename = os.path.basename(filepath)
        rel_path = os.path.relpath(filepath, base_dir)
        source_folder = os.path.dirname(rel_path) or os.path.basename(base_dir)
        stmt_date = detect_statement_date_from_filename(filename)

        # Skip non-statement files
        skip_patterns = [
            'Direct Debit', 'DD Form', 'Cancellation',
            'Year End Summary', 'YearEndSummary',
        ]
        if any(p in filename for p in skip_patterns):
            skipped.append(f"  SKIP: {rel_path}")
            continue

        try:
            pdf = pdfplumber.open(filepath)
            full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
            fmt = detect_format(full_text)

            if fmt == 'GS_UK':
                txns = parse_uk_amex(pdf, stmt_date, source_folder, filepath, 'GS_UK')
            elif fmt == 'UK_PERSONAL':
                txns = parse_uk_amex(pdf, stmt_date, source_folder, filepath, 'UK_PERSONAL')
            elif fmt == 'CS_UK':
                txns = parse_uk_amex(pdf, stmt_date, source_folder, filepath, 'CS_UK')
            elif fmt == 'CS_US':
                txns = parse_us_amex(pdf, stmt_date, source_folder, filepath, 'CS_US')
            elif fmt == 'US_PERSONAL':
                txns = parse_us_amex(pdf, stmt_date, source_folder, filepath, 'US_PERSONAL')
            elif fmt == 'DB_UK':
                txns = parse_db_uk(pdf, stmt_date, source_folder, filepath)
            elif fmt == 'CITIBANK':
                txns = parse_citibank(pdf, stmt_date, source_folder, filepath)
            else:
                errors.append(f"  UNKNOWN FORMAT: {os.path.relpath(filepath, base_dir)}")
                pdf.close()
                continue

            # Fill in missing card numbers from known mappings
            card_fallbacks = {
                'CS US Corp Amex': {'CS_US': '3785-052496-81000'},
                'UK Amex': {'UK_PERSONAL': '3742-893067-17007'},
            }
            for txn in txns:
                if not txn.get('card_number'):
                    fallback = card_fallbacks.get(source_folder, {}).get(txn['format'], '')
                    if fallback:
                        txn['card_number'] = fallback

            print(f"  {fmt:15s} {len(txns):3d} txns  {rel_path}")
            all_transactions.extend(txns)
            pdf.close()

        except Exception as e:
            errors.append(f"  ERROR: {rel_path}: {e}")
            import traceback
            traceback.print_exc()

    # Write CSV
    fieldnames = [
        'statement_date', 'source', 'format', 'card_number', 'cardholder',
        'currency', 'transaction_date', 'process_date',
        'description', 'category',
        'foreign_amount', 'foreign_currency', 'amount', 'is_credit',
        'fx_rate', 'fx_fee', 'filename',
    ]

    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for txn in sorted(all_transactions,
                          key=lambda t: (t.get('transaction_date') or '', t.get('statement_date') or '')):
            writer.writerow(txn)

    print(f"\n{'='*60}")
    print(f"Total transactions: {len(all_transactions)}")
    print(f"Output: {OUTPUT_CSV}")

    if skipped:
        print(f"\nSkipped ({len(skipped)}):")
        for s in skipped:
            print(s)

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for e in errors:
            print(e)

    # Summary stats
    print(f"\nBy format:")
    formats = {}
    for t in all_transactions:
        formats[t['format']] = formats.get(t['format'], 0) + 1
    for k, v in sorted(formats.items()):
        print(f"  {k}: {v}")

    print(f"\nBy card_number:")
    cards = {}
    for t in all_transactions:
        key = t.get('card_number', '?') or '?'
        cards[key] = cards.get(key, 0) + 1
    for k, v in sorted(cards.items()):
        print(f"  {k}: {v}")

    print(f"\nBy year:")
    years = {}
    for t in all_transactions:
        yr = t.get('transaction_date', '')[:4] if t.get('transaction_date') else 'unknown'
        years[yr] = years.get(yr, 0) + 1
    for k, v in sorted(years.items()):
        print(f"  {k}: {v}")

    # Validate: check for missing amounts
    missing_amount = sum(1 for t in all_transactions if t.get('amount') is None)
    if missing_amount:
        print(f"\nWARNING: {missing_amount} transactions with missing amount")

    missing_date = sum(1 for t in all_transactions if not t.get('transaction_date'))
    if missing_date:
        print(f"WARNING: {missing_date} transactions with missing date")


if __name__ == '__main__':
    process_all()
