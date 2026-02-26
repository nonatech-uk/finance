"""CSV import endpoints."""

import tempfile
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from src.api.deps import get_conn
from src.api.models import CsvPreviewResult, CsvPreviewTransaction, CsvMismatch, CsvImportResult
from src.ingestion.csv_dispatch import (
    detect_format,
    parse_csv,
    preview_import,
    execute_import,
    run_post_import,
)

router = APIRouter()

# In-memory store for parsed transactions between preview and confirm.
# Keyed by (institution, account_ref). Single-user app so this is fine.
_pending_imports: dict[tuple[str, str], list[dict]] = {}


@router.post("/imports/csv/preview", response_model=CsvPreviewResult)
def csv_preview(
    file: UploadFile = File(...),
    institution: str = Form(...),
    account_ref: str = Form(...),
    conn=Depends(get_conn),
):
    """Upload a CSV and preview what would be imported.

    Auto-detects the CSV format, parses it, and compares against
    existing transactions in the database.
    """
    file_bytes = file.file.read()
    if not file_bytes:
        raise HTTPException(400, "Empty file")

    fmt = detect_format(file_bytes)
    if not fmt:
        raise HTTPException(400, "Unrecognised CSV format. Check the file has a valid header row.")

    try:
        txns = parse_csv(file_bytes, fmt, institution, account_ref)
    except Exception as e:
        raise HTTPException(400, f"Failed to parse CSV: {e}")

    if not txns:
        raise HTTPException(400, "No valid transactions found in CSV")

    preview = preview_import(txns, institution, account_ref, conn)

    # Stash parsed transactions for confirm step
    _pending_imports[(institution, account_ref)] = txns

    return CsvPreviewResult(
        format=fmt,
        total_rows=preview["total_rows"],
        new_count=preview["new_count"],
        existing_count=preview["existing_count"],
        mismatch_count=preview["mismatch_count"],
        new_transactions=[CsvPreviewTransaction(**t) for t in preview["new"]],
        mismatches=[CsvMismatch(**m) for m in preview["mismatches"]],
    )


@router.post("/imports/csv/confirm", response_model=CsvImportResult)
def csv_confirm(
    institution: str = Form(...),
    account_ref: str = Form(...),
    conn=Depends(get_conn),
):
    """Confirm and execute a previously previewed CSV import.

    Imports new transactions, then runs cleaning and dedup pipeline.
    """
    key = (institution, account_ref)
    txns = _pending_imports.pop(key, None)
    if not txns:
        raise HTTPException(
            400,
            "No pending import found. Upload a CSV for preview first.",
        )

    fmt = txns[0].get("source", "unknown")

    try:
        result = execute_import(txns, fmt, conn)
    except Exception as e:
        raise HTTPException(500, f"Import failed: {e}")

    # Run cleaning + dedup pipeline
    try:
        pipeline = run_post_import()
    except Exception as e:
        # Import succeeded but pipeline had issues — still return success
        pipeline = {"error": str(e)}

    return CsvImportResult(
        inserted=result["inserted"],
        skipped=result["skipped"],
        pipeline=pipeline,
    )
