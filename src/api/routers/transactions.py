"""Transaction endpoints."""

from datetime import date
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.deps import get_conn
from src.api.models import (
    BulkCategoryUpdate,
    BulkMerchantNameUpdate,
    BulkNoteUpdate,
    BulkTagAdd,
    BulkTagRemove,
    BulkTagReplace,
    CategoryUpdate,
    DedupGroupInfo,
    DedupMember,
    EconomicEventInfo,
    EconomicEventLeg,
    LinkTransferRequest,
    NoteUpdate,
    TagItem,
    TagUpdate,
    TransactionDetail,
    TransactionItem,
    TransactionList,
)

router = APIRouter()


@router.get("/transactions", response_model=TransactionList)
def list_transactions(
    cursor: str | None = Query(None, description="Cursor for pagination (posted_at,id)"),
    limit: int = Query(50, ge=1, le=200),
    institution: str | None = None,
    account_ref: str | None = None,
    source: str | None = None,
    category: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    amount_min: Decimal | None = None,
    amount_max: Decimal | None = None,
    currency: str | None = None,
    search: str | None = Query(None, description="Search merchant name"),
    tag: str | None = Query(None, description="Filter by tag"),
    conn=Depends(get_conn),
):
    """List deduplicated transactions with full merchant/category chain."""
    cur = conn.cursor()

    # Build WHERE clauses
    conditions = []
    params: dict = {"limit": limit + 1}  # fetch one extra to detect has_more

    # Keyset pagination: cursor is "posted_at,id"
    if cursor:
        try:
            cursor_date_str, cursor_id = cursor.split(",", 1)
            params["cursor_date"] = cursor_date_str
            params["cursor_id"] = cursor_id
            conditions.append(
                "(rt.posted_at, rt.id) < (%(cursor_date)s::date, %(cursor_id)s::uuid)"
            )
        except ValueError:
            raise HTTPException(400, "Invalid cursor format, expected 'date,uuid'")

    if institution:
        conditions.append("rt.institution = %(institution)s")
        params["institution"] = institution
    if account_ref:
        conditions.append("rt.account_ref = %(account_ref)s")
        params["account_ref"] = account_ref
    if source:
        conditions.append("rt.source = %(source)s")
        params["source"] = source
    if date_from:
        conditions.append("rt.posted_at >= %(date_from)s")
        params["date_from"] = date_from
    if date_to:
        conditions.append("rt.posted_at <= %(date_to)s")
        params["date_to"] = date_to
    if amount_min is not None:
        conditions.append("rt.amount >= %(amount_min)s")
        params["amount_min"] = amount_min
    if amount_max is not None:
        conditions.append("rt.amount <= %(amount_max)s")
        params["amount_max"] = amount_max
    if currency:
        conditions.append("rt.currency = %(currency)s")
        params["currency"] = currency
    if search:
        conditions.append(
            "(cm.name ILIKE %(search)s OR ct.cleaned_merchant ILIKE %(search)s "
            "OR rt.raw_merchant ILIKE %(search)s OR tn.note ILIKE %(search)s "
            "OR trim(trailing '.' from trim(trailing '0' from rt.amount::text)) LIKE %(amount_search)s)"
        )
        params["search"] = f"%{search}%"
        params["amount_search"] = f"%{search}%"
    if category:
        conditions.append("COALESCE(tcat.full_path, cat.full_path) LIKE %(category)s")
        params["category"] = f"{category}%"
    if tag:
        conditions.append(
            "EXISTS (SELECT 1 FROM transaction_tag tt WHERE tt.raw_transaction_id = rt.id AND tt.tag = %(tag)s)"
        )
        params["tag"] = tag

    # Always exclude archived accounts
    conditions.append("(a.is_archived IS NOT TRUE)")

    where = "WHERE " + " AND ".join(conditions)

    sql = f"""
        SELECT
            rt.id, rt.source, rt.institution, rt.account_ref,
            rt.posted_at, rt.amount, rt.currency,
            rt.raw_merchant, rt.raw_memo,
            ct.cleaned_merchant,
            cm.id AS canonical_merchant_id,
            cm.name AS canonical_merchant_name,
            mrm.match_type AS merchant_match_type,
            COALESCE(tcat.full_path, cat.full_path) AS category_path,
            COALESCE(tcat.name, cat.name) AS category_name,
            COALESCE(tcat.category_type, cat.category_type) AS category_type,
            (tco.raw_transaction_id IS NOT NULL) AS category_is_override,
            tn.note
        FROM active_transaction rt
        LEFT JOIN account a
            ON a.institution = rt.institution AND a.account_ref = rt.account_ref
        LEFT JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id
        LEFT JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        LEFT JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        LEFT JOIN category cat ON cat.full_path = cm.category_hint
        LEFT JOIN transaction_category_override tco ON tco.raw_transaction_id = rt.id
        LEFT JOIN category tcat ON tcat.full_path = tco.category_path
        LEFT JOIN transaction_note tn ON tn.raw_transaction_id = rt.id
        {where}
        ORDER BY rt.posted_at DESC, rt.id DESC
        LIMIT %(limit)s
    """

    cur.execute(sql, params)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    items = [TransactionItem(**dict(zip(columns, row))) for row in rows]

    # Batch-load tags for returned items
    if items:
        txn_ids = [str(item.id) for item in items]
        cur.execute("""
            SELECT raw_transaction_id, array_agg(tag ORDER BY tag)
            FROM transaction_tag
            WHERE raw_transaction_id = ANY(%s::uuid[])
            GROUP BY raw_transaction_id
        """, (txn_ids,))
        tags_by_txn = {str(r[0]): r[1] for r in cur.fetchall()}
        for item in items:
            item.tags = tags_by_txn.get(str(item.id), [])

    next_cursor = None
    if has_more and items:
        last = items[-1]
        next_cursor = f"{last.posted_at},{last.id}"

    return TransactionList(items=items, next_cursor=next_cursor, has_more=has_more)


@router.get("/transactions/{transaction_id}", response_model=TransactionDetail)
def get_transaction(
    transaction_id: UUID,
    conn=Depends(get_conn),
):
    """Get full transaction detail including dedup group and economic event."""
    cur = conn.cursor()

    # Base transaction with merchant/category chain + note
    cur.execute("""
        SELECT
            rt.id, rt.source, rt.institution, rt.account_ref,
            rt.posted_at, rt.amount, rt.currency,
            rt.raw_merchant, rt.raw_memo, rt.raw_data,
            ct.cleaned_merchant,
            cm.id AS canonical_merchant_id,
            cm.name AS canonical_merchant_name,
            mrm.match_type AS merchant_match_type,
            COALESCE(tcat.full_path, cat.full_path) AS category_path,
            COALESCE(tcat.name, cat.name) AS category_name,
            COALESCE(tcat.category_type, cat.category_type) AS category_type,
            (tco.raw_transaction_id IS NOT NULL) AS category_is_override,
            tn.note,
            tn.source AS note_source
        FROM raw_transaction rt
        LEFT JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id
        LEFT JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        LEFT JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        LEFT JOIN category cat ON cat.full_path = cm.category_hint
        LEFT JOIN transaction_category_override tco ON tco.raw_transaction_id = rt.id
        LEFT JOIN category tcat ON tcat.full_path = tco.category_path
        LEFT JOIN transaction_note tn ON tn.raw_transaction_id = rt.id
        WHERE rt.id = %s
    """, (str(transaction_id),))

    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Transaction not found")

    columns = [desc[0] for desc in cur.description]
    txn_data = dict(zip(columns, row))

    # Dedup group info
    dedup_group = None
    cur.execute("""
        SELECT dg.id, dg.match_rule, dg.confidence,
               dgm2.raw_transaction_id, rt2.source, dgm2.is_preferred
        FROM dedup_group_member dgm
        JOIN dedup_group dg ON dg.id = dgm.dedup_group_id
        JOIN dedup_group_member dgm2 ON dgm2.dedup_group_id = dg.id
        JOIN raw_transaction rt2 ON rt2.id = dgm2.raw_transaction_id
        WHERE dgm.raw_transaction_id = %s
    """, (str(transaction_id),))

    dedup_rows = cur.fetchall()
    if dedup_rows:
        members = [
            DedupMember(
                raw_transaction_id=r[3],
                source=r[4],
                is_preferred=r[5],
            )
            for r in dedup_rows
        ]
        dedup_group = DedupGroupInfo(
            group_id=dedup_rows[0][0],
            match_rule=dedup_rows[0][1],
            confidence=dedup_rows[0][2],
            members=members,
        )

    # Economic event info
    economic_event = None
    cur.execute("""
        SELECT ee.id, ee.event_type, ee.initiated_at, ee.description,
               eel2.raw_transaction_id, eel2.leg_type, eel2.amount, eel2.currency
        FROM economic_event_leg eel
        JOIN economic_event ee ON ee.id = eel.economic_event_id
        JOIN economic_event_leg eel2 ON eel2.economic_event_id = ee.id
        WHERE eel.raw_transaction_id = %s
    """, (str(transaction_id),))

    event_rows = cur.fetchall()
    if event_rows:
        legs = [
            EconomicEventLeg(
                raw_transaction_id=r[4],
                leg_type=r[5],
                amount=r[6],
                currency=r[7],
            )
            for r in event_rows
        ]
        economic_event = EconomicEventInfo(
            event_id=event_rows[0][0],
            event_type=event_rows[0][1],
            initiated_at=event_rows[0][2],
            description=event_rows[0][3],
            legs=legs,
        )

    # Tags
    cur.execute("""
        SELECT tag, source FROM transaction_tag
        WHERE raw_transaction_id = %s
        ORDER BY tag
    """, (str(transaction_id),))
    tags = [TagItem(tag=r[0], source=r[1]) for r in cur.fetchall()]

    return TransactionDetail(
        **txn_data,
        tags=tags,
        dedup_group=dedup_group,
        economic_event=economic_event,
    )


@router.put("/transactions/{transaction_id}/note")
def update_note(
    transaction_id: UUID,
    body: NoteUpdate,
    conn=Depends(get_conn),
):
    """Create, update, or delete a transaction note."""
    cur = conn.cursor()

    # Verify transaction exists
    cur.execute("SELECT 1 FROM raw_transaction WHERE id = %s", (str(transaction_id),))
    if not cur.fetchone():
        raise HTTPException(404, "Transaction not found")

    note_text = body.note.strip()

    if not note_text:
        # Delete note
        cur.execute(
            "DELETE FROM transaction_note WHERE raw_transaction_id = %s",
            (str(transaction_id),),
        )
    else:
        # Upsert note
        cur.execute("""
            INSERT INTO transaction_note (raw_transaction_id, note, source)
            VALUES (%s, %s, 'user')
            ON CONFLICT (raw_transaction_id)
            DO UPDATE SET note = EXCLUDED.note, source = 'user', updated_at = now()
        """, (str(transaction_id), note_text))

    conn.commit()
    return {"ok": True}


@router.put("/transactions/{transaction_id}/category")
def update_category(
    transaction_id: UUID,
    body: CategoryUpdate,
    conn=Depends(get_conn),
):
    """Create, update, or delete a transaction category override."""
    cur = conn.cursor()

    # Verify transaction exists
    cur.execute("SELECT 1 FROM raw_transaction WHERE id = %s", (str(transaction_id),))
    if not cur.fetchone():
        raise HTTPException(404, "Transaction not found")

    category_path = body.category_path.strip()

    if not category_path:
        # Delete override — falls back to merchant category
        cur.execute(
            "DELETE FROM transaction_category_override WHERE raw_transaction_id = %s",
            (str(transaction_id),),
        )
    else:
        # Validate category exists
        cur.execute("SELECT 1 FROM category WHERE full_path = %s", (category_path,))
        if not cur.fetchone():
            raise HTTPException(400, f"Category not found: {category_path}")

        # Upsert override
        cur.execute("""
            INSERT INTO transaction_category_override (raw_transaction_id, category_path, source)
            VALUES (%s, %s, 'user')
            ON CONFLICT (raw_transaction_id)
            DO UPDATE SET category_path = EXCLUDED.category_path, source = 'user', updated_at = now()
        """, (str(transaction_id), category_path))

    conn.commit()
    return {"ok": True}


@router.post("/transactions/{transaction_id}/link-transfer")
def link_transfer(
    transaction_id: UUID,
    body: LinkTransferRequest,
    conn=Depends(get_conn),
):
    """Link two transactions as an inter-account transfer / FX conversion."""
    cur = conn.cursor()

    txn_id_a = str(transaction_id)
    txn_id_b = str(body.counterpart_id)

    if txn_id_a == txn_id_b:
        raise HTTPException(400, "Cannot link a transaction to itself")

    # Fetch both transactions
    cur.execute("""
        SELECT id, amount, currency, posted_at
        FROM raw_transaction WHERE id IN (%s, %s)
    """, (txn_id_a, txn_id_b))
    rows = cur.fetchall()
    if len(rows) != 2:
        raise HTTPException(404, "One or both transactions not found")

    txns = {str(r[0]): {"amount": r[1], "currency": r[2], "posted_at": r[3]} for r in rows}

    # Check neither is already in an economic event
    cur.execute("""
        SELECT raw_transaction_id FROM economic_event_leg
        WHERE raw_transaction_id IN (%s, %s)
    """, (txn_id_a, txn_id_b))
    existing = cur.fetchall()
    if existing:
        raise HTTPException(409, "One or both transactions are already linked to an economic event")

    a = txns[txn_id_a]
    b = txns[txn_id_b]

    # Determine event type
    is_fx = a["currency"] != b["currency"]
    event_type = "fx_conversion" if is_fx else "inter_account_transfer"

    # Determine source (debit) and target (credit)
    if a["amount"] < 0:
        source_id, target_id = txn_id_a, txn_id_b
        source, target = a, b
    else:
        source_id, target_id = txn_id_b, txn_id_a
        source, target = b, a

    description = f"{abs(source['amount'])} {source['currency']} -> {abs(target['amount'])} {target['currency']}"

    # Create economic event
    cur.execute("""
        INSERT INTO economic_event (event_type, initiated_at, description, match_status)
        VALUES (%s, %s, %s, 'manual')
        RETURNING id
    """, (event_type, source["posted_at"], description))
    event_id = cur.fetchone()[0]

    # Create legs
    cur.execute("""
        INSERT INTO economic_event_leg
            (economic_event_id, raw_transaction_id, leg_type, amount, currency)
        VALUES (%s, %s, 'source', %s, %s)
    """, (event_id, source_id, source["amount"], source["currency"]))

    cur.execute("""
        INSERT INTO economic_event_leg
            (economic_event_id, raw_transaction_id, leg_type, amount, currency)
        VALUES (%s, %s, 'target', %s, %s)
    """, (event_id, target_id, target["amount"], target["currency"]))

    # Create fx_event if cross-currency
    if is_fx and source["amount"] != 0:
        rate = abs(target["amount"] / source["amount"])
        cur.execute("""
            INSERT INTO fx_event
                (economic_event_id, source_amount, source_currency,
                 target_amount, target_currency, achieved_rate, provider)
            VALUES (%s, %s, %s, %s, %s, %s, 'manual')
        """, (
            event_id,
            abs(source["amount"]),
            source["currency"],
            abs(target["amount"]),
            target["currency"],
            rate,
        ))

    # Set +Transfer category override on both legs
    for tid in [source_id, target_id]:
        cur.execute("""
            INSERT INTO transaction_category_override (raw_transaction_id, category_path, source)
            VALUES (%s, '+Transfer', 'system')
            ON CONFLICT (raw_transaction_id)
            DO UPDATE SET category_path = '+Transfer', source = 'system', updated_at = now()
        """, (tid,))

    conn.commit()
    return {"ok": True, "event_id": str(event_id)}


@router.delete("/economic-events/{event_id}")
def unlink_event(
    event_id: UUID,
    conn=Depends(get_conn),
):
    """Delete an economic event and its legs (unlink transactions)."""
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM economic_event WHERE id = %s", (str(event_id),))
    if not cur.fetchone():
        raise HTTPException(404, "Economic event not found")

    # Get leg transaction IDs before deleting
    cur.execute(
        "SELECT raw_transaction_id FROM economic_event_leg WHERE economic_event_id = %s",
        (str(event_id),),
    )
    leg_txn_ids = [row[0] for row in cur.fetchall()]

    cur.execute("DELETE FROM fx_event WHERE economic_event_id = %s", (str(event_id),))
    cur.execute("DELETE FROM economic_event_leg WHERE economic_event_id = %s", (str(event_id),))
    cur.execute("DELETE FROM economic_event WHERE id = %s", (str(event_id),))

    # Remove system-set +Transfer overrides (preserve user overrides)
    for tid in leg_txn_ids:
        cur.execute("""
            DELETE FROM transaction_category_override
            WHERE raw_transaction_id = %s AND category_path = '+Transfer' AND source = 'system'
        """, (str(tid),))

    conn.commit()
    return {"ok": True}


# ── Tags ─────────────────────────────────────────────────────────────────────


@router.get("/tags")
def list_tags(conn=Depends(get_conn)):
    """List all known tags with usage counts (for autocomplete)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT tag, COUNT(*) AS usage_count
        FROM transaction_tag
        GROUP BY tag
        ORDER BY tag
    """)
    return {"items": [{"tag": r[0], "count": r[1]} for r in cur.fetchall()]}


@router.post("/transactions/{transaction_id}/tags")
def add_tag(
    transaction_id: UUID,
    body: TagUpdate,
    conn=Depends(get_conn),
):
    """Add a tag to a transaction."""
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM raw_transaction WHERE id = %s", (str(transaction_id),))
    if not cur.fetchone():
        raise HTTPException(404, "Transaction not found")

    tag_name = body.tag.strip()
    if not tag_name:
        raise HTTPException(400, "Tag cannot be empty")

    cur.execute("""
        INSERT INTO transaction_tag (raw_transaction_id, tag, source)
        VALUES (%s, %s, 'user')
        ON CONFLICT (raw_transaction_id, tag) DO NOTHING
    """, (str(transaction_id), tag_name))

    conn.commit()
    return {"ok": True}


@router.delete("/transactions/{transaction_id}/tags/{tag_name}")
def remove_tag(
    transaction_id: UUID,
    tag_name: str,
    conn=Depends(get_conn),
):
    """Remove a tag from a transaction."""
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM transaction_tag
        WHERE raw_transaction_id = %s AND tag = %s
    """, (str(transaction_id), tag_name))

    if cur.rowcount == 0:
        raise HTTPException(404, "Tag not found on this transaction")

    conn.commit()
    return {"ok": True}


# ── Bulk Operations ──────────────────────────────────────────────────────────


@router.post("/transactions/bulk/category")
def bulk_update_category(body: BulkCategoryUpdate, conn=Depends(get_conn)):
    """Bulk set or remove category override on multiple transactions."""
    cur = conn.cursor()
    ids = [str(tid) for tid in body.transaction_ids]
    if not ids:
        return {"ok": True, "affected": 0}

    category_path = body.category_path.strip()

    if not category_path:
        # Remove overrides
        cur.execute("""
            DELETE FROM transaction_category_override
            WHERE raw_transaction_id = ANY(%s::uuid[])
        """, (ids,))
        affected = cur.rowcount
    else:
        # Validate category exists
        cur.execute("SELECT 1 FROM category WHERE full_path = %s", (category_path,))
        if not cur.fetchone():
            raise HTTPException(400, f"Category not found: {category_path}")

        # Upsert overrides
        cur.execute("""
            INSERT INTO transaction_category_override (raw_transaction_id, category_path, source)
            SELECT unnest(%s::uuid[]), %s, 'user'
            ON CONFLICT (raw_transaction_id)
            DO UPDATE SET category_path = EXCLUDED.category_path,
                          source = 'user', updated_at = now()
        """, (ids, category_path))
        affected = cur.rowcount

    conn.commit()
    return {"ok": True, "affected": affected}


@router.post("/transactions/bulk/merchant-name")
def bulk_update_merchant_name(body: BulkMerchantNameUpdate, conn=Depends(get_conn)):
    """Bulk update display_name for all canonical merchants of given transactions."""
    cur = conn.cursor()
    ids = [str(tid) for tid in body.transaction_ids]
    if not ids:
        return {"ok": True, "affected": 0, "merchant_ids": []}

    # Find distinct canonical_merchant IDs for these transactions
    cur.execute("""
        SELECT DISTINCT cm.id
        FROM raw_transaction rt
        JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id
        JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        WHERE rt.id = ANY(%s::uuid[])
          AND cm.merged_into_id IS NULL
    """, (ids,))
    merchant_ids = [str(r[0]) for r in cur.fetchall()]

    if not merchant_ids:
        return {"ok": True, "affected": 0, "merchant_ids": []}

    cur.execute("""
        UPDATE canonical_merchant
        SET display_name = %s
        WHERE id = ANY(%s::uuid[])
    """, (body.display_name, merchant_ids))
    affected = cur.rowcount

    conn.commit()
    return {"ok": True, "affected": affected, "merchant_ids": merchant_ids}


@router.post("/transactions/bulk/tags/add")
def bulk_add_tags(body: BulkTagAdd, conn=Depends(get_conn)):
    """Add tag(s) to multiple transactions."""
    cur = conn.cursor()
    ids = [str(tid) for tid in body.transaction_ids]
    tags = [t.strip() for t in body.tags if t.strip()]
    if not ids or not tags:
        return {"ok": True, "affected": 0}

    cur.execute("""
        INSERT INTO transaction_tag (raw_transaction_id, tag, source)
        SELECT t_id, t_tag, 'user'
        FROM unnest(%s::uuid[]) AS t_id
        CROSS JOIN unnest(%s::text[]) AS t_tag
        ON CONFLICT (raw_transaction_id, tag) DO NOTHING
    """, (ids, tags))
    affected = cur.rowcount

    conn.commit()
    return {"ok": True, "affected": affected}


@router.post("/transactions/bulk/tags/remove")
def bulk_remove_tag(body: BulkTagRemove, conn=Depends(get_conn)):
    """Remove a single tag from multiple transactions."""
    cur = conn.cursor()
    ids = [str(tid) for tid in body.transaction_ids]
    tag = body.tag.strip()
    if not ids or not tag:
        return {"ok": True, "affected": 0}

    cur.execute("""
        DELETE FROM transaction_tag
        WHERE raw_transaction_id = ANY(%s::uuid[]) AND tag = %s
    """, (ids, tag))
    affected = cur.rowcount

    conn.commit()
    return {"ok": True, "affected": affected}


@router.post("/transactions/bulk/tags/replace")
def bulk_replace_tags(body: BulkTagReplace, conn=Depends(get_conn)):
    """Replace all tags on selected transactions with a new set."""
    cur = conn.cursor()
    ids = [str(tid) for tid in body.transaction_ids]
    tags = [t.strip() for t in body.tags if t.strip()]
    if not ids:
        return {"ok": True, "affected": 0, "removed": 0}

    # Delete all existing tags
    cur.execute("""
        DELETE FROM transaction_tag
        WHERE raw_transaction_id = ANY(%s::uuid[])
    """, (ids,))
    removed = cur.rowcount

    added = 0
    if tags:
        cur.execute("""
            INSERT INTO transaction_tag (raw_transaction_id, tag, source)
            SELECT t_id, t_tag, 'user'
            FROM unnest(%s::uuid[]) AS t_id
            CROSS JOIN unnest(%s::text[]) AS t_tag
            ON CONFLICT (raw_transaction_id, tag) DO NOTHING
        """, (ids, tags))
        added = cur.rowcount

    conn.commit()
    return {"ok": True, "affected": added, "removed": removed}


@router.post("/transactions/bulk/note")
def bulk_update_note(body: BulkNoteUpdate, conn=Depends(get_conn)):
    """Bulk set or append notes on multiple transactions."""
    cur = conn.cursor()
    ids = [str(tid) for tid in body.transaction_ids]
    note_text = body.note.strip()
    mode = body.mode  # "replace" or "append"

    if not ids:
        return {"ok": True, "affected": 0}

    if not note_text and mode == "replace":
        # Delete notes
        cur.execute("""
            DELETE FROM transaction_note
            WHERE raw_transaction_id = ANY(%s::uuid[])
        """, (ids,))
        affected = cur.rowcount
    elif mode == "append":
        # Append: for existing notes, concat with newline; for missing, insert
        cur.execute("""
            INSERT INTO transaction_note (raw_transaction_id, note, source)
            SELECT unnest(%s::uuid[]), %s, 'user'
            ON CONFLICT (raw_transaction_id)
            DO UPDATE SET note = transaction_note.note || E'\n' || EXCLUDED.note,
                          source = 'user', updated_at = now()
        """, (ids, note_text))
        affected = cur.rowcount
    else:
        # Replace
        cur.execute("""
            INSERT INTO transaction_note (raw_transaction_id, note, source)
            SELECT unnest(%s::uuid[]), %s, 'user'
            ON CONFLICT (raw_transaction_id)
            DO UPDATE SET note = EXCLUDED.note,
                          source = 'user', updated_at = now()
        """, (ids, note_text))
        affected = cur.rowcount

    conn.commit()
    return {"ok": True, "affected": affected}
