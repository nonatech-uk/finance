"""Tag rule endpoints — automatic transaction tagging by date range, account, merchant, and category."""

import re

from fastapi import APIRouter, Depends, HTTPException

from src.api.deps import CurrentUser, get_conn, get_current_user, require_admin
from src.api.models import (
    TagRuleApplyResult,
    TagRuleCreate,
    TagRuleItem,
    TagRuleList,
    TagRuleUpdate,
)

router = APIRouter()

_COLS = ("id, name, date_from, date_to, account_ids::text[], merchant_pattern, "
         "category_pattern, tags, is_active, priority, created_at, updated_at")


def _row_to_item(columns: list[str], row: tuple) -> TagRuleItem:
    return TagRuleItem(**dict(zip(columns, row)))


@router.get("/tag-rules", response_model=TagRuleList)
def list_tag_rules(
    conn=Depends(get_conn),
    user: CurrentUser = Depends(get_current_user),
):
    """List all tag rules ordered by priority."""
    cur = conn.cursor()
    cur.execute(f"SELECT {_COLS} FROM tag_rule ORDER BY priority, id")
    columns = [desc[0] for desc in cur.description]
    return TagRuleList(items=[_row_to_item(columns, r) for r in cur.fetchall()])


@router.post("/tag-rules", response_model=TagRuleItem, status_code=201)
def create_tag_rule(
    body: TagRuleCreate,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Create a new tag rule."""
    if not body.tags:
        raise HTTPException(400, "At least one tag is required")

    if body.merchant_pattern:
        try:
            re.compile(body.merchant_pattern)
        except re.error as e:
            raise HTTPException(400, f"Invalid regex: {e}")

    cur = conn.cursor()

    if body.account_ids:
        str_ids = [str(aid) for aid in body.account_ids]
        cur.execute("SELECT id FROM account WHERE id = ANY(%s::uuid[])", (str_ids,))
        found = {str(r[0]) for r in cur.fetchall()}
        missing = set(str_ids) - found
        if missing:
            raise HTTPException(400, f"Account IDs not found: {missing}")

    cur.execute(f"""
        INSERT INTO tag_rule (name, date_from, date_to, account_ids, merchant_pattern,
                              category_pattern, tags, is_active, priority)
        VALUES (%s, %s, %s, %s::uuid[], %s, %s, %s, %s, %s)
        RETURNING {_COLS}
    """, (
        body.name,
        body.date_from,
        body.date_to,
        [str(aid) for aid in body.account_ids],
        body.merchant_pattern,
        body.category_pattern,
        body.tags,
        body.is_active,
        body.priority,
    ))
    columns = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    conn.commit()
    return _row_to_item(columns, row)


@router.put("/tag-rules/{rule_id}", response_model=TagRuleItem)
def update_tag_rule(
    rule_id: int,
    body: TagRuleUpdate,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Update an existing tag rule (partial update)."""
    data = body.model_dump(exclude_unset=True)
    if not data:
        raise HTTPException(400, "No fields to update")

    if "tags" in data and not data["tags"]:
        raise HTTPException(400, "At least one tag is required")

    if "merchant_pattern" in data and data["merchant_pattern"]:
        try:
            re.compile(data["merchant_pattern"])
        except re.error as e:
            raise HTTPException(400, f"Invalid regex: {e}")

    cur = conn.cursor()

    if "account_ids" in data:
        data["account_ids"] = [str(aid) for aid in data["account_ids"]]

    set_parts = []
    params = {"rule_id": rule_id}
    for key, val in data.items():
        # uuid[] column needs explicit cast — psycopg2 sends text[]
        cast = "::uuid[]" if key == "account_ids" else ""
        set_parts.append(f"{key} = %({key})s{cast}")
        params[key] = val
    set_parts.append("updated_at = now()")

    cur.execute(f"""
        UPDATE tag_rule SET {", ".join(set_parts)}
        WHERE id = %(rule_id)s
        RETURNING {_COLS}
    """, params)
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Tag rule not found")

    columns = [desc[0] for desc in cur.description]
    conn.commit()
    return _row_to_item(columns, row)


@router.delete("/tag-rules/{rule_id}")
def delete_tag_rule(
    rule_id: int,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Delete a tag rule and remove all tags it generated."""
    cur = conn.cursor()

    cur.execute("DELETE FROM transaction_tag WHERE tag_rule_id = %s", (rule_id,))
    tags_removed = cur.rowcount

    cur.execute("DELETE FROM tag_rule WHERE id = %s", (rule_id,))
    if cur.rowcount == 0:
        raise HTTPException(404, "Tag rule not found")

    conn.commit()
    return {"id": rule_id, "deleted": True, "tags_removed": tags_removed}


@router.post("/tag-rules/apply", response_model=TagRuleApplyResult)
def apply_tag_rules(
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Apply all active tag rules (full reconciliation).

    1. Delete all rule-generated tags
    2. For each active rule, insert matching tags
    3. ON CONFLICT DO NOTHING preserves manual tags
    """
    cur = conn.cursor()

    # Step 1: Clear tags only for ACTIVE rules (inactive rules' tags are preserved)
    cur.execute("""
        DELETE FROM transaction_tag
        WHERE tag_rule_id IN (SELECT id FROM tag_rule WHERE is_active)
    """)
    tags_removed = cur.rowcount

    # Step 2: Load active rules
    cur.execute("""
        SELECT id, date_from, date_to, account_ids::text[], merchant_pattern,
               category_pattern, tags
        FROM tag_rule
        WHERE is_active
        ORDER BY priority, id
    """)
    rules = cur.fetchall()

    total_created = 0

    for rule_id, date_from, date_to, account_ids, merchant_pattern, category_pattern, tags in rules:
        conditions = []
        params: dict = {"rule_id": rule_id, "tags": tags}
        joins = []
        has_merchant_join = False

        if date_from:
            conditions.append("rt.posted_at >= %(date_from)s")
            params["date_from"] = date_from
        if date_to:
            conditions.append("rt.posted_at <= %(date_to)s")
            params["date_to"] = date_to
        if account_ids:
            joins.append(
                "JOIN account a ON a.institution = rt.institution "
                "AND a.account_ref = rt.account_ref"
            )
            conditions.append("a.id = ANY(%(account_ids)s::uuid[])")
            params["account_ids"] = [str(aid) for aid in account_ids]
        if merchant_pattern:
            has_merchant_join = True
            joins.append("JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id")
            joins.append("JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant")
            joins.append("JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id")
            conditions.append("cm.name ~* %(merchant_pattern)s")
            params["merchant_pattern"] = merchant_pattern
        if category_pattern:
            # Resolve effective category: override > merchant override > default merchant
            joins.append("LEFT JOIN transaction_category_override tco ON tco.raw_transaction_id = rt.id")
            if not has_merchant_join:
                joins.append("LEFT JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id")
                joins.append("LEFT JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant")
                joins.append("LEFT JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id")
            joins.append("LEFT JOIN transaction_merchant_override tmo ON tmo.raw_transaction_id = rt.id")
            joins.append("LEFT JOIN canonical_merchant cm_ov ON cm_ov.id = tmo.canonical_merchant_id")
            conditions.append(
                "COALESCE(tco.category_path, cm_ov.category_hint, cm.category_hint) "
                "LIKE %(category_pattern)s"
            )
            params["category_pattern"] = category_pattern + "%"

        join_clause = "\n".join(joins)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        cur.execute(f"""
            INSERT INTO transaction_tag (raw_transaction_id, tag, source, tag_rule_id)
            SELECT DISTINCT rt.id, t.tag, 'rule', %(rule_id)s
            FROM active_transaction rt
            {join_clause}
            CROSS JOIN unnest(%(tags)s::text[]) AS t(tag)
            {where}
            ON CONFLICT (raw_transaction_id, tag) DO NOTHING
        """, params)
        total_created += cur.rowcount

    conn.commit()

    return TagRuleApplyResult(
        rules_applied=len(rules),
        tags_created=total_created,
        tags_removed=tags_removed,
    )
