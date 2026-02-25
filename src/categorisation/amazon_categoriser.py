"""Amazon per-transaction categorisation via Haiku.

Phase 5 in the categorisation pipeline. Uses item descriptions from transaction_note
(written by amazon_enrich_notes.py) to categorise individual Amazon transactions.

Amazon merchants are excluded from merchant-level categorisation because one Amazon
order could be groceries and the next electronics. This module works at the
transaction level, writing to transaction_category_override.
"""

import json

from config.settings import settings


BATCH_SIZE = 50
CONFIDENCE_THRESHOLD = 0.70


def categorise_amazon_transactions(conn, *, dry_run: bool = False) -> dict:
    """Categorise Amazon transactions using item descriptions and Haiku.

    Finds Amazon transactions with enriched notes but no category override,
    sends item descriptions to Haiku, and writes high-confidence results
    to transaction_category_override with source='enrichment'.
    """
    if not settings.anthropic_api_key:
        print("  ANTHROPIC_API_KEY not set — skipping Amazon categorisation")
        return {"amazon_categorised": 0, "amazon_skipped": 0}

    try:
        import anthropic
    except ImportError:
        print("  anthropic package not installed — run: pip install anthropic")
        return {"amazon_categorised": 0, "amazon_skipped": 0}

    cur = conn.cursor()

    # Get category tree
    cur.execute("SELECT id, full_path FROM category WHERE is_active = true ORDER BY full_path")
    categories = [(str(r[0]), r[1]) for r in cur.fetchall()]
    category_list = "\n".join(f"- {path}" for _, path in categories)
    cat_id_by_path = {path: cid for cid, path in categories}

    # Find Amazon transactions with notes but no override
    cur.execute("""
        SELECT tn.raw_transaction_id, tn.note, at.amount, at.posted_at
        FROM transaction_note tn
        JOIN active_transaction at ON at.id = tn.raw_transaction_id
        WHERE tn.source = 'amazon_match'
          AND NOT EXISTS (
              SELECT 1 FROM transaction_category_override tco
              WHERE tco.raw_transaction_id = tn.raw_transaction_id
          )
        ORDER BY at.posted_at DESC
    """)
    transactions = [(str(r[0]), r[1], float(r[2]), r[3]) for r in cur.fetchall()]

    if not transactions:
        print("  No Amazon transactions to categorise")
        return {"amazon_categorised": 0, "amazon_skipped": 0}

    print(f"  {len(transactions)} Amazon transactions to categorise")

    if dry_run:
        for txn_id, note, amount, date in transactions[:10]:
            # Show first line of note only
            first_line = note.split('\n')[0][:80]
            print(f"    {date} £{abs(amount):.2f} — {first_line}")
        if len(transactions) > 10:
            print(f"    ... and {len(transactions) - 10} more")
        return {"amazon_categorised": 0, "amazon_skipped": 0}

    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    total_categorised = 0
    total_skipped = 0

    for i in range(0, len(transactions), BATCH_SIZE):
        batch = transactions[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(transactions) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} transactions)...")

        txn_lines = []
        for txn_id, note, amount, date in batch:
            # Extract just the item descriptions (first line of note)
            first_line = note.split('\n')[0]
            txn_lines.append(f"- ID:{txn_id} | {date} | £{abs(amount):.2f} | {first_line}")
        txn_text = "\n".join(txn_lines)

        prompt = f"""You are categorising Amazon purchases for a personal finance system.

Each transaction below includes the items ordered from Amazon. Based on the items,
choose the single best matching category from the category tree.

CATEGORY TREE:
{category_list}

AMAZON TRANSACTIONS:
{txn_text}

Respond with a JSON array. Each element must have:
- "id": the transaction ID
- "category": the exact full_path from the category tree, or "SKIP"
- "confidence": a number 0.0-1.0
- "reasoning": brief explanation (max 20 words)

Respond ONLY with the JSON array, no other text."""

        try:
            response = client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )

            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1]
                if text.endswith("```"):
                    text = text[:-3].strip()

            results = json.loads(text)

            batch_categorised = 0
            batch_skipped = 0
            for result in results:
                txn_id = result.get("id", "")
                cat_path = result.get("category", "SKIP")
                confidence = float(result.get("confidence", 0))

                if cat_path == "SKIP" or confidence < CONFIDENCE_THRESHOLD:
                    batch_skipped += 1
                    continue

                if cat_path not in cat_id_by_path:
                    batch_skipped += 1
                    continue

                cur.execute("""
                    INSERT INTO transaction_category_override
                        (raw_transaction_id, category_path, source)
                    VALUES (%s, %s, 'enrichment')
                    ON CONFLICT (raw_transaction_id) DO NOTHING
                """, (txn_id, cat_path))
                if cur.rowcount:
                    batch_categorised += 1
                else:
                    batch_skipped += 1

            conn.commit()
            total_categorised += batch_categorised
            total_skipped += batch_skipped
            print(f"    {batch_categorised} categorised, {batch_skipped} skipped")

        except json.JSONDecodeError as e:
            print(f"    ERROR: Failed to parse LLM response: {e}")
            continue
        except Exception as e:
            print(f"    ERROR: LLM call failed: {e}")
            continue

    print(f"  Amazon: {total_categorised} categorised, {total_skipped} skipped")
    return {"amazon_categorised": total_categorised, "amazon_skipped": total_skipped}
