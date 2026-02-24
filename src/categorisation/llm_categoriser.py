"""LLM-based categorisation using Anthropic Claude.

Batches uncategorised merchants and asks Claude Haiku to suggest categories
from the existing hierarchy. Results are queued as pending suggestions.
"""

import json

from config.settings import settings


BATCH_SIZE = 50


def categorise_batch(conn, *, dry_run: bool = False) -> dict:
    """Run LLM categorisation for uncategorised merchants.

    Batches merchants and sends to Claude Haiku for category suggestions.
    All suggestions are queued as pending (never auto-accepted from LLM).
    """
    if not settings.anthropic_api_key:
        print("  ANTHROPIC_API_KEY not set — skipping LLM categorisation")
        return {"llm_queued": 0}

    try:
        import anthropic
    except ImportError:
        print("  anthropic package not installed — run: pip install anthropic")
        return {"llm_queued": 0}

    cur = conn.cursor()

    # Get the full category tree
    cur.execute("SELECT id, full_path FROM category WHERE is_active = true ORDER BY full_path")
    categories = [(str(r[0]), r[1]) for r in cur.fetchall()]
    category_list = "\n".join(f"- {path}" for _, path in categories)
    cat_id_by_path = {path: cid for cid, path in categories}

    # Find uncategorised, unmerged, non-Amazon merchants
    cur.execute("""
        SELECT cm.id, cm.name, cm.display_name
        FROM canonical_merchant cm
        WHERE cm.category_hint IS NULL
          AND cm.merged_into_id IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM category_suggestion cs
              WHERE cs.canonical_merchant_id = cm.id
          )
        ORDER BY cm.name
    """)
    merchants = [(str(r[0]), r[1], r[2]) for r in cur.fetchall()]

    # Filter out Amazon
    merchants = [(mid, name, dname) for mid, name, dname in merchants
                 if not _is_amazon(name)]

    if not merchants:
        print("  No uncategorised merchants remaining for LLM")
        return {"llm_queued": 0}

    print(f"  {len(merchants)} merchants to categorise via LLM")

    if dry_run:
        print(f"  Would process {len(merchants)} merchants in {(len(merchants) + BATCH_SIZE - 1) // BATCH_SIZE} batches")
        for m in merchants[:10]:
            print(f"    {m[1]}")
        if len(merchants) > 10:
            print(f"    ... and {len(merchants) - 10} more")
        return {"llm_queued": 0}

    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    total_queued = 0

    for i in range(0, len(merchants), BATCH_SIZE):
        batch = merchants[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(merchants) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} merchants)...")

        merchant_lines = []
        for mid, name, dname in batch:
            display = f" (display: {dname})" if dname else ""
            merchant_lines.append(f"- ID:{mid} | {name}{display}")
        merchant_text = "\n".join(merchant_lines)

        prompt = f"""You are categorising merchants for a personal finance system.

For each merchant below, suggest the single best matching category from the category tree.
If you're not confident, say "SKIP".

CATEGORY TREE:
{category_list}

MERCHANTS TO CATEGORISE:
{merchant_text}

Respond with a JSON array. Each element must have:
- "id": the merchant ID
- "category": the exact full_path from the category tree, or "SKIP"
- "confidence": a number 0.0-1.0
- "reasoning": brief explanation (max 20 words)

Respond ONLY with the JSON array, no other text."""

        try:
            response = client.messages.create(
                model="claude-haiku-4-20250414",
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )

            text = response.content[0].text.strip()
            # Handle potential markdown wrapping
            if text.startswith("```"):
                text = text.split("\n", 1)[1]
                if text.endswith("```"):
                    text = text[:-3].strip()

            results = json.loads(text)

            batch_queued = 0
            for result in results:
                mid = result.get("id", "")
                cat_path = result.get("category", "SKIP")
                confidence = float(result.get("confidence", 0))
                reasoning = result.get("reasoning", "")

                if cat_path == "SKIP" or confidence < 0.3:
                    continue

                cat_id = cat_id_by_path.get(cat_path)
                if not cat_id:
                    continue

                # Cap LLM confidence at 0.80 (never auto-accept)
                confidence = min(confidence, 0.80)

                cur.execute("""
                    INSERT INTO category_suggestion
                        (canonical_merchant_id, suggested_category_id, method, confidence, reasoning)
                    VALUES (%s, %s, 'llm', %s, %s)
                    ON CONFLICT DO NOTHING
                """, (mid, cat_id, confidence, f"LLM: {reasoning}"))
                batch_queued += cur.rowcount

            conn.commit()
            total_queued += batch_queued
            print(f"    Queued {batch_queued} suggestions")

        except json.JSONDecodeError as e:
            print(f"    ERROR: Failed to parse LLM response: {e}")
            continue
        except Exception as e:
            print(f"    ERROR: LLM call failed: {e}")
            continue

    print(f"  Total LLM suggestions queued: {total_queued}")
    return {"llm_queued": total_queued}


def _is_amazon(name: str) -> bool:
    """Check if a merchant name is Amazon-related."""
    lower = name.lower()
    return 'amazon' in lower or 'amzn' in lower or 'amz ' in lower
