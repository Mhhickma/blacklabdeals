"""Purge expired product picks from public JSON feeds.

No product pick should remain on Black Lab Deals for more than 23 hours in a row.
This script physically rewrites feed files and removes products whose first-seen
stamp is missing, invalid, in the future, or older than the configured TTL.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PUBLIC_KEYS = [
    "asin", "title", "brand", "cat", "image", "price", "price_amount",
    "currency", "availability", "link", "desc", "seen_at", "updated_at",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def parse_time(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def clean_product(product: dict[str, Any]) -> dict[str, Any]:
    return {key: product.get(key) for key in PUBLIC_KEYS}


def is_current(product: dict[str, Any], cutoff: datetime, now: datetime) -> bool:
    # Use seen_at first because updated_at can change on refresh. The rule is
    # maximum 23 hours in a row from when the product first entered the feed.
    stamp = parse_time(product.get("seen_at")) or parse_time(product.get("updated_at"))
    if stamp is None:
        return False
    if stamp > now + timedelta(minutes=5):
        return False
    return stamp > cutoff


def purge_products(products: list[Any], ttl_hours: int) -> list[dict[str, Any]]:
    now = utc_now()
    cutoff = now - timedelta(hours=ttl_hours)
    kept: list[dict[str, Any]] = []
    seen: set[str] = set()
    for product in products:
        if not isinstance(product, dict):
            continue
        asin = str(product.get("asin") or product.get("ASIN") or "").strip().upper()
        key = asin or str(product.get("link") or product.get("title") or "").strip().upper()
        if not key or key in seen:
            continue
        if not is_current(product, cutoff, now):
            continue
        cleaned = clean_product(product)
        if asin:
            cleaned["asin"] = asin
        seen.add(key)
        kept.append(cleaned)
    return kept


def purge_file(path: Path, ttl_hours: int) -> tuple[int, int]:
    if not path.exists():
        return (0, 0)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Could not read {path}: {exc}") from exc

    if isinstance(data, dict) and isinstance(data.get("deals"), list):
        before = len(data["deals"])
        data["deals"] = purge_products(data["deals"], ttl_hours)
        after = len(data["deals"])
        data["count"] = after
        data["totalProducts"] = after
        data["updatedAt"] = iso_now()
        path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        return (before, after)

    if isinstance(data, dict):
        before = len(data)
        products = []
        for asin, product in data.items():
            if isinstance(product, dict):
                product.setdefault("asin", asin)
                products.append(product)
        kept = purge_products(products, ttl_hours)
        rewritten = {str(product.get("asin") or "").upper(): product for product in kept if product.get("asin")}
        after = len(rewritten)
        path.write_text(json.dumps(rewritten, indent=2) + "\n", encoding="utf-8")
        return (before, after)

    if isinstance(data, list):
        before = len(data)
        rewritten = purge_products(data, ttl_hours)
        after = len(rewritten)
        path.write_text(json.dumps(rewritten, indent=2) + "\n", encoding="utf-8")
        return (before, after)

    return (0, 0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Purge product picks older than the TTL.")
    parser.add_argument("paths", nargs="*", default=["deals.json", "deals_memory.json", "best_seller_deals.json", "amazon-sales-event-deals.json"])
    parser.add_argument("--ttl-hours", type=int, default=23)
    args = parser.parse_args()

    for name in args.paths:
        before, after = purge_file(Path(name), args.ttl_hours)
        if before or after:
            print(f"{name}: kept {after} of {before} product picks; removed {before - after} expired")


if __name__ == "__main__":
    main()
