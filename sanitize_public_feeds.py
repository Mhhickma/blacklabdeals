"""Remove internal pricing and deal-analysis fields from public JSON feeds."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PRODUCT_FIELDS = (
    "asin", "title", "brand", "cat", "image", "price", "price_amount",
    "currency", "availability", "link", "desc", "seen_at", "updated_at",
)
PAGE_FIELDS = (
    "pageTitle", "pageDescription", "source", "count", "totalProducts",
    "updatedAt", "deals",
)
FRESH_CUTOFF = datetime.now(timezone.utc) - timedelta(hours=23)


def is_fresh(product):
    if str(product.get("availability") or "").upper() == "UNAVAILABLE":
        return False
    value = product.get("updated_at") or product.get("seen_at")
    if not value:
        return False
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")) >= FRESH_CUTOFF
    except (TypeError, ValueError):
        return False


def clean_product(product):
    cleaned = {key: product[key] for key in PRODUCT_FIELDS if product.get(key) is not None}
    desc = str(cleaned.get("desc") or "")
    risky = ("price drop", "discount", "% off", "lowest price", "hot deal")
    if any(term in desc.lower() for term in risky):
        cleaned["desc"] = cleaned.get("brand") or "Current Amazon product information"
    return cleaned


def sanitize(path, defaults):
    data = json.loads(path.read_text(encoding="utf-8"))
    products = data if isinstance(data, list) else data.get("deals", [])
    products = [clean_product(product) for product in products if is_fresh(product)]
    output = {
        **defaults,
        "count": len(products),
        "totalProducts": len(products),
        "updatedAt": data.get("updatedAt"),
        "deals": products,
    }
    output = {key: output[key] for key in PAGE_FIELDS if output.get(key) is not None}
    path.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(f"Sanitized {path.name}: {len(products)} products")


sanitize(
    ROOT / "deals.json",
    {
        "pageTitle": "Current Amazon Product Picks",
        "pageDescription": "Current Amazon product information organized by category.",
        "source": "Amazon Creators API",
    },
)
sanitize(
    ROOT / "best_seller_deals.json",
    {
        "pageTitle": "Amazon Best Seller Product Picks",
        "pageDescription": "Current product picks from popular Amazon categories.",
        "source": "Amazon Creators API",
    },
)
