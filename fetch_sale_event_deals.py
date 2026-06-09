"""Validate selected sale-event ASINs through Amazon Creators API."""

import json
import os
from datetime import datetime, timedelta, timezone

from fetch_best_seller_deals import compact_image_url, get_amazon_items, iso_now

WATCHLIST_FILE = "sale_event_watchlist.json"
STATE_FILE = "sale_event_state.json"
OUTPUT_FILE = "sale_event_deals.json"
TTL_HOURS = 23
ASINS_PER_RUN = int(os.getenv("SALE_EVENT_ASINS_PER_RUN", "250"))


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")


def fresh(product):
    try:
        updated = datetime.fromisoformat(product.get("updated_at", "").replace("Z", "+00:00"))
        return updated >= datetime.now(timezone.utc) - timedelta(hours=TTL_HOURS)
    except (TypeError, ValueError):
        return False


def item_to_product(asin, item, previous):
    try:
        listing = item.offers_v2.listings[0]
        price_amount = float(listing.price.money.amount)
        price_display = listing.price.money.display_amount
        currency = listing.price.money.currency
        title = item.item_info.title.display_value
    except Exception:
        return None
    try:
        if listing.condition.value and listing.condition.value.lower() != "new":
            return None
    except Exception:
        pass
    try:
        availability = str(listing.availability.type or "")
    except Exception:
        availability = ""
    if availability.upper() == "UNAVAILABLE":
        return None
    try:
        brand = item.item_info.by_line_info.brand.display_value
    except Exception:
        brand = None
    try:
        category = item.item_info.classifications.product_group.display_value
    except Exception:
        category = "Amazon Products"
    try:
        image = compact_image_url(item.images.primary.large.url, size=300)
    except Exception:
        image = None
    try:
        link = item.detail_page_url
    except Exception:
        link = f"https://www.amazon.com/dp/{asin}"
    timestamp = iso_now()
    return {
        "asin": asin, "title": title, "brand": brand, "cat": category,
        "image": image, "price": price_display, "price_amount": price_amount,
        "currency": currency, "availability": availability, "link": link,
        "desc": brand or "Current Amazon product information",
        "seen_at": previous.get("seen_at", timestamp), "updated_at": timestamp,
    }


def main():
    watchlist = load_json(WATCHLIST_FILE, {"asins": []})
    asins = list(dict.fromkeys(str(asin).strip().upper() for asin in watchlist.get("asins", []) if str(asin).strip()))
    existing = load_json(OUTPUT_FILE, {"deals": [], "updatedAt": None})
    products = {p["asin"]: p for p in existing.get("deals", []) if p.get("asin") and fresh(p)}
    state = load_json(STATE_FILE, {"cursor": 0})

    if asins:
        cursor = int(state.get("cursor", 0)) % len(asins)
        count = min(max(1, ASINS_PER_RUN), len(asins))
        selected = [asins[(cursor + index) % len(asins)] for index in range(count)]
        amazon_items = get_amazon_items(selected)
        for asin in selected:
            product = item_to_product(asin, amazon_items.get(asin), products.get(asin, {})) if amazon_items.get(asin) else None
            if product:
                products[asin] = product
            else:
                products.pop(asin, None)
        state = {"cursor": (cursor + count) % len(asins), "lastRunAt": iso_now(), "watchlistCount": len(asins)}
    else:
        state = {"cursor": 0, "watchlistCount": 0}

    allowed = set(asins)
    deals = [product for asin, product in products.items() if asin in allowed and fresh(product)]
    deals.sort(key=lambda product: product.get("updated_at", ""), reverse=True)
    output = {
        "pageTitle": "Amazon Sale Event Product Picks",
        "pageDescription": "Current Amazon product information for selected sale-event products.",
        "source": "Amazon Creators API", "count": len(deals), "totalProducts": len(deals),
        "updatedAt": iso_now() if asins else existing.get("updatedAt"), "deals": deals,
    }
    save_json(STATE_FILE, state)
    save_json(OUTPUT_FILE, output)
    print(f"Saved {len(deals)} fresh Creators API products from a {len(asins)} ASIN watchlist.")


if __name__ == "__main__":
    main()
