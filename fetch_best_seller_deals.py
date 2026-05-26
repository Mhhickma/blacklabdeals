"""Compliant Black Lab Deals best-seller product-picks generator.

Keepa is used only to maintain a best-seller ASIN watchlist. Public pricing,
images, titles, availability, and links are fetched from Amazon Creators API.
The public JSON intentionally excludes old/was pricing, savings, percentage-off,
price-drop labels, hot-deal flags, Keepa stats, ranks, and qualification reasons.
"""

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import keepa
from amazon_creatorsapi import AmazonCreatorsApi, Country
from amazon_creatorsapi.models import GetItemsResource

KEEPA_API_KEY = os.getenv("KEEPA_API_KEY")
CREDENTIAL_ID = os.getenv("CREATORS_CREDENTIAL_ID")
CREDENTIAL_SECRET = os.getenv("CREATORS_CREDENTIAL_SECRET")
PARTNER_TAG = os.getenv("AFFILIATE_TAG", "sawdustsavings-20")

CONFIG_FILE = "best_seller_categories.json"
WATCHLIST_FILE = "best_seller_watchlist.json"
STATE_FILE = "best_seller_state.json"
PRODUCTS_FILE = "best_seller_deals.json"

AMAZON_BATCH_SIZE = 10
AMAZON_CONCURRENT_BATCHES = int(os.getenv("BEST_SELLER_AMAZON_CONCURRENT_BATCHES", "3"))
AMAZON_REQUEST_DELAY_SECONDS = float(os.getenv("BEST_SELLER_AMAZON_REQUEST_DELAY_SECONDS", "1"))
PRODUCT_TTL_HOURS = int(os.getenv("BEST_SELLER_PRODUCT_TTL_HOURS", "23"))

PUBLIC_KEYS = [
    "asin", "title", "brand", "cat", "image", "price", "price_amount",
    "currency", "availability", "link", "desc", "seen_at", "updated_at",
]

BAD_KEYWORDS = [
    "sex", "doll", "erotic", "fetish", "penis", "vagina", "dildo", "vibrator",
    "nude", "naked", "porn", "xxx", "bdsm", "bondage",
]

BLACKLISTED_ASINS = {
    "B0CNSFQ988", "B0CNSDDJ1C", "B0CNSDNT27", "B0CNSCN4KW", "B0CNSCZQ1W", "B0CNSBX4ZK",
}


def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat()


def env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(f"Warning: could not load {path}: {exc}")
        return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def clean_product(product):
    return {key: product.get(key) for key in PUBLIC_KEYS}


def compact_image_url(url, size=160):
    return re.sub(r"\._SL\d+_\.", f"._SL{size}_.", str(url)) if url else None


def parse_time(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def is_bad_title(title):
    if not title or len(title.strip()) < 3:
        return True
    lower = title.lower()
    return any(word in lower for word in BAD_KEYWORDS)


def load_config():
    config = load_json(CONFIG_FILE, {})
    if not config:
        raise RuntimeError(f"Missing {CONFIG_FILE}")
    return config


def refresh_needed(watchlist, refresh_hours):
    generated_at = parse_time(watchlist.get("generatedAt"))
    if not generated_at:
        return True
    return utc_now() - generated_at >= timedelta(hours=refresh_hours)


def build_watchlist(config):
    print("Building best-seller ASIN watchlist from Keepa...")
    if not KEEPA_API_KEY:
        raise RuntimeError("Missing KEEPA_API_KEY")
    api = keepa.Keepa(KEEPA_API_KEY)
    top_per_category = int(config.get("topPerCategory", 200))
    domain = "US"

    items_by_asin = {}
    categories = [category for category in config.get("categories", []) if category.get("enabled", True)]

    for category in categories:
        category_id = str(category["categoryId"])
        category_name = category.get("name", category_id)
        category_slug = category.get("slug", f"category-{category_id}")
        try:
            asins = api.best_sellers_query(category_id, domain=domain)
            top_asins = asins[:top_per_category]
            print(f"  {category_name}: {len(top_asins)} ASINs")
        except Exception as exc:
            print(f"  Failed to fetch {category_name} ({category_id}): {exc}")
            top_asins = []

        for position, asin in enumerate(top_asins, start=1):
            if asin in BLACKLISTED_ASINS:
                continue
            if asin not in items_by_asin:
                items_by_asin[asin] = {
                    "asin": asin,
                    "categories": [],
                    "bestRank": position,
                }
            items_by_asin[asin]["categories"].append({
                "categoryId": int(category_id),
                "name": category_name,
                "slug": category_slug,
            })
            items_by_asin[asin]["bestRank"] = min(items_by_asin[asin]["bestRank"], position)
        time.sleep(1)

    items = sorted(items_by_asin.values(), key=lambda item: (item.get("bestRank", 999999), item.get("asin", "")))
    watchlist = {
        "generatedAt": iso_now(),
        "source": "Best-seller ASIN watchlist",
        "topPerCategory": top_per_category,
        "count": len(items),
        "items": items,
    }
    save_json(WATCHLIST_FILE, watchlist)
    print(f"Saved {len(items)} unique ASINs to {WATCHLIST_FILE}")
    return watchlist


def amazon_resources():
    return [
        GetItemsResource.ITEM_INFO_DOT_TITLE,
        GetItemsResource.ITEM_INFO_DOT_BY_LINE_INFO,
        GetItemsResource.ITEM_INFO_DOT_CLASSIFICATIONS,
        GetItemsResource.IMAGES_DOT_PRIMARY_DOT_LARGE,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_PRICE,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_AVAILABILITY,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_CONDITION,
    ]


def fetch_amazon_batch(batch):
    amazon = AmazonCreatorsApi(
        credential_id=CREDENTIAL_ID,
        credential_secret=CREDENTIAL_SECRET,
        version="3.1",
        tag=PARTNER_TAG,
        country=Country.US,
    )
    return amazon.get_items(batch, resources=amazon_resources())


def get_amazon_items(asins):
    if not CREDENTIAL_ID or not CREDENTIAL_SECRET:
        raise RuntimeError("Missing CREATORS_CREDENTIAL_ID or CREATORS_CREDENTIAL_SECRET")
    batches = [asins[i:i + AMAZON_BATCH_SIZE] for i in range(0, len(asins), AMAZON_BATCH_SIZE)]
    worker_count = max(1, min(AMAZON_CONCURRENT_BATCHES, len(batches) or 1))
    all_items = {}

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = []
        for batch in batches:
            futures.append(executor.submit(fetch_amazon_batch, batch))
            if AMAZON_REQUEST_DELAY_SECONDS > 0:
                time.sleep(AMAZON_REQUEST_DELAY_SECONDS)
        for future in as_completed(futures):
            try:
                for item in future.result():
                    all_items[item.asin] = item
            except Exception as exc:
                print(f"Warning: Amazon batch failed: {exc}")
    return all_items


def load_existing_products():
    existing_output = load_json(PRODUCTS_FILE, {"deals": []})
    products = {}
    cutoff = utc_now() - timedelta(hours=PRODUCT_TTL_HOURS)
    for product in existing_output.get("deals", []):
        if not isinstance(product, dict):
            continue
        asin = product.get("asin")
        if not asin:
            continue
        updated = parse_time(product.get("updated_at") or product.get("seen_at"))
        if updated and updated >= cutoff:
            products[asin] = clean_product(product)
    return products


def amazon_item_to_product(asin, item, watch_meta, existing):
    try:
        title = item.item_info.title.display_value
    except Exception:
        return None
    if is_bad_title(title):
        return None

    try:
        listing = item.offers_v2.listings[0]
        price_amount = float(listing.price.money.amount)
        price_display = listing.price.money.display_amount
        currency = listing.price.money.currency
    except Exception:
        return None
    if not price_amount:
        return None

    try:
        condition = listing.condition.value
        if condition and condition.lower() != "new":
            return None
    except Exception:
        pass

    try:
        brand = item.item_info.by_line_info.brand.display_value
    except Exception:
        brand = None

    try:
        raw_category = item.item_info.classifications.product_group.display_value
    except Exception:
        raw_category = None

    try:
        image = compact_image_url(item.images.primary.large.url)
    except Exception:
        image = None

    try:
        availability = listing.availability.type
    except Exception:
        availability = None

    try:
        url = item.detail_page_url
    except Exception:
        url = f"https://www.amazon.com/dp/{asin}?tag={PARTNER_TAG}"

    primary_category = (watch_meta.get("categories") or [{}])[0]
    category = primary_category.get("name") or raw_category or "Best Sellers"
    now = iso_now()
    product = {
        "asin": asin,
        "title": title,
        "brand": brand,
        "cat": category,
        "image": image,
        "price": price_display,
        "price_amount": price_amount,
        "currency": currency,
        "availability": availability,
        "link": url,
        "desc": brand or "",
        "seen_at": (existing or {}).get("seen_at", now),
        "updated_at": now,
    }
    return clean_product(product)


def main():
    config = load_config()
    refresh_hours = int(config.get("refreshBestSellerListHours", 168))
    refresh_watchlist = env_bool("BEST_SELLER_REFRESH_WATCHLIST", False)
    watchlist_only = env_bool("BEST_SELLER_WATCHLIST_ONLY", False)

    watchlist = load_json(WATCHLIST_FILE, {})
    has_saved_watchlist = bool(watchlist.get("items"))
    should_refresh = refresh_watchlist and refresh_needed(watchlist, refresh_hours)

    if not has_saved_watchlist or should_refresh:
        watchlist = build_watchlist(config)
    elif refresh_needed(watchlist, refresh_hours):
        print("Saved best-seller watchlist is stale; using saved ASIN list for this run.")

    if watchlist_only:
        print("Watchlist-only mode complete; skipping product checks.")
        return

    items = watchlist.get("items", [])
    if not items:
        save_json(PRODUCTS_FILE, {
            "pageTitle": "Amazon Best Seller Product Picks",
            "pageDescription": "Current product picks from popular Amazon categories.",
            "source": "Amazon current product information feed",
            "count": 0,
            "totalProducts": 0,
            "updatedAt": iso_now(),
            "deals": [],
        })
        return

    asins_per_run = int(os.getenv("BEST_SELLER_ASINS_PER_RUN", config.get("asinsPerRun", 125)))
    state = load_json(STATE_FILE, {"cursor": 0, "asins": {}})
    cursor = int(state.get("cursor", 0))
    batch_meta = [items[(cursor + i) % len(items)] for i in range(asins_per_run)]
    next_cursor = (cursor + asins_per_run) % len(items)
    batch_asins = [meta["asin"] for meta in batch_meta]

    amazon_items = get_amazon_items(batch_asins)
    products_by_asin = load_existing_products()
    state_asins = state.setdefault("asins", {})

    for meta in batch_meta:
        asin = meta["asin"]
        item = amazon_items.get(asin)
        if not item:
            continue
        product = amazon_item_to_product(asin, item, meta, products_by_asin.get(asin))
        if product:
            products_by_asin[asin] = product
        state_asins[asin] = {
            "lastCheckedAt": iso_now(),
            "lastPrice": product.get("price_amount") if product else state_asins.get(asin, {}).get("lastPrice"),
            "title": product.get("title") if product else state_asins.get(asin, {}).get("title"),
        }

    state["cursor"] = next_cursor
    state["lastRunAt"] = iso_now()
    state["watchlistCount"] = len(items)
    save_json(STATE_FILE, state)

    products = sorted(products_by_asin.values(), key=lambda item: item.get("updated_at", ""), reverse=True)
    output = {
        "pageTitle": "Amazon Best Seller Product Picks",
        "pageDescription": "Current product picks from popular Amazon categories.",
        "source": "Amazon current product information feed",
        "count": len(products),
        "totalProducts": len(products),
        "updatedAt": iso_now(),
        "deals": [clean_product(product) for product in products],
    }
    save_json(PRODUCTS_FILE, output)
    print(f"Saved {len(products)} best-seller product picks to {PRODUCTS_FILE}")


if __name__ == "__main__":
    main()
