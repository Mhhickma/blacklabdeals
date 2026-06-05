"""Compliant Black Lab Deals product-picks generator.

Keepa is used only to discover and internally rank candidate ASINs. Public
pricing, images, titles, availability, and links are fetched from Amazon
Creators API. The public JSON intentionally excludes old/was pricing, savings,
percentage-off, price-drop labels, hot-deal flags, Keepa stats, and coupon
fields.
"""

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import requests
from amazon_creatorsapi import AmazonCreatorsApi, Country
from amazon_creatorsapi.models import GetItemsResource

KEEPA_API_KEY = os.getenv("KEEPA_API_KEY")
CREDENTIAL_ID = os.getenv("CREATORS_CREDENTIAL_ID")
CREDENTIAL_SECRET = os.getenv("CREATORS_CREDENTIAL_SECRET")
PARTNER_TAG = os.getenv("AFFILIATE_TAG", "sawdustsavings-20")

OUTPUT_FILE = "deals.json"
MEMORY_FILE = "deals_memory.json"
PRODUCT_TTL_HOURS = int(os.getenv("PRODUCT_TTL_HOURS", "23"))
MAX_DISPLAY = int(os.getenv("MAX_DISPLAY", "5000"))
KEEPA_DEAL_PAGES = int(os.getenv("KEEPA_DEAL_PAGES", "15"))
MAX_NEW_ASINS_PER_RUN = int(os.getenv("MAX_NEW_ASINS_PER_RUN", "0"))
AMAZON_BATCH_SIZE = 10
AMAZON_CONCURRENT_BATCHES = int(os.getenv("AMAZON_CONCURRENT_BATCHES", "5"))
AMAZON_REQUEST_DELAY_SECONDS = float(os.getenv("AMAZON_REQUEST_DELAY_SECONDS", "1"))
DEAL_REQUEST_DELAY_SECONDS = float(os.getenv("DEAL_REQUEST_DELAY_SECONDS", "3"))
KEEPA_DEALS_URL = "https://api.keepa.com/deal"
PRICE_TYPES = [0, 1, 10]

PUBLIC_KEYS = [
    "asin", "title", "brand", "cat", "image", "price", "price_amount",
    "currency", "availability", "link", "desc", "seen_at", "updated_at",
]

EXCLUDED_CATEGORIES = [
    283155, 5174, 133140011, 2625373011, 7141123011, 163856011,
    18145289011, 2350149011, 2238192011, 4991425011, 229534,
    18981045011, 11260432011, 16310091,
]

BAD_KEYWORDS = [
    "sex", "doll", "erotic", "fetish", "penis", "vagina", "dildo",
    "vibrator", "nude", "naked", "porn", "xxx", "bdsm", "bondage",
    "abrasive", "torque", "fiber optic", "qsfp", "sfp", "evaporator",
    "flame retardant", "safety vest", "hard hat", "bearing", "set screw",
    "end mill", "clamp", "permaculture", "grass paint", "field line",
    "marking paint", "hydraulic", "pneumatic", "actuator", "splice",
    "scotchcast", "schuko", "waffle polish", "roller refill", "dental",
    "vapor-tight", "jute", "bohemian", "hinge", "barrel hinge", "mortise",
    "water pump", "latex glove", "circuit breaker", "conduit", "junction box",
    "wire connector",
]

BLACKLISTED_ASINS = {
    "B0CNSFQ988", "B0CNSDDJ1C", "B0CNSDNT27", "B0CNSCN4KW", "B0CNSCZQ1W", "B0CNSBX4ZK",
}

CATEGORY_MAP = {
    "health": "Health & Household", "beauty": "Health & Household", "personal care": "Health & Household",
    "grocery": "Health & Household", "electronics": "Electronics", "computer": "Electronics",
    "camera": "Electronics", "television": "Electronics", "audio": "Electronics",
    "headphone": "Electronics", "speaker": "Electronics", "tablet": "Electronics",
    "laptop": "Electronics", "cell phone": "Cell Phones & Accessories", "smartphone": "Cell Phones & Accessories",
    "wireless": "Cell Phones & Accessories", "kitchen": "Home & Kitchen", "home": "Home & Kitchen",
    "bedding": "Home & Kitchen", "furniture": "Home & Kitchen", "lighting": "Home & Kitchen",
    "vacuum": "Home & Kitchen", "appliance": "Home & Kitchen", "cookware": "Home & Kitchen",
    "patio": "Patio, Lawn & Garden", "lawn": "Patio, Lawn & Garden", "garden": "Patio, Lawn & Garden",
    "outdoor": "Patio, Lawn & Garden", "toy": "Toys & Games", "game": "Toys & Games",
    "kids": "Toys & Games", "sport": "Sports & Outdoors", "fitness": "Sports & Outdoors",
    "camping": "Sports & Outdoors", "automotive": "Automotive", "vehicle": "Automotive", "car": "Automotive",
    "office": "Office Products", "baby": "Baby Products", "pet": "Pet Supplies", "dog": "Pet Supplies",
    "tool": "Tools & Home Improvement", "hardware": "Tools & Home Improvement",
    "home improvement": "Tools & Home Improvement", "craft": "Arts, Crafts & Sewing",
    "sewing": "Arts, Crafts & Sewing", "musical": "Musical Instruments",
}

KNOWN_CATEGORIES = {v.lower() for v in CATEGORY_MAP.values()} | {"everything else", "appliances"}


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def clean_product(product):
    return {key: product.get(key) for key in PUBLIC_KEYS}


def decode_title(raw):
    if isinstance(raw, list):
        try:
            return "".join(chr(c) for c in raw if isinstance(c, int))
        except Exception:
            return ""
    return raw if isinstance(raw, str) else ""


def is_bad_title(title):
    if not title or len(title.strip()) < 3:
        return True
    lower = title.lower()
    return any(word in lower for word in BAD_KEYWORDS)


def normalize_category(raw_cat):
    if not raw_cat:
        return "Everything Else"
    lower = str(raw_cat).lower()
    if lower in KNOWN_CATEGORIES:
        return str(raw_cat)
    for key, mapped in CATEGORY_MAP.items():
        if key in lower:
            return mapped
    return "Everything Else"


def compact_image_url(url, size=160):
    return re.sub(r"\._SL\d+_\.", f"._SL{size}_.", str(url)) if url else None


def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def load_memory():
    raw = load_json(MEMORY_FILE, {})
    return {asin: clean_product(item) for asin, item in raw.items() if isinstance(item, dict)}


def save_memory(memory):
    with open(MEMORY_FILE, "w", encoding="utf-8") as f:
        json.dump({asin: clean_product(item) for asin, item in memory.items()}, f, indent=2)


def purge_expired(memory):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=PRODUCT_TTL_HOURS)
    kept = {}
    for asin, item in memory.items():
        try:
            seen_at = datetime.fromisoformat(item.get("seen_at") or item.get("updated_at") or iso_now())
        except Exception:
            seen_at = datetime.now(timezone.utc)
        if seen_at > cutoff:
            kept[asin] = clean_product(item)
    return kept


def timestamp_value(item):
    raw = item.get("updated_at") or item.get("seen_at") or ""
    try:
        return datetime.fromisoformat(raw).timestamp()
    except Exception:
        return 0


def get_keepa_candidates(cached_asins):
    if not KEEPA_API_KEY:
        raise RuntimeError("Missing KEEPA_API_KEY")
    candidates = []
    for pt in PRICE_TYPES:
        for page in range(KEEPA_DEAL_PAGES):
            payload = {
                "domainId": 1,
                "priceTypes": [pt],
                "dateRange": 4,
                "sortType": 4,
                "page": page,
                "filterErotic": True,
                "hasReviews": True,
                "minRating": 40,
                "deltaPercentRange": [-100, -5],
                "excludeCategories": EXCLUDED_CATEGORIES,
            }
            try:
                response = requests.post(KEEPA_DEALS_URL, params={"key": KEEPA_API_KEY, "domain": 1}, json=payload, timeout=15)
                response.raise_for_status()
                candidates.extend(response.json().get("deals", {}).get("dr", []))
            except Exception as exc:
                print(f"Keepa candidate page failed: {exc}")
            time.sleep(DEAL_REQUEST_DELAY_SECONDS)

    seen = set(BLACKLISTED_ASINS)
    ordered_asins = []
    for item in candidates:
        asin = item.get("asin")
        if not asin or asin in seen:
            continue
        if is_bad_title(decode_title(item.get("title", ""))):
            continue
        seen.add(asin)
        ordered_asins.append(asin)

    candidate_rank = {asin: index for index, asin in enumerate(ordered_asins)}
    new_asins = [asin for asin in ordered_asins if asin not in cached_asins]
    if MAX_NEW_ASINS_PER_RUN > 0:
        new_asins = new_asins[:MAX_NEW_ASINS_PER_RUN]
    return candidate_rank, new_asins


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


def fetch_batch(batch):
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
    items = {}
    with ThreadPoolExecutor(max_workers=max(1, min(AMAZON_CONCURRENT_BATCHES, len(batches) or 1))) as executor:
        futures = []
        for batch in batches:
            futures.append(executor.submit(fetch_batch, batch))
            time.sleep(AMAZON_REQUEST_DELAY_SECONDS)
        for future in as_completed(futures):
            try:
                for item in future.result():
                    items[item.asin] = item
            except Exception as exc:
                print(f"Amazon batch failed: {exc}")
    return items


def item_to_product(asin, item, existing):
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

    now = iso_now()
    return clean_product({
        "asin": asin,
        "title": title,
        "brand": brand,
        "cat": normalize_category(raw_category),
        "image": image,
        "price": price_display,
        "price_amount": price_amount,
        "currency": currency,
        "availability": availability,
        "link": url,
        "desc": brand or "",
        "seen_at": (existing or {}).get("seen_at", now),
        "updated_at": now,
    })


def main():
    memory = purge_expired(load_memory())
    cached_asins = set(memory.keys())
    candidate_rank, asins = get_keepa_candidates(cached_asins)
    if asins:
        amazon_items = get_amazon_items(asins)
        for asin in asins:
            product = item_to_product(asin, amazon_items.get(asin), memory.get(asin)) if amazon_items.get(asin) else None
            if product:
                memory[asin] = product
    save_memory(memory)

    fallback_rank = len(candidate_rank) + 1
    products = sorted(
        memory.values(),
        key=lambda item: (
            candidate_rank.get(item.get("asin", ""), fallback_rank),
            -timestamp_value(item),
            str(item.get("title") or "").lower(),
        ),
    )[:MAX_DISPLAY]
    output = {
        "source": "Amazon current product information feed",
        "count": len(products),
        "totalProducts": len(products),
        "updatedAt": iso_now(),
        "deals": [clean_product(item) for item in products],
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"Saved {len(products)} product picks to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
