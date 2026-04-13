"""
DealDrop — fetch_deals.py
--------------------------
Pipeline:
  1. Keepa API        — finds candidate deals using price history + coupons
  2. Amazon Creators  — fetches live titles, prices, images, and links
  3. Writes deals.json for the site

Required env vars:
  KEEPA_API_KEY
  AMAZON_CREATOR_CREDENTIAL_ID
  AMAZON_CREATOR_CREDENTIAL_SECRET
  AFFILIATE_TAG

Optional env vars:
  AMAZON_CREATOR_VERSION   (default: 3.1)
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from typing import Any

import requests
from amazon_creatorsapi import AmazonCreatorsApi, Country

# ─── CONFIG ───────────────────────────────────────────────────────────────────

KEEPA_API_KEY = os.environ.get("KEEPA_API_KEY", "").strip()

AMAZON_CREATOR_CREDENTIAL_ID = os.environ.get("AMAZON_CREATOR_CREDENTIAL_ID", "").strip()
AMAZON_CREATOR_CREDENTIAL_SECRET = os.environ.get("AMAZON_CREATOR_CREDENTIAL_SECRET", "").strip()
AMAZON_CREATOR_VERSION = os.environ.get("AMAZON_CREATOR_VERSION", "3.1").strip()
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG", "").strip()

OUTPUT_FILE = "deals.json"
KEEPA_BASE = "https://api.keepa.com"

MAX_DEALS = 50
MIN_DISCOUNT_PCT = 20
HOT_DEAL_PCT = 50
MIN_COUPON_VALUE = 3
MIN_COUPON_PCT = 5

# You asked earlier about removing books/magazines.
EXCLUDED_CATEGORIES = {
    "Books",
    "Magazine Subscriptions",
    "Music",
}

CATEGORY_NAMES = {
    281052: "Electronics",
    1055398: "Home & Kitchen",
    7141123011: "Clothing, Shoes & Jewelry",
    3760901: "Luggage & Travel",
    3375251: "Sports & Outdoors",
    165793011: "Toys & Games",
    2619525011: "Tools & Home Improvement",
    51574011: "Pet Supplies",
    165796011: "Baby",
    172282: "Electronics",
    1064954: "Health & Household",
    3760911: "Beauty & Personal Care",
    2238192011: "Musical Instruments",
    979455011: "Garden & Outdoor",
    1285128: "Office Products",
    468642: "Video Games",
    283155: "Books",
    16310101: "Grocery & Gourmet Food",
    9482648011: "Kitchen & Dining",
}

CATEGORY_EMOJI = {
    "Electronics": "💻",
    "Home & Kitchen": "🏠",
    "Clothing, Shoes & Jewelry": "👗",
    "Beauty & Personal Care": "💄",
    "Health & Household": "💊",
    "Toys & Games": "🧸",
    "Sports & Outdoors": "⚽",
    "Automotive": "🚗",
    "Pet Supplies": "🐾",
    "Baby": "🍼",
    "Garden & Outdoor": "🌱",
    "Office Products": "📎",
    "Tools & Home Improvement": "🔧",
    "Kitchen & Dining": "🍳",
    "Video Games": "🎮",
    "Books": "📚",
    "Musical Instruments": "🎸",
    "Grocery & Gourmet Food": "🛒",
    "Luggage & Travel": "🧳",
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def truncate(text: str, limit: int = 95) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def safe_get(obj: Any, path: list[str], default: Any = None) -> Any:
    cur = obj
    for key in path:
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(key)
        else:
            cur = getattr(cur, key, None)
    return default if cur is None else cur


def cents_to_dollars(value: Any) -> float | None:
    try:
        if value is None:
            return None
        value = float(value)
        if value <= 0:
            return None
        return value / 100.0
    except Exception:
        return None


# ─── KEEPA DEAL REQUEST ───────────────────────────────────────────────────────

def keepa_deal_request(deal_params: dict[str, Any]) -> dict[str, Any]:
    """Call Keepa deal endpoint — POST with JSON body."""
    url = f"{KEEPA_BASE}/deal"
    params = {"key": KEEPA_API_KEY}
    headers = {"Content-Type": "application/json"}

    print(f"    POST {url}")
    print(f"    Body: {json.dumps(deal_params)}")

    r = requests.post(url, params=params, json=deal_params, headers=headers, timeout=60)
    print(f"    Status: {r.status_code}")
    if r.status_code != 200:
        print(f"    Response: {r.text[:500]}")
    r.raise_for_status()
    return r.json()


# ─── KEEPA PRODUCT REQUEST ────────────────────────────────────────────────────

def keepa_product_request(asins: list[str]) -> dict[str, Any]:
    """
    Call Keepa product endpoint with minimal params.
    """
    url = f"{KEEPA_BASE}/product"
    params = {
        "key": KEEPA_API_KEY,
        "asin": ",".join(asins),
        "domainId": 1,
    }

    r = requests.get(url, params=params, timeout=60)
    print(f"    Product status: {r.status_code}")
    if r.status_code != 200:
        print(f"    Product response: {r.text[:300]}")
    r.raise_for_status()
    return r.json()


def get_category(product: dict[str, Any]) -> str:
    root = product.get("rootCategory")
    if root and root in CATEGORY_NAMES:
        return CATEGORY_NAMES[root]

    for cat_id in (product.get("categories") or []):
        if cat_id in CATEGORY_NAMES:
            return CATEGORY_NAMES[cat_id]

    title = (product.get("title") or "").lower()

    if any(w in title for w in ["drill", "sander", "router", "saw", "clamp", "shop vac", "dust collector"]):
        return "Tools & Home Improvement"
    if any(w in title for w in ["laptop", "phone", "tablet", "camera", "monitor", "tv"]):
        return "Electronics"
    if any(w in title for w in ["shirt", "shoe", "dress", "jacket", "pants", "watch"]):
        return "Clothing, Shoes & Jewelry"
    if any(w in title for w in ["blender", "vacuum", "mattress", "pillow", "cookware", "kitchen"]):
        return "Home & Kitchen"

    return "Electronics"


def parse_coupon(product: dict[str, Any]) -> dict[str, Any] | None:
    coupon_history = product.get("coupon")
    if not coupon_history or len(coupon_history) < 3:
        return None

    idx = len(coupon_history) - 3
    while idx >= 0:
        one_time = coupon_history[idx + 1]
        sns = coupon_history[idx + 2]

        for val, coupon_type in [(one_time, "clip"), (sns, "sns")]:
            if not val:
                continue

            if val > 0 and val >= MIN_COUPON_PCT:
                return {
                    "type": coupon_type,
                    "kind": "percent",
                    "value": int(val),
                    "display": f"{int(val)}% off coupon",
                }

            if val < 0:
                dollars = abs(val) / 100.0
                if dollars >= MIN_COUPON_VALUE:
                    return {
                        "type": coupon_type,
                        "kind": "dollars",
                        "value": dollars,
                        "display": f"${dollars:.0f} off coupon",
                    }

        idx -= 3

    return None


# ─── STEP 1: KEEPA — FIND DEALS ───────────────────────────────────────────────

def fetch_keepa_asins() -> list[str]:
    print("\n[Keepa] Fetching deal ASINs...")

    body = {
        "domainId": 1,
        "priceTypes": [0],
        "deltaPercent": MIN_DISCOUNT_PCT,
        "interval": 10080,
        "page": 0,
    }

    data = keepa_deal_request(body)
    deals_raw = data.get("deals", {}).get("dr", [])

    asins = [d.get("asin") for d in deals_raw if d.get("asin")]
    unique_asins = list(dict.fromkeys(asins))

    print(f"[Keepa] Got {len(unique_asins)} unique ASINs")
    return unique_asins


def fetch_keepa_product_details(asins: list[str]) -> list[dict[str, Any]]:
    if not asins:
        return []

    print(f"\n[Keepa] Fetching product details ({len(asins)} ASINs)...")
    all_products: list[dict[str, Any]] = []

    batch_size = 20
    for start in range(0, len(asins), batch_size):
        batch = asins[start : start + batch_size]
        try:
            data = keepa_product_request(batch)
            products = data.get("products", [])
            all_products.extend(products)
            print(f"    Progress: {min(start + batch_size, len(asins))}/{len(asins)} ({len(all_products)} successful)")
            time.sleep(0.4)
        except Exception as e:
            print(f"  [Keepa] Batch error: {e}")

    print(f"[Keepa] Total products pulled: {len(all_products)}")
    return all_products


def build_keepa_candidates(products: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    keepa_deals: dict[str, dict[str, Any]] = {}

    for p in products:
        try:
            asin = p.get("asin", "")
            if not asin:
                continue

            cat = get_category(p)
            if cat in EXCLUDED_CATEGORIES:
                continue

            stats = p.get("stats", {})
            cur_raw = stats.get("current", [])
            avg_raw = stats.get("avg90", [])

            current = cents_to_dollars(cur_raw[0] if cur_raw else None)
            avg90 = cents_to_dollars(avg_raw[0] if avg_raw else None)

            pct = 0
            if current and avg90 and avg90 > 0 and current < avg90:
                pct = round((1 - current / avg90) * 100)

            coupon = parse_coupon(p)

            if pct < MIN_DISCOUNT_PCT and coupon is None:
                continue

            keepa_deals[asin] = {
                "asin": asin,
                "category": cat,
                "pct": pct,
                "coupon": coupon,
                "title_fallback": truncate(p.get("title") or "", 95),
                "avg90": avg90,
            }

        except Exception as e:
            print(f"  [Keepa] Skipping product: {e}")

    return keepa_deals


# ─── STEP 2: AMAZON CREATORS API ──────────────────────────────────────────────

def build_creators_client() -> AmazonCreatorsApi:
    if not AMAZON_CREATOR_CREDENTIAL_ID:
        raise RuntimeError("Missing AMAZON_CREATOR_CREDENTIAL_ID")
    if not AMAZON_CREATOR_CREDENTIAL_SECRET:
        raise RuntimeError("Missing AMAZON_CREATOR_CREDENTIAL_SECRET")
    if not AMAZON_PARTNER_TAG:
        raise RuntimeError("Missing AFFILIATE_TAG")

    return AmazonCreatorsApi(
        credential_id=AMAZON_CREATOR_CREDENTIAL_ID,
        credential_secret=AMAZON_CREATOR_CREDENTIAL_SECRET,
        version=AMAZON_CREATOR_VERSION,
        tag=AMAZON_PARTNER_TAG,
        country=Country.US,
    )


def extract_item_data(item: Any) -> dict[str, Any]:
    asin = safe_get(item, ["asin"]) or safe_get(item, ["ASIN"]) or ""

    title = (
        safe_get(item, ["item_info", "title", "display_value"])
        or safe_get(item, ["itemInfo", "title", "displayValue"])
        or ""
    )

    detail_url = safe_get(item, ["detail_page_url"]) or safe_get(item, ["detailPageURL"]) or ""

    image = (
        safe_get(item, ["images", "primary", "large", "url"])
        or safe_get(item, ["images", "primary", "medium", "url"])
        or safe_get(item, ["images", "primary", "large", "URL"])
        or safe_get(item, ["images", "primary", "medium", "URL"])
        or ""
    )

    price_display = ""
    prime = False

    listings_v2 = safe_get(item, ["offers_v2", "listings"], []) or safe_get(item, ["offersV2", "listings"], [])
    if listings_v2:
        first = listings_v2[0]
        price_display = (
            safe_get(first, ["price", "money", "display_amount"])
            or safe_get(first, ["price", "money", "displayAmount"])
            or ""
        )
        prime = bool(
            safe_get(first, ["delivery_info", "is_prime_eligible"], False)
            or safe_get(first, ["deliveryInfo", "isPrimeEligible"], False)
        )

    return {
        "asin": asin,
        "title": title,
        "price_display": price_display,
        "image": image,
        "prime": prime,
        "detail_url": detail_url,
    }


def fetch_amazon_live_data(asins: list[str]) -> dict[str, dict[str, Any]]:
    if not asins:
        return {}

    print(f"\n[Creators API] Fetching live data ({len(asins)} ASINs)...")
    client = build_creators_client()
    results: dict[str, dict[str, Any]] = {}

    batch_size = 10
    for start in range(0, len(asins), batch_size):
        batch = asins[start : start + batch_size]
        try:
            items = client.get_items(batch)
            for item in items or []:
                row = extract_item_data(item)
                asin = row.get("asin")
                if asin:
                    results[asin] = row
            print(f"    Progress: {min(start + batch_size, len(asins))}/{len(asins)} ({len(results)} successful)")
            time.sleep(0.8)
        except Exception as e:
            print(f"  [Creators API] Batch error on {batch}: {e}")

    print(f"[Creators API] Got data for {len(results)} products")
    return results


# ─── STEP 3: BUILD deals.json ─────────────────────────────────────────────────

def build_deals_json() -> None:
    print(f"\n[{dt.datetime.now().strftime('%H:%M:%S')}] Starting DealDrop Creators fetch...\n")

    if not KEEPA_API_KEY:
        raise RuntimeError("Missing KEEPA_API_KEY")

    all_asins = fetch_keepa_asins()

    if not all_asins:
        output = {
            "updatedAt": utc_now_iso(),
            "totalDeals": 0,
            "hotDeals": 0,
            "couponDeals": 0,
            "deals": [],
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        print("\nNo ASINs returned. Saving empty deals.json.")
        return

    keepa_products = fetch_keepa_product_details(all_asins[: MAX_DEALS + 40])
    keepa_deals = build_keepa_candidates(keepa_products)

    qualifying_asins = list(keepa_deals.keys())
    print(f"\n{len(qualifying_asins)} qualifying deals after Keepa filtering")

    amazon_data = fetch_amazon_live_data(qualifying_asins[: MAX_DEALS + 20])

    formatted = []
    deal_id = 1

    for asin in qualifying_asins:
        try:
            k = keepa_deals[asin]
            a = amazon_data.get(asin, {})

            title = a.get("title") or k["title_fallback"]
            if not title or len(title) < 5:
                continue

            price = a.get("price_display", "")
            image = a.get("image", "")
            prime = a.get("prime", False)
            coupon = k["coupon"]
            pct = k["pct"]
            cat = k["category"]
            avg90 = k.get("avg90")

            if cat in EXCLUDED_CATEGORIES:
                continue

            effective_pct = pct
            if coupon and coupon["kind"] == "percent":
                effective_pct = min(99, pct + coupon["value"])

            parts = []
            if pct >= MIN_DISCOUNT_PCT:
                parts.append(f"{pct}% off recent price")
            if coupon:
                parts.append(coupon["display"])
            if prime:
                parts.append("Prime eligible")

            formatted.append({
                "id": deal_id,
                "asin": asin,
                "cat": cat,
                "emoji": CATEGORY_EMOJI.get(cat, "🛒"),
                "title": truncate(title, 95),
                "desc": " · ".join(parts),
                "price": price,
                "was": f"${avg90:.2f}" if avg90 else "",
                "hasLivePrice": bool(price),
                "pct": pct,
                "effectivePct": effective_pct,
                "hot": effective_pct >= HOT_DEAL_PCT,
                "discount": f"{pct}% off",
                "hasCoupon": coupon is not None,
                "couponDisplay": coupon["display"] if coupon else None,
                "image": image,
                "prime": prime,
                "link": a.get("detail_url") or f"https://www.amazon.com/dp/{asin}?tag={AMAZON_PARTNER_TAG}",
                "updatedAt": utc_now_iso(),
            })
            deal_id += 1

        except Exception as e:
            print(f"  Skipping {asin}: {e}")

    formatted.sort(key=lambda d: (not d["hot"], -d["effectivePct"], d["title"]))
    formatted = formatted[:MAX_DEALS]

    output = {
        "updatedAt": utc_now_iso(),
        "totalDeals": len(formatted),
        "hotDeals": sum(1 for d in formatted if d["hot"]),
        "couponDeals": sum(1 for d in formatted if d["hasCoupon"]),
        "deals": formatted,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Saved {len(formatted)} deals to {OUTPUT_FILE}")
    print(f"  Hot deals:    {output['hotDeals']}")
    print(f"  Coupon deals: {output['couponDeals']}")
    print(f"  Updated:      {output['updatedAt']}")


if __name__ == "__main__":
    build_deals_json()
