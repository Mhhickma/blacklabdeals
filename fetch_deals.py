"""
DealDrop — fetch_deals.py
--------------------------
Two-API pipeline:
  1. Keepa API     — finds deals using recent price movement + coupon detection
  2. Amazon PA API — fetches live prices, images, titles (TOS compliant to display)

This version improves variety by:
- pulling multiple Keepa deal pages
- filtering out books by default
- limiting overrepresented categories like clothing/shoes
- keeping Amazon PA API as the display source for compliance

Requirements:
    pip install requests
"""

import json
import os
import time
import hmac
import hashlib
import datetime
from collections import Counter

import requests

# ─── CONFIG ───────────────────────────────────────────────────────────────────

KEEPA_API_KEY      = os.environ.get("KEEPA_API_KEY", "")
AMAZON_ACCESS_KEY  = os.environ.get("AMAZON_ACCESS_KEY", "")
AMAZON_SECRET_KEY  = os.environ.get("AMAZON_SECRET_KEY", "")
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG", "")
AMAZON_HOST        = "webservices.amazon.com"
AMAZON_REGION      = "us-east-1"

OUTPUT_FILE        = "deals.json"

# Final number of deals shown on site
MAX_DEALS          = 60

# Keepa search controls
KEEPA_PAGES        = 3          # pages 0,1,2
PAGE_DELAY_SEC     = 1.2
PRODUCT_DELAY_SEC  = 0.35

# Deal thresholds
MIN_DISCOUNT_PCT   = 20
HOT_DEAL_PCT       = 50
MIN_COUPON_VALUE   = 3
MIN_COUPON_PCT     = 5

# Variety controls
EXCLUDED_CATEGORIES = {
    "Books",
}

# Limit category dominance in final results
CATEGORY_LIMITS = {
    "Clothing, Shoes & Jewelry": 6,
    "Home & Kitchen": 8,
    "Electronics": 8,
    "Tools & Home Improvement": 10,
}

DEFAULT_CATEGORY_LIMIT = 5

KEEPA_BASE = "https://api.keepa.com"

# ─── CATEGORY MAPPING ─────────────────────────────────────────────────────────

CATEGORY_NAMES = {
    281052:      "Electronics",
    1055398:     "Home & Kitchen",
    7141123011:  "Clothing, Shoes & Jewelry",
    3760901:     "Luggage & Travel",
    3375251:     "Sports & Outdoors",
    165793011:   "Toys & Games",
    2619525011:  "Tools & Home Improvement",
    51574011:    "Pet Supplies",
    165796011:   "Baby",
    172282:      "Electronics",
    1064954:     "Health & Household",
    3760911:     "Beauty & Personal Care",
    2238192011:  "Musical Instruments",
    979455011:   "Garden & Outdoor",
    1285128:     "Office Products",
    468642:      "Video Games",
    283155:      "Books",
    16310101:    "Grocery & Gourmet Food",
    9482648011:  "Kitchen & Dining",
}

CATEGORY_EMOJI = {
    "Electronics":               "💻",
    "Home & Kitchen":            "🏠",
    "Clothing, Shoes & Jewelry": "👗",
    "Beauty & Personal Care":    "💄",
    "Health & Household":        "💊",
    "Toys & Games":              "🧸",
    "Sports & Outdoors":         "⚽",
    "Automotive":                "🚗",
    "Pet Supplies":              "🐾",
    "Baby":                      "🍼",
    "Garden & Outdoor":          "🌱",
    "Office Products":           "📎",
    "Tools & Home Improvement":  "🔧",
    "Kitchen & Dining":          "🍳",
    "Video Games":               "🎮",
    "Books":                     "📚",
    "Musical Instruments":       "🎸",
    "Grocery & Gourmet Food":    "🛒",
    "Luggage & Travel":          "🧳",
}

# ─── KEEPA DEAL REQUEST ───────────────────────────────────────────────────────

def keepa_deal_request(deal_params):
    """Call Keepa deal endpoint — POST with JSON body."""
    url = f"{KEEPA_BASE}/deal"
    params = {"key": KEEPA_API_KEY}
    headers = {"Content-Type": "application/json"}

    r = requests.post(url, params=params, json=deal_params, headers=headers, timeout=60)
    if r.status_code != 200:
        print(f"    Deal request failed: {r.status_code}")
        print(f"    Response: {r.text[:500]}")
    r.raise_for_status()
    return r.json()

# ─── KEEPA PRODUCT REQUEST ────────────────────────────────────────────────────

def keepa_product_request(asins):
    """
    Keepa product lookup.
    We intentionally use small batches for reliability.
    """
    url = f"{KEEPA_BASE}/product"

    param_sets = [
        {"key": KEEPA_API_KEY, "asin": ",".join(asins)},
        {"key": KEEPA_API_KEY, "asin": ",".join(asins), "domainId": 1},
    ]

    for i, params in enumerate(param_sets):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 200:
                return r.json()
            print(f"    Product attempt {i+1} failed: {r.status_code} {r.text[:250]}")
        except Exception as e:
            print(f"    Product attempt {i+1} error: {e}")

    return {"products": []}

def get_category(product):
    root = product.get("rootCategory")
    if root and root in CATEGORY_NAMES:
        return CATEGORY_NAMES[root]

    for cat_id in (product.get("categories") or []):
        if cat_id in CATEGORY_NAMES:
            return CATEGORY_NAMES[cat_id]

    title = (product.get("title") or "").lower()

    if any(w in title for w in ["laptop", "phone", "tablet", "camera", "headphone", "speaker", "monitor", "tv"]):
        return "Electronics"
    if any(w in title for w in ["shirt", "shoe", "dress", "jacket", "pants", "bag", "watch", "bra", "sandal", "sneaker", "slipper", "mule"]):
        return "Clothing, Shoes & Jewelry"
    if any(w in title for w in ["blender", "vacuum", "mattress", "pillow", "cookware", "kitchen", "rug", "ottoman", "tumbler"]):
        return "Home & Kitchen"
    if any(w in title for w in ["protein", "vitamin", "supplement", "fitness", "yoga"]):
        return "Health & Household"
    if any(w in title for w in ["toy", "game", "lego", "puzzle", "kids"]):
        return "Toys & Games"

    return "Electronics"

def parse_coupon(product):
    coupon_history = product.get("coupon")
    if not coupon_history or len(coupon_history) < 3:
        return None

    idx = len(coupon_history) - 3
    while idx >= 0:
        one_time = coupon_history[idx + 1]
        sns = coupon_history[idx + 2]

        for val, ctype in [(one_time, "clip"), (sns, "sns")]:
            if val and val != 0:
                if val > 0 and val >= MIN_COUPON_PCT:
                    return {
                        "type": ctype,
                        "kind": "percent",
                        "value": val,
                        "display": f"{val}% off coupon",
                    }
                if val < 0:
                    dollars = abs(val) / 100.0
                    if dollars >= MIN_COUPON_VALUE:
                        return {
                            "type": ctype,
                            "kind": "dollars",
                            "value": dollars,
                            "display": f"${dollars:.0f} off coupon",
                        }
        idx -= 3

    return None

# ─── STEP 1: KEEPA — FIND DEAL ASINS ──────────────────────────────────────────

def fetch_keepa_asins():
    print("\n  [Keepa] Fetching deals across multiple pages...")

    all_asins = []
    seen = set()

    for page in range(KEEPA_PAGES):
        body = {
            "domainId": 1,
            "priceTypes": [0],
            "deltaPercent": MIN_DISCOUNT_PCT,
            "interval": 10080,
            "page": page,
        }

        try:
            data = keepa_deal_request(body)
            deals_raw = data.get("deals", {}).get("dr", [])
            page_asins = [d.get("asin") for d in deals_raw if d.get("asin")]

            new_count = 0
            for asin in page_asins:
                if asin not in seen:
                    seen.add(asin)
                    all_asins.append(asin)
                    new_count += 1

            print(f"  [Keepa] Page {page}: {len(page_asins)} candidates, {new_count} new unique")
        except Exception as e:
            print(f"  [Keepa] Page {page} failed: {e}")

        time.sleep(PAGE_DELAY_SEC)

    print(f"  [Keepa] {len(all_asins)} unique ASINs across {KEEPA_PAGES} pages")
    return all_asins

# ─── STEP 2: KEEPA — PRODUCT DETAILS ──────────────────────────────────────────

def fetch_keepa_product_details(asins):
    if not asins:
        return []

    print(f"  [Keepa] Fetching product details for {len(asins)} ASINs...")

    all_products = []
    success = 0

    for i, asin in enumerate(asins, start=1):
        try:
            data = keepa_product_request([asin])
            products = data.get("products", [])
            if products:
                all_products.extend(products)
                success += 1

            if i % 20 == 0 or i == len(asins):
                print(f"    Progress: {i}/{len(asins)} ({success} successful)")
        except Exception as e:
            print(f"    Error on {asin}: {e}")

        time.sleep(PRODUCT_DELAY_SEC)

    print(f"  [Keepa] Total products returned: {len(all_products)}")
    return all_products

# ─── STEP 3: AMAZON PA API ────────────────────────────────────────────────────

def sign_aws(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def get_aws_signing_key(secret, date_stamp, region, service):
    k = sign_aws(("AWS4" + secret).encode("utf-8"), date_stamp)
    k = sign_aws(k, region)
    k = sign_aws(k, service)
    k = sign_aws(k, "aws4_request")
    return k

def fetch_amazon_live_data(asin_batch):
    if not AMAZON_ACCESS_KEY:
        print("  [Amazon PA API] Not configured — skipping.")
        return {}

    service = "ProductAdvertisingAPI"
    path = "/paapi5/getitems"
    endpoint = f"https://{AMAZON_HOST}{path}"

    payload = {
        "ItemIds": asin_batch,
        "PartnerTag": AMAZON_PARTNER_TAG,
        "PartnerType": "Associates",
        "Marketplace": "www.amazon.com",
        "Resources": [
            "Images.Primary.Large",
            "ItemInfo.Title",
            "Offers.Listings.Price",
            "Offers.Listings.Availability.Message",
            "Offers.Listings.DeliveryInfo.IsPrimeEligible",
        ],
    }

    body = json.dumps(payload)
    now = datetime.datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    canonical_headers = (
        f"content-encoding:amz-1.0\n"
        f"content-type:application/json; charset=utf-8\n"
        f"host:{AMAZON_HOST}\n"
        f"x-amz-date:{amz_date}\n"
        f"x-amz-target:com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems\n"
    )
    signed_headers = "content-encoding;content-type;host;x-amz-date;x-amz-target"
    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    canonical_request = "\n".join(["POST", path, "", canonical_headers, signed_headers, payload_hash])
    credential_scope = f"{date_stamp}/{AMAZON_REGION}/{service}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    signing_key = get_aws_signing_key(AMAZON_SECRET_KEY, date_stamp, AMAZON_REGION, service)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={AMAZON_ACCESS_KEY}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    headers = {
        "content-encoding": "amz-1.0",
        "content-type": "application/json; charset=utf-8",
        "host": AMAZON_HOST,
        "x-amz-date": amz_date,
        "x-amz-target": "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems",
        "Authorization": authorization,
    }

    try:
        r = requests.post(endpoint, headers=headers, data=body, timeout=15)
        r.raise_for_status()

        items = r.json().get("ItemsResult", {}).get("Items", [])
        result = {}

        for item in items:
            asin = item.get("ASIN")
            listing = (item.get("Offers", {}).get("Listings") or [{}])[0]
            price_obj = listing.get("Price", {})
            img_obj = item.get("Images", {}).get("Primary", {}).get("Large", {})
            title = item.get("ItemInfo", {}).get("Title", {}).get("DisplayValue", "")
            prime = listing.get("DeliveryInfo", {}).get("IsPrimeEligible", False)

            result[asin] = {
                "price_display": price_obj.get("DisplayAmount", ""),
                "image": img_obj.get("URL", ""),
                "title": title,
                "prime": prime,
            }

        print(f"  [Amazon PA API] Got data for {len(result)} products")
        return result

    except Exception as e:
        print(f"  [Amazon PA API] ERROR: {e}")
        return {}

# ─── SCORING / VARIETY ────────────────────────────────────────────────────────

def category_limit_for(category):
    return CATEGORY_LIMITS.get(category, DEFAULT_CATEGORY_LIMIT)

def compute_sort_score(deal):
    """
    Higher score = better candidate.
    Prioritize hot deals, then stronger discount, then coupons, then prime.
    Penalize clothing to keep variety broader.
    """
    score = 0

    effective_pct = deal.get("effectivePct", 0)
    pct = deal.get("pct", 0)

    score += effective_pct * 10
    score += pct * 4

    if deal.get("hot"):
        score += 250
    if deal.get("hasCoupon"):
        score += 60
    if deal.get("prime"):
        score += 12
    if deal.get("hasLivePrice"):
        score += 20

    cat = deal.get("cat", "")
    if cat == "Clothing, Shoes & Jewelry":
        score -= 90
    elif cat == "Home & Kitchen":
        score -= 10

    return score

def apply_variety_limits(deals):
    """
    Keep the best deals while preventing one category from taking over.
    """
    sorted_deals = sorted(
        deals,
        key=lambda d: (
            -compute_sort_score(d),
            d.get("cat", ""),
            d.get("title", ""),
        )
    )

    selected = []
    counts = Counter()

    for deal in sorted_deals:
        cat = deal.get("cat", "Other")

        if cat in EXCLUDED_CATEGORIES:
            continue

        if counts[cat] >= category_limit_for(cat):
            continue

        selected.append(deal)
        counts[cat] += 1

        if len(selected) >= MAX_DEALS:
            break

    # Backfill if limits were too strict
    if len(selected) < MAX_DEALS:
        selected_asins = {d.get("asin") for d in selected}
        for deal in sorted_deals:
            asin = deal.get("asin")
            if asin in selected_asins:
                continue
            if deal.get("cat") in EXCLUDED_CATEGORIES:
                continue

            selected.append(deal)
            selected_asins.add(asin)

            if len(selected) >= MAX_DEALS:
                break

    return selected

# ─── STEP 4: BUILD deals.json ─────────────────────────────────────────────────

def build_deals_json():
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting DealDrop deal fetch...\n")

    all_asins = fetch_keepa_asins()

    if not all_asins:
        print("\n  No ASINs returned. Saving empty deals.json.")
        output = {
            "updatedAt": datetime.datetime.utcnow().isoformat() + "Z",
            "totalDeals": 0,
            "hotDeals": 0,
            "couponDeals": 0,
            "deals": [],
        }
        with open(OUTPUT_FILE, "w") as f:
            json.dump(output, f, indent=2)
        return

    keepa_products = fetch_keepa_product_details(all_asins)

    keepa_deals = {}
    for p in keepa_products:
        try:
            asin = p.get("asin", "")
            stats = p.get("stats", {})
            cur_raw = stats.get("current", [])
            avg_raw = stats.get("avg90", [])

            def to_d(v):
                return v / 100.0 if v and v > 0 else None

            current = to_d(cur_raw[0] if cur_raw and cur_raw[0] and cur_raw[0] > 0 else None)
            avg90 = to_d(avg_raw[0] if avg_raw and avg_raw[0] and avg_raw[0] > 0 else None)
            coupon = parse_coupon(p)
            pct = 0

            if current and avg90 and avg90 > 0 and current < avg90:
                pct = round((1 - current / avg90) * 100)

            if pct < MIN_DISCOUNT_PCT and coupon is None:
                continue

            category = get_category(p)
            if category in EXCLUDED_CATEGORIES:
                continue

            keepa_deals[asin] = {
                "asin": asin,
                "category": category,
                "pct": pct,
                "coupon": coupon,
                "title_fallback": (p.get("title") or "")[:120],
            }
        except Exception as e:
            print(f"  Skipping Keepa product: {e}")

    qualifying_asins = list(keepa_deals.keys())
    print(f"\n  {len(qualifying_asins)} qualifying deals after Keepa filtering")

    amazon_data = {}
    for i in range(0, len(qualifying_asins), 10):
        batch = qualifying_asins[i:i+10]
        result = fetch_amazon_live_data(batch)
        amazon_data.update(result)
        time.sleep(1)

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
                "title": title[:90] + ("..." if len(title) > 90 else ""),
                "desc": " · ".join(parts),
                "price": price,
                "was": "",
                "hasLivePrice": bool(price),
                "pct": pct,
                "effectivePct": effective_pct,
                "hot": effective_pct >= HOT_DEAL_PCT,
                "discount": f"{pct}% off",
                "hasCoupon": coupon is not None,
                "couponDisplay": coupon["display"] if coupon else None,
                "image": image,
                "prime": prime,
                "link": f"https://www.amazon.com/dp/{asin}?tag={AMAZON_PARTNER_TAG}",
                "updatedAt": datetime.datetime.utcnow().isoformat() + "Z",
            })
            deal_id += 1

        except Exception as e:
            print(f"  Skipping formatted deal {asin}: {e}")

    final_deals = apply_variety_limits(formatted)

    # Re-number after variety filtering
    for idx, deal in enumerate(final_deals, start=1):
        deal["id"] = idx

    output = {
        "updatedAt": datetime.datetime.utcnow().isoformat() + "Z",
        "totalDeals": len(final_deals),
        "hotDeals": sum(1 for d in final_deals if d["hot"]),
        "couponDeals": sum(1 for d in final_deals if d["hasCoupon"]),
        "deals": final_deals,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Saved {len(final_deals)} deals to {OUTPUT_FILE}")
    print(f"  Hot deals:    {output['hotDeals']}")
    print(f"  Coupon deals: {output['couponDeals']}")
    print(f"  Updated:      {output['updatedAt']}")

    by_cat = Counter(d["cat"] for d in final_deals)
    print("  Category mix:")
    for cat, count in by_cat.most_common():
        print(f"    {cat}: {count}")

if __name__ == "__main__":
    build_deals_json()
