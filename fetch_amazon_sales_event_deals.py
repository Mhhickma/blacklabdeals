"""
Amazon Sales Event ASIN Feed + Amazon API Enrichment
----------------------------------------------------
This event-only process reads ASINs from the Google Sheet, fetches product
data from Amazon's API with signed requests, and writes amazon-sales-event-deals.json.
"""

import csv
import hashlib
import hmac
import io
import json
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import quote

import requests


# --------------------------------------------------
# SETTINGS
# --------------------------------------------------
SHEET_ID = os.getenv("AMAZON_SALES_EVENT_SHEET_ID", "19xrjgm9FJdiJaptD7iO2cMzxLFk5LbqivvgvaGZ3qsI")
SHEET_GID = os.getenv("AMAZON_SALES_EVENT_SHEET_GID", "0")
SHEET_CSV_URL = os.getenv(
    "AMAZON_SALES_EVENT_SHEET_CSV_URL",
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}",
)

OUTPUT_FILE = os.getenv("AMAZON_SALES_EVENT_OUTPUT_FILE", "amazon-sales-event-deals.json")
PARTNER_TAG = os.getenv("AFFILIATE_TAG", "blacklabdealsprime-20")
ACCESS_KEY = os.getenv("CREATORS_CREDENTIAL_ID") or os.getenv("PAAPI_ACCESS_KEY") or os.getenv("AMAZON_ACCESS_KEY")
SECRET_KEY = os.getenv("CREATORS_CREDENTIAL_SECRET") or os.getenv("PAAPI_SECRET_KEY") or os.getenv("AMAZON_SECRET_KEY")
PARTNER_TYPE = os.getenv("AMAZON_PARTNER_TYPE", "Associates")
MARKETPLACE = os.getenv("AMAZON_MARKETPLACE", "www.amazon.com")
PAAPI_HOST = os.getenv("AMAZON_PAAPI_HOST", "webservices.amazon.com")
PAAPI_REGION = os.getenv("AMAZON_PAAPI_REGION", "us-east-1")
PAAPI_SERVICE = "ProductAdvertisingAPI"
PAAPI_PATH = "/paapi5/getitems"
PAAPI_TARGET = "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems"
AMAZON_BATCH_SIZE = int(os.getenv("AMAZON_SALES_EVENT_BATCH_SIZE", "10"))
AMAZON_REQUEST_DELAY_SECONDS = float(os.getenv("AMAZON_SALES_EVENT_REQUEST_DELAY_SECONDS", "1"))

PAGE_COLUMNS = [
    "amazon-deal-event",
    "amazon-tool-deals",
    "amazon-electronics-deals",
    "amazon-home-kitchen-deals",
    "amazon-device-deals",
    "amazon-deals-under-50",
    "amazon-household-essentials-deals",
]

ASIN_RE = re.compile(r"\b[A-Z0-9]{10}\b", re.IGNORECASE)

BASE_RESOURCES = [
    "Images.Primary.Large",
    "ItemInfo.ByLineInfo",
    "ItemInfo.Classifications",
    "ItemInfo.Title",
    "Offers.Listings.Availability.Message",
    "Offers.Listings.Condition",
    "Offers.Listings.DeliveryInfo.IsPrimeEligible",
    "Offers.Listings.Price",
    "Offers.Listings.SavingBasis",
]

REVIEW_RESOURCES = [
    "CustomerReviews.Count",
    "CustomerReviews.StarRating",
]


# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def compact_image_url(url, size=220):
    if not url:
        return None
    text = str(url)
    text = re.sub(r"\._SL\d+_\.", f"._SL{size}_.", text)
    text = re.sub(r"\._[A-Z0-9,]+_\.", f"._SL{size}_.", text)
    return text


def clean_header(value):
    return str(value or "").strip().lower().replace(" ", "-").replace("_", "-")


def extract_asins(value):
    return [m.group(0).upper() for m in ASIN_RE.finditer(str(value or ""))]


def get_nested(data, path, default=None):
    cur = data
    for key in path:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return default
    return cur


def to_float(value):
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def to_int(value):
    try:
        if value is None or value == "":
            return None
        return int(float(str(value).replace(",", "")))
    except Exception:
        return None


def normalize_rating(value):
    if isinstance(value, dict):
        value = value.get("Value") or value.get("DisplayValue") or value.get("Rating")
    if isinstance(value, str):
        match = re.search(r"\d+(?:\.\d+)?", value)
        value = match.group(0) if match else None
    rating = to_float(value)
    if rating and 0 < rating <= 5:
        return round(rating, 1)
    return None


def canonical_amazon_link(asin):
    return f"https://www.amazon.com/dp/{asin}?tag={PARTNER_TAG}"


def signed_headers(payload):
    if not ACCESS_KEY or not SECRET_KEY:
        raise RuntimeError("Missing CREATORS_CREDENTIAL_ID/CREATORS_CREDENTIAL_SECRET or PAAPI_ACCESS_KEY/PAAPI_SECRET_KEY")

    now = datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    canonical_headers = (
        f"content-encoding:amz-1.0\n"
        f"content-type:application/json; charset=utf-8\n"
        f"host:{PAAPI_HOST}\n"
        f"x-amz-date:{amz_date}\n"
        f"x-amz-target:{PAAPI_TARGET}\n"
    )
    signed_header_names = "content-encoding;content-type;host;x-amz-date;x-amz-target"
    canonical_request = "\n".join([
        "POST",
        PAAPI_PATH,
        "",
        canonical_headers,
        signed_header_names,
        payload_hash,
    ])

    credential_scope = f"{date_stamp}/{PAAPI_REGION}/{PAAPI_SERVICE}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])

    def sign(key, msg):
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    signing_key = sign(("AWS4" + SECRET_KEY).encode("utf-8"), date_stamp)
    signing_key = sign(signing_key, PAAPI_REGION)
    signing_key = sign(signing_key, PAAPI_SERVICE)
    signing_key = sign(signing_key, "aws4_request")
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        "AWS4-HMAC-SHA256 "
        f"Credential={ACCESS_KEY}/{credential_scope}, "
        f"SignedHeaders={signed_header_names}, "
        f"Signature={signature}"
    )

    return payload_json, {
        "Content-Encoding": "amz-1.0",
        "Content-Type": "application/json; charset=utf-8",
        "Host": PAAPI_HOST,
        "X-Amz-Date": amz_date,
        "X-Amz-Target": PAAPI_TARGET,
        "Authorization": authorization,
    }


def call_get_items(asins, resources):
    payload = {
        "ItemIds": asins,
        "Resources": resources,
        "PartnerTag": PARTNER_TAG,
        "PartnerType": PARTNER_TYPE,
        "Marketplace": MARKETPLACE,
    }
    body, headers = signed_headers(payload)
    response = requests.post(f"https://{PAAPI_HOST}{PAAPI_PATH}", data=body.encode("utf-8"), headers=headers, timeout=30)
    if not response.ok:
        raise RuntimeError(f"Amazon API HTTP {response.status_code}: {response.text[:1000]}")
    data = response.json()
    if data.get("Errors"):
        raise RuntimeError(f"Amazon API errors: {json.dumps(data.get('Errors'), ensure_ascii=False)[:1000]}")
    return data


# --------------------------------------------------
# STEP 1: READ GOOGLE SHEET
# --------------------------------------------------
def load_page_asins():
    print("[1/3] Loading Amazon Sales Event ASINs from Google Sheets...")
    response = requests.get(SHEET_CSV_URL, timeout=30)
    response.raise_for_status()

    text = response.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))

    header_map = {clean_header(h): h for h in (reader.fieldnames or [])}
    page_asins = {page: [] for page in PAGE_COLUMNS}
    asin_pages = {}

    for row in reader:
        for page in PAGE_COLUMNS:
            source_header = header_map.get(page)
            if not source_header:
                continue
            for asin in extract_asins(row.get(source_header, "")):
                if asin not in page_asins[page]:
                    page_asins[page].append(asin)
                asin_pages.setdefault(asin, set()).add(page)

    print(f"    Found {len(asin_pages)} unique ASINs across {len(PAGE_COLUMNS)} page columns.")
    for page, asins in page_asins.items():
        print(f"    {page}: {len(asins)} ASINs")

    return page_asins, {asin: sorted(pages) for asin, pages in asin_pages.items()}


# --------------------------------------------------
# STEP 2: FETCH AMAZON API DATA
# --------------------------------------------------
def get_amazon_items(asins):
    print("[2/3] Fetching live Amazon product data...")
    batches = [asins[i:i + AMAZON_BATCH_SIZE] for i in range(0, len(asins), AMAZON_BATCH_SIZE)]
    all_items = {}
    all_errors = []
    total_batches = len(batches)

    for idx, batch in enumerate(batches, start=1):
        print(f"    Batch {idx}/{total_batches} ({len(batch)} ASINs)...")
        resources = BASE_RESOURCES + REVIEW_RESOURCES
        try:
            data = call_get_items(batch, resources)
        except Exception as exc:
            print(f"    Review resource/full request failed, retrying base resources only: {exc}")
            data = call_get_items(batch, BASE_RESOURCES)

        result = data.get("ItemsResult") or {}
        for item in result.get("Items", []):
            asin = item.get("ASIN")
            if asin:
                all_items[asin] = item

        for err in data.get("Errors", []) or []:
            all_errors.append(err)

        if AMAZON_REQUEST_DELAY_SECONDS > 0 and idx < total_batches:
            time.sleep(AMAZON_REQUEST_DELAY_SECONDS)

    print(f"    Retrieved {len(all_items)} products from Amazon API.")
    if all_errors:
        print(f"    Amazon returned {len(all_errors)} item-level warnings/errors.")
    return all_items


# --------------------------------------------------
# STEP 3: BUILD EVENT DEAL FEED
# --------------------------------------------------
def build_deal(asin, item, pages):
    title = get_nested(item, ["ItemInfo", "Title", "DisplayValue"])
    if not title:
        return None

    brand = get_nested(item, ["ItemInfo", "ByLineInfo", "Brand", "DisplayValue"])
    category = (
        get_nested(item, ["ItemInfo", "Classifications", "ProductGroup", "DisplayValue"])
        or get_nested(item, ["ItemInfo", "Classifications", "Binding", "DisplayValue"])
        or "Amazon Deals"
    )
    image = compact_image_url(get_nested(item, ["Images", "Primary", "Large", "URL"]))

    listing = (get_nested(item, ["Offers", "Listings"], []) or [{}])[0]
    price_amount = to_float(get_nested(listing, ["Price", "Amount"]))
    price_display = get_nested(listing, ["Price", "DisplayAmount"])
    currency = get_nested(listing, ["Price", "Currency"])
    if not price_amount or not price_display:
        return None

    condition = str(get_nested(listing, ["Condition", "Value"], "") or "").lower()
    if condition and condition != "new":
        return None

    availability = get_nested(listing, ["Availability", "Message"])
    url = item.get("DetailPageURL") or canonical_amazon_link(asin)

    saving_basis = get_nested(listing, ["SavingBasis", "Amount"])
    saving_basis_display = get_nested(listing, ["SavingBasis", "DisplayAmount"])
    pct_off = 0
    was_display = None
    discount_label = ""
    if saving_basis and price_amount:
        original = to_float(saving_basis)
        if original and original > price_amount:
            pct_off = round(((original - price_amount) / original) * 100)
            was_display = saving_basis_display or f"${original:.2f}"
            discount_label = f"-{pct_off}%"

    rating_value = normalize_rating(get_nested(item, ["CustomerReviews", "StarRating"]))
    review_count = to_int(get_nested(item, ["CustomerReviews", "Count"]))

    now = datetime.now(timezone.utc).isoformat()
    return {
        "asin": asin,
        "pages": pages,
        "title": title,
        "brand": brand,
        "cat": category or "Amazon Deals",
        "image": image,
        "price": price_display,
        "price_amount": price_amount,
        "currency": currency,
        "was": was_display,
        "savings": was_display,
        "pct": pct_off,
        "discount": discount_label,
        "deal_type": "Amazon API",
        "availability": availability,
        "link": url,
        "hot": pct_off >= 40,
        "hasCoupon": False,
        "couponDisplay": "",
        "rating": rating_value,
        "review_count": review_count,
        "desc": brand or "",
        "seen_at": now,
        "updated_at": now,
    }


def main():
    print("=" * 60)
    print("  Amazon Sales Event ASIN Feed")
    print("=" * 60)

    page_asins, asin_pages = load_page_asins()
    unique_asins = sorted(asin_pages.keys())
    amazon_items = get_amazon_items(unique_asins) if unique_asins else {}

    print("[3/3] Building Amazon Sales Event JSON feed...")
    deals = []
    skipped = []
    for asin in unique_asins:
        item = amazon_items.get(asin)
        if not item:
            skipped.append(asin)
            continue
        deal = build_deal(asin, item, asin_pages.get(asin, []))
        if deal:
            deals.append(deal)
        else:
            skipped.append(asin)

    deals.sort(key=lambda d: (len(d.get("pages", [])), d.get("pct", 0), d.get("price_amount", 0)), reverse=True)

    output = {
        "source": "Amazon Sales Event Google Sheet + Amazon API",
        "sheet_id": SHEET_ID,
        "partnerTag": PARTNER_TAG,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "count": len(deals),
        "totalDeals": len(deals),
        "pageCounts": {page: len(page_asins.get(page, [])) for page in PAGE_COLUMNS},
        "skippedAsins": skipped,
        "deals": deals,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"    Saved {len(deals)} deals to {OUTPUT_FILE}")
    if skipped:
        print(f"    Skipped {len(skipped)} ASINs with no usable Amazon pricing/result: {', '.join(skipped[:20])}")
    print("Done.")


if __name__ == "__main__":
    main()
