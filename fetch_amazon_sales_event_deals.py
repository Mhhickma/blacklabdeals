"""
Amazon Sales Event ASIN Feed + Amazon Creators API Enrichment
-------------------------------------------------------------
This is a separate event-only process. It does not use Keepa and does not
modify the regular deals.json workflow.

Source:
- Google Sheet columns are page slugs, such as amazon-tool-deals.
- Cells under each column contain ASINs for that page.

Output:
- amazon-sales-event-deals.json
"""

import csv
import io
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from amazon_creatorsapi import AmazonCreatorsApi, Country
from amazon_creatorsapi.models import GetItemsResource


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
PARTNER_TAG = os.getenv("AFFILIATE_TAG", "sawdustsavings-20")
CREDENTIAL_ID = os.getenv("CREATORS_CREDENTIAL_ID")
CREDENTIAL_SECRET = os.getenv("CREATORS_CREDENTIAL_SECRET")
AMAZON_BATCH_SIZE = int(os.getenv("AMAZON_SALES_EVENT_BATCH_SIZE", "10"))
AMAZON_CONCURRENT_BATCHES = int(os.getenv("AMAZON_SALES_EVENT_CONCURRENT_BATCHES", "3"))
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


# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def compact_image_url(url, size=160):
    if not url:
        return None
    return re.sub(r"\._SL\d+_\.", f"._SL{size}_.", str(url))


def clean_header(value):
    return str(value or "").strip().lower().replace(" ", "-").replace("_", "-")


def extract_asins(value):
    return [m.group(0).upper() for m in ASIN_RE.finditer(str(value or ""))]


def get_amazon_resources():
    return [
        GetItemsResource.ITEM_INFO_DOT_TITLE,
        GetItemsResource.ITEM_INFO_DOT_BY_LINE_INFO,
        GetItemsResource.ITEM_INFO_DOT_CLASSIFICATIONS,
        GetItemsResource.IMAGES_DOT_PRIMARY_DOT_LARGE,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_PRICE,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_AVAILABILITY,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_CONDITION,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_IS_BUY_BOX_WINNER,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_DEAL_DETAILS,
    ]


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
# STEP 2: FETCH AMAZON CREATORS API DATA
# --------------------------------------------------
def fetch_amazon_batch(batch, batch_num, total_batches):
    print(f"    Batch {batch_num}/{total_batches} ({len(batch)} ASINs)...")
    amazon = AmazonCreatorsApi(
        credential_id=CREDENTIAL_ID,
        credential_secret=CREDENTIAL_SECRET,
        version="3.1",
        tag=PARTNER_TAG,
        country=Country.US,
    )
    items = amazon.get_items(batch, resources=get_amazon_resources())
    return batch_num, items


def get_amazon_items(asins):
    print("[2/3] Fetching live Amazon product data...")
    if not CREDENTIAL_ID or not CREDENTIAL_SECRET:
        raise RuntimeError("Missing CREATORS_CREDENTIAL_ID or CREATORS_CREDENTIAL_SECRET")

    batches = [asins[i:i + AMAZON_BATCH_SIZE] for i in range(0, len(asins), AMAZON_BATCH_SIZE)]
    total_batches = len(batches)
    worker_count = max(1, min(AMAZON_CONCURRENT_BATCHES, total_batches))
    all_items = {}

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = []
        for idx, batch in enumerate(batches, start=1):
            futures.append(executor.submit(fetch_amazon_batch, batch, idx, total_batches))
            if AMAZON_REQUEST_DELAY_SECONDS > 0:
                time.sleep(AMAZON_REQUEST_DELAY_SECONDS)

        for future in as_completed(futures):
            try:
                _, items = future.result()
                for item in items:
                    all_items[item.asin] = item
            except Exception as exc:
                print(f"    Warning: Amazon batch failed - {exc}")

    print(f"    Retrieved {len(all_items)} products from Amazon Creators API.")
    return all_items


# --------------------------------------------------
# STEP 3: BUILD EVENT DEAL FEED
# --------------------------------------------------
def build_deal(asin, item, pages):
    try:
        title = item.item_info.title.display_value
    except Exception:
        title = None

    if not title:
        return None

    try:
        brand = item.item_info.by_line_info.brand.display_value
    except Exception:
        brand = None

    try:
        category = item.item_info.classifications.product_group.display_value
    except Exception:
        category = "Amazon Deals"

    try:
        image = compact_image_url(item.images.primary.large.url)
    except Exception:
        image = None

    try:
        listing = item.offers_v2.listings[0]
        price_amount = listing.price.money.amount
        price_display = listing.price.money.display_amount
        currency = listing.price.money.currency
    except Exception:
        listing = None
        price_amount = None
        price_display = None
        currency = None

    if not price_amount:
        return None

    try:
        condition = listing.condition.value
        if condition and condition.lower() != "new":
            return None
    except Exception:
        pass

    try:
        availability = listing.availability.type
    except Exception:
        availability = None

    try:
        deal_type = listing.deal_details.access_type
    except Exception:
        deal_type = "PRICE_DROP"

    try:
        url = item.detail_page_url
    except Exception:
        url = f"https://www.amazon.com/dp/{asin}?tag={PARTNER_TAG}"

    pct_off = 0
    was_display = None
    discount_label = ""
    is_hot = False

    try:
        savings = listing.price.savings
        if savings:
            pct_off = round(savings.percentage)
            was_display = f"${round(price_amount + savings.money.amount, 2)}"
            discount_label = f"-{pct_off}%"
            is_hot = pct_off >= 40
    except Exception:
        pass

    has_coupon = False
    coupon_display = ""
    try:
        deal_details = listing.deal_details
        if deal_details:
            dtype = str(getattr(deal_details, "type", "") or "").upper()
            damt = getattr(deal_details, "amount", None)
            dpct = getattr(deal_details, "percentage", None)
            if "PERCENT" in dtype and dpct:
                has_coupon = True
                coupon_display = f"Save extra {int(dpct)}%"
            elif damt:
                has_coupon = True
                coupon_display = f"Save extra ${float(damt):.0f}"
    except Exception:
        pass

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
        "deal_type": deal_type,
        "availability": availability,
        "link": url,
        "hot": is_hot,
        "hasCoupon": has_coupon,
        "couponDisplay": coupon_display,
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
        "source": "Amazon Sales Event Google Sheet",
        "sheet_id": SHEET_ID,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "count": len(deals),
        "totalDeals": len(deals),
        "pageCounts": {page: len(page_asins.get(page, [])) for page in PAGE_COLUMNS},
        "skippedAsins": skipped,
        "deals": deals,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"    Saved {len(deals)} deals to {OUTPUT_FILE}")
    if skipped:
        print(f"    Skipped {len(skipped)} ASINs with no usable Amazon pricing/result.")
    print("Done.")


if __name__ == "__main__":
    main()
