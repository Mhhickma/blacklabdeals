import csv
import io
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import quote

import requests

SHEET_ID = os.getenv("AMAZON_SALES_EVENT_SHEET_ID", "15kLY4nPg-ydq9kcqUcSTrzttQxtCkeXGoU2HoO4KTkc")
OUTPUT_FILE = os.getenv("AMAZON_SALES_EVENT_SAMPLE_OUTPUT", "amazon-sales-event-deals-sample.json")
PARTNER_TAG = os.getenv("AFFILIATE_TAG", "blacklabdealsprime-20")
SAMPLE_PER_CATEGORY = int(os.getenv("AMAZON_SALES_EVENT_SAMPLE_PER_CATEGORY", "5"))
HOUSEHOLD_LIMIT = int(os.getenv("AMAZON_SALES_EVENT_HOUSEHOLD_LIMIT", "100"))

TAB_PAGE_MAP = {
    "Electronics": "amazon-electronics-deals",
    "Furniture": "amazon-furniture-deals",
    "Health & Personal Care": "amazon-health-personal-care-deals",
    "Home": "amazon-home-deals",
    "Home Improvement": "amazon-home-improvement-deals",
    "Home Entertainment": "amazon-home-entertainment-deals",
    "Lawn and Garden": "amazon-lawn-garden-deals",
    "Office Products": "amazon-office-products-deals",
    "Outdoors": "amazon-outdoors-deals",
    "PC": "amazon-pc-deals",
    "Kitchen": "amazon-kitchen-deals",
    "Pet Products": "amazon-pet-products-deals",
    "Sports": "amazon-sports-deals",
    "Tools": "amazon-tool-deals",
    "Toys": "amazon-toys-deals",
    "Video Devices": "amazon-video-devices-deals",
    "Wireless": "amazon-wireless-deals",
}

ASIN_RE = re.compile(r"\b[A-Z0-9]{10}\b", re.IGNORECASE)
HOUSEHOLD_TERMS = (
    "toilet paper", "paper towel", "paper towels", "bath tissue", "tissues", "napkins",
    "laundry detergent", "detergent", "dryer sheets", "fabric softener",
    "dish soap", "dishwasher pods", "dishwasher detergent", "dishwasher tablets",
    "hand soap", "body wash", "bar soap", "shampoo", "conditioner",
    "trash bags", "garbage bags", "storage bags", "zip bags", "freezer bags",
    "cleaning wipes", "disinfecting wipes", "all purpose cleaner", "all-purpose cleaner",
    "bathroom cleaner", "kitchen cleaner", "glass cleaner", "floor cleaner",
    "sponges", "scrub sponge", "scrub brush", "air freshener",
    "foil", "plastic wrap", "parchment paper", "batteries", "aa batteries", "aaa batteries",
)
HOUSEHOLD_EXCLUDES = (
    "power tool battery", "drill battery", "lithium battery pack", "car battery", "marine battery",
    "solar battery", "trolling motor battery", "lifepo4", "inverter", "generator battery",
)
AMAZON_DEVICE_TERMS = ("echo", "fire tv", "kindle", "ring", "blink", "eero", "alexa", "amazon basics", "amazonbasics")


def clean_key(value):
    return str(value or "").strip().lower().replace(" ", "_").replace("-", "_")


def first(row, *keys):
    lookup = {clean_key(k): v for k, v in row.items()}
    for key in keys:
        value = str(lookup.get(clean_key(key), "") or "").strip()
        if value:
            return value
    return ""


def parse_float(value):
    text = str(value or "").replace("$", "").replace(",", "").replace("%", "").strip()
    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_percent(value):
    amount = parse_float(value)
    return amount * 100 if 0 < amount < 1 else amount


def extract_asin(*values):
    for value in values:
        match = ASIN_RE.search(str(value or ""))
        if match:
            return match.group(0).upper()
    return ""


def sheet_csv_url(tab_name):
    return f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={quote(tab_name)}"


def add_page(pages, page):
    if page not in pages:
        pages.append(page)


def row_text(row):
    return " ".join(str(v or "") for v in row.values()).lower()


def is_household(row):
    text = row_text(row)
    return any(term in text for term in HOUSEHOLD_TERMS) and not any(term in text for term in HOUSEHOLD_EXCLUDES)


def affiliate_url(asin, raw_url):
    url = str(raw_url or "").strip() or f"https://www.amazon.com/dp/{asin}"
    if url.startswith("www."):
        url = "https://" + url
    if "amazon." in url and "tag=" not in url:
        url = f"{url}{'&' if '?' in url else '?'}tag={PARTNER_TAG}"
    return url


def build_deal(row, tab_name, page_slug, now):
    text = row_text(row)
    asin = extract_asin(first(row, "asin"), first(row, "asin_url", "url"), text)
    if not asin:
        return None

    title = first(row, "asin_name", "title", "product_title", "name") or f"{tab_name} Deal {asin}"
    brand = first(row, "brand")
    category = first(row, "category_description", "category") or tab_name
    deal_price = parse_float(first(row, "deal_price", "price"))
    lowest_ytd = parse_float(first(row, "lowest_price_ytd"))
    discount_pct = round(parse_percent(first(row, "discount_pct", "discount_percent")))
    rating = parse_float(first(row, "star_rating", "rating")) or None
    price_band = first(row, "deal_price_band").lower()
    asin_url = first(row, "asin_url", "url")

    pages = ["amazon-deal-event", page_slug]
    if deal_price and deal_price <= 50:
        add_page(pages, "amazon-deals-under-50")
    if "under 50" in price_band or "under-$50" in price_band or "under $50" in price_band:
        add_page(pages, "amazon-deals-under-50")
    if any(term in text for term in AMAZON_DEVICE_TERMS) or brand.lower() in {"amazon", "amazon basics", "amazonbasics", "ring", "blink", "eero"}:
        add_page(pages, "amazon-device-deals")
    if is_household(row):
        add_page(pages, "amazon-household-essentials-deals")

    was_display = f"${lowest_ytd:.2f}" if lowest_ytd and deal_price and lowest_ytd > deal_price else ""
    return {
        "asin": asin,
        "pages": pages,
        "title": title,
        "brand": brand,
        "cat": category,
        "image": f"https://images-na.ssl-images-amazon.com/images/P/{asin}.01._SL160_.jpg",
        "price": f"${deal_price:.2f}" if deal_price else "See deal",
        "price_amount": deal_price,
        "was": was_display,
        "savings": was_display,
        "pct": discount_pct,
        "discount": f"-{discount_pct}%" if discount_pct else "",
        "deal_type": "SAMPLE_FROM_SHEET",
        "availability": "",
        "link": affiliate_url(asin, asin_url),
        "hot": discount_pct >= 30,
        "hasCoupon": False,
        "couponDisplay": "",
        "rating": rating,
        "review_count": None,
        "desc": brand,
        "seen_at": now,
        "updated_at": now,
    }


def iter_tab_rows(tab_name):
    response = requests.get(sheet_csv_url(tab_name), timeout=90)
    response.raise_for_status()
    yield from csv.DictReader(io.StringIO(response.content.decode("utf-8-sig")))


def merge_deal(deals_by_asin, deal):
    existing = deals_by_asin.get(deal["asin"])
    if existing:
        for page in deal["pages"]:
            add_page(existing["pages"], page)
        return existing
    deals_by_asin[deal["asin"]] = deal
    return deal


def main():
    now = datetime.now(timezone.utc).isoformat()
    deals_by_asin = {}
    sampled_tabs = {}
    household_added = 0

    # Pass 1: always pull the requested number of samples from every category tab.
    for tab_name, page_slug in TAB_PAGE_MAP.items():
        sampled_tabs[tab_name] = 0
        for row in iter_tab_rows(tab_name):
            deal = build_deal(row, tab_name, page_slug, now)
            if not deal:
                continue
            merge_deal(deals_by_asin, deal)
            sampled_tabs[tab_name] += 1
            if sampled_tabs[tab_name] >= SAMPLE_PER_CATEGORY:
                break

    # Pass 2: scan across all tabs for household essentials until the household page has 100 unique items.
    household_asins = {
        asin for asin, deal in deals_by_asin.items()
        if "amazon-household-essentials-deals" in deal.get("pages", [])
    }
    for tab_name, page_slug in TAB_PAGE_MAP.items():
        if len(household_asins) >= HOUSEHOLD_LIMIT:
            break
        for row in iter_tab_rows(tab_name):
            if not is_household(row):
                continue
            deal = build_deal(row, tab_name, page_slug, now)
            if not deal:
                continue
            merged = merge_deal(deals_by_asin, deal)
            add_page(merged["pages"], "amazon-household-essentials-deals")
            household_asins.add(deal["asin"])
            if len(household_asins) >= HOUSEHOLD_LIMIT:
                break

    household_added = len(household_asins)
    deals = list(deals_by_asin.values())
    page_counts = {}
    for deal in deals:
        for page in deal["pages"]:
            page_counts[page] = page_counts.get(page, 0) + 1

    output = {
        "source": "5-ASIN category sample plus household essentials scan from Google Sheet tabs",
        "sheet_id": SHEET_ID,
        "partnerTag": PARTNER_TAG,
        "sampleMode": True,
        "samplePerCategory": SAMPLE_PER_CATEGORY,
        "householdScanCount": HOUSEHOLD_LIMIT,
        "householdAdded": household_added,
        "updatedAt": now,
        "count": len(deals),
        "totalDeals": len(deals),
        "pageCounts": page_counts,
        "sampledTabs": sampled_tabs,
        "tabPageMap": TAB_PAGE_MAP,
        "deals": deals,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(deals)} product cards to {OUTPUT_FILE}")
    print(f"Household essentials on page: {household_added}")
    print(json.dumps(page_counts, indent=2))


if __name__ == "__main__":
    main()
