import requests
import json
import time
import os

KEEPA_API_KEY = os.getenv("KEEPA_API_KEY")
AFFILIATE_TAG = os.getenv("AFFILIATE_TAG")

CREATOR_API_KEY = os.getenv("CREATOR_API_KEY")
CREATOR_API_SECRET = os.getenv("CREATOR_API_SECRET")

CREATOR_ENDPOINT = "https://creators-api-na.amazon.com"

OUTPUT_FILE = "deals.json"

MAX_DEALS = 150
MIN_DISCOUNT = 10


# -------------------------------
# KEEP A SIMPLE KEEPA FETCH
# -------------------------------
def get_keepa_deals():
    print("[Keepa] Fetching deals...")

    url = "https://api.keepa.com/deal"
    params = {"key": KEEPA_API_KEY}

    body = {
        "domainId": 1,
        "priceTypes": [0],
        "deltaPercent": MIN_DISCOUNT,
    }

    r = requests.post(url, params=params, json=body)
    r.raise_for_status()

    deals = r.json().get("deals", {}).get("dr", [])
    asins = [d["asin"] for d in deals[:MAX_DEALS]]

    print(f"[Keepa] Found {len(asins)} ASINs")
    return asins


# -------------------------------
# CREATORS API CALL
# -------------------------------
def fetch_creator_data(asins):
    print("[Creator API] Fetching product data...")

    url = f"{CREATOR_ENDPOINT}/getitems"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {CREATOR_API_KEY}"
    }

    payload = {
        "itemIds": asins,
        "marketplace": "www.amazon.com",
        "partnerTag": AFFILIATE_TAG,
        "resources": [
            "images.primary.large",
            "itemInfo.title",
            "offersV2.listings.price"
        ]
    }

    r = requests.post(url, headers=headers, json=payload)
    r.raise_for_status()

    data = r.json()
    items = data.get("itemResults", {}).get("items", [])

    results = {}

    for item in items:
        asin = item.get("asin")

        title = item.get("itemInfo", {}).get("title", {}).get("displayValue", "")

        image = item.get("images", {}).get("primary", {}).get("large", {}).get("url", "")

        listings = item.get("offersV2", {}).get("listings", [])
        price = ""

        if listings:
            price = listings[0].get("price", {}).get("displayAmount", "")

        results[asin] = {
            "title": title,
            "image": image,
            "price": price
        }

    print(f"[Creator API] Got {len(results)} items")
    return results


# -------------------------------
# BUILD DEALS JSON
# -------------------------------
def build_deals():
    asins = get_keepa_deals()
    creator_data = fetch_creator_data(asins)

    deals = []

    for asin in asins:
        data = creator_data.get(asin)
        if not data:
            continue

        if not data["price"]:
            continue  # skip no price

        deals.append({
            "asin": asin,
            "title": data["title"],
            "image": data["image"],
            "price": data["price"],
            "link": f"https://www.amazon.com/dp/{asin}?tag={AFFILIATE_TAG}",
            "pct": 15  # placeholder until we re-add Keepa %
        })

        if len(deals) >= MAX_DEALS:
            break

    return deals


# -------------------------------
# MAIN
# -------------------------------
def main():
    print("Starting DealDrop fetch...")

    deals = build_deals()

    output = {
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totalDeals": len(deals),
        "hotDeals": len([d for d in deals if d["pct"] >= 30]),
        "couponDeals": 0,
        "deals": deals
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(deals)} deals")


if __name__ == "__main__":
    main()
