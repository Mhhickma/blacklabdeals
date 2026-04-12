import requests
import json
import os
import time

# ENV VARS
CLIENT_ID = os.getenv("CREATOR_CLIENT_ID")
CLIENT_SECRET = os.getenv("CREATOR_CLIENT_SECRET")
AFFILIATE_TAG = os.getenv("AFFILIATE_TAG")
KEEPA_API_KEY = os.getenv("KEEPA_API_KEY")

TOKEN_URL = "https://creatorsapi.auth.us-east-1.amazoncognito.com/oauth2/token"
CREATOR_URL = "https://creators-api-na.amazon.com/getitems"

MAX_DEALS = 150


# --------------------------
# 1. GET ACCESS TOKEN
# --------------------------
def get_access_token():
    print("[Auth] Getting token...")

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "creators::api"
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    r = requests.post(TOKEN_URL, data=payload, headers=headers)
    r.raise_for_status()

    token = r.json()["access_token"]

    print("[Auth] Token received")
    return token


# --------------------------
# 2. GET KEEPA ASINS
# --------------------------
def get_keepa_asins():
    print("[Keepa] Fetching deals...")

    url = "https://api.keepa.com/deal"
    params = {"key": KEEPA_API_KEY}

    body = {
        "domainId": 1,
        "priceTypes": [0],
        "deltaPercent": 10
    }

    r = requests.post(url, params=params, json=body)
    r.raise_for_status()

    deals = r.json()["deals"]["dr"]

    asins = [d["asin"] for d in deals[:MAX_DEALS]]

    print(f"[Keepa] Found {len(asins)} ASINs")
    return asins


# --------------------------
# 3. CALL CREATORS API
# --------------------------
def fetch_creator_data(asins, token):
    print("[Creator API] Fetching product data...")

    headers = {
        "Authorization": f"Bearer {token}, Version 2.1",
        "Content-Type": "application/json",
        "x-marketplace": "www.amazon.com"
    }

    payload = {
        "itemIds": asins,
        "itemIdType": "ASIN",
        "marketplace": "www.amazon.com",
        "partnerTag": AFFILIATE_TAG,
        "resources": [
            "images.primary.large",
            "itemInfo.title",
            "offersV2.listings.price"
        ]
    }

    r = requests.post(CREATOR_URL, headers=headers, json=payload)
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


# --------------------------
# 4. BUILD DEALS
# --------------------------
def build_deals():
    token = get_access_token()
    asins = get_keepa_asins()
    creator_data = fetch_creator_data(asins, token)

    deals = []

    for asin in asins:
        data = creator_data.get(asin)

        if not data:
            continue

        if not data["price"]:
            continue

        deals.append({
            "asin": asin,
            "title": data["title"],
            "image": data["image"],
            "price": data["price"],
            "link": f"https://www.amazon.com/dp/{asin}?tag={AFFILIATE_TAG}",
            "pct": 15
        })

    return deals


# --------------------------
# MAIN
# --------------------------
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

    with open("deals.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(deals)} deals")


if __name__ == "__main__":
    main()
