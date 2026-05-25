import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

KEEPA_API_URL = "https://api.keepa.com/product"
SHEET_ID = os.getenv("AMAZON_SALES_EVENT_SHEET_ID", "15kLY4nPg-ydq9kcqUcSTrzttQxtCkeXGoU2HoO4KTkc")
KEEPA_DOMAIN = int(os.getenv("KEEPA_DOMAIN", "1"))  # 1 = Amazon US
KEEPA_STATS_DAYS = int(os.getenv("KEEPA_STATS_DAYS", "90"))
BATCH_SIZE = int(os.getenv("KEEPA_BATCH_SIZE", "10"))
REQUEST_DELAY_SECONDS = float(os.getenv("KEEPA_REQUEST_DELAY_SECONDS", "1"))
ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")

PRICE_TYPE_AMAZON = 0
PRICE_TYPE_NEW = 1
PRICE_TYPE_SALES_RANK = 3
PRICE_TYPE_BUY_BOX = 10

KEEPA_OUTPUT_COLUMNS = [
    "keepa_current_sales_rank",
    "keepa_avg30_sales_rank",
    "keepa_avg90_sales_rank",
    "keepa_rank_improvement_90",
    "keepa_current_buy_box_price",
    "keepa_current_amazon_price",
    "keepa_current_new_price",
    "keepa_avg30_buy_box_price",
    "keepa_avg90_buy_box_price",
    "keepa_rating",
    "keepa_review_count",
    "keepa_monthly_sold",
    "keepa_category",
    "keepa_last_checked",
    "keepa_deal_score",
]

DEFAULT_TABS = [
    "Electronics",
    "Furniture",
    "Health & Personal Care",
    "Home",
    "Home Improvement",
    "Home Entertainment",
    "Lawn and Garden",
    "Office Products",
    "Outdoors",
    "PC",
    "Kitchen",
    "Pet Products",
    "Sports",
    "Tools",
    "Toys",
    "Video Devices",
    "Wireless",
]


def chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def clean_header(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace("-", "_")


def parse_number(value: Any) -> float:
    text = str(value or "").replace("$", "").replace(",", "").replace("%", "").strip()
    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_percent(value: Any) -> float:
    number = parse_number(value)
    return number * 100 if 0 < number < 1 else number


def cents_to_dollars(value: Any) -> Optional[float]:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return round(number / 100, 2)


def safe_list_get(values: Any, index: int) -> Optional[Any]:
    if isinstance(values, list) and 0 <= index < len(values):
        value = values[index]
        if value not in (-1, None, ""):
            return value
    return None


def rating_to_stars(value: Any) -> Optional[float]:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return round(number / 10, 1)


def best_sales_rank(product: Dict[str, Any], stats: Dict[str, Any]) -> Optional[int]:
    current_rank = safe_list_get(stats.get("current"), PRICE_TYPE_SALES_RANK)
    if current_rank:
        return int(current_rank)
    sales_ranks = product.get("salesRanks") or {}
    best = None
    for history in sales_ranks.values():
        if isinstance(history, list) and len(history) >= 2:
            rank = history[-1]
            if isinstance(rank, int) and rank > 0:
                best = rank if best is None else min(best, rank)
    return best


def sales_rank_average(values: Any) -> Optional[int]:
    rank = safe_list_get(values, PRICE_TYPE_SALES_RANK)
    return int(rank) if rank else None


def category_name(product: Dict[str, Any]) -> str:
    tree = product.get("categoryTree") or []
    if tree and isinstance(tree, list):
        last = tree[-1]
        if isinstance(last, dict) and last.get("name"):
            return str(last["name"])
    return str(product.get("rootCategory") or "")


def extract_product_summary(product: Dict[str, Any]) -> Dict[str, Any]:
    stats = product.get("stats") or {}
    current = stats.get("current") or []
    avg30 = stats.get("avg30") or []
    avg90 = stats.get("avg90") or []

    current_sales_rank = best_sales_rank(product, stats)
    avg30_sales_rank = sales_rank_average(avg30)
    avg90_sales_rank = sales_rank_average(avg90)
    rank_improvement_90 = None
    if current_sales_rank and avg90_sales_rank:
        rank_improvement_90 = avg90_sales_rank - current_sales_rank

    return {
        "asin": product.get("asin"),
        "keepa_current_sales_rank": current_sales_rank,
        "keepa_avg30_sales_rank": avg30_sales_rank,
        "keepa_avg90_sales_rank": avg90_sales_rank,
        "keepa_rank_improvement_90": rank_improvement_90,
        "keepa_current_buy_box_price": cents_to_dollars(safe_list_get(current, PRICE_TYPE_BUY_BOX)),
        "keepa_current_amazon_price": cents_to_dollars(safe_list_get(current, PRICE_TYPE_AMAZON)),
        "keepa_current_new_price": cents_to_dollars(safe_list_get(current, PRICE_TYPE_NEW)),
        "keepa_avg30_buy_box_price": cents_to_dollars(safe_list_get(avg30, PRICE_TYPE_BUY_BOX)),
        "keepa_avg90_buy_box_price": cents_to_dollars(safe_list_get(avg90, PRICE_TYPE_BUY_BOX)),
        "keepa_rating": rating_to_stars(stats.get("rating") or product.get("rating")),
        "keepa_review_count": stats.get("reviewCount") or product.get("reviewCount"),
        "keepa_monthly_sold": product.get("monthlySold"),
        "keepa_category": category_name(product),
    }


def lookup_keepa(asins: List[str], api_key: str) -> Tuple[Dict[str, Dict[str, Any]], Optional[int]]:
    results: Dict[str, Dict[str, Any]] = {}
    tokens_left = None
    for batch in chunks(asins, BATCH_SIZE):
        params = {
            "key": api_key,
            "domain": KEEPA_DOMAIN,
            "asin": ",".join(batch),
            "stats": KEEPA_STATS_DAYS,
            "history": 0,
            "rating": 1,
        }
        response = requests.get(KEEPA_API_URL, params=params, timeout=90)
        response.raise_for_status()
        payload = response.json()
        if payload.get("error"):
            raise RuntimeError(payload["error"])
        tokens_left = payload.get("tokensLeft")
        for product in payload.get("products") or []:
            summary = extract_product_summary(product)
            asin = str(summary.get("asin") or "").upper()
            if asin:
                results[asin] = summary
        print(f"Looked up {len(batch)} ASINs | tokens left: {tokens_left}")
        time.sleep(REQUEST_DELAY_SECONDS)
    return results, tokens_left


def deal_score(row_data: Dict[str, Any], keepa_data: Dict[str, Any]) -> int:
    score = 0
    discount = parse_percent(row_data.get("discount_pct"))
    deal_price = parse_number(row_data.get("deal_price"))
    ytd_low = parse_number(row_data.get("lowest_price_ytd"))
    rating = keepa_data.get("keepa_rating") or 0
    reviews = keepa_data.get("keepa_review_count") or 0
    rank = keepa_data.get("keepa_current_sales_rank") or 0
    rank_improvement = keepa_data.get("keepa_rank_improvement_90") or 0

    if deal_price and ytd_low and deal_price <= ytd_low:
        score += 25
    if discount >= 50:
        score += 30
    elif discount >= 40:
        score += 25
    elif discount >= 30:
        score += 20
    elif discount >= 25:
        score += 12
    elif discount >= 15:
        score += 6

    if rank:
        if rank <= 1000:
            score += 35
        elif rank <= 5000:
            score += 28
        elif rank <= 10000:
            score += 22
        elif rank <= 25000:
            score += 14
        elif rank <= 50000:
            score += 8

    if rank_improvement and rank_improvement > 0:
        score += min(15, int(rank_improvement / 1000))
    if rating >= 4.5:
        score += 12
    elif rating >= 4.2:
        score += 8
    elif rating >= 4.0:
        score += 4
    if reviews >= 5000:
        score += 12
    elif reviews >= 1000:
        score += 9
    elif reviews >= 250:
        score += 5
    if deal_price and deal_price <= 50:
        score += 8
    return int(score)


def authorize_sheet() -> gspread.Client:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise SystemExit("Missing GOOGLE_SERVICE_ACCOUNT_JSON secret.")
    info = json.loads(raw)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(credentials)


def ensure_columns(worksheet: gspread.Worksheet, headers: List[str]) -> Dict[str, int]:
    normalized = {clean_header(header): i + 1 for i, header in enumerate(headers)}
    next_col = len(headers) + 1
    cells_to_update = []
    for column_name in KEEPA_OUTPUT_COLUMNS:
        key = clean_header(column_name)
        if key not in normalized:
            normalized[key] = next_col
            cells_to_update.append({"range": rowcol_to_a1(1, next_col), "values": [[column_name]]})
            next_col += 1
    if cells_to_update:
        worksheet.batch_update(cells_to_update, value_input_option="USER_ENTERED")
        print(f"Added {len(cells_to_update)} Keepa columns to {worksheet.title}")
    return normalized


def requested_tabs() -> List[str]:
    raw = os.getenv("KEEPA_TABS", "Tools")
    if raw.strip().lower() in {"all", "*"}:
        return DEFAULT_TABS
    return [tab.strip() for tab in raw.split(",") if tab.strip()]


def enrich_tab(spreadsheet: gspread.Spreadsheet, tab: str, api_key: str, limit: int, start_row: int, dry_run: bool) -> None:
    worksheet = spreadsheet.worksheet(tab)
    headers = worksheet.row_values(1)
    header_map = ensure_columns(worksheet, headers)
    asin_col = header_map.get("asin")
    if not asin_col:
        raise RuntimeError(f"No asin column found in tab {tab}")

    end_row = start_row + limit - 1
    read_range = f"A{start_row}:ZZ{end_row}"
    rows = worksheet.get(read_range)
    records = []
    asins = []
    for offset, values in enumerate(rows):
        row_number = start_row + offset
        asin = values[asin_col - 1].strip().upper() if len(values) >= asin_col else ""
        if not ASIN_RE.fullmatch(asin):
            continue
        row_data = {clean_header(headers[i]): values[i] if i < len(values) else "" for i in range(len(headers))}
        records.append((row_number, asin, row_data))
        asins.append(asin)

    # Preserve order while removing duplicates.
    unique_asins = list(dict.fromkeys(asins))
    print(f"{tab}: Found {len(unique_asins)} unique ASINs from rows {start_row}-{end_row}")
    if not unique_asins:
        return

    keepa_results, tokens_left = lookup_keepa(unique_asins, api_key)
    checked_at = datetime.now(timezone.utc).isoformat()
    updates = []
    preview = []
    for row_number, asin, row_data in records:
        data = keepa_results.get(asin)
        if not data:
            continue
        data = dict(data)
        data["keepa_last_checked"] = checked_at
        data["keepa_deal_score"] = deal_score(row_data, data)
        preview.append({"row": row_number, **data})
        for column_name in KEEPA_OUTPUT_COLUMNS:
            col = header_map[clean_header(column_name)]
            value = data.get(column_name)
            updates.append({"range": rowcol_to_a1(row_number, col), "values": [["" if value is None else value]]})

    print(json.dumps(preview[:5], indent=2, ensure_ascii=False))
    print(f"{tab}: Prepared {len(updates)} cell updates. Tokens left: {tokens_left}")
    if dry_run:
        print(f"{tab}: DRY RUN enabled. No sheet cells were updated.")
        return
    if updates:
        worksheet.batch_update(updates, value_input_option="USER_ENTERED")
        print(f"{tab}: Wrote Keepa data for {len(preview)} rows.")


def main() -> None:
    api_key = os.getenv("KEEPA_API_KEY")
    if not api_key:
        raise SystemExit("Missing KEEPA_API_KEY secret.")
    limit = int(os.getenv("KEEPA_LIMIT", "20"))
    start_row = int(os.getenv("KEEPA_START_ROW", "2"))
    dry_run = os.getenv("KEEPA_DRY_RUN", "true").strip().lower() in {"1", "true", "yes", "y"}

    client = authorize_sheet()
    spreadsheet = client.open_by_key(SHEET_ID)
    print(f"Opened sheet: {spreadsheet.title}")
    print(f"Tabs: {requested_tabs()} | limit={limit} | start_row={start_row} | dry_run={dry_run}")
    for tab in requested_tabs():
        enrich_tab(spreadsheet, tab, api_key, limit, start_row, dry_run)


if __name__ == "__main__":
    main()
