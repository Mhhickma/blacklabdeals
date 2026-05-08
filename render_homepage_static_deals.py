"""Render crawlable homepage deal cards from deals.json.

The homepage still refreshes with JavaScript in the browser. This script gives
search engines and no-JavaScript visitors the first batch of live deals directly
in index.html after each automated deal refresh.
"""

from __future__ import annotations

import html
import json
import re
from pathlib import Path

INDEX_FILE = Path("index.html")
DEALS_FILE = Path("deals.json")
DEALS_LIMIT = 50
START_MARKER = "<!-- BLD STATIC DEALS START -->"
END_MARKER = "<!-- BLD STATIC DEALS END -->"


def esc(value: object) -> str:
    return html.escape(str(value or ""), quote=True)


def deal_image(deal: dict) -> str:
    image = deal.get("image")
    title = deal.get("title") or "Amazon deal"
    if image:
        return f'<img src="{esc(image)}" alt="{esc(title)}" loading="lazy">'
    return '<span class="img-fallback">Deal image unavailable</span>'


def deal_card(deal: dict) -> str:
    title = esc(deal.get("title") or "Amazon Deal")
    link = esc(deal.get("link") or "#")
    category = esc(deal.get("cat") or "Amazon Deals")
    description = esc(deal.get("desc") or deal.get("brand") or "")
    price = esc(deal.get("price") or "See price on Amazon")
    was = deal.get("was")
    discount = esc(deal.get("discount") or "")
    pct = int(float(deal.get("pct") or 0))
    is_hot = bool(deal.get("hot")) or pct >= 40
    has_coupon = bool(deal.get("hasCoupon"))
    coupon_text = esc(deal.get("couponDisplay") or "Coupon available")

    hot_badge = '<span class="card-badge-hot">Hot</span>' if is_hot else ""
    was_html = f'<span class="price-was">{esc(was)}</span>' if was else ""
    coupon_html = f'<span class="coupon-badge">{coupon_text}</span>' if has_coupon else ""

    return f'''<a class="deal-card" href="{link}" target="_blank" rel="noopener sponsored">
        <div class="card-img">{deal_image(deal)}</div>
        <div class="card-body">
          <div class="card-meta">
            <span class="card-category">{category}</span>
            {hot_badge}
          </div>
          <div class="card-title">{title}</div>
          <div class="card-desc">{description}</div>
          <div class="card-footer">
            <div class="price-block">
              <span class="price-now">{price}</span>
              {was_html}
            </div>
            <span class="discount-badge">{discount}</span>
          </div>
          {coupon_html}
          <button class="btn-deal">See Deal on Amazon &rarr;</button>
        </div>
      </a>'''


def render_static_deals(deals: list[dict]) -> str:
    cards = "\n      ".join(deal_card(deal) for deal in deals[:DEALS_LIMIT])
    return f"{START_MARKER}\n      {cards}\n      {END_MARKER}"


def replace_static_block(index_html: str, static_block: str) -> str:
    if START_MARKER in index_html and END_MARKER in index_html:
        return re.sub(
            rf"{re.escape(START_MARKER)}.*?{re.escape(END_MARKER)}",
            static_block,
            index_html,
            flags=re.DOTALL,
        )

    empty_grid = '<div class="deals-grid" id="deals-grid"></div>'
    populated_grid = f'<div class="deals-grid" id="deals-grid">\n      {static_block}\n    </div>'
    if empty_grid not in index_html:
        raise RuntimeError("Could not find empty homepage deals grid")
    return index_html.replace(empty_grid, populated_grid, 1)


def main() -> None:
    if not INDEX_FILE.exists():
        raise RuntimeError("Missing index.html")
    if not DEALS_FILE.exists():
        raise RuntimeError("Missing deals.json")

    data = json.loads(DEALS_FILE.read_text(encoding="utf-8"))
    deals = data.get("deals") if isinstance(data, dict) else data
    if not isinstance(deals, list):
        raise RuntimeError("deals.json does not contain a deals list")

    index_html = INDEX_FILE.read_text(encoding="utf-8")
    index_html = index_html.replace(
        '<section class="deals-section" id="deals-section" style="display:none;">',
        '<section class="deals-section" id="deals-section">',
        1,
    )
    index_html = replace_static_block(index_html, render_static_deals(deals))

    label = f"Showing {min(DEALS_LIMIT, len(deals))} of {len(deals)} deals"
    index_html = re.sub(
        r'(<span class="deal-count" id="count-label">)(.*?)(</span>)',
        rf"\1{esc(label)}\3",
        index_html,
        count=1,
        flags=re.DOTALL,
    )

    INDEX_FILE.write_text(index_html, encoding="utf-8")
    print(f"Rendered {min(DEALS_LIMIT, len(deals))} static homepage deals into index.html")


if __name__ == "__main__":
    main()
