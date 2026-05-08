"""Render crawlable deal cards from deals.json.

The pages still refresh with JavaScript in the browser. This script gives
search engines and no-JavaScript visitors the first batch of live deals directly
in key HTML pages after each automated deal refresh.
"""

from __future__ import annotations

import html
import json
import re
from pathlib import Path

DEALS_FILE = Path("deals.json")
DEALS_LIMIT = 50
HOME_START_MARKER = "<!-- BLD STATIC DEALS START -->"
HOME_END_MARKER = "<!-- BLD STATIC DEALS END -->"
TOOLS_START_MARKER = "<!-- BLD STATIC TOOL DEALS START -->"
TOOLS_END_MARKER = "<!-- BLD STATIC TOOL DEALS END -->"


def esc(value: object) -> str:
    return html.escape(str(value or ""), quote=True)


def money(value: object) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, str):
        return value
    try:
        return f"${float(value):.2f}"
    except (TypeError, ValueError):
        return str(value)


def pct(deal: dict) -> int:
    try:
        return int(float(deal.get("pct") or 0))
    except (TypeError, ValueError):
        return 0


def is_hot(deal: dict) -> bool:
    return bool(deal.get("hot")) or pct(deal) >= 40


def is_tool_deal(deal: dict) -> bool:
    category = str(deal.get("cat") or "").lower()
    title = str(deal.get("title") or "").lower()
    return "tool" in category or "home improvement" in category or "tool" in title


def deal_image(deal: dict, class_name: str = "") -> str:
    image = deal.get("image")
    title = deal.get("title") or "Amazon deal"
    class_attr = f' class="{class_name}"' if class_name else ""
    if image:
        return f'<img src="{esc(image)}" alt="{esc(title)}" loading="lazy"{class_attr}>'
    return '<span class="img-fallback">Deal image unavailable</span>'


def homepage_deal_card(deal: dict) -> str:
    title = esc(deal.get("title") or "Amazon Deal")
    link = esc(deal.get("link") or "#")
    category = esc(deal.get("cat") or "Amazon Deals")
    description = esc(deal.get("desc") or deal.get("brand") or "")
    price = esc(deal.get("price") or "See price on Amazon")
    was = deal.get("was")
    discount = esc(deal.get("discount") or "")
    has_coupon = bool(deal.get("hasCoupon"))
    coupon_text = esc(deal.get("couponDisplay") or "Coupon available")

    hot_badge = '<span class="card-badge-hot">Hot</span>' if is_hot(deal) else ""
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


def category_deal_card(deal: dict, rank: int | None = None) -> str:
    title = esc(deal.get("title") or "Amazon Deal")
    link = esc(deal.get("link") or "#")
    category = esc(deal.get("cat") or "Amazon Deals")
    brand = esc(str(deal.get("brand") or "")[:24])
    price = esc(deal.get("price") or money(deal.get("price_amount")) or "See deal")
    was = deal.get("was")
    discount = pct(deal)
    badge = "Hot Deal" if is_hot(deal) else "Coupon" if deal.get("hasCoupon") else "Deal"
    rank_badge = f'<div class="rank-badge">#{rank}</div>' if rank else ""
    was_html = f'<span class="hot-price-was">{esc(was)}</span>' if was else ""
    off_html = f'<span class="hot-off">{discount}% off</span>' if discount else ""

    return f'''<a class="hot-card" href="{link}" target="_blank" rel="nofollow sponsored noopener" data-asin="{esc(deal.get('asin') or '')}" data-deal-title="{title}" data-deal-category="{category}" data-deal-price="{esc(deal.get('price_amount') or '')}" data-deal-discount="{discount}">
        <div class="hot-card-img">{deal_image(deal)}{rank_badge}<div class="hot-card-badge">{badge}</div></div>
        <div class="hot-card-body">
          <div class="category-pill">{category}</div>
          <div class="stars">{'★★★★★' if is_hot(deal) else '★★★★☆'} {brand}</div>
          <div class="hot-card-title">{title}</div>
          <div class="hot-card-prices"><span class="hot-price-now">{price}</span>{was_html}{off_html}</div>
          <span class="hot-btn">See Deal on Amazon &rarr;</span>
        </div>
      </a>'''


def render_block(deals: list[dict], start_marker: str, end_marker: str, card_renderer) -> str:
    cards = "\n      ".join(card_renderer(deal) for deal in deals[:DEALS_LIMIT])
    return f"{start_marker}\n      {cards}\n      {end_marker}"


def replace_between_markers(page_html: str, start_marker: str, end_marker: str, block: str) -> str | None:
    if start_marker in page_html and end_marker in page_html:
        return re.sub(
            rf"{re.escape(start_marker)}.*?{re.escape(end_marker)}",
            block,
            page_html,
            flags=re.DOTALL,
        )
    return None


def render_homepage(deals: list[dict]) -> None:
    path = Path("index.html")
    page_html = path.read_text(encoding="utf-8")
    page_html = page_html.replace(
        '<section class="deals-section" id="deals-section" style="display:none;">',
        '<section class="deals-section" id="deals-section">',
        1,
    )

    block = render_block(deals, HOME_START_MARKER, HOME_END_MARKER, homepage_deal_card)
    replaced = replace_between_markers(page_html, HOME_START_MARKER, HOME_END_MARKER, block)
    if replaced is not None:
        page_html = replaced
    else:
        empty_grid = '<div class="deals-grid" id="deals-grid"></div>'
        populated_grid = f'<div class="deals-grid" id="deals-grid">\n      {block}\n    </div>'
        if empty_grid not in page_html:
            raise RuntimeError("Could not find empty homepage deals grid")
        page_html = page_html.replace(empty_grid, populated_grid, 1)

    label = f"Showing {min(DEALS_LIMIT, len(deals))} of {len(deals)} deals"
    page_html = re.sub(
        r'(<span class="deal-count" id="count-label">)(.*?)(</span>)',
        rf"\1{esc(label)}\3",
        page_html,
        count=1,
        flags=re.DOTALL,
    )
    path.write_text(page_html, encoding="utf-8")
    print(f"Rendered {min(DEALS_LIMIT, len(deals))} static homepage deals into index.html")


def render_tools_page(deals: list[dict]) -> None:
    path = Path("best-amazon-tool-deals/index.html")
    page_html = path.read_text(encoding="utf-8")
    tool_deals = [deal for deal in deals if is_tool_deal(deal)]

    def card_with_rank(item: tuple[int, dict]) -> str:
        rank, deal = item
        return category_deal_card(deal, rank=rank)

    ranked_deals = list(enumerate(tool_deals[:DEALS_LIMIT], start=1))
    cards = "\n      ".join(card_with_rank(item) for item in ranked_deals)
    block = f"{TOOLS_START_MARKER}\n      {cards}\n      {TOOLS_END_MARKER}"

    replaced = replace_between_markers(page_html, TOOLS_START_MARKER, TOOLS_END_MARKER, block)
    if replaced is not None:
        page_html = replaced
    else:
        empty_grid = '<div class="hot-grid" id="hot-grid"></div>'
        populated_grid = f'<div class="hot-grid" id="hot-grid">\n      {block}\n    </div>'
        if empty_grid not in page_html:
            raise RuntimeError("Could not find empty tools page deals grid")
        page_html = page_html.replace(empty_grid, populated_grid, 1)

    page_html = re.sub(
        r'(<div class="deal-count" id="deal-count">)(.*?)(</div>)',
        rf"\1{min(DEALS_LIMIT, len(tool_deals))} of {len(tool_deals)} deals\3",
        page_html,
        count=1,
        flags=re.DOTALL,
    )
    page_html = re.sub(
        r'(<div class="status-line" id="status-line">)(.*?)(</div>)',
        r"\1Showing live tool deals from the Black Lab Deals feed.\3",
        page_html,
        count=1,
        flags=re.DOTALL,
    )
    path.write_text(page_html, encoding="utf-8")
    print(f"Rendered {min(DEALS_LIMIT, len(tool_deals))} static tool deals into {path}")


def main() -> None:
    if not DEALS_FILE.exists():
        raise RuntimeError("Missing deals.json")

    data = json.loads(DEALS_FILE.read_text(encoding="utf-8"))
    deals = data.get("deals") if isinstance(data, dict) else data
    if not isinstance(deals, list):
        raise RuntimeError("deals.json does not contain a deals list")

    render_homepage(deals)
    render_tools_page(deals)


if __name__ == "__main__":
    main()
