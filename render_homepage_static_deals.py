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
HOMEPAGE_CLS_CSS = (
    '#site-header{display:block;min-height:176px;background:var(--surface);}'
    '@media(max-width:760px){#site-header{min-height:154px}.deal-statement-bar{display:none!important}'
    'main.page-shell{display:flex!important;flex-direction:column!important;gap:0!important;padding:0 16px 70px!important}'
    '.mobile-deal-nav{min-height:49px}.popular-category-nav,.stats-bar,.divider{display:none!important}'
    'main.page-shell>.hero{order:1!important}main.page-shell>.mobile-deal-nav{order:2!important}'
    'main.page-shell>.loading-bar,main.page-shell>.error-msg{order:3!important}'
    'main.page-shell>.hot-section,main.page-shell>#hot-section{order:4!important}'
    'main.page-shell>.toolbar,main.page-shell>#toolbar{order:5!important}'
    'main.page-shell>.filters-wrap,main.page-shell>#filters-wrap{order:6!important}'
    'main.page-shell>#deals-section{order:7!important}'
    '#hot-section[style*="display:none"]{display:block!important;visibility:hidden;min-height:870px}'
    '.hero{padding:12px 0 10px!important}.hero-text h1,.hero h1{font-size:25px!important;line-height:1.05!important;margin-bottom:0!important}'
    '.hero-text p,.hero p,.hero-text p:nth-of-type(2){display:none!important}'
    '.hot-card,.deal-card{transition:none!important}.hot-card:hover,.deal-card:hover{transform:none!important;box-shadow:var(--shadow)!important}}'
    '@media(max-width:380px){#hot-section[style*="display:none"]{min-height:802px}}'
    '@media(prefers-reduced-motion:reduce){*,*::before,*::after{animation-duration:.001ms!important;animation-iteration-count:1!important;transition-duration:.001ms!important;scroll-behavior:auto!important}}'
)
HOMEPAGE_MOBILE_NAV_HTML = (
    '<nav class="mobile-deal-nav" aria-label="Quick deal navigation">'
    '<button type="button" data-jump="hot">Hot Deals</button>'
    '<button type="button" data-jump="all">All Deals</button>'
    '<button type="button" data-cat="All">All</button>'
    '<a href="/best-amazon-deals-under-50/">Under $50</a>'
    '<a href="/top-100-amazon-deals-today/">Top 100</a>'
    '<button type="button" data-jump="categories">Categories</button>'
    '</nav>'
)

CATEGORY_KEYWORDS = {
    "electronics": ["electronics", "cell phones", "cell phone", "computers", "computer", "camera", "audio", "headphones", "tablet", "tv", "television"],
    "automotive": ["automotive", "car", "truck", "vehicle", "garage"],
    "patio": ["patio", "lawn", "garden", "outdoor", "yard"],
    "sports": ["sports", "outdoors", "outdoor", "camping", "fitness", "hunting", "fishing"],
    "pet": ["pet", "pets", "pet supplies", "dog", "cat"],
    "toys": ["toys", "games", "toy", "game"],
    "office": ["office", "office products", "school supplies"],
    "health": ["health", "household", "beauty", "personal care", "cleaning"],
    "baby": ["baby", "baby products"],
    "music": ["musical instruments", "music", "instrument"],
    "tools": ["tools", "home improvement", "tool"],
    "home": ["home", "kitchen"],
}

CATEGORY_PAGES = [
    {"path": "best-amazon-tool-deals/index.html", "key": "tools", "label": "tool", "marker": "TOOL", "rank": True},
    {"path": "best-amazon-home-kitchen-deals/index.html", "key": "home", "label": "Home & Kitchen", "marker": "HOME KITCHEN"},
    {"path": "best-amazon-deals-under-50/index.html", "key": "under50", "label": "under $50", "marker": "UNDER 50"},
    {"path": "best-amazon-electronics-deals/index.html", "key": "electronics", "label": "Electronics", "marker": "ELECTRONICS"},
    {"path": "best-amazon-automotive-deals/index.html", "key": "automotive", "label": "Automotive", "marker": "AUTOMOTIVE"},
    {"path": "best-amazon-baby-products-deals/index.html", "key": "baby", "label": "Baby Products", "marker": "BABY"},
    {"path": "best-amazon-health-household-deals/index.html", "key": "health", "label": "Health & Household", "marker": "HEALTH HOUSEHOLD"},
    {"path": "best-amazon-musical-instruments-deals/index.html", "key": "music", "label": "Musical Instruments", "marker": "MUSICAL INSTRUMENTS"},
    {"path": "best-amazon-office-products-deals/index.html", "key": "office", "label": "Office Products", "marker": "OFFICE"},
    {"path": "best-amazon-patio-lawn-garden-deals/index.html", "key": "patio", "label": "Patio, Lawn & Garden", "marker": "PATIO LAWN GARDEN"},
    {"path": "best-amazon-pet-supplies-deals/index.html", "key": "pet", "label": "Pet Supplies", "marker": "PET SUPPLIES"},
    {"path": "best-amazon-sports-outdoors-deals/index.html", "key": "sports", "label": "Sports & Outdoors", "marker": "SPORTS OUTDOORS"},
    {"path": "best-amazon-toys-games-deals/index.html", "key": "toys", "label": "Toys & Games", "marker": "TOYS GAMES"},
    {"path": "top-100-amazon-deals-today/index.html", "key": "top100", "label": "top 100", "marker": "TOP 100", "rank": True},
]


def esc(value: object) -> str:
    return html.escape(str(value or ""), quote=True)


def norm(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower().replace("&", "and")).strip()


def money(value: object) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, str):
        return value
    try:
        return f"${float(value):.2f}"
    except (TypeError, ValueError):
        return str(value)


def price_amount(deal: dict) -> float:
    for key in ("price_amount", "current_price", "currentPrice", "sale_price", "price"):
        value = deal.get(key)
        if isinstance(value, str):
            value = re.sub(r"[^0-9.]", "", value)
        try:
            amount = float(value)
            if amount > 0:
                return amount
        except (TypeError, ValueError):
            pass
    return 0.0


def compact_image_url(value: object, size: int = 160) -> str:
    return re.sub(r"\._SL\d+_\.", f"._SL{size}_.", str(value or ""))


def pct(deal: dict) -> int:
    for key in ("pct", "drop_percent", "discount_percent", "discountPercent", "percent_off", "percentOff"):
        try:
            return int(float(deal.get(key) or 0))
        except (TypeError, ValueError):
            pass
    return 0


def is_hot(deal: dict) -> bool:
    return bool(deal.get("hot") or deal.get("is_hot") or deal.get("isHot")) or pct(deal) >= 40


def has_coupon(deal: dict) -> bool:
    return bool(deal.get("hasCoupon") or deal.get("has_coupon") or deal.get("couponDisplay") or deal.get("coupon"))


def category(deal: dict) -> str:
    return str(deal.get("cat") or deal.get("category") or deal.get("product_category") or "Amazon Deals")


def title(deal: dict) -> str:
    return str(deal.get("title") or deal.get("name") or deal.get("product_title") or "Amazon Deal")


def score(deal: dict) -> float:
    updated = deal.get("updated_at") or deal.get("updatedAt") or deal.get("seen_at") or deal.get("seenAt") or ""
    updated_score = sum(ord(ch) for ch in str(updated)[-12:]) / 1000
    return (1000 if is_hot(deal) else 0) + (180 if has_coupon(deal) else 0) + pct(deal) * 12 + max(0, 80 - price_amount(deal)) + updated_score


def deal_matches(deal: dict, key: str) -> bool:
    if key == "top100":
        return True
    if key == "under50":
        amount = price_amount(deal)
        return 0 < amount <= 50
    haystack = f"{norm(category(deal))} {norm(title(deal))}"
    return any(norm(word) in haystack for word in CATEGORY_KEYWORDS.get(key, [key]))


def sorted_deals(deals: list[dict]) -> list[dict]:
    return sorted(deals, key=score, reverse=True)


def deal_image(deal: dict, class_name: str = "") -> str:
    image = deal.get("image") or deal.get("image_url") or deal.get("imageUrl") or deal.get("thumbnail")
    card_title = title(deal)
    class_attr = f' class="{class_name}"' if class_name else ""
    if image:
        return f'<img src="{esc(compact_image_url(image))}" alt="{esc(card_title)}" loading="lazy" decoding="async"{class_attr} onerror="this.outerHTML=\'&lt;div class=&quot;img-fallback&quot;&gt;Deal image unavailable&lt;/div&gt;\'">'
    return '<span class="img-fallback">Deal image unavailable</span>'


def homepage_deal_card(deal: dict) -> str:
    card_title = esc(title(deal))
    link = esc(deal.get("link") or deal.get("amazon_url") or deal.get("url") or "#")
    cat_label = esc(category(deal))
    description = esc(deal.get("desc") or deal.get("brand") or "")
    price = esc(deal.get("price") or money(price_amount(deal)) or "See price on Amazon")
    was = deal.get("was") or deal.get("old_price") or deal.get("previous_price")
    discount = esc(deal.get("discount") or (f"-{pct(deal)}%" if pct(deal) else ""))
    hot_badge = '<span class="card-badge-hot">Hot</span>' if is_hot(deal) else ""
    was_html = f'<span class="price-was">{esc(was)}</span>' if was else ""
    coupon_html = f'<span class="coupon-badge">{esc(deal.get("couponDisplay") or "Coupon available")}</span>' if has_coupon(deal) else ""

    return f'''<a class="deal-card" href="{link}" target="_blank" rel="noopener sponsored">
        <div class="card-img">{deal_image(deal)}</div>
        <div class="card-body">
          <div class="card-meta">
            <span class="card-category">{cat_label}</span>
            {hot_badge}
          </div>
          <div class="card-title">{card_title}</div>
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
    card_title = esc(title(deal))
    link = esc(deal.get("link") or deal.get("amazon_url") or deal.get("url") or "#")
    cat_label = esc(category(deal))
    brand = esc(str(deal.get("brand") or "")[:24])
    price = esc(deal.get("price") or money(price_amount(deal)) or "See deal")
    was = deal.get("was") or deal.get("old_price") or deal.get("previous_price")
    discount = pct(deal)
    badge = "Hot Deal" if is_hot(deal) else "Coupon" if has_coupon(deal) else "Deal"
    rank_badge = f'<div class="rank-badge">#{rank}</div>' if rank else ""
    was_html = f'<span class="hot-price-was">{esc(was)}</span>' if was else ""
    off_html = f'<span class="hot-off">{discount}% off</span>' if discount else ""

    return f'''<a class="hot-card" href="{link}" target="_blank" rel="nofollow sponsored noopener" data-asin="{esc(deal.get('asin') or '')}" data-deal-title="{card_title}" data-deal-category="{cat_label}" data-deal-price="{esc(price_amount(deal) or '')}" data-deal-discount="{discount}">
        <div class="hot-card-img">{deal_image(deal)}{rank_badge}<div class="hot-card-badge">{badge}</div></div>
        <div class="hot-card-body">
          <div class="category-pill">{cat_label}</div>
          <div class="stars">{'Top deal' if is_hot(deal) else 'Deal'} {brand}</div>
          <div class="hot-card-title">{card_title}</div>
          <div class="hot-card-prices"><span class="hot-price-now">{price}</span>{was_html}{off_html}</div>
          <span class="hot-btn">See Deal on Amazon &rarr;</span>
        </div>
      </a>'''


def render_block(deals: list[dict], start_marker: str, end_marker: str, card_renderer) -> str:
    cards = "\n      ".join(card_renderer(deal) for deal in deals[:DEALS_LIMIT])
    return f"{start_marker}\n      {cards}\n      {end_marker}"


def replace_between_markers(page_html: str, start_marker: str, end_marker: str, block: str) -> str | None:
    if start_marker in page_html and end_marker in page_html:
        return re.sub(rf"{re.escape(start_marker)}.*?{re.escape(end_marker)}", block, page_html, flags=re.DOTALL)
    return None


def sanitize_display_text(page_html: str) -> str:
    replacements = {
        "â€”": "-",
        "â†—": "&#8599;",
        "â†’": "&rarr;",
        "â˜…â˜…â˜…â˜…â˜…": "Top deal",
        "â˜…â˜…â˜…â˜…â˜†": "Deal",
        "â€™": "'",
        "â€œ": '"',
        "â€": '"',
        "â€“": "-",
    }
    for bad, good in replacements.items():
        page_html = page_html.replace(bad, good)
    return page_html


def render_homepage(deals: list[dict]) -> None:
    path = Path("index.html")
    page_html = path.read_text(encoding="utf-8")
    page_html = page_html.replace('/site-header.js?v=5', '/site-header.js?v=6')
    page_html = re.sub(r'#site-header\{display:block;min-height:176px.*?scroll-behavior:auto!important\}\}', '', page_html)
    page_html = page_html.replace('</style>', HOMEPAGE_CLS_CSS + '</style>', 1)
    page_html = re.sub(r'<nav class="mobile-deal-nav" aria-label="Quick deal navigation">.*?</nav>\s*', '', page_html, count=1, flags=re.DOTALL)
    page_html = page_html.replace('<main class="page-shell">', '<main class="page-shell">  ' + HOMEPAGE_MOBILE_NAV_HTML, 1)
    page_html = page_html.replace(
        '<section class="deals-section" id="deals-section" style="display:none;">',
        '<section class="deals-section" id="deals-section">',
        1,
    )
    ordered = sorted_deals(deals)
    block = render_block(ordered, HOME_START_MARKER, HOME_END_MARKER, homepage_deal_card)
    replaced = replace_between_markers(page_html, HOME_START_MARKER, HOME_END_MARKER, block)
    if replaced is not None:
        page_html = replaced
    else:
        empty_grid = '<div class="deals-grid" id="deals-grid"></div>'
        populated_grid = f'<div class="deals-grid" id="deals-grid">\n      {block}\n    </div>'
        if empty_grid not in page_html:
            raise RuntimeError("Could not find empty homepage deals grid")
        page_html = page_html.replace(empty_grid, populated_grid, 1)
    homepage_img_function = """function img(src, emoji, size) {
  const pad = size === 'hot' ? '8px' : '12px';
  const fs = size === 'hot' ? '34px' : '44px';
  const fallback = '<div class="img-fallback">Deal image unavailable</div>';
  if (src) {
    return '<img src="' + escUrl(optimizeDealImage(src)) + '" alt="" loading="lazy" decoding="async" style="width:100%;height:100%;object-fit:contain;padding:' + pad + ';" onerror="this.parentElement.innerHTML=\\'' + fallback.replace(/'/g, '&#039;') + '\\'">';
  }
  return emoji ? '<span style="font-size:' + fs + '">' + esc(emoji) + '</span>' : fallback;
}"""
    page_html = re.sub(
        r'function img\(src, emoji, size\) \{.*?\}function prioritizeCardImages',
        homepage_img_function + 'function prioritizeCardImages',
        page_html,
        count=1,
        flags=re.DOTALL,
    )
    page_html = sanitize_display_text(page_html)
    label = f"Showing {min(DEALS_LIMIT, len(ordered))} of {len(ordered)} deals"
    page_html = re.sub(r'(<span class="deal-count" id="count-label">)(.*?)(</span>)', rf"\g<1>{esc(label)}\g<3>", page_html, count=1, flags=re.DOTALL)
    path.write_text(page_html, encoding="utf-8")
    print(f"Rendered {min(DEALS_LIMIT, len(ordered))} static homepage deals into index.html")


def set_category_deal_count(page_html: str, label: str) -> str:
    count_html = f'<div class="deal-count" id="deal-count">{esc(label)}</div>'
    if re.search(r'<div class="deal-count" id="deal-count">.*?</div>', page_html, flags=re.DOTALL):
        return re.sub(r'<div class="deal-count" id="deal-count">.*?</div>', count_html, page_html, count=1, flags=re.DOTALL)
    if re.search(r'<section class="section-head">.*?</section>', page_html, flags=re.DOTALL):
        return re.sub(
            r'(<section class="section-head">.*?</div>)(?:[^<]*?\bof\s+\d+\s+deals)?(?:</div>)?(</section>)',
            rf"\g<1>{count_html}\g<2>",
            page_html,
            count=1,
            flags=re.DOTALL,
        )
    return page_html


def render_category_page(deals: list[dict], config: dict) -> None:
    path = Path(config["path"])
    page_html = path.read_text(encoding="utf-8")
    filtered = sorted_deals([deal for deal in deals if deal_matches(deal, config["key"])])
    if config["key"] == "top100":
        filtered = filtered[:100]

    start_marker = f"<!-- BLD STATIC {config['marker']} DEALS START -->"
    end_marker = f"<!-- BLD STATIC {config['marker']} DEALS END -->"

    def card_for(item: tuple[int, dict]) -> str:
        rank, deal = item
        return category_deal_card(deal, rank=rank if config.get("rank") else None)

    ranked = list(enumerate(filtered[:DEALS_LIMIT], start=1))
    cards = "\n      ".join(card_for(item) for item in ranked)
    block = f"{start_marker}\n      {cards}\n      {end_marker}"
    replaced = replace_between_markers(page_html, start_marker, end_marker, block)
    if replaced is not None:
        page_html = replaced
    else:
        grid_match = re.search(r'<div class="hot-grid" id="hot-grid"(?:\s+[^>]*)?>.*?</div>', page_html, flags=re.DOTALL)
        if not grid_match:
            raise RuntimeError(f"Could not find deal grid in {path}")
        attrs = grid_match.group(0).split('>', 1)[0]
        populated_grid = f'{attrs}>\n      {block}\n    </div>'
        page_html = page_html[:grid_match.start()] + populated_grid + page_html[grid_match.end():]

    label = f"Showing {min(DEALS_LIMIT, len(filtered))} of {len(filtered)} deals"
    page_html = set_category_deal_count(page_html, label)
    page_html = re.sub(r'(<div class="status-line" id="status-line">)(.*?)(</div>)', rf"\g<1>Showing live {esc(config['label'])} deals from the Black Lab Deals feed.\g<3>", page_html, count=1, flags=re.DOTALL)
    page_html = sanitize_display_text(page_html)
    path.write_text(page_html, encoding="utf-8")
    print(f"Rendered {min(DEALS_LIMIT, len(filtered))} static {config['label']} deals into {path}")


def main() -> None:
    if not DEALS_FILE.exists():
        raise RuntimeError("Missing deals.json")

    data = json.loads(DEALS_FILE.read_text(encoding="utf-8"))
    deals = data.get("deals") if isinstance(data, dict) else data
    if not isinstance(deals, list):
        raise RuntimeError("deals.json does not contain a deals list")

    render_homepage(deals)
    for config in CATEGORY_PAGES:
        render_category_page(deals, config)


if __name__ == "__main__":
    main()
