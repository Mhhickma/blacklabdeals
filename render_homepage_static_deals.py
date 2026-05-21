"""Render crawlable deal cards from deals.json.

The pages still refresh with JavaScript in the browser. This script gives
search engines and no-JavaScript visitors the first batch of live deals directly
in key HTML pages after each automated deal refresh.
"""

from __future__ import annotations

import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path

DEALS_FILE = Path("deals.json")
DEALS_LIMIT = 50
HOME_START_MARKER = "<!-- BLD STATIC DEALS START -->"
HOME_END_MARKER = "<!-- BLD STATIC DEALS END -->"
HOMEPAGE_CLS_CSS = (
    '#site-header{display:block;min-height:214px;background:var(--surface);}'
    '#hot-section[style*="display:none"]{display:block!important;visibility:hidden;min-height:900px}'
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
    '.popular-category-link,.browse-page-card,.bld-mega-link{transition:none!important}'
    '.popular-category-link:hover,.browse-page-card:hover{transform:none!important}'
    '@media(prefers-reduced-motion:reduce){*,*::before,*::after{animation-duration:.001ms!important;animation-iteration-count:1!important;transition-duration:.001ms!important;scroll-behavior:auto!important}}'
)
HOMEPAGE_CARD_CSS = (
    '/* BLD HOMEPAGE UNIFIED CARD CSS START */'
    '.deals-grid{display:grid!important;grid-template-columns:repeat(4,minmax(0,1fr))!important;gap:18px!important;align-items:stretch!important}'
    '.deals-grid>.best-seller-card{width:auto!important;max-width:none!important}'
    '.best-seller-card{background:var(--surface);border:1px solid var(--border);border-radius:18px;overflow:hidden;box-shadow:var(--shadow);display:flex!important;flex-direction:column!important;min-height:100%;text-decoration:none;color:inherit}'
    '.best-seller-img{height:180px;background:#fff;display:flex;align-items:center;justify-content:center;border-bottom:1px solid #eee;overflow:hidden}'
    '.best-seller-img img{max-width:100%;max-height:100%;width:auto;height:auto;object-fit:contain;padding:14px}'
    '.best-seller-body{padding:15px;display:flex;flex-direction:column;gap:8px;flex:1}'
    '.best-seller-badges{display:flex;gap:6px;flex-wrap:wrap}'
    '.best-seller-badge{font-size:11px;font-weight:800;border-radius:999px;padding:3px 8px;background:var(--red-light);color:var(--red);line-height:1.35}'
    '.best-seller-badge.rank{background:var(--accent-light);color:var(--accent)}'
    '.best-seller-title{font-size:14px;font-weight:800;color:var(--text-primary);line-height:1.35;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}'
    '.best-seller-category{font-size:12px;color:var(--text-muted)}'
    '.best-seller-price-row{display:flex;gap:8px;align-items:baseline;margin-top:auto;flex-wrap:wrap}'
    '.best-seller-price{font-size:22px;font-weight:900;color:var(--red);line-height:1}'
    '.best-seller-was{font-size:13px;color:var(--text-muted);text-decoration:line-through}'
    '.best-seller-btn{margin-top:8px;display:block;text-align:center;text-decoration:none;background:var(--accent);color:#fff;border-radius:10px;padding:10px 12px;font-weight:900;font-size:14px}'
    '.best-seller-btn:hover{background:var(--accent-mid);color:#fff}'
    '@media(max-width:1100px){.deals-grid{grid-template-columns:repeat(3,minmax(0,1fr))!important}}'
    '@media(max-width:820px){.deals-grid{grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:10px}.best-seller-img{height:135px}.best-seller-body{padding:11px}.best-seller-price{font-size:18px}.best-seller-title{font-size:12px}}'
    '@media(max-width:520px){.deals-grid{grid-template-columns:1fr!important}}'
    '/* BLD HOMEPAGE UNIFIED CARD CSS END */'
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


def image_url(deal: dict) -> str:
    image = deal.get("image") or deal.get("image_url") or deal.get("imageUrl") or deal.get("img") or deal.get("thumbnail")
    if image:
        return str(image)
    asin = str(deal.get("asin") or "").strip().upper()
    if re.fullmatch(r"[A-Z0-9]{10}", asin):
        return f"https://images-na.ssl-images-amazon.com/images/P/{asin}.01._SL160_.jpg"
    return ""


def has_image(deal: dict) -> bool:
    return bool(image_url(deal))


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
    words = [norm(word) for word in CATEGORY_KEYWORDS.get(key, [key])]
    deal_category = norm(category(deal))
    if any(word in deal_category for word in words):
        return True
    if deal_category in {"", "amazon deals"}:
        deal_title = norm(title(deal))
        return any(word in deal_title for word in words)
    return False


def sorted_deals(deals: list[dict]) -> list[dict]:
    return sorted(deals, key=score, reverse=True)


def static_stats(deals: list[dict], updated_at: str = "") -> dict[str, str]:
    priced = [price_amount(deal) for deal in deals if price_amount(deal) > 0]
    discounts = [pct(deal) for deal in deals if pct(deal) > 0]
    return {
        "total": str(len(deals)),
        "hot": str(sum(1 for deal in deals if is_hot(deal))),
        "avg_price": f"${(sum(priced) / len(priced)):.2f}" if priced else "-",
        "avg_discount": f"{round(sum(discounts) / len(discounts))}% off" if discounts else "-",
        "updated": static_updated_label(updated_at),
    }


def static_updated_label(updated_at: str) -> str:
    if not updated_at:
        return "Just now"
    try:
        updated = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
        minutes = max(0, round((datetime.now(timezone.utc) - updated.astimezone(timezone.utc)).total_seconds() / 60))
        if minutes < 1:
            return "Just now"
        if minutes < 60:
            return f"{minutes}m ago"
        hours = round(minutes / 60)
        if hours < 24:
            return f"{hours}h ago"
        return f"{round(hours / 24)}d ago"
    except (TypeError, ValueError):
        return "Just now"


def image_first(deals: list[dict]) -> list[dict]:
    return sorted(deals, key=lambda deal: (0 if has_image(deal) else 1))


def displayable_deals(deals: list[dict]) -> list[dict]:
    image_deals = [deal for deal in deals if has_image(deal)]
    return image_deals or deals


def deal_image(deal: dict, class_name: str = "") -> str:
    image = image_url(deal)
    card_title = title(deal)
    class_attr = f' class="{class_name}"' if class_name else ""
    if image:
        return f'<img src="{esc(compact_image_url(image))}" alt="{esc(card_title)}" width="160" height="160" loading="lazy" decoding="async"{class_attr}>'
    return '<span class="img-fallback">Deal image unavailable</span>'


def homepage_deal_card(deal: dict) -> str:
    card_title = esc(title(deal))
    link = esc(deal.get("link") or deal.get("amazon_url") or deal.get("url") or "#")
    cat_label = esc(category(deal))
    price = esc(deal.get("price") or money(price_amount(deal)) or "See price on Amazon")
    was = deal.get("was") or deal.get("old_price") or deal.get("previous_price")
    discount = pct(deal)
    primary_badge = f"{discount}% off" if discount else "Deal"
    secondary_badge = "Hot Deal" if is_hot(deal) else "Coupon" if has_coupon(deal) else cat_label
    was_html = f'<span class="best-seller-was">{esc(was)}</span>' if was else ""

    return f'''<article class="best-seller-card deal-card-unified" data-asin="{esc(deal.get('asin') or '')}" data-deal-title="{card_title}" data-deal-category="{cat_label}" data-deal-price="{esc(price_amount(deal) or '')}" data-deal-discount="{discount}">
        <div class="best-seller-img">{deal_image(deal)}</div>
        <div class="best-seller-body">
          <div class="best-seller-badges"><span class="best-seller-badge">{esc(primary_badge)}</span><span class="best-seller-badge rank">{esc(secondary_badge)}</span></div>
          <div class="best-seller-title">{card_title}</div>
          <div class="best-seller-category">{cat_label}</div>
          <div class="best-seller-price-row"><span class="best-seller-price">{price}</span>{was_html}</div>
          <a class="best-seller-btn" href="{link}" target="_blank" rel="nofollow sponsored noopener">View on Amazon</a>
        </div>
      </article>'''


def category_deal_card(deal: dict, rank: int | None = None) -> str:
    card_title = esc(title(deal))
    link = esc(deal.get("link") or deal.get("amazon_url") or deal.get("url") or "#")
    cat_label = esc(category(deal))
    price = esc(deal.get("price") or money(price_amount(deal)) or "See deal")
    was = deal.get("was") or deal.get("old_price") or deal.get("previous_price")
    discount = pct(deal)
    primary_badge = f"{discount}% off" if discount else "Deal"
    secondary_badge = f"#{rank}" if rank else "Hot Deal" if is_hot(deal) else "Coupon" if has_coupon(deal) else cat_label
    was_html = f'<span class="best-seller-was">{esc(was)}</span>' if was else ""

    return f'''<article class="best-seller-card deal-card-unified" data-asin="{esc(deal.get('asin') or '')}" data-deal-title="{card_title}" data-deal-category="{cat_label}" data-deal-price="{esc(price_amount(deal) or '')}" data-deal-discount="{discount}">
        <div class="best-seller-img">{deal_image(deal)}</div>
        <div class="best-seller-body">
          <div class="best-seller-badges"><span class="best-seller-badge">{esc(primary_badge)}</span><span class="best-seller-badge rank">{esc(secondary_badge)}</span></div>
          <div class="best-seller-title">{card_title}</div>
          <div class="best-seller-category">{cat_label}</div>
          <div class="best-seller-price-row"><span class="best-seller-price">{price}</span>{was_html}</div>
          <a class="best-seller-btn" href="{link}" target="_blank" rel="nofollow sponsored noopener">View on Amazon</a>
        </div>
      </article>'''


def render_block(deals: list[dict], start_marker: str, end_marker: str, card_renderer) -> str:
    cards = "\n      ".join(card_renderer(deal) for deal in deals[:DEALS_LIMIT])
    return f"{start_marker}\n      {cards}\n      {end_marker}"


def replace_between_markers(page_html: str, start_marker: str, end_marker: str, block: str) -> str | None:
    if start_marker in page_html and end_marker in page_html:
        return re.sub(rf"{re.escape(start_marker)}.*?{re.escape(end_marker)}", block, page_html, flags=re.DOTALL)
    return None


def sanitize_display_text(page_html: str) -> str:
    replacements = {
        b"\xc3\xa2\xe2\x82\xac\xe2\x80\x9d".decode("utf-8"): "-",
        b"\xc3\xa2\xe2\x80\xa0\xe2\x80\x94".decode("utf-8"): "&#8599;",
        b"\xc3\xa2\xe2\x80\xa0\xe2\x80\x99".decode("utf-8"): "",
        b"\xc3\xa2\xcb\x9c\xe2\x80\xa6\xc3\xa2\xcb\x9c\xe2\x80\xa6\xc3\xa2\xcb\x9c\xe2\x80\xa6\xc3\xa2\xcb\x9c\xe2\x80\xa6\xc3\xa2\xcb\x9c\xe2\x80\xa6".decode("utf-8"): "Top deal",
        b"\xc3\xa2\xcb\x9c\xe2\x80\xa6\xc3\xa2\xcb\x9c\xe2\x80\xa6\xc3\xa2\xcb\x9c\xe2\x80\xa6\xc3\xa2\xcb\x9c\xe2\x80\xa6\xc3\xa2\xcb\x9c\xe2\x80\xa0".decode("utf-8"): "Deal",
        b"\xc3\xa2\xe2\x82\xac\xe2\x84\xa2".decode("utf-8"): "'",
        b"\xc3\xa2\xe2\x82\xac\xc5\x93".decode("utf-8"): '"',
        b"\xc3\xa2\xe2\x82\xac\xc2\x9d".decode("utf-8"): '"',
        b"\xc3\xa2\xe2\x82\xac\xe2\x80\x9c".decode("utf-8"): "-",
    }
    for bad, good in replacements.items():
        page_html = page_html.replace(bad, good)
    return page_html

def render_homepage(deals: list[dict], updated_at: str = "") -> None:
    path = Path("index.html")
    page_html = path.read_text(encoding="utf-8")
    page_html = page_html.replace('/site-header.js?v=5', '/site-header.js?v=6')
    page_html = re.sub(r'/\* BLD HOMEPAGE UNIFIED CARD CSS START \*/.*?/\* BLD HOMEPAGE UNIFIED CARD CSS END \*/', '', page_html, flags=re.DOTALL)
    page_html = re.sub(r'#site-header\{display:block;min-height:\d+px.*?scroll-behavior:auto!important\}\}', '', page_html)
    page_html = page_html.replace('</style>', HOMEPAGE_CARD_CSS + HOMEPAGE_CLS_CSS + '</style>', 1)
    page_html = re.sub(
        r'const HOT_DEALS_PREVIEW_LIMIT = 6;(?:const HOT_DEALS_LOAD_MORE_COUNT = 25;)*',
        'const HOT_DEALS_PREVIEW_LIMIT = 6;const HOT_DEALS_LOAD_MORE_COUNT = 25;',
        page_html,
        count=1,
    )
    if 'let visibleHotDealsCount = HOT_DEALS_PREVIEW_LIMIT;' not in page_html:
        page_html = page_html.replace(
            'let visibleDealsCount = DEALS_PER_PAGE;',
            'let visibleDealsCount = DEALS_PER_PAGE;let visibleHotDealsCount = HOT_DEALS_PREVIEW_LIMIT;',
            1,
        )
    page_html = re.sub(r'<nav class="mobile-deal-nav" aria-label="Quick deal navigation">.*?</nav>\s*', '', page_html, count=1, flags=re.DOTALL)
    page_html = page_html.replace('<main class="page-shell">', '<main class="page-shell">  ' + HOMEPAGE_MOBILE_NAV_HTML, 1)
    page_html = page_html.replace(
        '<section class="deals-section" id="deals-section" style="display:none;">',
        '<section class="deals-section" id="deals-section">',
        1,
    )
    ordered = image_first(displayable_deals(sorted_deals(deals)))
    hot_ordered = image_first(displayable_deals(sorted_deals([deal for deal in deals if is_hot(deal)])))
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
    hot_cards = "\n      ".join(homepage_deal_card(deal) for deal in hot_ordered[:6])
    hot_count = f"Top {min(6, len(hot_ordered))} of {len(hot_ordered)} deals" if len(hot_ordered) > 6 else f"{len(hot_ordered)} deal{'s' if len(hot_ordered) != 1 else ''}"
    hot_button_html = ""
    if len(hot_ordered) > 6:
        remaining = len(hot_ordered) - 6
        hot_button_html = (
            f'<div id="hot-load-more-wrap" class="load-more-wrap">'
            f'<button id="hot-load-more-btn" class="load-more-btn" type="button">'
            f'Load {min(25, remaining)} More Hot Deals ({remaining} remaining)'
            f'</button></div>'
        )
    hot_section_html = f'''<section class="hot-section" id="hot-section">
    <div class="hot-header">
      <span class="hot-title">Hot Deals</span>
      <span class="hot-subtitle">- 40% off or more</span>
      <span class="hot-pill" id="hot-count-pill">{esc(hot_count)}</span>
    </div>
    <div class="hot-strip">
      <div class="hot-grid" id="hot-grid">
      {hot_cards}
      </div>{hot_button_html}
    </div>
  </section>'''
    page_html = re.sub(
        r'<section class="hot-section" id="hot-section"(?:\s+style="[^"]*")?>.*?</section>',
        hot_section_html,
        page_html,
        count=1,
        flags=re.DOTALL,
    )
    page_html = re.sub(
        r'<div class="loading-bar" id="loading-bar"(?:\s+style="[^"]*")?>',
        '<div class="loading-bar" id="loading-bar" style="display:none;">',
        page_html,
        count=1,
    )
    homepage_img_function = """function asinImageUrl(asin) {
  const value = String(asin || '').trim().toUpperCase();
  return /^[A-Z0-9]{10}$/.test(value) ? 'https://images-na.ssl-images-amazon.com/images/P/' + value + '.01._SL160_.jpg' : '';
}
function img(src, emoji, size) {
  const pad = size === 'hot' ? '8px' : '12px';
  const fs = size === 'hot' ? '34px' : '44px';
  const fallback = '<div class="img-fallback">Deal image unavailable</div>';
  if (src) {
    return '<img src="' + escUrl(optimizeDealImage(src)) + '" alt="" width="160" height="160" loading="lazy" decoding="async" style="width:100%;height:100%;object-fit:contain;padding:' + pad + ';">';
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
    homepage_card_functions = """function dealCardHtml(d, index, mode) {
  const dealTitle = d.title || 'Amazon deal';
  const dealCat = d.cat || d.category || 'Amazon Deals';
  const dealPrice = d.price || 'See deal';
  const pct = Number(d.pct) || 0;
  const primaryBadge = pct ? pct + '% off' : (d.discount || 'Deal');
  const secondaryBadge = mode === 'hot'
    ? 'Hot Deal'
    : (d.hot || pct >= 40 ? 'Hot Deal' : (d.hasCoupon ? 'Coupon' : dealCat));
  return '<article class="best-seller-card deal-card-unified" data-asin="' + esc(d.asin || '') + '" data-deal-title="' + esc(dealTitle) + '" data-deal-category="' + esc(dealCat) + '" data-deal-price="' + esc(d.price_amount || '') + '" data-deal-discount="' + esc(pct) + '">' +
    '<div class="best-seller-img">' + img(d.image || d.image_url || d.imageUrl || d.img || d.thumbnail || asinImageUrl(d.asin), d.emoji, mode === 'hot' ? 'hot' : 'card') + '</div>' +
    '<div class="best-seller-body">' +
      '<div class="best-seller-badges"><span class="best-seller-badge">' + esc(primaryBadge).replace('-', '') + '</span><span class="best-seller-badge rank">' + esc(secondaryBadge) + '</span></div>' +
      '<div class="best-seller-title">' + esc(dealTitle) + '</div>' +
      '<div class="best-seller-category">' + esc(dealCat) + '</div>' +
      '<div class="best-seller-price-row"><span class="best-seller-price">' + esc(dealPrice) + '</span>' + (d.was ? '<span class="best-seller-was">' + esc(d.was) + '</span>' : '') + '</div>' +
      '<a class="best-seller-btn" href="' + escUrl(d.link || '#') + '" target="_blank" rel="nofollow sponsored noopener">View on Amazon</a>' +
    '</div>' +
  '</article>';
}
function hasDealImage(d) {
  return Boolean(d && (d.image || d.image_url || d.imageUrl || d.img || d.thumbnail || asinImageUrl(d.asin)));
}
function imageFirst(arr) {
  return [...arr].sort((a, b) => Number(!hasDealImage(a)) - Number(!hasDealImage(b)));
}
function displayableDeals(arr) {
  const imageDeals = arr.filter(hasDealImage);
  return imageDeals.length ? imageDeals : arr;
}
function renderHotDeals() {
  const hot = allDeals.filter(d => Number(d.pct) >= 40 || d.hot);
  const displayHot = displayableDeals(sortDeals(hot));
  const visibleHot = imageFirst(displayHot).slice(0, visibleHotDealsCount);
  document.getElementById('hot-count-pill').textContent = displayHot.length > HOT_DEALS_PREVIEW_LIMIT
    ? 'Top ' + visibleHot.length + ' of ' + displayHot.length + ' deals'
    : displayHot.length + ' deal' + (displayHot.length !== 1 ? 's' : '');
  document.getElementById('hot-grid').innerHTML = visibleHot.length
    ? visibleHot.map((d, i) => dealCardHtml(d, i, 'hot')).join('')
    : '<div class="loading-bar" style="grid-column:1/-1;">No hot deals yet - check back soon.</div>';
  prioritizeCardImages(document.getElementById('hot-grid'));
  updateHotLoadMoreButton(displayHot.length);
}
function ensureHotLoadMoreButton() {
  let wrap = document.getElementById('hot-load-more-wrap');
  let button = document.getElementById('hot-load-more-btn');
  if (!wrap) {
    wrap = document.createElement('div');
    wrap.id = 'hot-load-more-wrap';
    wrap.className = 'load-more-wrap hidden';
    wrap.hidden = true;
    wrap.innerHTML = '<button id="hot-load-more-btn" class="load-more-btn" type="button">Load 25 More Hot Deals</button>';
    const grid = document.getElementById('hot-grid');
    if (grid) grid.insertAdjacentElement('afterend', wrap);
    button = document.getElementById('hot-load-more-btn');
  }
  if (button && !button.dataset.bound) {
    button.dataset.bound = 'true';
    button.addEventListener('click', async () => {
      if (!allDeals.length) {
        await loadDeals();
      }
      visibleHotDealsCount += HOT_DEALS_LOAD_MORE_COUNT;
      renderHotDeals();
    });
  }
  return { wrap, button };
}
function updateHotLoadMoreButton(total) {
  const { wrap, button } = ensureHotLoadMoreButton();
  if (!wrap || !button) return;
  const remaining = Math.max(0, total - visibleHotDealsCount);
  if (remaining > 0) {
    wrap.hidden = false;
    wrap.classList.remove('hidden');
    button.hidden = false;
    button.disabled = false;
    button.textContent = 'Load ' + Math.min(HOT_DEALS_LOAD_MORE_COUNT, remaining) + ' More Hot Deals (' + remaining + ' remaining)';
  } else {
    wrap.hidden = true;
    wrap.classList.add('hidden');
  }
}
function getFilteredDeals() {
  let filtered = currentCategory === 'All'
    ? allDeals
    : allDeals.filter(d => d.cat === currentCategory);
  if (searchQuery) {
    filtered = filtered.filter(d =>
      (d.title || '').toLowerCase().includes(searchQuery) ||
      (d.brand || '').toLowerCase().includes(searchQuery) ||
      (d.cat || '').toLowerCase().includes(searchQuery)
    );
  }
  return sortDeals(filtered);
}
function ensureLoadMoreButton() {
  let wrap = document.getElementById('main-load-more-wrap');
  let button = document.getElementById('main-load-more-btn');
  if (!wrap) {
    wrap = document.createElement('div');
    wrap.id = 'main-load-more-wrap';
    wrap.className = 'load-more-wrap hidden';
    wrap.hidden = true;
    wrap.innerHTML = '<button id="main-load-more-btn" class="load-more-btn" type="button">Load 50 More Deals</button>';
    const grid = document.getElementById('deals-grid');
    if (grid) grid.insertAdjacentElement('afterend', wrap);
    button = document.getElementById('main-load-more-btn');
  }
  if (button && !button.dataset.bound) {
    button.dataset.bound = 'true';
    button.addEventListener('click', () => {
      visibleDealsCount += DEALS_PER_PAGE;
      renderDeals();
    });
  }
  return { wrap, button };
}
function updateLoadMoreButton(total) {
  const { wrap, button } = ensureLoadMoreButton();
  if (!wrap || !button) return;
  const remaining = Math.max(0, total - visibleDealsCount);
  if (remaining > 0) {
    wrap.hidden = false;
    wrap.classList.remove('hidden');
    button.hidden = false;
    button.disabled = false;
    button.textContent = 'Load ' + Math.min(DEALS_PER_PAGE, remaining) + ' More Deals (' + remaining + ' remaining)';
  } else {
    wrap.hidden = true;
    wrap.classList.add('hidden');
  }
}
function resetVisibleDeals() {
  visibleDealsCount = DEALS_PER_PAGE;
  visibleHotDealsCount = HOT_DEALS_PREVIEW_LIMIT;
}
function renderDeals() {
  const filtered = getFilteredDeals();
  const displayDeals = displayableDeals(filtered);
  const visibleDeals = imageFirst(displayDeals).slice(0, visibleDealsCount);
  document.getElementById('count-label').textContent = displayDeals.length
    ? 'Showing ' + Math.min(visibleDeals.length, displayDeals.length) + ' of ' + displayDeals.length + ' deal' + (displayDeals.length !== 1 ? 's' : '')
    : '0 deals';
  document.getElementById('deals-grid').innerHTML = displayDeals.length
    ? visibleDeals.map((d, i) => dealCardHtml(d, i, 'all')).join('')
    : '<div class="loading-bar" style="grid-column:1/-1;text-align:center;padding:48px;">' +
        (searchQuery ? 'No deals found for "' + esc(searchQuery) + '". Try a different search.' : 'No deals in this category right now - check back soon!') +
      '</div>';
  prioritizeCardImages(document.getElementById('deals-grid'));
  updateLoadMoreButton(displayDeals.length);
}"""
    dynamic_card_pattern = (
        r'function dealCardHtml\(d, index, mode\) \{.*?\}function handleSearch'
        if 'function dealCardHtml(d, index, mode)' in page_html
        else r'function renderHotDeals\(\) \{.*?\}function handleSearch'
    )
    page_html = re.sub(
        dynamic_card_pattern,
        homepage_card_functions + 'function handleSearch',
        page_html,
        count=1,
        flags=re.DOTALL,
    )
    homepage_static_hot_function = """function renderInitialHotFromStatic() {
  const hotSection = document.getElementById('hot-section');
  const hotGrid = document.getElementById('hot-grid');
  const hotCount = document.getElementById('hot-count-pill');
  const loading = document.getElementById('loading-bar');
  const staticCards = [...document.querySelectorAll('#deals-grid .deal-card-unified,#deals-grid .best-seller-card')].slice(0, HOT_DEALS_PREVIEW_LIMIT);
  if (!hotSection || !hotGrid || !staticCards.length) return;
  hotGrid.innerHTML = staticCards.map(card => {
    const clone = card.cloneNode(true);
    clone.classList.remove('deal-card');
    clone.classList.add('best-seller-card', 'deal-card-unified');
    return clone.outerHTML;
  }).join('');
  prioritizeCardImages(hotGrid);
  hotSection.style.display = '';
  if (loading) loading.style.display = 'none';
  if (hotCount) hotCount.textContent = 'Top ' + staticCards.length + ' deals';
  const { wrap, button } = ensureHotLoadMoreButton();
  if (wrap && button) {
    wrap.hidden = false;
    wrap.classList.remove('hidden');
    button.hidden = false;
    button.disabled = false;
    button.textContent = 'Load 25 More Hot Deals';
  }
}"""
    page_html = re.sub(
        r'function renderInitialHotFromStatic\(\) \{.*?\}function getSortOrder',
        homepage_static_hot_function + 'function getSortOrder',
        page_html,
        count=1,
        flags=re.DOTALL,
    )
    page_html = sanitize_display_text(page_html)
    page_html = page_html.replace(
        'ensureHomepageMobileNav();moveHomepageBrowseBelowDeals();hydrateDeferredImages();',
        'ensureHomepageMobileNav();hydrateDeferredImages();',
        1,
    )
    page_html = page_html.replace(
        'ensureHomepageMobileNav();hydrateDeferredImages();renderInitialHotFromStatic();scheduleDealFeedLoad();',
        'ensureHomepageMobileNav();hydrateDeferredImages();ensureHotLoadMoreButton();scheduleDealFeedLoad();',
        1,
    )
    stats = static_stats(deals, updated_at)
    page_html = re.sub(r'(<span class="stat-num" id="stat-total">)(.*?)(</span>)', rf"\g<1>{esc(stats['total'])}\g<3>", page_html, count=1, flags=re.DOTALL)
    page_html = re.sub(r'(<span class="stat-num" id="stat-hot">)(.*?)(</span>)', rf"\g<1>{esc(stats['hot'])}\g<3>", page_html, count=1, flags=re.DOTALL)
    page_html = re.sub(r'(<span class="stat-num" id="stat-avg-price">)(.*?)(</span>)', rf"\g<1>{esc(stats['avg_price'])}\g<3>", page_html, count=1, flags=re.DOTALL)
    page_html = re.sub(r'(<span class="stat-num" id="stat-avg-discount">)(.*?)(</span>)', rf"\g<1>{esc(stats['avg_discount'])}\g<3>", page_html, count=1, flags=re.DOTALL)
    page_html = re.sub(r'(<span class="stat-num" id="stat-updated">)(.*?)(</span>)', rf"\g<1>{esc(stats['updated'])}\g<3>", page_html, count=1, flags=re.DOTALL)
    page_html = re.sub(r'(<div class="hero-pill" id="hero-pill-text">)(.*?)(</div>)', rf"\g<1>{esc(stats['total'])} deals live right now\g<3>", page_html, count=1, flags=re.DOTALL)
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
    display_filtered = displayable_deals(filtered)

    start_marker = f"<!-- BLD STATIC {config['marker']} DEALS START -->"
    end_marker = f"<!-- BLD STATIC {config['marker']} DEALS END -->"

    def card_for(item: tuple[int, dict]) -> str:
        rank, deal = item
        return category_deal_card(deal, rank=rank if config.get("rank") else None)

    ranked = list(enumerate(image_first(display_filtered)[:DEALS_LIMIT], start=1))
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

    label = f"Showing {min(DEALS_LIMIT, len(display_filtered))} of {len(display_filtered)} deals"
    page_html = set_category_deal_count(page_html, label)
    page_html = re.sub(r'(<div class="status-line" id="status-line">)(.*?)(</div>)', rf"\g<1>Showing live {esc(config['label'])} deals from the Black Lab Deals feed.\g<3>", page_html, count=1, flags=re.DOTALL)
    page_html = sanitize_display_text(page_html)
    path.write_text(page_html, encoding="utf-8")
    print(f"Rendered {min(DEALS_LIMIT, len(display_filtered))} static {config['label']} deals into {path}")


def main() -> None:
    if not DEALS_FILE.exists():
        raise RuntimeError("Missing deals.json")

    data = json.loads(DEALS_FILE.read_text(encoding="utf-8"))
    deals = data.get("deals") if isinstance(data, dict) else data
    if not isinstance(deals, list):
        raise RuntimeError("deals.json does not contain a deals list")

    updated_at = str(data.get("updatedAt") or data.get("updated_at") or "") if isinstance(data, dict) else ""
    render_homepage(deals, updated_at)
    for config in CATEGORY_PAGES:
        render_category_page(deals, config)


if __name__ == "__main__":
    main()

