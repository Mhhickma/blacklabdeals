"""Remove legacy public deal rendering and align page copy with Product Picks."""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent
EXCLUDED_PARTS = {".git", ".github", "__pycache__", "homepage-header-test"}

PHRASES = (
    ("Best Deals Found on Amazon Today", "Amazon Product Picks Today"),
    ("Top 100 Deals Found on Amazon Today", "Top 100 Amazon Product Picks"),
    ("Amazon Best Seller Deals Today", "Amazon Best Seller Product Picks"),
    ("Amazon Deal Categories", "Amazon Product Categories"),
    ("Popular Amazon Deal Categories", "Popular Amazon Product Categories"),
    ("Hot Deals", "Product Picks"),
    ("Hot Deal", "Product Pick"),
    ("hot deals", "product picks"),
    ("All Deals", "All Product Picks"),
    ("See Deal on Amazon", "View on Amazon"),
    ("See Deal", "View on Amazon"),
    ("Get Deal Alerts", "Get Product Alerts"),
    ("Get Deals", "Get Product Picks"),
    ("deal alerts", "product alerts"),
    ("Deal Watchlist", "Product Watchlist"),
    ("deal watchlist", "product watchlist"),
    ("Active deals", "Current products"),
    ("Hot deals", "Product picks"),
    ("Avg. discount", "Feed status"),
    ("Loading budget deals", "Loading product picks"),
    ("Search best seller deals", "Search best seller product picks"),
    ("Loading best seller deals", "Loading best seller product picks"),
    ("Amazon Best Sellers With Price Drops", "Amazon Best Seller Product Picks"),
    ("Today’s best Amazon", "Current Amazon"),
    ("Todayâ€™s best Amazon", "Current Amazon"),
    ("Best Deals Under $50 Found on Amazon", "Amazon Product Picks Under $50"),
    ("Best Amazon Deals Under $50 Today", "Amazon Product Picks Under $50"),
    ("Amazon Deal Event Deals Today", "Amazon Shopping Event Product Picks"),
    ("Amazon Deals Under $50 for Deal Events", "Amazon Product Picks Under $50 for Shopping Events"),
    ("Amazon Device Deals for Deal Events", "Amazon Device Product Picks for Shopping Events"),
    ("Amazon Electronics Deals for Deal Events", "Amazon Electronics Product Picks for Shopping Events"),
    ("Amazon Home &amp; Kitchen Deals for Deal Events", "Amazon Home &amp; Kitchen Product Picks for Shopping Events"),
    ("Amazon Tool Deals for Deal Events", "Amazon Tool Product Picks for Shopping Events"),
    ("Product Product Picks", "Product Picks"),
    ("price drops", "current product information"),
    ("price drop", "current product information"),
    ("limited-time discounts", "current product picks"),
    ("discounts", "product picks"),
    ("strongest deals first", "current product picks clearly"),
    ("Loading todayâ€™s top 100 deals found on Amazon", "Loading current Amazon product picks"),
    ("daily deals", "daily product picks"),
)


def reconcile(text):
    text = re.sub(
        r"<!-- BLD STATIC DEALS START -->.*?<!-- BLD STATIC DEALS END -->",
        "<!-- Product cards load from the current approved public feed. -->",
        text,
        flags=re.S,
    )
    text = re.sub(
        r"<script(?![^>]*src=)[^>]*>.*?</script>",
        lambda match: "" if re.search(r"d\.was|d\.pct|discount_desc|renderHotDeals|card-badge-hot", match.group(0), flags=re.I) else match.group(0),
        text,
        flags=re.I | re.S,
    )
    text = re.sub(r'<a\s+class="(?:hot-card|deal-card)[^"]*"[^>]*>.*?</a>', "", text, flags=re.I | re.S)
    text = re.sub(r"\sdata-deal-discount=\"[^\"]*\"", "", text, flags=re.I)
    text = text.replace('<option value="discount">Biggest discount</option>', '')
    text = text.replace('<option value="rank">Best seller rank</option>', '<option value="price-high">Price: high to low</option>')
    for old, new in PHRASES:
        text = text.replace(old, new)
    text = re.sub(r"Best Amazon ([^<|]+?) Deals Today", r"Amazon \1 Product Picks", text)
    text = re.sub(r"Best ([^<|]+?) Deals Found on Amazon Today", r"Amazon \1 Product Picks", text)
    return text


def repair_homepage(text):
    if "<body" in text:
        return text
    body = """</head><body data-bld-homepage="true" data-mode="all">
<div id="site-header"></div>
<main class="page-shell">
  <section class="hero"><div class="hero-text"><div class="hero-pill" id="hero-pill">Loading current product picks...</div><h1>Amazon Product Picks Today</h1><p>Browse current Amazon product picks organized by category, with product information refreshed from the approved public feed.</p></div></section>
  <section class="hot-strip"><div class="section-head"><h2>Current Amazon Product Picks</h2><div class="deal-count" id="deal-count">Loading</div></div><div class="status-line" id="status-line">Loading current product picks...</div><div class="hot-grid" id="hot-grid" aria-live="polite"></div></section>
  <section class="seo-content"><h2>Helpful Shopping Guidance</h2><p>Use the category shortcuts to narrow the product list, then review the current product information before visiting Amazon.</p></section>
</main>
<footer class="bld-site-footer"><div class="footer-inner"><p class="footer-disclosure">As an Amazon Associate, Black Lab Deals may earn from qualifying purchases.</p></div></footer>
"""
    marker = '<script src="/site-header.js'
    if marker in text:
        return text.replace(marker, body + marker, 1)
    return text.replace("</body>", body + "</body>", 1)


changed = 0
for path in ROOT.rglob("*.html"):
    if any(part in EXCLUDED_PARTS for part in path.parts):
        continue
    original = path.read_text(encoding="utf-8", errors="ignore")
    updated = reconcile(original)
    if path.name == "index.html" and path.parent == ROOT:
        updated = repair_homepage(updated)
        updated = updated.replace('<body data-bld-homepage="true">', '<body data-bld-homepage="true" data-mode="all">')
        if '<meta name="description"' not in updated:
            updated = updated.replace(
                "</title>",
                '</title><meta name="description" content="Browse current Amazon product picks organized by category. Confirm final price and availability on Amazon.">',
                1,
            )
        if "/site-common.js" not in updated:
            updated = updated.replace("</body>", '<script src="/site-common.js?v=3" defer></script></body>')
    if path.name == "search.html":
        updated = re.sub(
            r'<meta\s+name="robots"\s+content="[^"]*">',
            '<meta name="robots" content="noindex, follow">',
            updated,
            count=1,
            flags=re.I,
        )
    if updated != original:
        path.write_text(updated, encoding="utf-8")
        changed += 1

print(f"Reconciled {changed} public HTML pages")
