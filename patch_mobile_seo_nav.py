from pathlib import Path


def upsert_css_block(text, marker, block_name, block):
    start = f"/* {block_name} START */"
    end = f"/* {block_name} END */"
    if start in text and end in text:
        before = text[:text.find(start)]
        after = text[text.find(end) + len(end):]
        return before + block + after
    idx = text.find(marker)
    if idx == -1:
        raise SystemExit(f"Missing CSS insertion marker: {marker}")
    return text[:idx] + block + "\n" + text[idx:]


def patch_site_common_js():
    path = Path("site-common.js")
    text = path.read_text(encoding="utf-8")
    block = r'''function ensureMobileDealNav() {
  if (document.querySelector('.bld-mobile-deal-nav')) return;
  const main = document.querySelector('main.page-shell') || document.querySelector('main');
  if (!main) return;

  const nav = document.createElement('nav');
  nav.className = 'bld-mobile-deal-nav';
  nav.setAttribute('aria-label', 'Quick deal navigation');
  nav.innerHTML = '<button type="button" data-scroll-target="deals">Deals</button><button type="button" data-filter-target="hot">Hot</button><a href="/best-amazon-deals-under-50/">Under $50</a><button type="button" data-scroll-target="categories">Categories</button><a href="/top-100-amazon-deals-today/">Top 100</a>';

  const anchor = main.querySelector('.breadcrumbs');
  if (anchor && anchor.parentNode) {
    anchor.insertAdjacentElement('afterend', nav);
  } else {
    main.insertBefore(nav, main.firstChild);
  }

  nav.addEventListener('click', event => {
    const item = event.target.closest('button,a');
    if (!item || item.tagName === 'A') return;
    const filter = item.dataset.filterTarget;
    const scrollTarget = item.dataset.scrollTarget;
    if (filter) {
      const filterButton = document.querySelector(`.filter-btn[data-filter="${filter}"], .filter-btn[data-cat="${filter}"]`);
      if (filterButton) {
        filterButton.click();
        const dealSection = document.querySelector('.hot-strip,#hot-section,#deals-section');
        if (dealSection) dealSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
        return;
      }
    }
    const target = scrollTarget === 'categories'
      ? document.querySelector('.popular-category-nav,.browse-pages-section,.related-deal-pages')
      : document.querySelector('.hot-strip,#hot-section,#deals-section');
    if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
}

'''
    if "function ensureMobileDealNav()" not in text:
        idx = text.find("async function fetchDealsFeed() {")
        if idx == -1:
            raise SystemExit("Missing fetchDealsFeed anchor")
        text = text[:idx] + block + text[idx:]
    if "ensureMobileDealNav();\nloadDeals();" not in text:
        text = text.replace("initFilters();\nloadDeals();", "initFilters();\nensureMobileDealNav();\nloadDeals();")
    path.write_text(text, encoding="utf-8")


def patch_site_common_css():
    path = Path("site-common.css")
    text = path.read_text(encoding="utf-8")
    block = """/* BLD MOBILE SEO NAV START */
.bld-mobile-deal-nav{display:none}@media(max-width:760px){.bld-mobile-deal-nav{position:sticky;top:74px;z-index:260;display:flex;gap:8px;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none;margin:0 -16px 12px;padding:8px 16px;background:rgba(249,248,245,.96);border-top:1px solid var(--border);border-bottom:1px solid var(--border);box-shadow:0 6px 16px rgba(26,58,92,.08)}.bld-mobile-deal-nav::-webkit-scrollbar,.filter-row::-webkit-scrollbar,.filters-scroll::-webkit-scrollbar{display:none}.bld-mobile-deal-nav a,.bld-mobile-deal-nav button{flex:0 0 auto;border:1px solid var(--border);border-radius:999px;background:var(--surface);color:var(--accent);font-family:'DM Sans',sans-serif;font-size:12px;font-weight:900;line-height:1;padding:10px 12px;white-space:nowrap;text-decoration:none}.bld-mobile-deal-nav button:first-child{background:var(--accent);border-color:var(--accent);color:#fff}.filter-row,.filters-scroll{display:flex;flex-wrap:nowrap;gap:8px;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none;margin-left:-16px;margin-right:-16px;padding:0 16px 8px}.filter-btn{white-space:nowrap}.hero{margin-bottom:10px}.hero h1{font-size:30px}.hero p{font-size:14px}.section-head h2{font-size:22px}.status-line{margin-bottom:10px}.hot-grid{gap:12px}.hot-card-img{height:138px}.popular-category-nav-head p{font-size:13px}.popular-category-link small{display:none}}@media(max-width:520px){.bld-mobile-deal-nav{top:68px}.breadcrumbs{display:none}.page-shell{padding-top:10px}.hero-pill{margin-bottom:8px}.stats-bar{display:none}.hot-strip{border-radius:12px}.load-more-wrap{margin-top:14px}.load-more-btn{width:100%;max-width:320px}}
/* BLD MOBILE SEO NAV END */"""
    text = upsert_css_block(text, "\n", "BLD MOBILE SEO NAV", block)
    path.write_text(text, encoding="utf-8")


def patch_homepage():
    path = Path("index.html")
    text = path.read_text(encoding="utf-8")
    css = """/* BLD HOMEPAGE MOBILE NAV START */
.mobile-deal-nav{display:none}@media(max-width:760px){.mobile-deal-nav{position:sticky;top:68px;z-index:260;display:flex;gap:8px;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none;margin:0 -16px 12px;padding:8px 16px;background:rgba(249,248,245,.96);border-top:1px solid var(--border);border-bottom:1px solid var(--border);box-shadow:0 6px 16px rgba(26,58,92,.08)}.mobile-deal-nav::-webkit-scrollbar,.filters-scroll::-webkit-scrollbar{display:none}.mobile-deal-nav a,.mobile-deal-nav button{flex:0 0 auto;border:1px solid var(--border);border-radius:999px;background:var(--surface);color:var(--accent);font-family:'DM Sans',sans-serif;font-size:12px;font-weight:900;line-height:1;padding:10px 12px;white-space:nowrap;text-decoration:none}.mobile-deal-nav button:first-child{background:var(--accent);border-color:var(--accent);color:#fff}.filters-scroll{flex-wrap:nowrap;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none;margin-left:-16px;margin-right:-16px;padding:0 16px 8px}.filter-btn{white-space:nowrap}.toolbar{margin-bottom:12px}.search-box input{box-shadow:none}.hot-strip,.deals-grid{padding:12px;border-radius:12px}.hot-card-img{height:118px}.deal-card .card-img{width:104px;min-width:104px;height:104px}.hero-text p:nth-of-type(2){display:none}}
/* BLD HOMEPAGE MOBILE NAV END */"""
    text = upsert_css_block(text, "</style>", "BLD HOMEPAGE MOBILE NAV", css)

    helper = r'''function ensureHomepageMobileNav() {
  if (document.querySelector('.mobile-deal-nav')) return;
  const main = document.querySelector('main.page-shell');
  if (!main) return;
  const nav = document.createElement('nav');
  nav.className = 'mobile-deal-nav';
  nav.setAttribute('aria-label', 'Quick deal navigation');
  nav.innerHTML = '<button type="button" data-jump="hot">Hot Deals</button><button type="button" data-jump="all">All Deals</button><button type="button" data-cat="All">All</button><a href="/best-amazon-deals-under-50/">Under $50</a><a href="/top-100-amazon-deals-today/">Top 100</a><button type="button" data-jump="categories">Categories</button>';
  main.insertBefore(nav, main.firstChild);
  nav.addEventListener('click', event => {
    const item = event.target.closest('button,a');
    if (!item || item.tagName === 'A') return;
    if (item.dataset.cat) {
      const btn = [...document.querySelectorAll('.filter-btn')].find(b => b.dataset.cat === item.dataset.cat);
      if (btn) btn.click();
    }
    const target = item.dataset.jump === 'categories'
      ? document.querySelector('.browse-pages-section,.related-deal-pages')
      : item.dataset.jump === 'all'
        ? document.getElementById('deals-section')
        : document.getElementById('hot-section');
    if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
}

'''
    if "function ensureHomepageMobileNav()" not in text:
        anchor = "function moveHomepageBrowseBelowDeals() {"
        idx = text.find(anchor)
        if idx == -1:
            raise SystemExit("Missing homepage browse helper anchor")
        text = text[:idx] + helper + text[idx:]
    if "ensureHomepageMobileNav();\nmoveHomepageBrowseBelowDeals();" not in text:
        text = text.replace("moveHomepageBrowseBelowDeals();\nloadDeals();", "ensureHomepageMobileNav();\nmoveHomepageBrowseBelowDeals();\nloadDeals();")
    path.write_text(text, encoding="utf-8")


def main():
    patch_site_common_js()
    patch_site_common_css()
    patch_homepage()
    print("Added mobile quick navigation, swipe filters, and compact mobile deal layout.")


if __name__ == "__main__":
    main()
