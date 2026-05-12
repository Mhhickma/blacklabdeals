from pathlib import Path


def replace_between(text, start, end, replacement):
    start_i = text.find(start)
    if start_i == -1:
        raise SystemExit(f"Missing marker: {start}")
    end_i = text.find(end, start_i)
    if end_i == -1:
        raise SystemExit(f"Missing end marker after: {start}")
    return text[:start_i] + replacement + text[end_i:]


def upsert_before(text, marker, block, label):
    start = f"/* {label} START */"
    end = f"/* {label} END */"
    if start in text and end in text:
        before = text[:text.find(start)]
        after = text[text.find(end) + len(end):]
        return before + block + after
    idx = text.find(marker)
    if idx == -1:
        raise SystemExit(f"Missing insertion marker: {marker}")
    return text[:idx] + block + "\n" + text[idx:]


def patch_site_common_js():
    path = Path("site-common.js")
    text = path.read_text(encoding="utf-8")
    old_start = "function ensureBrowseSection() {"
    old_end = "async function fetchDealsFeed() {"
    new_block = """function ensureBrowseSection() {
  const main = document.querySelector('main.page-shell') || document.querySelector('main');
  if (!main) return;

  let nav = document.querySelector('.popular-category-nav');
  if (!nav) {
    const holder = document.createElement('div');
    holder.innerHTML = renderPopularCategoryNav();
    nav = holder.firstElementChild;
  }

  markCurrentPopularCategoryNav(nav);

  const dealSection = main.querySelector('.hot-strip');
  const intro = main.querySelector('.category-intro-section');
  if (dealSection && dealSection.parentNode) {
    let afterDeals = dealSection;
    if (intro && intro !== dealSection.nextElementSibling) {
      dealSection.insertAdjacentElement('afterend', intro);
      afterDeals = intro;
    } else if (intro) {
      afterDeals = intro;
    }
    afterDeals.insertAdjacentElement('afterend', nav);
    return;
  }

  const fallback = main.querySelector('.section-head') || main.querySelector('.filter-row');
  if (fallback && fallback.parentNode) {
    fallback.parentNode.insertBefore(nav, fallback);
  } else {
    main.appendChild(nav);
  }
}

"""
    text = replace_between(text, old_start, old_end, new_block)
    path.write_text(text, encoding="utf-8")


def patch_site_common_css():
    path = Path("site-common.css")
    text = path.read_text(encoding="utf-8")
    block = """/* BLD FASTER DEAL ACCESS START */
.page-shell{padding-top:18px}.breadcrumbs{margin-bottom:12px;padding:8px 12px}.hero{margin-bottom:14px}.hero-inner{gap:20px;align-items:start}.hero h1{font-size:clamp(32px,4.2vw,50px);margin-bottom:8px}.hero p{font-size:15px;line-height:1.55}.hero-image-wrap{max-width:460px;justify-self:end}.stats-bar{margin:4px 0 14px;gap:24px}.divider{margin-bottom:14px}.section-head{margin-bottom:10px}.filter-row{margin-bottom:12px}.hot-strip{padding:18px;margin-bottom:22px}.category-intro-section{margin:22px 0 18px;padding:14px 0}.popular-category-nav{margin:24px auto;padding:18px 0}.popular-category-nav-head{margin-bottom:12px}.popular-category-grid{gap:8px 10px}.popular-category-link{padding:10px 12px}@media(max-width:900px){.bld-header-main{min-height:86px;padding:12px 16px}.bld-brand-logo{width:64px;height:64px}.bld-brand-title{font-size:24px}.bld-brand-tagline{font-size:11px}.page-shell{padding-top:14px}.hero-image-wrap{display:none}.stats-bar{gap:16px}.hot-strip{padding:14px}}@media(max-width:520px){.hero h1{font-size:32px}.hero p{font-size:14px}.category-intro-section{margin:18px 0 14px}.popular-category-nav{margin:20px auto}}
/* BLD FASTER DEAL ACCESS END */"""
    text = upsert_before(text, "\n", block, "BLD FASTER DEAL ACCESS")
    path.write_text(text, encoding="utf-8")


def patch_site_nav_css():
    path = Path("site-nav.css")
    text = path.read_text(encoding="utf-8")
    block = """/* BLD UNIVERSAL MOBILE DEAL CARDS START */
@media (max-width: 760px) {
  .hot-grid,
  .deals-grid,
  .best-seller-grid,
  .search-results-grid {
    display: grid !important;
    grid-template-columns: 1fr !important;
    gap: 10px !important;
  }

  .hot-card,
  .deal-card,
  .best-seller-card,
  .search-card {
    display: grid !important;
    grid-template-columns: 112px minmax(0, 1fr) !important;
    min-height: 132px !important;
    border-radius: 10px !important;
    overflow: hidden !important;
  }

  .hot-card-img,
  .card-img,
  .deal-card .card-img,
  .best-seller-img,
  .search-img {
    width: 112px !important;
    min-width: 112px !important;
    height: 100% !important;
    min-height: 132px !important;
    border-bottom: 0 !important;
  }

  .hot-card-img img,
  .card-img img,
  .deal-card .card-img img,
  .best-seller-img img,
  .search-img img {
    width: 100% !important;
    height: 100% !important;
    max-width: none !important;
    max-height: none !important;
    object-fit: contain !important;
    padding: 8px !important;
  }

  .hot-card-body,
  .hot-card .card-body,
  .card-body,
  .deal-card .card-body,
  .best-seller-body,
  .search-body {
    min-width: 0 !important;
    padding: 10px 10px 10px 12px !important;
    display: flex !important;
    flex-direction: column !important;
    gap: 5px !important;
  }

  .hot-card-title,
  .card-title,
  .deal-card .card-title,
  .best-seller-title,
  .search-title {
    display: -webkit-box !important;
    -webkit-box-orient: vertical !important;
    -webkit-line-clamp: 2 !important;
    overflow: hidden !important;
    font-size: 13px !important;
    font-weight: 800 !important;
    line-height: 1.3 !important;
    margin: 0 !important;
    padding: 0 !important;
  }

  .hot-card-prices,
  .price-block,
  .card-footer,
  .deal-card .card-footer,
  .best-seller-price-row,
  .search-price-row {
    display: flex !important;
    align-items: center !important;
    justify-content: flex-start !important;
    gap: 6px !important;
    flex-wrap: wrap !important;
    border-top: 0 !important;
    padding-top: 0 !important;
    margin-top: 0 !important;
  }

  .hot-price-now,
  .price-now,
  .best-seller-price,
  .search-price {
    font-size: 18px !important;
    line-height: 1.1 !important;
    color: var(--red, #c94040) !important;
  }

  .hot-price-was,
  .price-was,
  .best-seller-was,
  .search-was {
    font-size: 11px !important;
  }

  .hot-btn,
  .btn-deal,
  .deal-card .btn-deal,
  .best-seller-btn,
  .search-btn {
    min-height: 40px !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    width: 100% !important;
    border-radius: 8px !important;
    font-size: 12px !important;
    line-height: 1.1 !important;
    margin-top: auto !important;
    padding: 0 8px !important;
    text-align: center !important;
  }

  .hot-off,
  .discount-badge {
    font-size: 11px !important;
    padding: 3px 7px !important;
    background: #ebfff5 !important;
    border: 1px solid #cdebdc !important;
    color: var(--green, #12744a) !important;
    border-radius: 999px !important;
  }

  .category-pill,
  .stars,
  .card-category,
  .card-desc,
  .deal-card .card-category,
  .deal-card .card-desc,
  .best-seller-category,
  .best-seller-badges,
  .search-meta {
    display: none !important;
  }
}

@media (max-width: 380px) {
  .hot-card,
  .deal-card,
  .best-seller-card,
  .search-card {
    grid-template-columns: 100px minmax(0, 1fr) !important;
  }

  .hot-card-img,
  .card-img,
  .deal-card .card-img,
  .best-seller-img,
  .search-img {
    width: 100px !important;
    min-width: 100px !important;
  }
}
/* BLD UNIVERSAL MOBILE DEAL CARDS END */"""
    text = upsert_before(text, "\n", block, "BLD UNIVERSAL MOBILE DEAL CARDS")
    path.write_text(text, encoding="utf-8")


def patch_homepage():
    path = Path("index.html")
    text = path.read_text(encoding="utf-8")

    css_block = """/* BLD HOMEPAGE FASTER DEAL ACCESS START */
.hero{padding:30px 0 18px;gap:28px}.hero-text h1{font-size:clamp(30px,3.5vw,44px);margin-bottom:8px}.hero-text p{line-height:1.55}.signup-box{padding:22px}.stats-bar{margin:0 0 18px}.divider{margin:0 0 18px}.browse-pages-section{margin:22px 0}.hot-section{margin-top:0}@media(max-width:900px){.hero{padding:22px 0 16px;gap:18px}.signup-box{display:none}.stats-bar{display:none}.browse-pages-section{margin:18px 0}.hot-title{font-size:24px}}@media(max-width:600px){.hero-text h1{font-size:30px}.hero-pill{margin-bottom:10px}.hot-header{margin-bottom:10px}}
/* BLD HOMEPAGE FASTER DEAL ACCESS END */"""
    text = upsert_before(text, "</style>", css_block, "BLD HOMEPAGE FASTER DEAL ACCESS")

    old_render = """function renderHotDeals() {
  const hot = allDeals.filter(d => Number(d.pct) >= 40 || d.hot);
  document.getElementById('hot-count-pill').textContent = hot.length + ' deal' + (hot.length !== 1 ? 's' : '');

  document.getElementById('hot-grid').innerHTML = hot.length
    ? hot.map(d => {
      return '<a class=\"hot-card\" href=\"' + escUrl(d.link || '#') + '\" target=\"_blank\" rel=\"noopener sponsored\">' +
        '<div class=\"hot-card-img\">' + img(d.image, d.emoji, 'hot') + '</div>' +
        '<span class=\"hot-card-badge\">' + esc(d.discount || (d.pct + '% off')) + '</span>' +
        '<div class=\"card-body\">' +
          '<div class=\"hot-card-title\">' + esc(d.title) + '</div>' +
          '<div class=\"price-block\">' +
            '<span class=\"hot-price-now\">' + esc(d.price || 'See price') + '</span>' +
            (d.was ? '<span class=\"hot-price-was\">' + esc(d.was) + '</span>' : '') +
          '</div>' +
          '<button class=\"hot-btn\">See Deal on Amazon →</button>' +
        '</div>' +
      '</a>';
    }).join('')
    : '<div class=\"loading-bar\" style=\"grid-column:1/-1;\">No hot deals yet — check back soon.</div>';
}

"""
    new_render = """function renderHotDeals() {
  const hot = allDeals.filter(d => Number(d.pct) >= 40 || d.hot);
  document.getElementById('hot-count-pill').textContent = hot.length + ' deal' + (hot.length !== 1 ? 's' : '');

  document.getElementById('hot-grid').innerHTML = hot.length
    ? hot.map(d => {
      const offText = d.discount || (d.pct ? d.pct + '% off' : 'Hot Deal');
      return '<a class=\"hot-card\" href=\"' + escUrl(d.link || '#') + '\" target=\"_blank\" rel=\"noopener sponsored\">' +
        '<div class=\"hot-card-img\">' + img(d.image, d.emoji, 'hot') + '<div class=\"hot-card-badge\">Hot Deal</div></div>' +
        '<div class=\"hot-card-body\">' +
          '<div class=\"hot-card-title\">' + esc(d.title) + '</div>' +
          '<div class=\"hot-card-prices\">' +
            '<span class=\"hot-price-now\">' + esc(d.price || 'See price') + '</span>' +
            (d.was ? '<span class=\"hot-price-was\">' + esc(d.was) + '</span>' : '') +
            '<span class=\"hot-off\">' + esc(offText).replace('-', '') + '</span>' +
          '</div>' +
          '<span class=\"hot-btn\">See Deal on Amazon →</span>' +
        '</div>' +
      '</a>';
    }).join('')
    : '<div class=\"loading-bar\" style=\"grid-column:1/-1;\">No hot deals yet — check back soon.</div>';
}

"""
    if old_render in text:
        text = text.replace(old_render, new_render)
    elif "function renderHotDeals()" in text and "hot-card-body" not in text:
        text = replace_between(text, "function renderHotDeals() {", "function getFilteredDeals() {", new_render)

    helper = """function moveHomepageBrowseBelowDeals() {
  const browse = document.querySelector('.browse-pages-section');
  const deals = document.getElementById('hot-section') || document.getElementById('deals-section');
  if (browse && deals && deals.parentNode) deals.insertAdjacentElement('afterend', browse);
}

"""
    if "function moveHomepageBrowseBelowDeals()" not in text:
        anchor = "setInterval(loadDeals, 30 * 60 * 1000);"
        idx = text.find(anchor)
        if idx == -1:
            raise SystemExit("Missing homepage load interval anchor")
        text = text[:idx] + helper + text[idx:]
    if "moveHomepageBrowseBelowDeals();\nloadDeals();" not in text:
        text = text.replace("loadDeals();\n</script>", "moveHomepageBrowseBelowDeals();\nloadDeals();\n</script>")

    path.write_text(text, encoding="utf-8")


def main():
    patch_site_common_js()
    patch_site_common_css()
    patch_site_nav_css()
    patch_homepage()
    print("Normalized mobile deal cards across homepage, category, best-seller, and search pages.")


if __name__ == "__main__":
    main()
