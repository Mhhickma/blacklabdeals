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


def patch_homepage():
    path = Path("index.html")
    text = path.read_text(encoding="utf-8")

    css_block = """/* BLD HOMEPAGE FASTER DEAL ACCESS START */
.hero{padding:30px 0 18px;gap:28px}.hero-text h1{font-size:clamp(30px,3.5vw,44px);margin-bottom:8px}.hero-text p{line-height:1.55}.signup-box{padding:22px}.stats-bar{margin:0 0 18px}.divider{margin:0 0 18px}.browse-pages-section{margin:22px 0}.hot-section{margin-top:0}@media(max-width:900px){.hero{padding:22px 0 16px;gap:18px}.signup-box{display:none}.stats-bar{display:none}.browse-pages-section{margin:18px 0}.hot-title{font-size:24px}}@media(max-width:600px){.hero-text h1{font-size:30px}.hero-pill{margin-bottom:10px}.hot-header{margin-bottom:10px}}
/* BLD HOMEPAGE FASTER DEAL ACCESS END */"""
    text = upsert_before(text, "</style>", css_block, "BLD HOMEPAGE FASTER DEAL ACCESS")

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
    patch_homepage()
    print("Moved supporting navigation/content below deal sections and compacted above-the-fold spacing.")


if __name__ == "__main__":
    main()
