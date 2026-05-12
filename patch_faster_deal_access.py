from pathlib import Path


def upsert_css_block(text, label, block):
    start = f"/* {label} START */"
    end = f"/* {label} END */"
    if start in text and end in text:
        before = text[:text.find(start)]
        after = text[text.find(end) + len(end):]
        text = before.rstrip() + "\n\n" + after.lstrip()
    return text.rstrip() + "\n\n" + block.strip() + "\n"


def patch_site_nav_css():
    path = Path("site-nav.css")
    text = path.read_text(encoding="utf-8")
    block = """/* BLD MOBILE DEAL CARD WIDTH FIX START */
@media (max-width: 760px) {
  .hot-grid,
  .deals-grid,
  .best-seller-grid,
  .search-results-grid {
    width: 100% !important;
    align-items: stretch !important;
  }

  .hot-grid > .hot-card,
  .deals-grid > .deal-card,
  .best-seller-grid > .best-seller-card,
  .search-results-grid > .search-card,
  .hot-card,
  .deal-card,
  .best-seller-card,
  .search-card {
    width: 100% !important;
    max-width: none !important;
    min-width: 0 !important;
    justify-self: stretch !important;
  }

  .hot-card > .hot-card-badge {
    display: none !important;
  }

  .hot-card .hot-off,
  .deal-card .discount-badge {
    display: inline-flex !important;
    align-items: center !important;
    white-space: nowrap !important;
    background: #ebfff5 !important;
    border: 1px solid #cdebdc !important;
    color: var(--green, #12744a) !important;
  }
}
/* BLD MOBILE DEAL CARD WIDTH FIX END */"""
    path.write_text(upsert_css_block(text, "BLD MOBILE DEAL CARD WIDTH FIX", block), encoding="utf-8")


def main():
    patch_site_nav_css()
    print("Forced mobile deal cards to stretch full width and removed the old floating percent badge.")


if __name__ == "__main__":
    main()
