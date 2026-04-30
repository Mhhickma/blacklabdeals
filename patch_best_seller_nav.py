"""
Patch best-seller-deals.html so its navigation matches the main Black Lab Deals site style.
This script is idempotent and safe to run repeatedly.
"""

from pathlib import Path

PAGE = Path("best-seller-deals.html")
html = PAGE.read_text(encoding="utf-8")
original = html

# Add site-style nav CSS overrides without rewriting the whole page.
if "/* Site-matched navigation */" not in html:
    nav_css = """
  /* Site-matched navigation */
  .topbar { background:#fffbf0 !important; border-bottom:1px solid #f0e8d0 !important; padding:8px 24px !important; text-align:center !important; font-size:11px !important; color:#8a6a20 !important; letter-spacing:.02em !important; }
  nav { background:#fff !important; border-bottom:1px solid #e8e6e1 !important; position:sticky !important; top:0 !important; z-index:300 !important; }
  .nav-inner { max-width:1100px !important; margin:0 auto !important; padding:0 24px !important; height:128px !important; display:flex !important; align-items:center !important; justify-content:space-between !important; gap:16px !important; }
  .brand img { width:120px !important; height:120px !important; border-radius:50% !important; object-fit:cover !important; border:2px solid #c9a84c !important; }
  .brand-title { font-family:'Playfair Display',serif !important; font-size:20px !important; color:#1a1a18 !important; letter-spacing:-.5px !important; line-height:1.1 !important; }
  .nav-links { display:flex !important; align-items:center !important; justify-content:flex-end !important; gap:4px !important; flex-wrap:wrap !important; }
  .nav-links a { text-decoration:none !important; color:#6b6b65 !important; font-size:14px !important; font-weight:500 !important; padding:6px 10px !important; border-radius:8px !important; transition:color .15s, background .15s !important; }
  .nav-links a:hover, .nav-links a.active { color:#1a3a5c !important; background:#e8eef5 !important; }
  .nav-links .home-pill { display:inline-flex !important; align-items:center !important; gap:8px !important; padding:10px 14px !important; border-radius:999px !important; background:#1a3a5c !important; color:#fff !important; font-size:13px !important; font-weight:700 !important; }
  .nav-links .home-pill:hover { background:#2a5a8c !important; color:#fff !important; }
  @media(max-width:900px){ .nav-inner{height:auto !important; min-height:112px !important; align-items:flex-start !important; flex-direction:column !important; padding:12px 18px 16px !important;} .brand img{width:92px !important;height:92px !important;} .nav-links{justify-content:flex-start !important;} }
"""
    html = html.replace("</style>", nav_css + "\n</style>", 1)

old_nav = """<div class=\"topbar\">As an Amazon Associate, Black Lab Deals may earn from qualifying purchases.</div>
<nav>
  <div class=\"nav-inner\">
    <a class=\"brand\" href=\"/\">
      <img src=\"/logo.png\" alt=\"Black Lab Deals logo\">
      <div><div class=\"brand-title\">Black Lab <span>Deals</span></div><div style=\"font-size:12px;color:#9e9e97;text-transform:uppercase;letter-spacing:.05em\">Amazon price drops</div></div>
    </a>
    <div class=\"nav-links\">
      <a href=\"/\">All Deals</a>
      <a class=\"active\" href=\"/best-seller-deals.html\">Best Seller Deals</a>
    </div>
  </div>
</nav>"""

new_nav = """<div class=\"topbar\"><span>As an Amazon Associate, Black Lab Deals may earn from qualifying purchases.</span></div>
<nav>
  <div class=\"nav-inner\">
    <a class=\"brand\" href=\"/\">
      <img src=\"/logo.png\" alt=\"Black Lab Deals logo\">
      <div>
        <div class=\"brand-title\">Black Lab <span>Deals</span></div>
        <div style=\"font-size:12px;color:#9e9e97;text-transform:uppercase;letter-spacing:.05em;margin-top:3px\">Amazon price drops</div>
      </div>
    </a>
    <div class=\"nav-links\" aria-label=\"Main navigation\">
      <a href=\"/\">All Deals</a>
      <a class=\"active\" href=\"/best-seller-deals.html\">Best Seller Deals</a>
      <a href=\"/#hot-deals\">Hot Deals</a>
      <a href=\"/#categories\">Categories</a>
      <a class=\"home-pill\" href=\"/\">Back to Home</a>
    </div>
  </div>
</nav>"""

if old_nav in html:
    html = html.replace(old_nav, new_nav, 1)
elif "Back to Home" not in html and '<div class="nav-links">' in html:
    html = html.replace(
        '<div class="nav-links">',
        '<div class="nav-links" aria-label="Main navigation">',
        1,
    )
    html = html.replace(
        '<a class="active" href="/best-seller-deals.html">Best Seller Deals</a>',
        '<a class="active" href="/best-seller-deals.html">Best Seller Deals</a>\n      <a href="/#hot-deals">Hot Deals</a>\n      <a href="/#categories">Categories</a>\n      <a class="home-pill" href="/">Back to Home</a>',
        1,
    )

if html != original:
    PAGE.write_text(html, encoding="utf-8")
    print("Updated best-seller page navigation to match site style.")
else:
    print("Best-seller page navigation already matched or no change was needed.")
