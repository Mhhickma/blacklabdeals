"""
Patch homepage navigation with the Best Seller Deals internal link.
This script is safe to run repeatedly. If the link already exists, it changes nothing.
"""

from pathlib import Path

INDEX = Path("index.html")
DESKTOP_LINK = '<a href="/best-seller-deals.html">Best Seller Deals</a>'
MOBILE_LINK = '<a class="muted-link" href="/best-seller-deals.html">🏆 Best Seller Deals</a>'

html = INDEX.read_text(encoding="utf-8")
original = html

if "/best-seller-deals.html" not in html:
    if '<div class="desktop-nav">' in html:
        html = html.replace(
            '<div class="desktop-nav">',
            '<div class="desktop-nav">\n      ' + DESKTOP_LINK,
            1,
        )
    else:
        print("Warning: desktop nav container not found")

    if '<div class="drawer-links">' in html:
        html = html.replace(
            '<div class="drawer-links">',
            '<div class="drawer-links">\n        ' + MOBILE_LINK,
            1,
        )
    else:
        print("Warning: mobile drawer links container not found")

if html != original:
    INDEX.write_text(html, encoding="utf-8")
    print("Added Best Seller Deals link to homepage navigation.")
else:
    print("Homepage navigation already has Best Seller Deals link or no change was needed.")
