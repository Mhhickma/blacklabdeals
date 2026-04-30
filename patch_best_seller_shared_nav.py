"""
Update best-seller-deals.html to use the shared navigation file.
The page will load navigation from shared-navigation.js using <div id="site-navigation"></div>.
"""

from pathlib import Path
import re

PAGE = Path("best-seller-deals.html")
html = PAGE.read_text(encoding="utf-8")
original = html

shared_block = '<div id="site-navigation"></div>\n<script src="/shared-navigation.js"></script>'

# Replace the existing local disclosure/nav block with shared nav mount point.
html = re.sub(
    r'<div class="(?:topbar|disclosure-bar)">.*?</nav>',
    shared_block,
    html,
    count=1,
    flags=re.DOTALL,
)

# Make sure the shared nav script is not duplicated.
html = re.sub(
    r'(?:<div id="site-navigation"></div>\s*)?(?:<script src="/shared-navigation\.js"></script>\s*)+',
    shared_block + '\n',
    html,
    count=1,
)

# Remove old page-specific nav overrides so shared-navigation.js is the source of truth.
html = re.sub(
    r'\n\s*/\* Site-matched navigation \*/.*?@media\(max-width:900px\).*?\}\s*\}\s*',
    '\n',
    html,
    flags=re.DOTALL,
)

# Mark version for easy verification in page source.
if '<!-- shared nav enabled -->' not in html:
    html = html.replace('<body>', '<body>\n<!-- shared nav enabled -->', 1)

if html != original:
    PAGE.write_text(html, encoding="utf-8")
    print("best-seller-deals.html now uses shared-navigation.js")
else:
    print("No change needed; page already uses shared navigation")
