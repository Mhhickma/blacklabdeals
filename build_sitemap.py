"""Build sitemap.xml from indexable canonical HTML pages."""

import html
import re
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SITE = "https://blacklabdeals.com"
EXCLUDED_PARTS = {".git", ".github", "__pycache__", "homepage-header-test"}


def canonical_for(path, text):
    match = re.search(r'<link\s+rel="canonical"\s+href="([^"]+)"', text, re.I)
    if match:
        return match.group(1)
    relative = path.relative_to(ROOT).as_posix()
    if relative == "index.html":
        return f"{SITE}/"
    if relative.endswith("/index.html"):
        return f"{SITE}/{relative[:-10]}"
    return f"{SITE}/{relative}"


urls = []
for path in ROOT.rglob("*.html"):
    if any(part in EXCLUDED_PARTS for part in path.parts):
        continue
    text = path.read_text(encoding="utf-8", errors="ignore")
    if re.search(r'<meta\s+name="robots"\s+content="[^"]*noindex', text, re.I):
        continue
    canonical = canonical_for(path, text)
    if canonical.endswith("/search.html"):
        continue
    modified = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).date().isoformat()
    urls.append((canonical, modified))

urls.sort(key=lambda item: (item[0] != f"{SITE}/", item[0]))
lines = ['<?xml version="1.0" encoding="UTF-8"?>', '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
for url, modified in urls:
    lines.extend(
        [
            "  <url>",
            f"    <loc>{html.escape(url)}</loc>",
            f"    <lastmod>{modified}</lastmod>",
            "  </url>",
        ]
    )
lines.append("</urlset>")
(ROOT / "sitemap.xml").write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"Built sitemap.xml with {len(urls)} URLs")
