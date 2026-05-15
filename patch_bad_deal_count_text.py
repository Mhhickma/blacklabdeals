import re
from pathlib import Path

HTML_FILES = list(Path('.').glob('*.html')) + list(Path('.').glob('*/index.html'))
SECTION_HEAD_RE = re.compile(r'(<section class="section-head">.*?</div>)(.*?)(</section>)', re.DOTALL)
COUNT_RE = re.compile(r'(?:<div class="deal-count" id="deal-count">)?(?:Showing\s+)?([0-9,]+)\s+of\s+([0-9,]+)\s+deals(?:</div>)?', re.IGNORECASE)
changed = 0


def normalize_counter(match: re.Match) -> str:
    prefix, body, suffix = match.groups()
    count_match = COUNT_RE.search(body)
    if not count_match:
        return match.group(0)
    shown, total = count_match.groups()
    count_html = f'<div class="deal-count" id="deal-count">Showing {shown} of {total} deals</div>'
    return f'{prefix}{count_html}{suffix}'


for path in HTML_FILES:
    text = path.read_text(encoding='utf-8')
    new_text = SECTION_HEAD_RE.sub(normalize_counter, text)
    if new_text != text:
        path.write_text(new_text, encoding='utf-8')
        print(f'Fixed {path}')
        changed += 1

print(f'Fixed {changed} files')
