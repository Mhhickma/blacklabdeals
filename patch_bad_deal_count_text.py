import re
from pathlib import Path

HTML_FILES = list(Path('.').glob('*.html')) + list(Path('.').glob('*/index.html'))
pattern = re.compile(r'>h of ([0-9,]+) deals<')
changed = 0

for path in HTML_FILES:
    text = path.read_text(encoding='utf-8')
    new_text = pattern.sub(r'>Showing 50 of \1 deals<', text)
    if new_text != text:
        path.write_text(new_text, encoding='utf-8')
        print(f'Fixed {path}')
        changed += 1

print(f'Fixed {changed} files')
