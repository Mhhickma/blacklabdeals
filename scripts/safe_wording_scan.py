#!/usr/bin/env python3
"""Scan public site files for risky Amazon wording and public JSON fields.

This guard is intentionally conservative. It focuses on visible page copy,
product-card wording, and public JSON field names that should not be published.
Internal docs, workflow files, and utility scripts are excluded so the scanner can
mention risky terms in this file without failing itself.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_EXTENSIONS = {'.html', '.js', '.css', '.json'}
EXCLUDED_DIRS = {
    '.git',
    '.github',
    'node_modules',
    'scripts',
    'docs',
    '__pycache__',
}
EXCLUDED_FILES = {
    'Best-Practices-for-Pages.txt',
    'Every-page-has-analytics-installed.txt',
    'Site Appearance Feedback.txt',
}

RISKY_TEXT_PATTERNS = [
    (re.compile(r'\bhot\s+deals?\b', re.I), 'Use Product Picks instead of Hot Deal/Hot Deals.'),
    (re.compile(r'\bavg\.?\s+discount\b', re.I), 'Remove average discount claims.'),
    (re.compile(r'\baverage\s+discount\b', re.I), 'Remove average discount claims.'),
    (re.compile(r'\bbiggest\s+discounts?\b', re.I), 'Remove biggest discount claims.'),
    (re.compile(r'\bverified\s+price\s+drop\b', re.I), 'Remove verified price-drop claims.'),
    (re.compile(r'\bprice\s+dropped\b', re.I), 'Remove price-dropped claims.'),
    (re.compile(r'\bprice\s+drops?\b', re.I), 'Use current product information wording instead of price drops.'),
    (re.compile(r'\blowest\s+prices?\b', re.I), 'Remove lowest-price claims.'),
    (re.compile(r'\bwas\s+price\b', re.I), 'Remove was-price wording.'),
    (re.compile(r'\bold\s+price\b', re.I), 'Remove old-price wording.'),
    (re.compile(r'\bsee\s+deal\b', re.I), 'Use View on Amazon instead of See Deal.'),
    (re.compile(r'\bget\s+deal\b', re.I), 'Use View on Amazon instead of Get Deal.'),
    (re.compile(r'\bclaim\s+deal\b', re.I), 'Use View on Amazon instead of Claim Deal.'),
    (re.compile(r'\bgrab\s+discount\b', re.I), 'Use View on Amazon instead of Grab Discount.'),
    (re.compile(r'\bdeal\s+score\b', re.I), 'Remove deal-score wording.'),
    (re.compile(r'\b\d{1,3}\s*%\s+off\b', re.I), 'Remove percent-off claims.'),
    (re.compile(r'\b%\s+off\b', re.I), 'Remove percent-off claims.'),
    (re.compile(r'\bstrikethrough\b', re.I), 'Remove strikethrough pricing references from public files.'),
]

RISKY_JSON_KEYS = {
    'was',
    'old_price',
    'list_price',
    'strikethrough_price',
    'savings',
    'pct',
    'percent_off',
    'discount',
    'discount_amount',
    'discount_percent',
    'deal_type',
    'price_drop',
    'drop_amount',
    'drop_percent',
    'hot',
    'hotDeal',
    'hotDeals',
    'couponDeals',
    'hasCoupon',
    'couponDisplay',
    'dealReasons',
    'keepaAvg30',
    'keepaMin30',
    'keepaMax30',
    'keepaCurrent',
    'keepaStats',
    'lowestPrice',
    'lowest_price',
    'ytd_low',
    'avg_price',
}


def is_public_file(path: Path) -> bool:
    rel = path.relative_to(ROOT)
    if any(part in EXCLUDED_DIRS for part in rel.parts):
        return False
    if path.name in EXCLUDED_FILES:
        return False
    if path.suffix not in PUBLIC_EXTENSIONS:
        return False
    return True


def line_number(text: str, offset: int) -> int:
    return text.count('\n', 0, offset) + 1


def scan_text(path: Path, text: str) -> list[str]:
    findings: list[str] = []
    for pattern, message in RISKY_TEXT_PATTERNS:
        for match in pattern.finditer(text):
            line = line_number(text, match.start())
            snippet = text[match.start():match.end()]
            findings.append(f'{path.relative_to(ROOT)}:{line}: "{snippet}" — {message}')
    return findings


def scan_json_keys(path: Path, value: Any, trail: str = '$') -> list[str]:
    findings: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key in RISKY_JSON_KEYS:
                findings.append(f'{path.relative_to(ROOT)}:{trail}.{key}: public JSON key is not approved')
            findings.extend(scan_json_keys(path, child, f'{trail}.{key}'))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            findings.extend(scan_json_keys(path, child, f'{trail}[{index}]'))
    return findings


def main() -> int:
    findings: list[str] = []
    for path in sorted(ROOT.rglob('*')):
        if not path.is_file() or not is_public_file(path):
            continue
        try:
            text = path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            continue
        findings.extend(scan_text(path, text))
        if path.suffix == '.json' and text.strip():
            try:
                findings.extend(scan_json_keys(path, json.loads(text)))
            except json.JSONDecodeError:
                findings.append(f'{path.relative_to(ROOT)}: JSON could not be parsed')

    if findings:
        print('Risky public wording or JSON fields found:')
        for finding in findings:
            print(f'- {finding}')
        return 1

    print('Safe wording scan passed. No risky public wording or blocked JSON fields found.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
