"""No-op static renderer retained for workflow compatibility.

The site now renders compliant product cards from sanitized JSON using the shared
Product Picks template. This script intentionally does not write static cards,
badges, old/was pricing, percentage-off labels, hot-deal sections, coupon fields,
or price-drop copy into HTML.
"""

from pathlib import Path


def main():
    expected_files = [Path("index.html")]
    missing = [str(path) for path in expected_files if not path.exists()]
    if missing:
        print(f"Static renderer skipped; missing files: {missing}")
        return
    print("Static renderer skipped. Product cards are rendered from compliant JSON at runtime.")


if __name__ == "__main__":
    main()
