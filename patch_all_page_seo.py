import html
import json
import re
from pathlib import Path

SITE = "https://blacklabdeals.com"

COMMON_LINKS = [
    ("Top 100 Deals", "/top-100-amazon-deals-today/"),
    ("Tool Deals", "/best-amazon-tool-deals/"),
    ("Home & Kitchen", "/best-amazon-home-kitchen-deals/"),
    ("Electronics", "/best-amazon-electronics-deals/"),
    ("Deals Under $50", "/best-amazon-deals-under-50/"),
    ("Best Seller Deals", "/best-seller-deals.html"),
]

PAGES = {
    "index.html": {
        "url": "/",
        "title": "Best Deals Found on Amazon Today | Black Lab Deals",
        "name": "Best Deals Found on Amazon Today",
        "description": "Find the best deals found on Amazon today, updated frequently with price drops, coupons, trending products, and limited-time discounts.",
        "keywords": "Amazon deals, deals found on Amazon, daily deals, price drops, coupons",
        "image": "/black-lab-deals-social-preview.png?v=1",
        "crumbs": [("Home", "/")],
        "about": ["Amazon deals", "price drops", "coupons", "limited-time discounts"],
        "related": COMMON_LINKS,
        "faq": [
            ("How does Black Lab Deals find deals?", "Black Lab Deals organizes current Amazon deals, price drops, coupons, and trending products so shoppers can review active offers more quickly."),
            ("How often are Amazon deals updated?", "The main deal feed is refreshed regularly, and prices or availability can change at any time."),
            ("Should shoppers confirm the final price on Amazon?", "Yes. Always confirm the final price, coupon status, shipping, and availability on Amazon before purchasing."),
        ],
    },
    "top-100-amazon-deals-today/index.html": {
        "url": "/top-100-amazon-deals-today/",
        "title": "Top 100 Deals Found on Amazon Today | Updated Often",
        "name": "Top 100 Deals Found on Amazon Today",
        "description": "Browse the top 100 deals found on Amazon today, ranked from current discounts, coupons, and price drops across popular categories.",
        "keywords": "top 100 Amazon deals, best Amazon deals today, Amazon price drops",
        "image": "/top-100-amazon-deals-today/top-100-deals-found-on-amazon-right-now-black-lab-deals.png",
        "crumbs": [("Home", "/"), ("Top 100 Deals", "/top-100-amazon-deals-today/")],
        "about": ["top Amazon deals", "ranked deals", "price drops", "coupons"],
        "related": [("Tool Deals", "/best-amazon-tool-deals/"), ("Deals Under $50", "/best-amazon-deals-under-50/"), ("Home & Kitchen", "/best-amazon-home-kitchen-deals/"), ("Electronics", "/best-amazon-electronics-deals/"), ("Best Seller Deals", "/best-seller-deals.html")],
        "faq": [
            ("How are the top 100 Amazon deals ranked?", "The page ranks current deals from the Black Lab Deals feed using signals such as discount strength, coupons, price drops, and deal freshness."),
            ("How often does the top 100 deals page update?", "The page is refreshed regularly from the current deal feed so shoppers can scan active offers."),
            ("Are these official Amazon rankings?", "No. These are deals found on Amazon and organized by Black Lab Deals, not official Amazon rankings."),
        ],
    },
    "best-seller-deals.html": {
        "url": "/best-seller-deals.html",
        "title": "Amazon Best Seller Deals Today | Black Lab Deals",
        "name": "Amazon Best Seller Deals Today",
        "description": "Find Amazon best-seller deals checked from a saved best-seller watchlist and filtered for current price drops.",
        "keywords": "Amazon best seller deals, best seller price drops, Amazon deals today",
        "image": "/black-lab-deals-social-preview.png?v=1",
        "crumbs": [("Home", "/"), ("Best Seller Deals", "/best-seller-deals.html")],
        "about": ["Amazon best sellers", "price drops", "best seller watchlist"],
        "related": [("Top 100 Deals", "/top-100-amazon-deals-today/"), ("Tool Deals", "/best-amazon-tool-deals/"), ("Home & Kitchen", "/best-amazon-home-kitchen-deals/"), ("Electronics", "/best-amazon-electronics-deals/"), ("Deals Under $50", "/best-amazon-deals-under-50/")],
        "faq": [
            ("What are best-seller deals?", "Best-seller deals are products from a saved Amazon best-seller watchlist that Black Lab Deals checks for current price drops."),
            ("How often are best-seller ASINs checked?", "The best-seller workflow rotates through the saved ASIN list throughout the day so the full list can be checked on a daily cycle."),
            ("Are these official Amazon best-seller rankings?", "The watchlist is based on Amazon best-seller data, but the deal page is organized by Black Lab Deals and should not be treated as an official Amazon ranking page."),
        ],
    },
    "categories/index.html": {
        "url": "/categories/",
        "title": "Amazon Deal Categories | Black Lab Deals",
        "name": "Amazon Deal Categories",
        "description": "Browse Amazon deal categories on Black Lab Deals, including tools, home and kitchen, electronics, automotive, pet supplies, sports, toys, office products, baby products, and more.",
        "keywords": "Amazon deal categories, Amazon tool deals, Amazon electronics deals, Amazon home deals",
        "image": "/black-lab-deals-social-preview.png?v=1",
        "crumbs": [("Home", "/"), ("Categories", "/categories/")],
        "about": ["Amazon deal categories", "shopping categories", "deal pages"],
        "related": COMMON_LINKS,
        "faq": [
            ("How do I browse Amazon deal categories?", "Use the category hub to jump to Black Lab Deals pages for tools, home and kitchen, electronics, automotive, pet supplies, toys, office products, and more."),
            ("Do category deal pages update?", "Yes. Public deal category pages are refreshed from the current Black Lab Deals feed."),
            ("Can category pages change over time?", "Yes. Black Lab Deals may add, remove, or adjust category pages as the deal feed changes."),
        ],
    },
}

CATEGORY_PAGES = [
    ("best-amazon-tool-deals/index.html", "/best-amazon-tool-deals/", "Best Amazon Tool Deals", "Best Tool Deals Found on Amazon Today | Black Lab Deals", "Shop Amazon tool deals today, including power tools, hand tools, tool storage, workshop accessories, and home improvement finds.", "Amazon tool deals, power tool deals, hand tool deals", "/best-amazon-tool-deals/best-tool-deals-found-on-amazon-right-now-black-lab-deals.png", [("Patio, Lawn & Garden", "/best-amazon-patio-lawn-garden-deals/"), ("Automotive", "/best-amazon-automotive-deals/"), ("Home & Kitchen", "/best-amazon-home-kitchen-deals/"), ("Deals Under $50", "/best-amazon-deals-under-50/")], "tool products", "power tools, hand tools, storage, and workshop finds"),
    ("best-amazon-deals-under-50/index.html", "/best-amazon-deals-under-50/", "Best Amazon Deals Under $50", "Best Amazon Deals Under $50 Today | Black Lab Deals", "Find Amazon deals under $50 across home, tech, kitchen, tools, toys, office products, and everyday essentials.", "Amazon deals under 50, budget Amazon deals, cheap Amazon deals", "/best-amazon-deals-under-50/best-deals-under-50-found-on-amazon-right-now-black-lab-deals.png", [("Top 100 Deals", "/top-100-amazon-deals-today/"), ("Home & Kitchen", "/best-amazon-home-kitchen-deals/"), ("Electronics", "/best-amazon-electronics-deals/"), ("Tool Deals", "/best-amazon-tool-deals/")], "under-$50 products", "budget-friendly deals across popular Amazon categories"),
    ("best-amazon-home-kitchen-deals/index.html", "/best-amazon-home-kitchen-deals/", "Best Amazon Home & Kitchen Deals", "Best Amazon Home & Kitchen Deals Today | Black Lab Deals", "Shop Amazon Home & Kitchen deals today, including kitchen gadgets, cookware, storage, bedding, cleaning, and household essentials.", "Amazon home deals, Amazon kitchen deals, home and kitchen deals", "/best-amazon-home-kitchen-deals/best-home-kitchen-deals-found-on-amazon-right-now-black-lab-deals.png", [("Deals Under $50", "/best-amazon-deals-under-50/"), ("Health & Household", "/best-amazon-health-household-deals/"), ("Patio, Lawn & Garden", "/best-amazon-patio-lawn-garden-deals/"), ("Top 100 Deals", "/top-100-amazon-deals-today/")], "home and kitchen products", "kitchen gadgets, cookware, storage, bedding, and household essentials"),
    ("best-amazon-electronics-deals/index.html", "/best-amazon-electronics-deals/", "Best Amazon Electronics Deals", "Best Electronics Deals Found on Amazon Today | Black Lab Deals", "Find Amazon electronics deals today, including tech accessories, audio, smart home devices, headphones, chargers, and gadgets.", "Amazon electronics deals, tech deals, headphone deals", "/best-amazon-electronics-deals/best-electronics-deals-found-on-amazon-right-now-black-lab-deals.png", [("Top 100 Deals", "/top-100-amazon-deals-today/"), ("Deals Under $50", "/best-amazon-deals-under-50/"), ("Office Products", "/best-amazon-office-products-deals/"), ("Best Seller Deals", "/best-seller-deals.html")], "electronics products", "tech accessories, audio, smart home devices, and useful gadgets"),
    ("best-amazon-health-household-deals/index.html", "/best-amazon-health-household-deals/", "Best Amazon Health & Household Deals", "Best Amazon Health & Household Deals Today | Black Lab Deals", "Shop Amazon Health & Household deals today, including cleaning supplies, personal care, wellness items, and household basics.", "Amazon health deals, household deals, cleaning supply deals", "/best-amazon-health-household-deals/best-health-household-deals-found-on-amazon-right-now-black-lab-deals.png", [("Home & Kitchen", "/best-amazon-home-kitchen-deals/"), ("Pet Supplies", "/best-amazon-pet-supplies-deals/"), ("Deals Under $50", "/best-amazon-deals-under-50/")], "health and household products", "cleaning, personal care, wellness, and household basics"),
    ("best-amazon-patio-lawn-garden-deals/index.html", "/best-amazon-patio-lawn-garden-deals/", "Best Amazon Patio, Lawn & Garden Deals", "Best Amazon Patio, Lawn & Garden Deals Today | Black Lab Deals", "Find Amazon Patio, Lawn & Garden deals today, including outdoor tools, yard care, patio gear, garden items, and backyard finds.", "Amazon patio deals, lawn and garden deals, outdoor deals", "/best-amazon-patio-lawn-garden-deals/best-patio-lawn-garden-deals-found-on-amazon-right-now-black-lab-deals.png", [("Tool Deals", "/best-amazon-tool-deals/"), ("Sports & Outdoors", "/best-amazon-sports-outdoors-deals/"), ("Home & Kitchen", "/best-amazon-home-kitchen-deals/")], "patio, lawn, and garden products", "outdoor tools, yard care, patio gear, and garden finds"),
    ("best-amazon-pet-supplies-deals/index.html", "/best-amazon-pet-supplies-deals/", "Best Amazon Pet Supplies Deals", "Best Amazon Pet Supplies Deals Today | Black Lab Deals", "Shop Amazon Pet Supplies deals today, including pet essentials, grooming, toys, beds, cleanup, and everyday pet finds.", "Amazon pet deals, pet supplies deals, dog and cat deals", "/best-amazon-pet-supplies-deals/best-pet-supplies-deals-found-on-amazon-right-now-black-lab-deals.png", [("Health & Household", "/best-amazon-health-household-deals/"), ("Home & Kitchen", "/best-amazon-home-kitchen-deals/"), ("Deals Under $50", "/best-amazon-deals-under-50/")], "pet supply products", "pet essentials, grooming, toys, beds, and cleanup"),
    ("best-amazon-sports-outdoors-deals/index.html", "/best-amazon-sports-outdoors-deals/", "Best Amazon Sports & Outdoors Deals", "Best Amazon Sports & Outdoors Deals Today | Black Lab Deals", "Find Amazon Sports & Outdoors deals today, including fitness gear, camping items, outdoor products, and recreation deals.", "Amazon sports deals, outdoor deals, fitness deals", "/best-amazon-sports-outdoors-deals/best-sports-outdoors-deals-found-on-amazon-right-now-black-lab-deals.png", [("Patio, Lawn & Garden", "/best-amazon-patio-lawn-garden-deals/"), ("Top 100 Deals", "/top-100-amazon-deals-today/"), ("Deals Under $50", "/best-amazon-deals-under-50/")], "sports and outdoor products", "fitness, camping, outdoor, and recreation deals"),
    ("best-amazon-automotive-deals/index.html", "/best-amazon-automotive-deals/", "Best Amazon Automotive Deals", "Best Amazon Automotive Deals Today | Black Lab Deals", "Shop Amazon Automotive deals today, including car care, garage items, tools, accessories, and vehicle finds.", "Amazon automotive deals, car accessory deals, garage deals", "/best-amazon-automotive-deals/best-automotive-deals-found-on-amazon-right-now-black-lab-deals.png", [("Tool Deals", "/best-amazon-tool-deals/"), ("Patio, Lawn & Garden", "/best-amazon-patio-lawn-garden-deals/"), ("Deals Under $50", "/best-amazon-deals-under-50/")], "automotive products", "car care, garage items, tools, and vehicle accessories"),
    ("best-amazon-toys-games-deals/index.html", "/best-amazon-toys-games-deals/", "Best Amazon Toys & Games Deals", "Best Amazon Toys & Games Deals Today | Black Lab Deals", "Find Amazon Toys & Games deals today, including toys, games, puzzles, gifts, learning finds, and family-friendly deals.", "Amazon toy deals, game deals, toys and games deals", "/best-amazon-toys-games-deals/best-toys-games-deals-found-on-amazon-right-now-black-lab-deals.png", [("Baby Products", "/best-amazon-baby-products-deals/"), ("Deals Under $50", "/best-amazon-deals-under-50/"), ("Top 100 Deals", "/top-100-amazon-deals-today/")], "toy and game products", "toys, games, puzzles, gifts, and learning finds"),
    ("best-amazon-office-products-deals/index.html", "/best-amazon-office-products-deals/", "Best Amazon Office Products Deals", "Best Amazon Office Products Deals Today | Black Lab Deals", "Shop Amazon Office Products deals today, including desk supplies, printer items, school gear, organization, and workspace finds.", "Amazon office deals, office product deals, desk supply deals", "/best-amazon-office-products-deals/best-office-products-deals-found-on-amazon-right-now-black-lab-deals.png", [("Electronics", "/best-amazon-electronics-deals/"), ("Deals Under $50", "/best-amazon-deals-under-50/"), ("Home & Kitchen", "/best-amazon-home-kitchen-deals/")], "office products", "desk supplies, printer items, school gear, and workspace finds"),
    ("best-amazon-baby-products-deals/index.html", "/best-amazon-baby-products-deals/", "Best Amazon Baby Product Deals", "Best Amazon Baby Product Deals Today | Black Lab Deals", "Find Amazon Baby Products deals today, including nursery items, feeding gear, bath products, travel items, and family supplies.", "Amazon baby deals, baby product deals, nursery deals", "/best-amazon-baby-products-deals/best-baby-products-deals-found-on-amazon-right-now-black-lab-deals.png", [("Toys & Games", "/best-amazon-toys-games-deals/"), ("Health & Household", "/best-amazon-health-household-deals/"), ("Home & Kitchen", "/best-amazon-home-kitchen-deals/")], "baby products", "nursery, feeding, bath, travel, and family supplies"),
    ("best-amazon-musical-instruments-deals/index.html", "/best-amazon-musical-instruments-deals/", "Best Amazon Musical Instrument Deals", "Best Amazon Musical Instrument Deals Today | Black Lab Deals", "Shop Amazon Musical Instrument deals today, including music accessories, stands, strings, audio gear, and instrument-related finds.", "Amazon musical instrument deals, music gear deals, audio gear deals", "/best-amazon-musical-instruments-deals/best-musical-instrument-deals-found-on-amazon-right-now-black-lab-deals.png", [("Electronics", "/best-amazon-electronics-deals/"), ("Office Products", "/best-amazon-office-products-deals/"), ("Deals Under $50", "/best-amazon-deals-under-50/")], "musical instrument products", "music accessories, stands, strings, and audio gear"),
]

for path, url, name, title, desc, keywords, image, related, product_label, includes in CATEGORY_PAGES:
    PAGES[path] = {
        "url": url,
        "title": title,
        "name": name,
        "description": desc,
        "keywords": keywords,
        "image": image,
        "crumbs": [("Home", "/"), ("Categories", "/categories/"), (name, url)],
        "about": [product_label, "Amazon deals", "price drops", "coupons"],
        "related": related,
        "faq": [
            (f"What {product_label} are included?", f"This page focuses on Amazon deals for {includes}."),
            ("How often are these deals refreshed?", "The page is refreshed from the current Black Lab Deals feed so shoppers can review active deals and price drops."),
            ("Can prices and availability change?", "Yes. Prices, coupons, shipping, and availability can change quickly, so confirm the final details on Amazon before purchasing."),
        ],
    }


def abs_url(path):
    if path.startswith("http"):
        return path
    if path == "/":
        return SITE + "/"
    return SITE + path


def json_script(data):
    return '<script type="application/ld+json" data-bld-seo="true">' + json.dumps(data, ensure_ascii=False, separators=(",", ":")) + '</script>'


def seo_block(page):
    title = page["title"]
    desc = page["description"]
    url = abs_url(page["url"])
    image = abs_url(page["image"])
    crumbs = [
        {"@type": "ListItem", "position": i + 1, "name": name, "item": abs_url(link)}
        for i, (name, link) in enumerate(page["crumbs"])
    ]
    webpage = {
        "@context": "https://schema.org",
        "@type": "WebPage",
        "name": page["name"],
        "description": desc,
        "url": url,
        "publisher": {"@type": "Organization", "name": "Black Lab Deals", "url": SITE + "/"},
        "about": page["about"],
        "isPartOf": {"@type": "WebSite", "name": "Black Lab Deals", "url": SITE + "/"},
    }
    breadcrumb = {"@context": "https://schema.org", "@type": "BreadcrumbList", "itemListElement": crumbs}
    faq = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {"@type": "Question", "name": q, "acceptedAnswer": {"@type": "Answer", "text": a}}
            for q, a in page["faq"]
        ],
    }
    lines = [
        "<!-- BLD SEO START -->",
        f'<meta name="description" content="{html.escape(desc, quote=True)}">',
        f'<meta name="keywords" content="{html.escape(page["keywords"], quote=True)}">',
        f'<link rel="canonical" href="{url}">',
        '<meta property="og:type" content="website">',
        '<meta property="og:site_name" content="Black Lab Deals">',
        f'<meta property="og:title" content="{html.escape(title, quote=True)}">',
        f'<meta property="og:description" content="{html.escape(desc, quote=True)}">',
        f'<meta property="og:url" content="{url}">',
        f'<meta property="og:image" content="{image}">',
        f'<meta property="og:image:secure_url" content="{image}">',
        '<meta property="og:image:width" content="1200">',
        '<meta property="og:image:height" content="630">',
        f'<meta property="og:image:alt" content="{html.escape(page["name"] + " from Black Lab Deals", quote=True)}">',
        '<meta name="twitter:card" content="summary_large_image">',
        f'<meta name="twitter:title" content="{html.escape(title, quote=True)}">',
        f'<meta name="twitter:description" content="{html.escape(desc, quote=True)}">',
        f'<meta name="twitter:image" content="{image}">',
        f'<meta name="twitter:image:alt" content="{html.escape(page["name"] + " from Black Lab Deals", quote=True)}">',
        json_script(webpage),
        json_script(breadcrumb),
        json_script(faq),
        "<!-- BLD SEO END -->",
    ]
    return "\n".join(lines)


def related_block(page):
    links = " &middot; ".join(f'<a href="{href}">{html.escape(label)}</a>' for label, href in page["related"])
    return (
        '<!-- BLD RELATED LINKS START -->\n'
        '<section class="related-deal-pages" aria-label="Related Amazon deal pages">\n'
        '  <span>More Amazon deal pages:</span> '
        f'{links}\n'
        '</section>\n'
        '<!-- BLD RELATED LINKS END -->'
    )


def patch_page(path, page):
    file_path = Path(path)
    if not file_path.exists():
        print(f"Skipping missing {path}")
        return False
    text = file_path.read_text(encoding="utf-8")
    original = text

    text = re.sub(r"<title>.*?</title>", f"<title>{html.escape(page['title'])}</title>", text, count=1, flags=re.S)

    text = re.sub(r"<!-- BLD SEO START -->.*?<!-- BLD SEO END -->", seo_block(page), text, flags=re.S)
    if "<!-- BLD SEO START -->" not in text:
        text = re.sub(r"\n\s*<!-- Black Lab Deals social preview meta -->.*?<!-- End Black Lab Deals social preview meta -->", "", text, flags=re.S)
        text = re.sub(r"\n<meta name=\"description\"[^>]*>", "", text, count=1)
        text = re.sub(r"\n<link rel=\"canonical\"[^>]*>", "", text, count=1)
        text = text.replace("</head>", seo_block(page) + "\n</head>", 1)

    text = re.sub(r"<!-- BLD TRUST START -->.*?<!-- BLD TRUST END -->", related_block(page), text, flags=re.S)
    if "<!-- BLD RELATED LINKS START -->" not in text:
        text = text.replace("<footer", related_block(page) + "\n<footer", 1)

    if text != original:
        file_path.write_text(text, encoding="utf-8")
        print(f"Updated {path}")
        return True
    print(f"No changes for {path}")
    return False


def main():
    changed = 0
    for path, page in PAGES.items():
        if patch_page(path, page):
            changed += 1
    print(f"Updated {changed} pages")


if __name__ == "__main__":
    main()
