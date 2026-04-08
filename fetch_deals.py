"""
DealDrop — fetch_deals.py
Corrected version:
- Uses the working Keepa product request pattern from the older file
- Keeps 24-hour deal memory
- Prevents overwriting deals.json with 0 deals
"""

import json
import os
import time
import datetime
import hashlib
import hmac
import requests

KEEPA_API_KEY      = os.environ.get("KEEPA_API_KEY", "")
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG", "")
AMAZON_ACCESS_KEY  = os.environ.get("AMAZON_ACCESS_KEY", "")
AMAZON_SECRET_KEY  = os.environ.get("AMAZON_SECRET_KEY", "")
AMAZON_HOST        = "webservices.amazon.com"
AMAZON_REGION      = "us-east-1"

OUTPUT_FILE        = "deals.json"
MEMORY_FILE        = "deals_memory.json"

MAX_DEALS          = 200
MIN_DISCOUNT_PCT   = 10
HOT_DEAL_PCT       = 50
DOMAIN_ID          = "1"
DEALS_TO_SHOW      = 200
DEAL_TTL_HOURS     = 24

KEEPA_BASE         = "https://api.keepa.com"

CATEGORY_NAMES = {
    172282:       "Electronics",
    493964:       "Electronics",
    541966:       "Electronics",
    1266092011:   "Electronics",
    13896617011:  "Computers",
    2335752011:   "Cell Phones & Accessories",
    2625373011:   "Cell Phones & Accessories",
    7141123011:   "Clothing, Shoes & Jewelry",
    1036592:      "Clothing, Shoes & Jewelry",
    1055398:      "Home & Kitchen",
    284507:       "Home & Kitchen",
    9482648011:   "Kitchen & Dining",
    228013:       "Tools & Home Improvement",
    2619525011:   "Tools & Home Improvement",
    15684181:     "Automotive",
    491244:       "Automotive",
    2619533011:   "Automotive",
    10399642011:  "Automotive",
    3375251:      "Sports & Outdoors",
    1064012:      "Sports & Outdoors",
    165793011:    "Toys & Games",
    1249140011:   "Toys & Games",
    51574011:     "Pet Supplies",
    2619534011:   "Pet Supplies",
    165796011:    "Baby",
    2619535011:   "Baby",
    1064954:      "Health & Household",
    3760911:      "Beauty & Personal Care",
    11055981:     "Beauty & Personal Care",
    7730994011:   "Beauty & Personal Care",
    2972638011:   "Patio, Lawn & Garden",
    979455011:    "Patio, Lawn & Garden",
    1064278:      "Office Products",
    1285128:      "Office Products",
    283155:       "Books",
    468642:       "Video Games",
    2858778011:   "Movies & TV",
    5174:         "Music",
    11091801:     "Musical Instruments",
    2238192011:   "Musical Instruments",
    409488:       "Software",
    16310101:     "Grocery & Gourmet Food",
    3780361:      "Luggage & Travel",
    9479199011:   "Luggage & Travel",
    3760901:      "Luggage & Travel",
    2582543011:   "Arts, Crafts & Sewing",
    3760931:      "Handmade Products",
}

CATEGORY_EMOJI = {
    "Electronics":               "💻",
    "Computers":                 "🖥️",
    "Cell Phones & Accessories": "📱",
    "Home & Kitchen":            "🏠",
    "Kitchen & Dining":          "🍳",
    "Clothing, Shoes & Jewelry": "👗",
    "Beauty & Personal Care":    "💄",
    "Health & Household":        "💊",
    "Toys & Games":              "🧸",
    "Sports & Outdoors":         "⚽",
    "Automotive":                "🚗",
    "Pet Supplies":              "🐾",
    "Baby":                      "🍼",
    "Patio, Lawn & Garden":      "🌱",
    "Office Products":           "📎",
    "Tools & Home Improvement":  "🔧",
    "Video Games":               "🎮",
    "Books":                     "📚",
    "Musical Instruments":       "🎸",
    "Movies & TV":               "🎬",
    "Music":                     "🎵",
    "Software":                  "💿",
    "Grocery & Gourmet Food":    "🛒",
    "Luggage & Travel":          "🧳",
    "Industrial & Scientific":   "🔩",
    "Arts, Crafts & Sewing":     "🎨",
    "Handmade Products":         "🤝",
}

BAD_CATEGORY_WORDS = [
    "strut","shock absorber","suspension","brake pad","brake rotor","brake kit",
    "caliper","wheel bearing","control arm","tie rod","ball joint","cv axle",
    "muffler","exhaust","radiator","alternator","fuel pump","water pump",
    "wiper blade","floor mat","car seat cover","oil filter","spark plug",
    "lawn mower","string trimmer","leaf blower","hedge trimmer","chainsaw",
    "garden hose","sprinkler","fire pit","bbq grill","patio chair","hammock",
    "sofa","sectional","recliner","dresser","bookcase","curtain","area rug",
    "door mat","welcome mat","air purifier","humidifier","space heater",
    "shirt","pants","dress","jacket","hoodie","sneakers","boots","handbag",
    "vitamin","supplement","protein powder","first aid","thermometer",
    "shampoo","moisturizer","foundation","mascara","perfume","razor",
    "dog food","cat food","dog bed","cat tree","litter box","fish tank",
    "diaper","stroller","car seat","crib","baby monitor","pacifier",
    "guitar","piano","drum","violin","saxophone","trumpet","ukulele",
    "yoga mat","dumbbell","barbell","treadmill","kayak","fishing rod",
    "puzzle","board game","action figure","lego","nerf","stuffed animal",
    "notebook","stapler","binder","whiteboard","pencil","calculator",
    "suitcase","luggage","travel pillow","passport holder","packing cube",
    "coffee","tea","protein bar","nuts","cereal","pasta","olive oil",
    "acrylic paint","canvas","embroidery","knitting","crochet","sewing",
]

KEYWORD_CATEGORIES = [
    (["strut","shock absorber","suspension","brake pad","brake rotor","brake kit","caliper","wheel bearing","control arm","tie rod","ball joint","cv axle","cv joint","catalytic converter","muffler","exhaust","radiator","alternator","starter motor","fuel pump","water pump","timing belt","serpentine belt","wiper blade","floor mat car","car seat cover","dash cam","jump starter","tow strap","oil filter","air filter cabin","spark plug","lug nut","wheel spacer","trailer hitch","tonneau cover","running board","mud flap","car cover","tire inflator","tire gauge","wheel cleaner"], "Automotive"),
    (["hydraulic press","shop press","drill press","lathe","bandsaw","table saw","miter saw","circular saw","jigsaw","reciprocating saw","angle grinder","bench grinder","air compressor","pressure washer","welder","welding","soldering iron","torque wrench","socket set","wrench set","tool set","tool box","toolbox","workbench","pipe wrench","pliers set","screwdriver set","clamp set","vise","saw blade","router table","planer","jointer","brad nailer","framing nailer","staple gun","nail gun","heat gun","caulk gun","wire stripper","crimping tool","voltage tester","stud finder","tape measure"], "Tools & Home Improvement"),
    (["machine screw","hex bolt","hex nut","lock nut","flange nut","carriage bolt","lag screw","sheet metal screw","anchor bolt","rivet set","threaded rod","shaft coupling","ball bearing","sprocket","conveyor","industrial valve","pneumatic fitting","hydraulic fitting","wire loom","heat shrink tubing","terminal block","relay switch","contactor","industrial motor","centrifugal pump","air compressor tank"], "Industrial & Scientific"),
    (["iphone case","samsung case","phone case","screen protector","tempered glass","phone charger","wireless charger","car phone mount","phone stand","magsafe","lightning cable","usb-c cable","phone holder","pop socket","airpods case","wireless earbuds","bluetooth earphone","phone wallet case"], "Cell Phones & Accessories"),
    (["gaming laptop","notebook computer","desktop computer","all-in-one pc","computer monitor","curved monitor","gaming monitor","mechanical keyboard","gaming keyboard","wireless keyboard","gaming mouse","wireless mouse","mousepad","usb hub","external hard drive","solid state drive","nvme ssd","graphics card","gpu","cpu cooler","pc case","power supply unit","motherboard","cpu processor","webcam","network card","wifi adapter","ethernet switch","nas drive","ups battery backup"], "Computers"),
    (["smart tv","4k tv","oled tv","qled tv","projector","soundbar","home theater","stereo receiver","turntable","record player","bluetooth speaker","smart speaker","security camera","doorbell camera","action camera","mirrorless camera","dslr camera","camera lens","drone","vr headset","streaming stick","hdmi switch","surge protector","smart plug","smart bulb","led strip light"], "Electronics"),
    (["t-shirt","polo shirt","dress shirt","button down","flannel shirt","hoodie","zip hoodie","pullover","crewneck","cardigan","sweater","windbreaker","rain jacket","winter coat","puffer jacket","cargo pants","chino pants","sweatpants","jogger pants","leggings","yoga pants","athletic shorts","board shorts","swim trunks","bikini","sports bra","underwear","boxer briefs","compression shorts","maxi dress","mini dress","blouse","tunic","midi skirt","skinny jeans","bootcut jeans","sneakers","running shoes","walking shoes","dress shoes","loafers","oxford shoes","ankle boots","chelsea boots","cowboy boots","sandals","flip flops","high heels","wedges","tote bag","crossbody bag","backpack purse","leather wallet","money clip","leather belt","necklace","bracelet","earrings","engagement ring","watch band"], "Clothing, Shoes & Jewelry"),
    (["air fryer","instant pot","pressure cooker","slow cooker","rice cooker","bread maker","waffle maker","panini press","electric griddle","toaster oven","convection oven","keurig","nespresso","espresso machine","french press","pour over coffee","vitamix","ninja blender","food processor","stand mixer","hand mixer","juicer","mandoline slicer","food dehydrator","cast iron skillet","nonstick pan","stainless steel pan","dutch oven","carbon steel wok","saucepan","stockpot","baking sheet","cake pan","muffin tin","loaf pan","pie dish","casserole dish","mixing bowl set","cutting board set","knife set","chef knife","santoku knife","bread knife","kitchen shears","measuring cups","colander","strainer","spatula set","ladle","whisk","tongs","oven mitt","dish rack","pot holder"], "Kitchen & Dining"),
    (["sofa","sectional sofa","loveseat","recliner chair","accent chair","dining chair","bar stool","bed frame","headboard","nightstand","dresser","chest of drawers","wardrobe","bookcase","bookshelf","tv stand","entertainment center","coffee table","end table","console table","standing desk","bathroom vanity","shower curtain","bath mat","towel rack","curtain rod","blackout curtain","throw pillow","bed sheet set","comforter","duvet cover","mattress topper","area rug","runner rug","welcome mat","wall art","picture frame","wall mirror","floor lamp","table lamp","ceiling fan","air purifier","humidifier","space heater","tower fan","robot vacuum","storage bin","closet organizer","shoe rack","trash can","recycling bin"], "Home & Kitchen"),
    (["multivitamin","vitamin c","vitamin d","vitamin b12","zinc supplement","magnesium supplement","calcium supplement","fish oil","omega 3","probiotics","collagen peptides","whey protein","pre workout","creatine","bcaa","melatonin","elderberry","turmeric supplement","ashwagandha","first aid kit","bandage","gauze pad","thermometer","blood pressure monitor","pulse oximeter","glucose meter","heating pad","knee brace","back brace","wrist brace","ankle brace","pill organizer","contact lens solution","electric toothbrush","water flosser","whitening strips","safety razor","electric shaver","hair trimmer","body trimmer","nail clipper set","cotton swabs"], "Health & Household"),
    (["face moisturizer","eye cream","face serum","retinol cream","hyaluronic acid","vitamin c serum","spf sunscreen","liquid foundation","concealer","setting powder","blush palette","bronzer","eyeshadow palette","eyeliner pencil","mascara","lipstick","lip gloss","setting spray","face primer","facial toner","face cleanser","face exfoliator","clay mask","sheet mask","micellar water","makeup remover","dry shampoo","hair mask","hair serum","hair oil","hair spray","hair gel","pomade","hair dye","flat iron","curling wand","hair dryer diffuser","body lotion","body butter","body wash","bath bomb set","perfume","cologne","body spray","deodorant","nail polish","nail gel kit","lip balm"], "Beauty & Personal Care"),
    (["lego set","duplo","action figure","barbie doll","hot wheels","remote control car","rc truck","nerf gun","nerf blaster","water gun","play set","dollhouse","toy kitchen","play doh","kinetic sand","slime kit","science kit","board game","card game","jigsaw puzzle","3d puzzle","rubiks cube","stuffed animal","plush toy","teddy bear","pokemon card","trading card","collectible figure","baby toy","infant toy","teether","rattle","play tent","trampoline"], "Toys & Games"),
    (["dog food","cat food","dog treat","cat treat","dog toy","cat toy","dog bed","cat bed","dog crate","cat carrier","dog collar","cat collar","dog leash","retractable leash","dog harness","dog bowl","cat bowl","pet fountain","dog shampoo","flea treatment","litter box","cat litter","cat tree","cat scratcher","bird cage","bird feeder","fish tank","aquarium","reptile tank","hamster cage"], "Pet Supplies"),
    (["diaper","baby wipe","baby lotion","baby shampoo","baby monitor","baby swing","baby bouncer","baby carrier","baby wrap","jogging stroller","travel system stroller","infant car seat","convertible car seat","crib","bassinet","pack and play","changing table","nursing pillow","breast pump","bottle warmer","baby bottle","sippy cup","pacifier","baby food","baby formula","high chair","baby gate","baby bathtub"], "Baby"),
    (["acoustic guitar","electric guitar","bass guitar","guitar amp","guitar pedal","guitar string","guitar strap","ukulele","banjo","violin","viola","cello","keyboard piano","digital piano","midi keyboard","drum set","drum kit","cymbal","drum stick","drum pad","electronic drum","trumpet","trombone","saxophone","clarinet","flute","harmonica","accordion","music stand","metronome","tuner clip","audio interface","studio monitor","xlr cable"], "Musical Instruments"),
    (["yoga mat","yoga block","foam roller","resistance band","pull up bar","dumbbell set","barbell","weight plate","kettlebell","weight bench","squat rack","power rack","treadmill","elliptical machine","stationary bike","rowing machine","jump rope","medicine ball","ab wheel","gym bag","gym gloves","weightlifting belt","knee sleeve","hiking boot","hiking pole","hydration pack","camping tent","sleeping bag","sleeping pad","camp stove","headlamp lantern","fishing rod","fishing reel","kayak paddle","life jacket","snorkel set","surfboard","skateboard","bike helmet","cycling jersey","bike lock","golf club","tennis racket","basketball hoop","swimming goggle","ski goggle","ski helmet","snowboard binding"], "Sports & Outdoors"),
]

def load_memory():
    try:
        with open(MEMORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_memory(memory):
    with open(MEMORY_FILE, "w", encoding="utf-8") as f:
        json.dump(memory, f, indent=2)

def prune_memory(memory):
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=DEAL_TTL_HOURS)
    pruned = {}
    for asin, item in memory.items():
        first_seen = item.get("firstSeen")
        if not first_seen:
            continue
        try:
            seen_dt = datetime.datetime.fromisoformat(first_seen.replace("Z", ""))
            if seen_dt >= cutoff:
                pruned[asin] = item
        except Exception:
            continue
    return pruned

def keepa_deal_request(deal_params):
    url = f"{KEEPA_BASE}/deal"
    params = {"key": KEEPA_API_KEY}
    headers = {"Content-Type": "application/json"}
    r = requests.post(url, params=params, json=deal_params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

def keepa_product_request(asins):
    """
    Working version:
    Try minimal params first, then domainId fallback.
    """
    url = f"{KEEPA_BASE}/product"

    param_sets = [
        {"key": KEEPA_API_KEY, "asin": ",".join(asins)},
        {"key": KEEPA_API_KEY, "asin": ",".join(asins), "domainId": 1},
    ]

    for i, params in enumerate(param_sets):
        print(f"    Product attempt {i+1} for {len(asins)} ASINs")
        try:
            r = requests.get(url, params=params, timeout=60)
            print(f"    Status: {r.status_code}")
            if r.status_code == 200:
                return r.json()
            else:
                print(f"    Response: {r.text[:300]}")
        except Exception as e:
            print(f"    Request error: {e}")

    return {"products": []}

def fetch_keepa_asins():
    print("\n[Keepa] Fetching deal ASINs...")
    body = {
        "domainId":     1,
        "priceTypes":   [0],
        "deltaPercent": MIN_DISCOUNT_PCT,
        "interval":     10080,
        "page":         0,
    }

    deal_asins = []
    try:
        data = keepa_deal_request(body)
        deals_raw = data.get("deals", {}).get("dr", [])
        deal_asins = [str(d.get("asin")).strip().upper() for d in deals_raw if d.get("asin")]
        print(f"[Keepa] Got {len(deal_asins)} candidates")
    except Exception as e:
        print(f"[Keepa] Deal request failed: {e}")

    all_asins = list(dict.fromkeys(deal_asins))
    print(f"[Keepa] {len(all_asins)} unique ASINs")
    return all_asins[:MAX_DEALS]

def fetch_keepa_product_details(asins):
    if not asins:
        return []

    print(f"\n[Keepa] Fetching product details ({len(asins)} ASINs)...")
    all_products = []

    for i, asin in enumerate(asins):
        try:
            data = keepa_product_request([asin])
            products = data.get("products", [])
            if products:
                all_products.extend(products)
                if i == 0:
                    print(f"    First product keys: {list(products[0].keys())[:10]}")
            if i % 10 == 0:
                print(f"    Progress: {i+1}/{len(asins)} ({len(all_products)} successful)")
            time.sleep(1.3)
        except Exception as e:
            print(f"[Keepa] Error on {asin}: {e}")

        if len(all_products) >= MAX_DEALS + 20:
            break

    print(f"[Keepa] Total: {len(all_products)} products")
    return all_products

def get_category(product):
    root = product.get("rootCategory")
    if root and root in CATEGORY_NAMES:
        return CATEGORY_NAMES[root]

    for cat_id in (product.get("categories") or []):
        if cat_id in CATEGORY_NAMES:
            return CATEGORY_NAMES[cat_id]

    title = (product.get("title") or "").lower()

    for words, category in KEYWORD_CATEGORIES:
        for w in words:
            if w in title:
                return category

    for bad_word in BAD_CATEGORY_WORDS:
        if bad_word in title:
            return "Other"

    return "Tools & Home Improvement"

def parse_coupon(product):
    coupon_history = product.get("coupon")
    if not coupon_history or len(coupon_history) < 3:
        return None

    idx = len(coupon_history) - 3
    while idx >= 0:
        one_time = coupon_history[idx + 1]
        sns      = coupon_history[idx + 2]

        for val, ctype in [(one_time, "clip"), (sns, "sns")]:
            if val and val != 0:
                if val > 0 and val >= 5:
                    return {
                        "type": ctype,
                        "kind": "percent",
                        "value": val,
                        "display": f"{val}% off coupon"
                    }
                elif val < 0:
                    dollars = abs(val) / 100.0
                    if dollars >= 3:
                        return {
                            "type": ctype,
                            "kind": "dollars",
                            "value": dollars,
                            "display": f"${dollars:.0f} off coupon"
                        }
        idx -= 3

    return None

def sign_aws(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def get_aws_signing_key(secret, date_stamp, region, service):
    k = sign_aws(("AWS4" + secret).encode("utf-8"), date_stamp)
    k = sign_aws(k, region)
    k = sign_aws(k, service)
    k = sign_aws(k, "aws4_request")
    return k

def fetch_amazon_live_data(asin_batch):
    if not AMAZON_ACCESS_KEY:
        print("[Amazon PA API] Not configured — skipping.")
        return {}

    service  = "ProductAdvertisingAPI"
    path     = "/paapi5/getitems"
    endpoint = f"https://{AMAZON_HOST}{path}"

    payload = {
        "ItemIds":     asin_batch,
        "PartnerTag":  AMAZON_PARTNER_TAG,
        "PartnerType": "Associates",
        "Marketplace": "www.amazon.com",
        "Resources": [
            "Images.Primary.Large",
            "ItemInfo.Title",
            "Offers.Listings.Price",
            "Offers.Listings.Availability.Message",
            "Offers.Listings.DeliveryInfo.IsPrimeEligible",
        ]
    }

    body = json.dumps(payload)
    now = datetime.datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    canonical_headers = (
        f"content-encoding:amz-1.0\n"
        f"content-type:application/json; charset=utf-8\n"
        f"host:{AMAZON_HOST}\n"
        f"x-amz-date:{amz_date}\n"
        f"x-amz-target:com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems\n"
    )
    signed_headers = "content-encoding;content-type;host;x-amz-date;x-amz-target"
    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    canonical_request = "\n".join([
        "POST", path, "", canonical_headers, signed_headers, payload_hash
    ])

    credential_scope = f"{date_stamp}/{AMAZON_REGION}/{service}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])

    signing_key = get_aws_signing_key(AMAZON_SECRET_KEY, date_stamp, AMAZON_REGION, service)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        f"AWS4-HMAC-SHA256 Credential={AMAZON_ACCESS_KEY}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    headers = {
        "content-encoding": "amz-1.0",
        "content-type":     "application/json; charset=utf-8",
        "host":             AMAZON_HOST,
        "x-amz-date":       amz_date,
        "x-amz-target":     "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems",
        "Authorization":    authorization,
    }

    try:
        r = requests.post(endpoint, headers=headers, data=body, timeout=15)
        r.raise_for_status()
        items  = r.json().get("ItemsResult", {}).get("Items", [])
        result = {}

        for item in items:
            asin = item.get("ASIN")
            listing = (item.get("Offers", {}).get("Listings") or [{}])[0]
            price_obj = listing.get("Price", {})
            img_obj = item.get("Images", {}).get("Primary", {}).get("Large", {})
            title = item.get("ItemInfo", {}).get("Title", {}).get("DisplayValue", "")
            prime = listing.get("DeliveryInfo", {}).get("IsPrimeEligible", False)

            result[asin] = {
                "price_display": price_obj.get("DisplayAmount", ""),
                "image":         img_obj.get("URL", ""),
                "title":         title,
                "prime":         prime,
            }

        print(f"[Amazon PA API] Got data for {len(result)} products")
        return result

    except Exception as e:
        print(f"[Amazon PA API] ERROR: {e}")
        return {}

def build_deals_json():
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting deal fetch...")

    if not KEEPA_API_KEY:
        raise RuntimeError("Missing KEEPA_API_KEY")

    memory = load_memory()
    memory = prune_memory(memory)

    all_asins = fetch_keepa_asins()
    if not all_asins:
        print("No ASINs returned from Keepa. Keeping existing deals.json.")
        return

    keepa_products = fetch_keepa_product_details(all_asins)

    keepa_deals = {}
    for p in keepa_products:
        try:
            asin = p.get("asin", "")
            stats = p.get("stats", {})
            cur_raw = stats.get("current", [])
            avg_raw = stats.get("avg90", [])

            def to_d(v):
                return v / 100.0 if v and v > 0 else None

            current = to_d(cur_raw[0] if cur_raw and len(cur_raw) > 0 else None)
            avg90 = to_d(avg_raw[0] if avg_raw and len(avg_raw) > 0 else None)

            coupon = parse_coupon(p)
            pct = 0
            if current and avg90 and avg90 > 0 and current < avg90:
                pct = round((1 - current / avg90) * 100)

            if pct < MIN_DISCOUNT_PCT and coupon is None:
                continue

            category = get_category(p)
            if category == "Other":
                continue

            keepa_deals[asin] = {
                "asin": asin,
                "category": category,
                "pct": pct,
                "coupon": coupon,
                "title_fallback": (p.get("title") or "")[:120],
                "avg90_price": avg90,
            }
        except Exception as e:
            print(f"Skipping product: {e}")

    qualifying_asins = list(keepa_deals.keys())
    print(f"\n{len(qualifying_asins)} qualifying deals")

    amazon_data = {}
    for i in range(0, len(qualifying_asins), 10):
        batch = qualifying_asins[i:i+10]
        result = fetch_amazon_live_data(batch)
        amazon_data.update(result)
        time.sleep(1)

    formatted = []
    deal_id = 1
    now_iso = datetime.datetime.utcnow().isoformat() + "Z"

    for asin in qualifying_asins:
        try:
            k = keepa_deals[asin]
            a = amazon_data.get(asin, {})

            title = a.get("title") or k["title_fallback"]
            if not title or len(title) < 5:
                continue

            price = a.get("price_display", "")
            image = a.get("image", "")
            prime = a.get("prime", False)
            coupon = k["coupon"]
            pct = k["pct"]
            cat = k["category"]
            effective_pct = pct

            if coupon and coupon["kind"] == "percent":
                effective_pct = min(99, pct + coupon["value"])

            parts = []
            if pct >= MIN_DISCOUNT_PCT:
                parts.append(f"{pct}% off recent price")
            if coupon:
                parts.append(coupon["display"])
            if prime:
                parts.append("Prime eligible")

            was_display = f"${k['avg90_price']:.2f}" if k.get("avg90_price") else ""

            deal = {
                "id": deal_id,
                "asin": asin,
                "cat": cat,
                "emoji": CATEGORY_EMOJI.get(cat, "🛒"),
                "title": title[:120] + ("..." if len(title) > 120 else ""),
                "desc": " · ".join(parts),
                "price": price,
                "was": was_display,
                "hasLivePrice": bool(price),
                "pct": pct,
                "effectivePct": effective_pct,
                "hot": effective_pct >= HOT_DEAL_PCT,
                "discount": f"{pct}% off" if pct > 0 else (coupon["display"] if coupon else "Deal"),
                "hasCoupon": coupon is not None,
                "couponDisplay": coupon["display"] if coupon else None,
                "image": image,
                "prime": prime,
                "link": f"https://www.amazon.com/dp/{asin}?tag={AMAZON_PARTNER_TAG}",
                "updatedAt": now_iso,
            }

            formatted.append(deal)

            existing = memory.get(asin, {})
            memory[asin] = {
                **deal,
                "firstSeen": existing.get("firstSeen", now_iso)
            }

            deal_id += 1
        except Exception as e:
            print(f"Skipping formatted deal {asin}: {e}")

    formatted.sort(key=lambda d: (not d["hot"], -d["effectivePct"]))
    formatted = formatted[:DEALS_TO_SHOW]

    print(f"\nFinal qualifying deals before save: {len(formatted)}")

    if len(formatted) == 0:
        print("No formatted deals found. Keeping existing deals.json and not overwriting.")
        return

    memory = prune_memory(memory)
    save_memory(memory)

    output = {
        "updatedAt":   now_iso,
        "totalDeals":  len(formatted),
        "hotDeals":    sum(1 for d in formatted if d["hot"]),
        "couponDeals": sum(1 for d in formatted if d["hasCoupon"]),
        "deals":       formatted,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved {len(formatted)} deals to {OUTPUT_FILE}")
    print(f"Hot deals: {output['hotDeals']}")
    print(f"Coupon deals: {output['couponDeals']}")
    print(f"Updated: {output['updatedAt']}")

if __name__ == "__main__":
    build_deals_json()
