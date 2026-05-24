import csv, io, json, os, re
from datetime import datetime, timezone
from urllib.parse import quote
import requests

SHEET_ID = os.getenv('AMAZON_SALES_EVENT_SHEET_ID', '15kLY4nPg-ydq9kcqUcSTrzttQxtCkeXGoU2HoO4KTkc')
OUTPUT_FILE = os.getenv('AMAZON_SALES_EVENT_SAMPLE_OUTPUT', 'amazon-sales-event-deals.json')
TAG = os.getenv('AFFILIATE_TAG', 'blacklabdealsprime-20')
PER_CAT = int(os.getenv('AMAZON_SALES_EVENT_SAMPLE_PER_CATEGORY', '5'))
HH_LIMIT = int(os.getenv('AMAZON_SALES_EVENT_HOUSEHOLD_LIMIT', '100'))

TABS = {
    'Electronics':'amazon-electronics-deals','Furniture':'amazon-furniture-deals','Health & Personal Care':'amazon-health-personal-care-deals','Home':'amazon-home-deals','Home Improvement':'amazon-home-improvement-deals','Home Entertainment':'amazon-home-entertainment-deals','Lawn and Garden':'amazon-lawn-garden-deals','Office Products':'amazon-office-products-deals','Outdoors':'amazon-outdoors-deals','PC':'amazon-pc-deals','Kitchen':'amazon-kitchen-deals','Pet Products':'amazon-pet-products-deals','Sports':'amazon-sports-deals','Tools':'amazon-tool-deals','Toys':'amazon-toys-deals','Video Devices':'amazon-video-devices-deals','Wireless':'amazon-wireless-deals'
}
HH_TABS = ['Health & Personal Care','Home','Kitchen','Office Products','Pet Products','Lawn and Garden','Home Improvement','Outdoors','Sports','Tools','Electronics','Wireless','PC','Furniture','Home Entertainment','Toys','Video Devices']
HH_TERMS = ['toilet paper','paper towel','paper towels','bath tissue','facial tissue','kleenex','napkins','laundry detergent','detergent','dryer sheets','fabric softener','stain remover','laundry pods','dish soap','dishwasher pods','dishwasher detergent','dishwasher tablets','hand soap','body wash','bar soap','shampoo','conditioner','trash bags','garbage bags','storage bags','zip bags','freezer bags','sandwich bags','cleaning wipes','disinfecting wipes','all purpose cleaner','all-purpose cleaner','bathroom cleaner','kitchen cleaner','glass cleaner','floor cleaner','toilet bowl cleaner','sponges','scrub sponge','scrub brush','air freshener','aluminum foil','plastic wrap','parchment paper','aa batteries','aaa batteries','9v batteries','d batteries','c batteries','button batteries']
HH_EXCLUDES = ['power tool battery','drill battery','lithium battery pack','car battery','marine battery','solar battery','trolling motor battery','lifepo4','inverter','generator battery','charger','rechargeable lead acid','12 volt','12v','24v','48v','battery backup','power station']
DEVICE_TERMS = ['echo','fire tv','kindle','ring','blink','eero','alexa','amazon basics','amazonbasics']
ASIN_RE = re.compile(r'\b[A-Z0-9]{10}\b', re.I)

def key(v): return str(v or '').strip().lower().replace(' ','_').replace('-','_')
def val(row,*names):
    d={key(k):v for k,v in row.items()}
    for n in names:
        x=str(d.get(key(n),'') or '').strip()
        if x: return x
    return ''
def num(v):
    try: return float(str(v or '').replace('$','').replace(',','').replace('%','').strip())
    except ValueError: return 0.0
def pct(v):
    n=num(v); return n*100 if 0<n<1 else n
def text(row): return ' '.join(str(v or '') for v in row.values()).lower()
def is_hh(row):
    t=text(row); return any(w in t for w in HH_TERMS) and not any(w in t for w in HH_EXCLUDES)
def asin_from(*vals):
    for v in vals:
        m=ASIN_RE.search(str(v or ''))
        if m: return m.group(0).upper()
    return ''
def add(pages,page):
    if page not in pages: pages.append(page)
def rows(tab):
    url=f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={quote(tab)}'
    r=requests.get(url,timeout=90); r.raise_for_status()
    return csv.DictReader(io.StringIO(r.content.decode('utf-8-sig')))
def link(asin,url):
    u=(url or f'https://www.amazon.com/dp/{asin}').strip()
    if u.startswith('www.'): u='https://'+u
    return u if 'tag=' in u else u + ('&' if '?' in u else '?') + 'tag=' + TAG

def build(row,tab,slug,now):
    asin=asin_from(val(row,'asin'),val(row,'asin_url','url'),text(row))
    if not asin: return None
    title=val(row,'asin_name','title','product_title','name') or f'{tab} Deal {asin}'
    brand=val(row,'brand')
    price=num(val(row,'deal_price','price'))
    ytd=num(val(row,'lowest_price_ytd'))
    disc=round(pct(val(row,'discount_pct','discount_percent')))
    pages=['amazon-deal-event',slug]
    band=val(row,'deal_price_band').lower()
    if (price and price<=50) or 'under 50' in band or 'under-$50' in band: add(pages,'amazon-deals-under-50')
    low=text(row)
    if any(w in low for w in DEVICE_TERMS) or brand.lower() in {'amazon','amazon basics','amazonbasics','ring','blink','eero'}: add(pages,'amazon-device-deals')
    if is_hh(row): add(pages,'amazon-household-essentials-deals')
    was=f'${ytd:.2f}' if ytd and price and ytd>price else ''
    return {'asin':asin,'pages':pages,'title':title,'brand':brand,'cat':val(row,'category_description','category') or tab,'image':f'https://images-na.ssl-images-amazon.com/images/P/{asin}.01._AC_SL500_.jpg','price':f'${price:.2f}' if price else 'See deal','price_amount':price,'was':was,'savings':was,'pct':disc,'discount':f'-{disc}%' if disc else '','deal_type':'SAMPLE_FROM_SHEET','availability':'','link':link(asin,val(row,'asin_url','url')),'hot':disc>=30,'hasCoupon':False,'couponDisplay':'','rating':num(val(row,'star_rating','rating')) or None,'review_count':None,'desc':brand,'seen_at':now,'updated_at':now}

def merge(d,deal):
    old=d.get(deal['asin'])
    if old:
        for p in deal['pages']: add(old['pages'],p)
        return old
    d[deal['asin']]=deal; return deal

def main():
    now=datetime.now(timezone.utc).isoformat(); deals={}; sampled={}
    for tab,slug in TABS.items():
        sampled[tab]=0
        for row in rows(tab):
            deal=build(row,tab,slug,now)
            if deal:
                merge(deals,deal); sampled[tab]+=1
            if sampled[tab]>=PER_CAT: break
    hh={a for a,d in deals.items() if 'amazon-household-essentials-deals' in d.get('pages',[])}
    for tab in HH_TABS:
        if len(hh)>=HH_LIMIT: break
        for row in rows(tab):
            if not is_hh(row): continue
            deal=build(row,tab,TABS[tab],now)
            if not deal: continue
            add(merge(deals,deal)['pages'],'amazon-household-essentials-deals'); hh.add(deal['asin'])
            if len(hh)>=HH_LIMIT: break
    page_counts={}
    for deal in deals.values():
        for p in deal['pages']: page_counts[p]=page_counts.get(p,0)+1
    out={'source':'Category-tab sample plus tightened household essentials scan','sheet_id':SHEET_ID,'partnerTag':TAG,'sampleMode':True,'samplePerCategory':PER_CAT,'householdScanCount':HH_LIMIT,'householdAdded':len(hh),'updatedAt':now,'count':len(deals),'totalDeals':len(deals),'pageCounts':page_counts,'sampledTabs':sampled,'tabPageMap':TABS,'deals':list(deals.values())}
    with open(OUTPUT_FILE,'w',encoding='utf-8') as f: json.dump(out,f,indent=2,ensure_ascii=False)
    print(f'Saved {len(deals)} product cards to {OUTPUT_FILE}')
    print(f'Household essentials on page: {len(hh)}')
    print(json.dumps(page_counts,indent=2))
if __name__=='__main__': main()
