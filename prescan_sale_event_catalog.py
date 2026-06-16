"""Balanced, resumable Amazon Creators API pricing refresh for the sale catalog."""
import json, os
from datetime import datetime, date
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo
from fetch_best_seller_deals import compact_image_url, get_amazon_items, iso_now

CATALOG=Path('sale-event-review-7f3k9'); MANIFEST=CATALOG/'manifest.json'; STATE=Path('sale_event_prescan_state.json'); REPORT=Path('sale_event_prescan_report.json')
BATCH_SIZE=int(os.getenv('SALE_PRESCAN_ASINS_PER_RUN','25000')); TAG=os.getenv('SALE_EVENT_AFFILIATE_TAG','blacklabdealsprime-20'); DAILY_REFRESH=os.getenv('SALE_PRESCAN_DAILY_REFRESH','0')=='1'
EVENT_WINDOWS=os.getenv('SALE_EVENT_REFRESH_WINDOWS','0')=='1'
EVENT_START=date.fromisoformat(os.getenv('SALE_EVENT_REFRESH_START_DATE','2026-06-23'))
EVENT_END=date.fromisoformat(os.getenv('SALE_EVENT_REFRESH_END_DATE','2026-06-25'))
EVENT_STOP_AT=os.getenv('SALE_EVENT_REFRESH_STOP_AT','2026-06-26T00:00:00-04:00')
def load(path,default):
    try:return json.loads(Path(path).read_text(encoding='utf-8'))
    except (FileNotFoundError,json.JSONDecodeError):return default
def save(path,data,compact=False):Path(path).write_text(json.dumps(data,separators=(',',':') if compact else None,indent=None if compact else 2)+'\n',encoding='utf-8')
def refresh_period(now_et):
    if EVENT_WINDOWS:
        stop_at=datetime.fromisoformat(EVENT_STOP_AT)
        if now_et>=stop_at:
            return None
        if EVENT_START<=now_et.date()<=EVENT_END:
            if now_et.hour>=15:return f'{now_et.date().isoformat()}-pm'
            if now_et.hour>=3:return f'{now_et.date().isoformat()}-am'
            return None
    return now_et.date().isoformat() if now_et.hour>=3 else None
def product_from_item(asin,item,previous):
    if not item:return None
    try:
        listing=item.offers_v2.listings[0]; amount=float(listing.price.money.amount); price=listing.price.money.display_amount; currency=listing.price.money.currency; title=item.item_info.title.display_value
    except Exception:return None
    try:
        if listing.condition.value and listing.condition.value.lower()!='new':return None
    except Exception:pass
    try:availability=str(listing.availability.type or '')
    except Exception:availability=''
    if availability.upper()=='UNAVAILABLE':return None
    try:image=compact_image_url(item.images.primary.large.url,size=300)
    except Exception:image=previous.get('image')
    try:link=item.detail_page_url
    except Exception:link=f'https://www.amazon.com/dp/{asin}'
    parsed=urlparse(link); query=dict(parse_qsl(parsed.query)); query['tag']=TAG
    return {'title':title,'image':image,'price':price,'price_amount':amount,'currency':currency,'availability':availability,'link':urlunparse(parsed._replace(query=urlencode(query))),'updated_at':iso_now()}
def main():
    manifest=load(MANIFEST,{}); all_categories=manifest.get('categories',[]); scan_categories=[c for c in all_categories if c.get('name')!='Amazon Basics']
    state=load(STATE,{'categoryCursors':{},'completedAsins':0,'runs':0})
    now_et=datetime.now(ZoneInfo('America/New_York')); today=now_et.date().isoformat(); period=refresh_period(now_et)
    if DAILY_REFRESH and not period:
        print('No sale refresh window is active now; skipping pricing scan')
        state.update({'lastSkippedAt':iso_now(),'skipReason':'outside-sale-refresh-window'})
        save(STATE,state)
        return
    if DAILY_REFRESH and state.get('refreshPeriod')!=period:
        state={'mode':'balanced-sale-event-pricing-refresh','categoryCursors':{},'completedAsins':0,'runs':0,'dailyRefreshDate':today,'refreshPeriod':period,'dailyRefreshStartedAt':iso_now(),'complete':False}
        print(f'Reset full catalog for sale pricing refresh window: {period}')
    cursors=state.get('categoryCursors',{}); stores={}; locations={}; pools={c['name']:[] for c in scan_categories}
    for category in all_categories:
        name=category['name']
        for filename in category.get('files',[]):
            rows=load(CATALOG/filename,[]); stores[filename]=rows
            for index,product in enumerate(rows):
                asin=str(product.get('asin','')).upper()
                if asin:
                    locations.setdefault(asin,[]).append((filename,index))
                    if name!='Amazon Basics':pools[name].append(asin)
    active=[n for n,a in pools.items() if a and int(cursors.get(n,0))<len(a)]; selected=[]
    while len(selected)<BATCH_SIZE and active:
        next_active=[]
        for name in active:
            cursor=int(cursors.get(name,0))
            if cursor<len(pools[name]):
                asin=pools[name][cursor]; cursors[name]=cursor+1
                if asin not in selected:selected.append(asin)
            if int(cursors.get(name,0))<len(pools[name]):next_active.append(name)
            if len(selected)>=BATCH_SIZE:break
        active=next_active
    if not selected:
        state.update({'complete':True,'completedAt':iso_now(),'categoryCursors':cursors}); save(STATE,state); print('Pricing refresh window already complete'); return
    items=get_amazon_items(selected); returned=prices=images=0; touched=set(); successful=set()
    for asin in selected:
        verified=None
        for filename,index in locations.get(asin,[]):
            row=stores[filename][index]
            if verified is None:verified=product_from_item(asin,items.get(asin),row.get('amazon',{}))
            if verified:
                row['amazon']=verified; touched.add(filename); row['link']=verified.get('link',row.get('link')); successful.add(asin)
            else:row['amazonAttemptedAt']=iso_now(); touched.add(filename)
    returned=len(successful); prices=sum(bool(product_from_item(a,items.get(a),{}).get('price')) for a in successful); images=sum(bool(product_from_item(a,items.get(a),{}).get('image')) for a in successful)
    for filename in touched:save(CATALOG/filename,stores[filename],compact=True)
    completed=sum(min(int(cursors.get(n,0)),len(pools[n])) for n in pools); total=sum(len(a) for a in pools.values())
    state={'mode':'balanced-sale-event-pricing-refresh','categoryCursors':cursors,'completedAsins':completed,'totalAsins':total,'remainingAsins':max(0,total-completed),'runs':int(state.get('runs',0))+1,'lastRunAt':iso_now(),'dailyRefreshDate':today,'refreshPeriod':period,'dailyRefreshStartedAt':state.get('dailyRefreshStartedAt',iso_now()),'complete':completed>=total}
    report={'selected':len(selected),'returnedProducts':returned,'returnedPrices':prices,'returnedImages':images,'completedAsins':completed,'totalAsins':total,'remainingAsins':max(0,total-completed),'dailyRefreshDate':state['dailyRefreshDate'],'refreshPeriod':period,'updatedAt':iso_now()}
    save(STATE,state); save(REPORT,report); print(json.dumps(report,indent=2))
if __name__=='__main__':main()
