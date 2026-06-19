"""Controlled Amazon Creators API rate test."""
import csv, io, json, os, time
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from datetime import datetime, timezone
from urllib.parse import quote
from urllib.request import Request, urlopen
from amazon_creatorsapi import AmazonCreatorsApi, Country
from amazon_creatorsapi.errors import TooManyRequestsError
from amazon_creatorsapi.models import GetItemsResource

SHEET_ID="15kLY4nPg-ydq9kcqUcSTrzttQxtCkeXGoU2HoO4KTkc"; SHEET_TAB=os.getenv("RATE_TEST_SHEET_TAB","Electronics")
SAMPLE_ASINS=int(os.getenv("RATE_TEST_SAMPLE_ASINS","1200")); LEVELS=[int(v) for v in os.getenv("RATE_TEST_LEVELS","2,3,4").split(",") if v.strip()]
REQUESTS_PER_LEVEL=int(os.getenv("RATE_TEST_REQUESTS_PER_LEVEL","8")); SECONDS_PER_LEVEL=int(os.getenv("RATE_TEST_SECONDS_PER_LEVEL","0")); PAUSE=float(os.getenv("RATE_TEST_LEVEL_PAUSE_SECONDS","10"))
CID=os.getenv("CREATORS_CREDENTIAL_ID"); SECRET=os.getenv("CREATORS_CREDENTIAL_SECRET"); TAG=os.getenv("AFFILIATE_TAG","sawdustsavings-20")

def sheet_asins():
    u=f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={quote(SHEET_TAB)}&tq={quote(f'select A limit {SAMPLE_ASINS+1}')}"
    r=Request(u,headers={"User-Agent":"BlackLabDeals-CreatorsRateTest/1.0"})
    text=urlopen(r,timeout=90).read().decode("utf-8-sig",errors="replace")
    rows=list(csv.reader(io.StringIO(text)))
    return list(dict.fromkeys(x[0].strip() for x in rows[1:] if x and x[0].strip()))[:SAMPLE_ASINS]

def request_batch(batch):
    started=time.perf_counter(); client=AmazonCreatorsApi(credential_id=CID,credential_secret=SECRET,version="3.1",tag=TAG,country=Country.US,throttling=0)
    try:
        items=client.get_items(batch,resources=[GetItemsResource.ITEM_INFO_DOT_TITLE,GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_PRICE,GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_AVAILABILITY])
        return {"status":"success","items":len(items),"seconds":round(time.perf_counter()-started,3)}
    except TooManyRequestsError as e:
        return {"status":"throttled","items":0,"seconds":round(time.perf_counter()-started,3),"error":str(e)}
    except Exception as e:
        return {"status":"error","items":0,"seconds":round(time.perf_counter()-started,3),"error":f"{type(e).__name__}: {e}"}

def summarize(c, started, results):
    elapsed=time.perf_counter()-started
    return {"concurrency":c,"requests":len(results),"successes":sum(x["status"]=="success" for x in results),"throttled":sum(x["status"]=="throttled" for x in results),"errors":sum(x["status"]=="error" for x in results),"itemsReturned":sum(x["items"] for x in results),"elapsedSeconds":round(elapsed,3),"observedRequestsPerSecond":round(len(results)/elapsed,3) if elapsed else 0,"results":results[-50:]}

def run_level(batches,c):
    started=time.perf_counter(); results=[]; idx=0
    if SECONDS_PER_LEVEL<=0:
        with ThreadPoolExecutor(max_workers=c) as ex:
            for f in as_completed([ex.submit(request_batch,b) for b in batches[:REQUESTS_PER_LEVEL]]): results.append(f.result())
    else:
        deadline=started+SECONDS_PER_LEVEL
        with ThreadPoolExecutor(max_workers=c) as ex:
            pending=set()
            while time.perf_counter()<deadline or pending:
                while time.perf_counter()<deadline and len(pending)<c:
                    pending.add(ex.submit(request_batch,batches[idx%len(batches)])); idx+=1
                done,pending=wait(pending,timeout=1,return_when=FIRST_COMPLETED)
                for f in done:
                    result=f.result(); results.append(result)
                    if result["status"]=="throttled":
                        deadline=0
    s=summarize(c,started,results); print(json.dumps(s,indent=2)); return s

def main():
    if not CID or not SECRET: raise RuntimeError("Missing Creators API credentials")
    a=sheet_asins(); batches=[a[i:i+10] for i in range(0,len(a),10) if len(a[i:i+10])==10]
    report={"testedAt":datetime.now(timezone.utc).isoformat(),"sheetTab":SHEET_TAB,"sampleAsins":len(a),"secondsPerLevel":SECONDS_PER_LEVEL,"requestsPerLevel":REQUESTS_PER_LEVEL,"levels":[]}
    for c in LEVELS:
        s=run_level(batches,c); report["levels"].append(s)
        if s["throttled"]: break
        time.sleep(PAUSE)
    open("creators_api_rate_test_report.json","w",encoding="utf-8").write(json.dumps(report,indent=2)+"\n")
if __name__=="__main__": main()
