#!/usr/bin/env python3
"""ES -> one bulk-ready NDJSON per 15-minute window. Resumes where it left off.
env:  ES_URL ES_INDEX ES_API_KEY [ES_TIME_FIELD]
cron: */15 * * * * ./dump.py >> dump.log 2>&1
load: ./load.sh https://other-cluster:9243 <api-key>"""
import json, os, datetime as dt, urllib.request as u

U, I = os.environ["ES_URL"], os.environ["ES_INDEX"]
TF = os.environ.get("ES_TIME_FIELD", "@timestamp")
H = {"Content-Type": "application/json", "Authorization": "ApiKey " + os.environ["ES_API_KEY"]}
W = 900000  # 15 min in ms
# no search_after on the first run -> starts at the oldest document
after = json.load(open("state.json")).get("after") if os.path.exists("state.json") else None
f = bucket = None
n = 0
while True:
    q = {"size": 10000, "track_total_hits": False, "sort": [{TF: "asc"}],
         "query": {"range": {TF: {"lt": "now-1m"}}}}
    if after: q["search_after"] = after
    hits = json.load(u.urlopen(u.Request(f"{U}/{I}/_search", json.dumps(q).encode(), H)))["hits"]["hits"]
    for h in hits:
        b = int(h["sort"][0]) // W
        if b != bucket:  # crossed into the next 15-minute window -> next file
            if f: f.close()
            bucket, name = b, dt.datetime.fromtimestamp(b * W / 1000, dt.timezone.utc)
            f = open(name.strftime("es-%Y%m%dT%H%M%SZ.ndjson"), "a")
        f.write(json.dumps({"index": {"_index": h["_index"], "_id": h["_id"]}}) + "\n"
                + json.dumps(h["_source"]) + "\n")
        n += 1
    if not hits: break
    after = hits[-1]["sort"]
    if f: f.flush()
    json.dump({"after": after}, open("state.json", "w"))  # resume point
    if len(hits) < 10000: break
if f: f.close()
print(n, "docs")
