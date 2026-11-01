#!/usr/bin/env python3
"""Black-box test: run against an isolated local Compose stack, not production."""
import concurrent.futures, http.cookiejar, json, os, time, urllib.request, urllib.error
BASE=os.getenv('BASE_URL','http://localhost:8080')
TOKEN=os.environ['ADMIN_TOKEN']
def request(path,body=None,opener=None,admin=False):
    headers={'Content-Type':'application/json'}
    if admin: headers['Authorization']='Bearer '+TOKEN
    req=urllib.request.Request(BASE+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
    with (opener.open(req,timeout=10) if opener else urllib.request.urlopen(req,timeout=10)) as r: return r.status,json.load(r)
now=time.time()
fmt=lambda t:time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime(t))
_,poll=request('/api/admin/polls',{'question':'Какой транспорт вы выбираете?','type':'single','options':['Общественный','Личный','Пешком'],'opens_at':fmt(now-1),'closes_at':fmt(now+600)},admin=True)
path='/api/polls/'+poll['id']
jar=http.cookiejar.CookieJar();browser=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
request('/api/session',opener=browser)
cookie='; '.join(c.name+'='+c.value for c in jar)
def vote(n):
    req=urllib.request.Request(BASE+path+'/votes',data=json.dumps({'options':['1' if n%2==0 else '2']}).encode(),headers={'Content-Type':'application/json','Cookie':cookie})
    with urllib.request.urlopen(req,timeout=10) as r: assert r.status==202
with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:list(pool.map(vote,range(40)))
for _ in range(60):
    _,receipt=request(path+'/receipt',opener=browser)
    if receipt['status']=='counted':break
    time.sleep(.5)
else:raise AssertionError('projection timed out')
# Also count one independent browser.
other=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
request('/api/session',opener=other);request(path+'/votes',{'options':['3']},opener=other)
for _ in range(60):
    _,result=request('/api/admin/polls/'+poll['id']+'/results',admin=True)
    if result['voters']==2:break
    time.sleep(.5)
assert result['voters']==2,result
assert sum(result['counts'].values())==2,result
assert result['counts']['3']==1,result
try: request(path+'/votes',{'options':['1']})
except urllib.error.HTTPError as e:assert e.code==428
else:raise AssertionError('missing cookie accepted')
print(json.dumps({'status':'PASS','repeated_requests':40,'unique_voters':2,'counts':result['counts'],'poll_url':BASE+'/p/'+poll['id']},ensure_ascii=False))
