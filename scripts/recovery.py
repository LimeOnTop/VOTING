#!/usr/bin/env python3
"""Exercise queued-vote recovery on the LOCAL Compose project. Restarts worker in finally."""
import http.cookiejar,json,os,subprocess,time,urllib.request,urllib.error
BASE='http://localhost:8080'; TOKEN=os.environ['ADMIN_TOKEN']
def req(path,body=None,client=None,admin=False):
    headers={'Content-Type':'application/json'}
    if admin:headers['Authorization']='Bearer '+TOKEN
    r=urllib.request.Request(BASE+path,data=json.dumps(body).encode() if body else None,headers=headers)
    with (client.open(r,timeout=10) if client else urllib.request.urlopen(r,timeout=10)) as response:return json.load(response)
fmt=lambda t:time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime(t))
now=time.time();p=req('/api/admin/polls',{'question':'Проверка восстановления очереди','type':'single','options':['Да','Нет'],'opens_at':fmt(now-1),'closes_at':fmt(now+600)},admin=True)
client=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()));req('/api/session',client=client)
subprocess.run(['docker','compose','stop','worker'],check=True)
try:
    response=req('/api/polls/'+p['id']+'/votes',{'options':['1']},client)
    assert response['status']=='accepted',response
    time.sleep(1)
    result=req('/api/admin/polls/'+p['id']+'/results',admin=True)
    assert result['voters']==0,result
finally:subprocess.run(['docker','compose','start','worker'],check=True)
for _ in range(60):
    result=req('/api/admin/polls/'+p['id']+'/results',admin=True)
    if result['voters']==1:break
    time.sleep(.5)
assert result['voters']==1,result
print('PASS: durable acceptance while worker stopped; exactly one vote after restart')
# A retry after API memory loss must preserve the first result.
subprocess.run(['docker','compose','restart','api'],check=True)
for _ in range(60):
    try:
        req('/readyz')
        break
    except (OSError,urllib.error.HTTPError):time.sleep(.5)
else:raise AssertionError('API did not become ready after restart')
receipt=req('/api/polls/'+p['id']+'/receipt',client=client)
assert receipt['status']=='not_submitted_or_forgotten',receipt
response=req('/api/polls/'+p['id']+'/votes',{'options':['2']},client)
assert response['status']=='accepted',response
# Wait for a new participant to reach the aggregate.
other=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
req('/api/session',client=other)
req('/api/polls/'+p['id']+'/votes',{'options':['2']},other)
for _ in range(60):
    result=req('/api/admin/polls/'+p['id']+'/results',admin=True)
    if result['voters']==2:break
    time.sleep(.5)
time.sleep(2)
result=req('/api/admin/polls/'+p['id']+'/results',admin=True)
assert result['voters']==2 and result['counts']=={'1':1,'2':1},result
print('PASS: API memory loss permits replay; worker preserves first answer and aggregate')
