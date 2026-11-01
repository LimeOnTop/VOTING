import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';
const accepted = new Rate('votes_accepted');
const base = __ENV.BASE_URL || 'http://localhost:8080';
// Fresh identity per iteration; measures intake, not projection.
export const options = { noCookiesReset: false, scenarios: { votes: { executor: 'constant-arrival-rate', rate: Number(__ENV.RATE || 100), timeUnit:'1s',duration:__ENV.DURATION||'30s',preAllocatedVUs:100,maxVUs:1000 } }, thresholds: { 'http_req_failed': ['rate<0.01'], 'votes_accepted':['rate>0.99'], 'http_req_duration{endpoint:vote}':['p(99)<1000'] } };
export default function(){if(!__ENV.POLL_ID)throw Error('POLL_ID is required');http.get(base+'/api/v1/session',{tags:{endpoint:'session'}});const r=http.post(base+'/api/v1/polls/'+__ENV.POLL_ID+'/votes',JSON.stringify({options:['1']}),{headers:{'Content-Type':'application/json'},tags:{endpoint:'vote'}});accepted.add(r.status===202);check(r,{'durably accepted':r=>r.status===202});}
