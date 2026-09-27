// The shell path, end to end, against a fake Cloudflare: sign-in (PKCE, browser round trip),
// account, KV, deploy, pulse, and the connector line. No questions are asked anywhere.
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const CLI = join(dirname(fileURLToPath(import.meta.url)), '..', 'create-mazel', 'bin', 'create-mazel.js');
let pass=0, fail=0; const ok=(l,c,x='')=>{ console.log((c?'PASS ':'FAIL ')+l+(x?'  -> '+String(x).replace(/\n/g,' ').slice(0,130):'')); c?pass++:fail++; };
const b64u = (b) => Buffer.from(b).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
const json = (res, obj, status=200) => { res.writeHead(status, {'content-type':'application/json'}); res.end(JSON.stringify(obj)); };

// A fake Cloudflare: the OAuth token endpoint and the bits of the API the installer touches.
const seen = { deploy: null, kv: null, crons: null, verifier: null, grant: null };
let challenge = null, scripts = [];
const resetChallenge = () => { challenge = null; };
const fake = createServer(async (req, res) => {
  const u = new URL(req.url, 'http://127.0.0.1');
  const body = await new Promise(r => { let b=''; req.on('data', c=>b+=c); req.on('end', ()=>r(b)); });
  if (u.pathname === '/portal/card') return json(res, { name:'unnamed@mazel' });  // the verify poll at the end
  if (u.pathname === '/oauth2/token') {
    const p = new URLSearchParams(body);
    seen.grant = p.get('grant_type'); seen.verifier = p.get('code_verifier');
    // PKCE: the verifier must hash to the challenge the browser was sent with.
    if (b64u(createHash('sha256').update(p.get('code_verifier')||'').digest()) !== challenge) return json(res, { error:'invalid_grant' }, 400);
    return json(res, { access_token:'fake-access', token_type:'bearer', expires_in:3600 });
  }
  if (req.headers.authorization !== 'Bearer fake-access') return json(res, { success:false, errors:[{message:'bad token'}] }, 401);
  if (u.pathname === '/accounts') return json(res, { success:true, result:[{ id:'acc1', name:"Someone's Account" }] });
  if (u.pathname.endsWith('/workers/subdomain')) return json(res, { success:true, result:{ subdomain:'mazel-abcd' } });
  if (u.pathname.endsWith('/workers/scripts')) return json(res, { success:true, result: scripts });
  if (u.pathname.endsWith('/storage/kv/namespaces') && req.method === 'GET') return json(res, { success:true, result: [] });
  if (u.pathname.endsWith('/storage/kv/namespaces')) { seen.kv = JSON.parse(body).title; return json(res, { success:true, result:{ id:'ns1', title: seen.kv } }); }
  if (u.pathname.endsWith('/schedules')) { seen.crons = JSON.parse(body); return json(res, { success:true, result:{} }); }
  if (/\/workers\/scripts\/mazel\/subdomain$/.test(u.pathname)) return json(res, { success:true, result:{} });
  if (/\/workers\/scripts\/mazel$/.test(u.pathname) && req.method === 'PUT') {
    const m = body.match(/\{"main_module".*?\}\]\}/s); seen.deploy = m ? JSON.parse(m[0]) : { raw: body.slice(0,200) };
    scripts = [{ id:'mazel' }];
    return json(res, { success:true, result:{ id:'mazel' } });
  }
  return json(res, { success:false, errors:[{message:'unexpected '+u.pathname}] }, 404);
});
await new Promise(r => fake.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${fake.address().port}`;
// A free port per run: two runs must never contend, and a run that outlives us must not wedge
// the next one.
const freePort = async () => { const s = createServer(); await new Promise(r => s.listen(0, '127.0.0.1', r)); const p = s.address().port; await new Promise(r => s.close(r)); return p; };
let PORT = 0;

const run = (mode = 'good') => new Promise((resolve) => {
  resetChallenge();
  let played = false;
  const p = spawn(process.execPath, [CLI], { env: { ...process.env,
    MAZEL_API_BASE: base, MAZEL_OAUTH_AUTH: base + '/oauth2/auth', MAZEL_OAUTH_TOKEN: base + '/oauth2/token',
    MAZEL_OAUTH_PORT: String(PORT), MAZEL_NO_BROWSER: '1', MAZEL_PORTAL_BASE: base + '/portal',
    CLOUDFLARE_API_TOKEN: '' } });
  let out = '';
  p.stdout.on('data', async (c) => {
    out += c;
    // The installer prints the URL it wants opened; play the browser and come back with a code.
    const m = out.match(/\/oauth2\/auth\?\S+/);
    if (m && !played) {
      played = true;
      const q = new URL(base + m[0]).searchParams;
      challenge = q.get('code_challenge');
      if (mode === 'good') {
        ok('sign-in uses PKCE with S256', q.get('code_challenge_method') === 'S256' && !!challenge);
        ok('sign-in asks for exactly the scopes it uses', q.get('scope') === 'account:read user:read workers:write workers_kv:write workers_scripts:write workers_routes:write offline_access', q.get('scope'));
        ok('sign-in sends the browser back to this machine only', q.get('redirect_uri') === `http://localhost:${PORT}/oauth/callback`, q.get('redirect_uri'));
      }
      const state = mode === 'badstate' ? 'wrong' : q.get('state');
      await fetch(`http://127.0.0.1:${PORT}/oauth/callback?code=thecode&state=${encodeURIComponent(state)}`).catch(()=>{});
    }
  });
  p.stderr.on('data', (c) => { out += c; });
  const giveUp = setTimeout(() => { p.kill('SIGKILL'); resolve({ code: 'hung', out }); }, 30000);
  p.on('close', (code) => { clearTimeout(giveUp); resolve({ code, out }); });
});

PORT = await freePort();
const first = await run();
if (process.env.DEBUG_CLI) console.log('--- installer said ---\n' + first.out + '\n---');
ok('it never asks a question', !/\?\s*$/m.test(first.out.replace(/https?:\S+/g,'')) && !/handle|persona|Need \(|Have \(/i.test(first.out), first.out.match(/.*handle.*/i)?.[0] || '');
ok('it says the consent screen will name wrangler, before opening it', /will say Wrangler/.test(first.out) && first.out.indexOf('will say Wrangler') < first.out.indexOf('/oauth2/auth'));
ok('it exchanges the code for a token with PKCE', seen.grant === 'authorization_code' && !!seen.verifier);
ok('it makes the mailbox', seen.kv === 'mazel-MAILBOX', seen.kv);
ok('it sets the pulse to every 30 minutes', JSON.stringify(seen.crons) === '[{"cron":"*/30 * * * *"}]', JSON.stringify(seen.crons));
const names = Object.fromEntries((seen.deploy?.bindings || []).map(b => [b.name, b.type]));
ok('a new portal deploys with a mailbox, a key, and no card at all', names.MAILBOX === 'kv_namespace' && names.INBOX_TOKEN === 'secret_text' && !('HANDLE' in names) && !('PERSONA' in names), JSON.stringify(names));
ok('it knows its own address, so the scheduled pulse can run', names.PORTAL_ORIGIN === 'plain_text' && names.RELAY_URL === 'plain_text');
ok('it prints one connector line to paste into an AI', /\/mcp\?token=[a-f0-9]{64}/.test(first.out) && /Paste this into your AI/.test(first.out));
ok('it says what to say next', /say "mazel"/.test(first.out));
ok('it ends clean', first.code === 0, 'exit ' + first.code);
ok('nothing was written to this machine', !/\.wrangler|config\.toml|saved|stored/i.test(first.out));

// A sign-in that comes back with the wrong state is an attempt, not a mistake: it stops there.
scripts = [];
PORT = await freePort();
const tampered = await run('badstate');
ok('a callback with the wrong state stops the install', tampered.code === 1 && /wrong state; nothing was done/.test(tampered.out), tampered.out.trim().split('\n').pop());

fake.close();
console.log(`\ncli: ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
