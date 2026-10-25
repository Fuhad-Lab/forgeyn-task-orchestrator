const dns = require('node:dns').promises;
const out = { ts: new Date().toISOString(), dns: null, v4: null, rest: null, engine: null, jwks: null };
try { out.dns = await dns.lookup('aws-0-us-west-2.pooler.supabase.com', { all: true }); } catch (e) { out.dns = String(e); }
function tryConnect(host, port, timeoutMs) {
  return new Promise((resolve) => {
    let done = false;
    const t = setTimeout(() => { if (!done) { done = true; resolve('TIMEOUT after ' + timeoutMs + 'ms'); } }, timeoutMs);
    try {
      Bun.connect({
        hostname: host, port,
        socket: {
          open(s) { if (!done) { done = true; clearTimeout(t); resolve('CONNECTED'); } s.end(); },
          error(s, err) { if (!done) { done = true; clearTimeout(t); resolve('ERROR: ' + String(err)); } },
          data() {}, close() {},
        },
      });
    } catch (e) { if (!done) { done = true; clearTimeout(t); resolve('THROW: ' + String(e)); } }
  });
}
out.v4 = await tryConnect('aws-0-us-west-2.pooler.supabase.com', 5432, 8000);
try { out.rest = (await fetch('https://phvqvypgzrdflbbwzyat.supabase.co/rest/v1/', { method: 'GET' })).status; } catch (e) { out.rest = String(e); }
try { out.engine = (await fetch('https://forgevi-engine-2ob6.onrender.com/health', { method: 'GET' })).status; } catch (e) { out.engine = String(e); }
try { out.jwks = (await fetch('https://forgevi-engine-2ob6.onrender.com/jwks', { method: 'GET' })).status; } catch (e) { out.jwks = String(e); }
Bun.serve({ port: 10000, fetch: () => new Response(JSON.stringify(out, null, 2)) });
console.log('diag serving on 10000');
