const { execFile } = require('node:child_process');
const dns = require('node:dns').promises;
const IPS = ['54.70.143.232', '44.238.118.41', '35.160.209.8'];
function psqlHost(host) {
  return new Promise((resolve) => {
    const t = setTimeout(() => resolve({ host, result: 'TIMEOUT 25s' }), 25000);
    execFile('psql', [
      `postgresql://forgeyn_orchestrator.phvqvypgzrdflbbwzyat@${host}:5432/postgres?sslmode=require`,
      '-c', 'select 1 as ok',
    ], { env: { ...process.env, PGPASSWORD: 'FrgT0rch_2026_8xQzLmVn4Rp7' }, timeout: 22000 }, (err, stdout, stderr) => {
      clearTimeout(t);
      resolve({ host, result: err ? ('ERR: ' + (stderr || String(err)).slice(0, 150)) : 'OK: ' + stdout.trim().slice(0, 60) });
    });
  });
}
Bun.serve({
  port: 10000,
  fetch: async () => {
    const out = { ts: new Date().toISOString(), dns: null, perIp: [] };
    try { out.dns = await dns.lookup('aws-0-us-west-2.pooler.supabase.com', { all: true }); } catch (e) { out.dns = String(e); }
    out.perIp = await Promise.all(IPS.map(psqlHost));
    return new Response(JSON.stringify(out, null, 2));
  },
});
console.log('per-IP psql diag on 10000');
