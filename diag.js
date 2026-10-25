const { execFile } = require('node:child_process');
const out = { ts: new Date().toISOString(), psql: null, psqlErr: null };
function psql() {
  return new Promise((resolve) => {
    execFile('psql', [
      'postgresql://forgeyn_orchestrator.phvqvypgzrdflbbwzyat@aws-0-us-west-2.pooler.supabase.com:5432/postgres?sslmode=require',
      '-c', 'select current_user, current_database()',
    ], { env: { ...process.env, PGPASSWORD: 'FrgT0rch_2026_8xQzLmVn4Rp7' }, timeout: 20000 }, (err, stdout, stderr) => {
      resolve({ err: err ? String(err).slice(0, 300) : null, stdout: stdout.slice(0, 400), stderr: stderr.slice(0, 400) });
    });
  });
}
Bun.serve({
  port: 10000,
  fetch: async () => {
    const r = await psql();
    return new Response(JSON.stringify({ ...out, psql: r }, null, 2));
  },
});
console.log('psql diag serving on 10000');
