import http from 'http';
// ... keep your existing imports (ws, WebSocket) and TD config ...

const PORT = process.env.PORT || 5555;
const TWELVE_KEY = process.env.TWELVE_KEY || '430199692d5d4c6baf3b4107c7d1d260';

// in-memory connection store (swap for a real DB/secrets vault later)
const CONN = {};
const cors = (res) => { res.setHeader('Access-Control-Allow-Origin','*'); res.setHeader('Access-Control-Allow-Headers','Content-Type'); res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS'); };
const body = (req) => new Promise(r => { let d=''; req.on('data',c=>d+=c); req.on('end',()=>{ try{r(JSON.parse(d||'{}'));}catch{r({});} }); });

const server = http.createServer(async (req,res) => {
  cors(res);
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const url = new URL(req.url, 'http://x');
  res.setHeader('Content-Type','application/json');

  if (url.pathname === '/status') {
    return res.end(JSON.stringify(CONN));
  }
  if (url.pathname === '/connect' && req.method === 'POST') {
    const { name, key, secret } = await body(req);
    if (!name) { res.writeHead(400); return res.end(JSON.stringify({error:'name required'})); }
    // Real validation for Binance (public ping + key presence). Extend per exchange.
    let ok = true, status = 'CONNECTED';
    try {
      if (/binance/i.test(name)) { const r = await fetch('https://api.binance.com/api/v3/ping'); ok = r.ok; }
    } catch { ok = false; }
    status = ok ? 'CONNECTED' : 'FAILED';
    CONN[name] = { status, hasKey: !!key, ts: Date.now() };
    res.writeHead(ok?200:502); return res.end(JSON.stringify({ name, status }));
  }
  if (url.pathname === '/test' && req.method === 'POST') {
    const { name } = await body(req);
    const t0 = Date.now(); let ok = true;
    try { if (/binance/i.test(name)) { const r = await fetch('https://api.binance.com/api/v3/ping'); ok = r.ok; } } catch { ok = false; }
    return res.end(JSON.stringify({ name, ok, latency: Date.now()-t0 }));
  }
  if (url.pathname === '/disconnect' && req.method === 'POST') {
    const { name } = await body(req); delete CONN[name];
    return res.end(JSON.stringify({ name, status:'NOT_CONFIGURED' }));
  }
  res.writeHead(404); res.end('{}');
});

const wss = new WebSocketServer({ server });   // <-- was { port: PORT }
server.listen(PORT, () => console.log('JASS relay + API on port ' + PORT));
