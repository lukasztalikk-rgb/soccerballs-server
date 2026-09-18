const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

/* ── HTTP server (Glitch keepalive ping endpoint) ── */
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('SoccerBalls server OK');
});

/* ── WebSocket server ── */
const wss = new WebSocketServer({ server: httpServer });

let waiting = null;          /* quick-match queue: one slot */
const codeWaiting = {};      /* private rooms: code -> the socket waiting there */
const rooms = {};
let nextRoom = 1;

/* A socket can sit in readyState OPEN long after the peer is gone, because the
   OS has not timed the TCP connection out yet. Pairing someone with such a
   zombie hands them a match that will never start, so require recent proof of
   life: a pong (every 30s) or any client message. */
const LIVENESS_MS = 45000;
const isLive = s =>
  s && s.readyState === s.OPEN && Date.now() - (s.lastSeen || 0) < LIVENESS_MS;

function pair(a, b, code) {
  const rid = nextRoom++;
  rooms[rid] = { id: rid, p1: a, p2: b };
  a.rid = rid; a.role = 'p1';
  b.rid = rid; b.role = 'p2';
  a.send(JSON.stringify({ type: 'matched', role: 'p1', roomId: rid, code: code || null }));
  b.send(JSON.stringify({ type: 'matched', role: 'p2', roomId: rid, code: code || null }));
}

function cleanCode(v) {
  return String(v || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6);
}

/* Quick match: pair with whoever is waiting, else become the one waiting. */
function joinQuick(ws) {
  if (waiting && waiting !== ws && !isLive(waiting)) {
    try { waiting.terminate(); } catch (e) {}
    waiting = null;
  }
  if (waiting && waiting !== ws && waiting.readyState === waiting.OPEN) {
    const w = waiting; waiting = null;
    pair(w, ws, null);
  } else {
    waiting = ws;
    ws.send(JSON.stringify({ type: 'waiting' }));
  }
}

/* Private room: the first to name a code waits there, the second joins them.
   No queue, no bot fallback on the client — you are waiting for a friend. */
function joinCode(ws, code) {
  const w = codeWaiting[code];
  if (w && w !== ws && isLive(w)) {
    delete codeWaiting[code];
    pair(w, ws, code);
    return;
  }
  if (w && w !== ws) { try { w.terminate(); } catch (e) {} }
  codeWaiting[code] = ws;
  ws.code = code;
  ws.send(JSON.stringify({ type: 'waiting', code }));
}

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.lastSeen = Date.now();
  ws.on('pong', () => { ws.isAlive = true; ws.lastSeen = Date.now(); });

  /* Clients announce what they want first. Old clients never do, so after a
     short grace they are treated as quick-match, exactly as before. */
  ws.helloTimer = setTimeout(() => {
    if (!ws.rid && !ws.hello && ws.readyState === ws.OPEN) { ws.hello = true; joinQuick(ws); }
  }, 1500);

  ws.on('message', (raw, isBinary) => {
    ws.lastSeen = Date.now();   /* also counts as proof of life while queuing */
    const room = rooms[ws.rid];
    if (room) {
      const other = ws.role === 'p1' ? room.p2 : room.p1;
      if (!other || other.readyState !== other.OPEN) return;
      /* ws v8 always hands us a Buffer; relaying it as-is would send a binary
         frame, which reaches the browser as a Blob and breaks JSON.parse */
      other.send(isBinary ? raw : raw.toString());
      return;
    }
    if (ws.hello) return;   /* already queued, nothing to relay yet */
    let msg = null;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!msg || msg.type !== 'hello') return;
    clearTimeout(ws.helloTimer);
    ws.hello = true;
    const code = msg.mode === 'code' ? cleanCode(msg.code) : '';
    if (code) joinCode(ws, code); else joinQuick(ws);
  });

  ws.on('close', () => {
    clearTimeout(ws.helloTimer);
    if (waiting === ws) { waiting = null; return; }
    if (ws.code && codeWaiting[ws.code] === ws) { delete codeWaiting[ws.code]; return; }
    const room = rooms[ws.rid];
    if (!room) return;
    const other = ws.role === 'p1' ? room.p2 : room.p1;
    if (other && other.readyState === other.OPEN) {
      other.send(JSON.stringify({ type: 'opponent_left' }));
    }
    delete rooms[ws.rid];
  });

  ws.on('error', () => {});
});

/* ── Heartbeat — drop dead connections every 30s ── */
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

httpServer.listen(PORT, () => console.log(`SoccerBalls server on :${PORT}`));
