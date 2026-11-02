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

/* Two quick-match slots instead of one.
   A match only counts for the ranking when BOTH players have an account: the
   rating is settled by the database once the two reports agree, and a guest
   has nothing to report and no rating to move. So players with an account
   are paired with each other first. If that were the whole rule, a lone
   ranked player and a lone guest would both sit in their own queue and then
   both get a bot, which is worse for everybody. After CROSS_MS the two
   queues are allowed to meet; that match simply is not counted, and the
   client already says so on the result screen. */
let waitingRanked = null;
let waitingCasual = null;
const CROSS_MS = 15000;
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

/* Gospodarzem zostaje wylosowany gracz, nie ten, kto dluzej czekal.
   Wczesniej p1 przypadalo zawsze osobie stojacej juz w kolejce, a to nie jest
   bez znaczenia: gospodarz liczy fizyke autorytatywnie, wiec nie oglada korekt
   predykcji. Kto szukal meczu w spokojniejszej porze i czekal dluzej, ten mial
   stala, cicha przewage. Rzut moneta to wyrownuje, a przy boiskach gospodarza
   staje sie wrecz konieczny, zeby przewaga wlasnego boiska rozkladala sie po
   polowie. */
function pair(a, b, code) {
  if (Math.random() < 0.5) { const t = a; a = b; b = t; }
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

/* Drop a slot whose occupant is gone or has gone quiet. */
function sweepSlot(which) {
  const s = which === 'ranked' ? waitingRanked : waitingCasual;
  if (s && !isLive(s)) {
    try { s.terminate(); } catch (e) {}
    if (which === 'ranked') waitingRanked = null; else waitingCasual = null;
  }
}

function takeSlot(which) {
  const s = which === 'ranked' ? waitingRanked : waitingCasual;
  if (which === 'ranked') waitingRanked = null; else waitingCasual = null;
  return s;
}

/* Quick match: meet your own kind first, then anyone. */
function joinQuick(ws, ranked) {
  ws.ranked = !!ranked;
  ws.queuedAt = Date.now();
  sweepSlot('ranked'); sweepSlot('casual');

  const mine  = ws.ranked ? 'ranked' : 'casual';
  const other = ws.ranked ? 'casual' : 'ranked';
  const same  = ws.ranked ? waitingRanked : waitingCasual;

  if (same && same !== ws) { pair(takeSlot(mine), ws, null); return; }

  /* somebody has been in the other queue long enough to stop being picky */
  const cross = ws.ranked ? waitingCasual : waitingRanked;
  if (cross && cross !== ws && Date.now() - (cross.queuedAt || 0) >= CROSS_MS) {
    pair(takeSlot(other), ws, null);
    return;
  }

  if (ws.ranked) waitingRanked = ws; else waitingCasual = ws;
  ws.send(JSON.stringify({ type: 'waiting' }));
}

/* Nobody new has to arrive for the two queues to meet, so check on a timer. */
setInterval(() => {
  sweepSlot('ranked'); sweepSlot('casual');
  if (!waitingRanked || !waitingCasual) return;
  const oldest = Math.min(waitingRanked.queuedAt || 0, waitingCasual.queuedAt || 0);
  if (Date.now() - oldest < CROSS_MS) return;
  const a = takeSlot('ranked'), b = takeSlot('casual');
  pair(a, b, null);
}, 2000);

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
    if (!ws.rid && !ws.hello && ws.readyState === ws.OPEN) { ws.hello = true; joinQuick(ws, false); }
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
    if (code) joinCode(ws, code); else joinQuick(ws, msg.ranked === true);
  });

  ws.on('close', () => {
    clearTimeout(ws.helloTimer);
    if (waitingRanked === ws) { waitingRanked = null; return; }
    if (waitingCasual === ws) { waitingCasual = null; return; }
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
