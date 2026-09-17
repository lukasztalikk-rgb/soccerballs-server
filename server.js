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

let waiting = null;
const rooms = {};
let nextRoom = 1;

/* A socket can sit in readyState OPEN long after the peer is gone, because the
   OS has not timed the TCP connection out yet. Pairing someone with such a
   zombie hands them a match that will never start, so require recent proof of
   life: a pong (every 30s) or any client message. */
const LIVENESS_MS = 45000;
const isLive = s =>
  s && s.readyState === s.OPEN && Date.now() - (s.lastSeen || 0) < LIVENESS_MS;

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.lastSeen = Date.now();
  ws.on('pong', () => { ws.isAlive = true; ws.lastSeen = Date.now(); });

  if (waiting && !isLive(waiting) && waiting !== ws) {
    /* stale straggler still occupying the queue — drop it */
    try { waiting.terminate(); } catch (e) {}
    waiting = null;
  }

  if (waiting && waiting.readyState === waiting.OPEN) {
    /* pair up */
    const rid = nextRoom++;
    const room = { id: rid, p1: waiting, p2: ws };
    rooms[rid] = room;

    waiting.rid  = rid; waiting.role  = 'p1';
    ws.rid       = rid; ws.role       = 'p2';

    waiting.send(JSON.stringify({ type: 'matched', role: 'p1', roomId: rid }));
    ws.send(JSON.stringify({ type: 'matched', role: 'p2', roomId: rid }));
    waiting = null;
  } else {
    waiting = ws;
    ws.send(JSON.stringify({ type: 'waiting' }));
  }

  ws.on('message', (raw, isBinary) => {
    ws.lastSeen = Date.now();   /* also counts as proof of life while queuing */
    const room = rooms[ws.rid];
    if (!room) return;
    const other = ws.role === 'p1' ? room.p2 : room.p1;
    if (!other || other.readyState !== other.OPEN) return;
    /* ws v8 always hands us a Buffer; relaying it as-is would send a binary
       frame, which reaches the browser as a Blob and breaks JSON.parse */
    other.send(isBinary ? raw : raw.toString());
  });

  ws.on('close', () => {
    if (waiting === ws) { waiting = null; return; }
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
