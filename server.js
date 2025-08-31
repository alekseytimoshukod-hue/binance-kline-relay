// server.js
import 'dotenv/config';
import http from 'http';
import axios from 'axios';
import WebSocket from 'ws';

/** ====== ENV ====== */
const PD_WEBHOOK_1H = process.env.PD_WEBHOOK_1H;         // из Pipedream (SPOT_LONG Generator)
const PD_WEBHOOK_4H = process.env.PD_WEBHOOK_4H;         // из Pipedream (Crypto Alert Bot)
const SECRET        = process.env.WEBHOOK_SECRET || '';

const SYMBOLS   = (process.env.SYMBOLS || 'BTCUSDT,ETHUSDT,SOLUSDT').split(',');
const INTERVALS = ['1h','4h'];

/** троттлинг / batching */
const THROTTLE_SEC   = parseInt(process.env.THROTTLE_SEC || '20', 10);
const MIN_PCT_MOVE   = parseFloat(process.env.MIN_PCT_MOVE || '0.10'); // 0.10%
const BATCH_WINDOWMS = parseInt(process.env.BATCH_WINDOW_MS || '3000', 10);

/** ====== WS URL ====== */
const streams = [];
for (const s of SYMBOLS) for (const i of INTERVALS) streams.push(`${s.toLowerCase()}@kline_${i}`);
const WS_URL = `wss://stream.binance.com:9443/stream?streams=${streams.join('/')}`;

/** ====== Служебные ====== */
let ws, pingTimer;
const lastSent = new Map();                      // key="SYMBOL|INTERVAL" -> { ts, price, high, low }
const buffers  = { '1h': [], '4h': [] };
const timers   = { '1h': null, '4h': null };

function queue(interval, payload) {
  buffers[interval].push(payload);
  if (!timers[interval]) {
    timers[interval] = setTimeout(async () => {
      const arr = buffers[interval].splice(0);
      timers[interval] = null;
      try {
        const url = interval === '1h' ? PD_WEBHOOK_1H : PD_WEBHOOK_4H;
        if (!url || arr.length === 0) return;
        await axios.post(url, arr, { headers: { 'X-Auth': SECRET } });
        console.log(`POST ${interval}:`, arr.length);
      } catch (e) {
        console.error(`POST ${interval} error:`, e.message);
      }
    }, BATCH_WINDOWMS);
  }
}
function shouldSendLive(key, price, high, low) {
  const now = Date.now();
  const s = lastSent.get(key) || { ts: 0, price: 0, high, low };
  const dt = (now - s.ts) / 1000;
  if (dt < THROTTLE_SEC) return false;
  if (s.price === 0) return true;
  const pct = Math.abs(price - s.price) / s.price * 100;
  return pct >= MIN_PCT_MOVE || high > s.high || low < s.low;
}
function markSent(key, price, high, low) {
  lastSent.set(key, { ts: Date.now(), price, high, low });
}

/** ====== WS client ====== */
function connect() {
  console.log('Connecting WS:', WS_URL);
  ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    console.log('WS connected');
    clearInterval(pingTimer);
    pingTimer = setInterval(() => { try { ws.ping(); } catch {} }, 60_000);
  });

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      const d = msg?.data;
      if (d?.e !== 'kline' || !d?.k) return;

      const k = d.k; // объект свечи
      const interval = k.i; // '1h' | '4h'
      if (interval !== '1h' && interval !== '4h') return;

      const key   = `${d.s}|${interval}`;
      const close = Number(k.c);
      const high  = Number(k.h);
      const low   = Number(k.l);
      const isFinal = !!k.x;

      if (isFinal || shouldSendLive(key, close, high, low)) {
        const payload = {
          symbol: d.s,
          interval,
          openTime: k.t,
          closeTime: k.T,
          open: k.o,
          high: k.h,
          low:  k.l,
          close: k.c,
          volume: k.v,
          isFinal,
          reason: isFinal ? 'candle_close' : 'live_update',
        };
        queue(interval, payload);
        markSent(key, close, high, low);
      }
    } catch (e) {
      console.error('WS message error:', e.message);
    }
  });

  ws.on('close', () => {
    console.warn('WS closed. Reconnecting in 3s...');
    clearInterval(pingTimer);
    setTimeout(connect, 3000);
  });

  ws.on('error', (err) => {
    console.error('WS error:', err.message);
    try { ws.close(); } catch {}
  });
}

/** ====== HTTP (health) ====== */
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('binance-kline-relay\n');
});

const port = process.env.PORT || 8080; // Render сам установит PORT
server.listen(port, () => {
  console.log('HTTP server on', port);
  connect();
});