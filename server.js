// server.js
import http from "http";
import WebSocket from "ws";
import axios from "axios";
import moment from "moment-timezone";

// --------- ENV & CONFIG ---------
const SYMBOLS = (process.env.SYMBOLS || "BTCUSDT,ETHUSDT,SOLUSDT")
  .split(",")
  .map(s => s.trim().toUpperCase())
  .filter(Boolean);

const PD_1H = process.env.PD_WEBHOOK_1H || "";   // Pipedream ingest для 1H-воркфлоу
const PD_4H = process.env.PD_WEBHOOK_4H || "";   // Pipedream ingest для 4H-воркфлоу (если используете)
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "";

const THROTTLE_SEC    = Number(process.env.THROTTLE_SEC    || 2);      // мин. интервал отправки по одному символу
const MIN_PCT_MOVE    = Number(process.env.MIN_PCT_MOVE    || 0.0005); // 0.05% — минимальное изменение
const BATCH_WINDOW_MS = Number(process.env.BATCH_WINDOW_MS || 800);

const SELF_URL = process.env.SELF_URL || ""; // https://<ваш-сервис>.onrender.com

const WS_BASE = "wss://stream.binance.com:9443";
const PORT = process.env.PORT || 3000;

// --------- HTTP (health) ---------
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    return res.end("ok");
  }
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("spot_long_ws running\n");
});

server.listen(PORT, () => console.log(`[HTTP] Listening on :${PORT}`));

// Самопинг, чтобы Render не «засыпал»
if (SELF_URL) {
  setInterval(() => {
    axios.head(`${SELF_URL}/health`).catch(() => {});
  }, 60_000); // раз в минуту
}

// --------- WS client ---------
let ws;
let pingTimer;
let reconnectTimer;

const makeStreams = (symbols) =>
  symbols.map(s => `${s.toLowerCase()}@miniTicker`).join("/");

const makeUrl = (symbols) =>
  `${WS_BASE}/stream?streams=${makeStreams(symbols)}`;

const lastPrice  = new Map(); // последняя цена для %-фильтра
const lastSentAt = new Map(); // когда по символу последний раз отправляли

let batch = [];         // буфер батча
let batchTimer = null;

// помощник: рассчитываем openTime бара
const barOpen = (ts, interval) => {
  const t = Number(ts) || Date.now();
  if (interval === "4h") {
    const fourH = 4 * 60 * 60 * 1000;
    return Math.floor(t / fourH) * fourH;
  }
  // по умолчанию 1h
  const oneH = 60 * 60 * 1000;
  return Math.floor(t / oneH) * oneH;
};

function queue(item) {
  batch.push(item);
  if (batchTimer) return;
  batchTimer = setTimeout(flushBatch, BATCH_WINDOW_MS);
}

async function flushBatch() {
  const items = batch;
  batch = [];
  clearTimeout(batchTimer);
  batchTimer = null;
  if (!items.length) return;

  // Для 1H и 4H формируем отдельные полезные нагрузки с корректным interval/openTime
  const items1h = items.map(({ symbol, price, ts }) => ({
    symbol, price, ts,
    interval: "1h",
    isFinal: false,
    openTime: barOpen(ts, "1h"),
  }));

  const items4h = items.map(({ symbol, price, ts }) => ({
    symbol, price, ts,
    interval: "4h",
    isFinal: false,
    openTime: barOpen(ts, "4h"),
  }));

  const headers = { "x-auth": WEBHOOK_SECRET };

  try {
    if (PD_1H) await axios.post(PD_1H, { items: items1h }, { headers, timeout: 8000 });
    if (PD_4H) await axios.post(PD_4H, { items: items4h }, { headers, timeout: 8000 });
    console.log(`[POST] ${items.length} items -> PD (1h${PD_1H?"+":""}, 4h${PD_4H?"+":""})`);
  } catch (e) {
    console.log(`[POST ERROR] ${e?.message || e}`);
  }
}

function connect() {
  const url = makeUrl(SYMBOLS);
  console.log(`[WS] Connecting: ${url}`);
  ws = new WebSocket(url);

  ws.on("open", () => {
    console.log("[WS open]");
    clearInterval(pingTimer);
    pingTimer = setInterval(() => { try { ws.ping(); } catch {} }, 15_000);
  });

  ws.on("message", (raw) => {
    try {
      const packet = JSON.parse(raw.toString());
      // combined stream: { stream: 'btcusdt@miniticker', data: {...} }
      const data = packet?.data || packet;
      const symbol = (data?.s || "").toUpperCase();
      const price = parseFloat(data?.c || "0");
      if (!symbol || !Number.isFinite(price)) return;

      const prevPrice = lastPrice.get(symbol);
      lastPrice.set(symbol, price);

      const now = Date.now();
      const lastTs = lastSentAt.get(symbol) || 0;
      const dtOk = now - lastTs >= THROTTLE_SEC * 1000;
      let pctOk = true;

      if (prevPrice && prevPrice > 0) {
        const pct = Math.abs(price / prevPrice - 1);
        pctOk = pct >= MIN_PCT_MOVE;
      }

      if (dtOk && pctOk) {
        lastSentAt.set(symbol, now);
        queue({ symbol, price, ts: now });

        // лёгкий лог раз в ~2% сообщений
        if (Math.random() < 0.02) {
          console.log(`[TICK] ${symbol} ${price} @ ${moment.utc(now).format("HH:mm:ss")} UTC`);
        }
      }
    } catch {
      // игнорим единичные проблемы парсинга
    }
  });

  ws.on("close", (code, reason) => {
    console.log(`[WS close] ${code} ${reason}`);
    cleanup();
    scheduleReconnect();
  });

  ws.on("error", (err) => {
    console.log("[WS error]", err?.message || err);
    cleanup();
    scheduleReconnect();
  });
}

function cleanup() {
  try { ws?.close(); } catch {}
  ws = null;
  clearInterval(pingTimer);
  pingTimer = null;
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, 3000);
}

connect();

// мягкое завершение
process.on("SIGTERM", () => { console.log("SIGTERM"); process.exit(0); });
process.on("SIGINT",  () => { console.log("SIGINT");  process.exit(0); });
