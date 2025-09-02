// ...выше без изменений...

ws.on("message", (raw) => {
  try {
    const packet = JSON.parse(raw.toString());
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

      // >>> ДОБАВКА: метки для 1H и открытие часа
      const openTime = Math.floor(now / 3600000) * 3600000; // начало текущего часа (UTC)
      queue({
        symbol,
        price,
        ts: now,
        interval: "1h",
        isFinal: false,
        openTime,
      });

      if (Math.random() < 0.02) {
        console.log(`[TICK] ${symbol} ${price} @ ${moment.utc(now).format("HH:mm:ss")}`);
      }
    }
  } catch (e) {
    // ignore
  }
});

// ...ниже без изменений...
