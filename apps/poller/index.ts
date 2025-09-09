import WebSocket from "ws";
import { prisma } from "store/client";

type TradePayload = {
  tradeId: number;
  asset: string;
  price: string;
  time: number;
  quantity: string;
};

const BATCH_SIZE = 100;
const batch: TradePayload[] = [];

const currentCandles = {
  "1m": new Map<string, any>(),
  "5m": new Map<string, any>(),
  "10m": new Map<string, any>(),
  "30m": new Map<string, any>(),
};

const INTERVALS = {
  "1m": 1 * 60 * 1000,
  "5m": 5 * 60 * 1000,
  "10m": 10 * 60 * 1000,
  "30m": 30 * 60 * 1000,
};

function getCandleStartTime(timestamp: number, intervalMs: number): number {
  return Math.floor(timestamp / intervalMs) * intervalMs;
}

function processCandles(tick: TradePayload) {
  const priceNum = parseFloat(tick.price);
  const time = tick.time;

  Object.entries(INTERVALS).forEach(([timeframe, intervalMs]) => {
    const candleStartTime = getCandleStartTime(time, intervalMs);
    const candleEndTime = candleStartTime + intervalMs - 1;
    const candleKey = `${tick.asset}-${candleStartTime}`;

    let candle =
      currentCandles[timeframe as keyof typeof INTERVALS].get(candleKey);

    if (!candle) {
      candle = {
        tradeId: tick.tradeId,
        asset: tick.asset,
        open: tick.price,
        high: tick.price,
        low: tick.price,
        close: tick.price,
        openTime: candleStartTime,
        closeTime: candleEndTime,
        // Track the earliest trade time observed for correct open
        firstTradeTime: time,
      };
      currentCandles[timeframe as keyof typeof INTERVALS].set(
        candleKey,
        candle
      );
    } else {
      const highNum = parseFloat(candle.high);
      const lowNum = parseFloat(candle.low);

      // Correct open if an earlier trade (by timestamp) arrives out-of-order
      if (time < candle.firstTradeTime) {
        candle.open = tick.price;
        candle.firstTradeTime = time;
      }

      candle.high = priceNum > highNum ? tick.price : candle.high;
      candle.low = priceNum < lowNum ? tick.price : candle.low;
      candle.close = tick.price;
      candle.tradeId = tick.tradeId;
    }

    if (time >= candleEndTime) {
      storeCandle(timeframe as keyof typeof INTERVALS, candle);
      currentCandles[timeframe as keyof typeof INTERVALS].delete(candleKey);
    }
  });
}

async function storeCandle(timeframe: string, candle: any) {
  const data = {
    tradeId: BigInt(candle.tradeId),
    asset: candle.asset,
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
    openTime: BigInt(candle.openTime),
    closeTime: BigInt(candle.closeTime),
  };

  try {
    let result;
    switch (timeframe) {
      case "1m":
        result = await prisma.oneMinTicks.create({ data });
        break;
      case "5m":
        result = await prisma.fiveMinTicks.create({ data });
        break;
      case "10m":
        result = await prisma.tenMinTicks.create({ data });
        break;
      case "30m":
        result = await prisma.thirtyMinTicks.create({ data });
        break;
      default:
        return;
    }
  } catch (error) {
    console.error(`Error storing ${timeframe} candle:`, error);
  }
}

async function uploadToDataBase(items: TradePayload[]) {
  if (items.length === 0) {
    return;
  }

  try {
    const result = await prisma.ticks.createMany({
      data: items.map((item) => ({
        tradeId: BigInt(item.tradeId),
        asset: item.asset,
        price: item.price,
        time: BigInt(item.time),
        quantity: item.quantity,
      })),
      skipDuplicates: true,
    });
  } catch (error) {
    console.log("Error uploading ticks:", error);
  }
}

function batchProcessor(item: TradePayload) {
  // batch.push(item);
  processCandles(item);

  // if (batch.length >= BATCH_SIZE) {
  //   const upload = batch.splice(0, BATCH_SIZE);
  //   uploadToDataBase(upload);
  // }
}

setInterval(() => {
  const now = Date.now();

  Object.entries(INTERVALS).forEach(([timeframe, intervalMs]) => {
    for (const [key, candle] of currentCandles[
      timeframe as keyof typeof INTERVALS
    ].entries()) {
      if (now - candle.openTime > intervalMs * 2) {
        storeCandle(timeframe, candle);
        currentCandles[timeframe as keyof typeof INTERVALS].delete(key);
      }
    }
  });
}, 60000);

// Retention policy: periodically prune old data (fallback if TimescaleDB policies aren't used)
const RETENTION = {
  ticksMs: 24 * 60 * 60 * 1000,        // 24 hours
  oneMinMs: 30 * 24 * 60 * 60 * 1000,  // 30 days
  fiveMinMs: 90 * 24 * 60 * 60 * 1000, // 90 days
  tenMinMs: 180 * 24 * 60 * 60 * 1000, // 180 days
  thirtyMinMs: 365 * 24 * 60 * 60 * 1000, // 365 days
};

async function retentionCleanup() {
  try {
    const nowMs = Date.now();

    const ticksThreshold = BigInt(nowMs - RETENTION.ticksMs);
    const oneMinThreshold = BigInt(nowMs - RETENTION.oneMinMs);
    const fiveMinThreshold = BigInt(nowMs - RETENTION.fiveMinMs);
    const tenMinThreshold = BigInt(nowMs - RETENTION.tenMinMs);
    const thirtyMinThreshold = BigInt(nowMs - RETENTION.thirtyMinMs);

    const [d1, d2, d3, d4, d5] = await Promise.all([
      prisma.ticks.deleteMany({ where: { time: { lt: ticksThreshold } } }),
      prisma.oneMinTicks.deleteMany({ where: { openTime: { lt: oneMinThreshold } } }),
      prisma.fiveMinTicks.deleteMany({ where: { openTime: { lt: fiveMinThreshold } } }),
      prisma.tenMinTicks.deleteMany({ where: { openTime: { lt: tenMinThreshold } } }),
      prisma.thirtyMinTicks.deleteMany({ where: { openTime: { lt: thirtyMinThreshold } } }),
    ]);

    if (
      (d1.count ?? 0) + (d2.count ?? 0) + (d3.count ?? 0) + (d4.count ?? 0) + (d5.count ?? 0) >
      0
    ) {
      console.log(
        "Retention cleanup:",
        JSON.stringify({
          ticks: d1.count,
          oneMin: d2.count,
          fiveMin: d3.count,
          tenMin: d4.count,
          thirtyMin: d5.count,
        })
      );
    }
  } catch (err) {
    console.error("Retention cleanup error:", err);
  }
}

// Run retention cleanup every 15 minutes, with initial delay of 1 minute
setTimeout(() => {
  retentionCleanup();
  setInterval(retentionCleanup, 15 * 60 * 1000);
}, 60 * 1000);

function poller() {
  const binanceWsUrl =
    "wss://stream.binance.com:9443/stream?streams=btcusdt@trade";
  const ws = new WebSocket(binanceWsUrl);

  ws.on("open", () => {
    console.log("WebSocket Functional!");
  });

  ws.on("message", async (data) => {
    try {
      const trade = JSON.parse(data.toString());

      if (!trade) {
        throw new Error("No Response from ws server!");
      }

      const tradeData = trade.data;

      const output = {
        tradeId: tradeData.t,
        asset: tradeData.s,
        price: tradeData.p,
        time: tradeData.T,
        quantity: tradeData.q,
      };

      batchProcessor(output);
    } catch (error) {
      console.log("WebSocket message error - ", error);
    }
  });

  ws.on("error", (error) => {
    console.log("WebSocket error -", error);
  });

  ws.on("close", () => {
    console.log("WebSocket connection closed! Restarting");
    poller();
  });
}

poller();