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
      };
      currentCandles[timeframe as keyof typeof INTERVALS].set(
        candleKey,
        candle
      );
    } else {
      const highNum = parseFloat(candle.high);
      const lowNum = parseFloat(candle.low);

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
  batch.push(item);
  processCandles(item);

  if (batch.length >= BATCH_SIZE) {
    const upload = batch.splice(0, BATCH_SIZE);
    uploadToDataBase(upload);
  }
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
