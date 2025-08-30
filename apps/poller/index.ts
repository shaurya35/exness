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

async function uploadToDataBase(items: TradePayload[]) {
  if (items.length === 0) {
    return;
  }
  try {
    await prisma.ticks.createMany({ data: items, skipDuplicates: true });
  } catch (error) {
    console.log(error);
  }
}

function batchProcessor(item: TradePayload) {
  batch.push(item);

  if (batch.length >= BATCH_SIZE) {
    const upload = batch.splice(0, BATCH_SIZE);
    console.log(upload)
    uploadToDataBase(upload);
  }
}

function poller() {
  const binanceWsUrl =
    "wss://stream.binance.com:9443/stream?streams=btcusdt@trade";
  const ws = new WebSocket(binanceWsUrl);

  ws.on("open", () => {
    console.log("Websocket Functional!");
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
      console.log(output)
      batchProcessor(output);
    } catch (error) {
      console.log("Websocket message error - ", error);
    }
  });

  ws.on("error", (error) => {
    console.log("Websocket error -", error);
  });

  ws.on("close", () => {
    console.log("Websockets server closed! starting again");
    poller();
  });
}

poller();
