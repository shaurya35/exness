import express from "express";
import type { Request, Response } from "express";
import cors from "cors";
import { prisma } from "store/client";

const app = express();

app.use(express.json());
app.use(cors());

app.get("/health", (req: Request, res: Response) => {
  res.json({ message: "Health Check!" });
});

app.post("/api/v1/user/signup", (req: Request, res: Response) => {
  try {
  } catch (error) {
    console.log(error);
  }
});

app.post("/api/v1/user/signin", (req: Request, res: Response) => {
  try {
  } catch (error) {
    console.log(error);
  }
});

app.post("/api/v1/trade", (req: Request, res: Response) => {
  try {
  } catch (error) {
    console.log(error);
  }
});

app.get("/api/v1/trades/open", (req: Request, res: Response) => {
  try {
  } catch (error) {
    console.log(error);
  }
});

app.get("/api/v1/trades", (req: Request, res: Response) => {
  try {
  } catch (error) {
    console.log(error);
  }
});

app.get("/api/v1/user/balance", (req: Request, res: Response) => {
  try {
  } catch (error) {
    console.log(error);
  }
});

app.get("/api/v1/candles", async (req: Request, res: Response) => {
  try {
    const { asset, startTime, endTime, ts } = req.query;

    if (!asset || !ts) {
      return res.status(400).json({
        error: "Missing required parameters: asset and ts are required"
      });
    }

    const validTimeframes = ["1m", "5m", "10m", "30m"];
    if (!validTimeframes.includes(ts as string)) {
      return res.status(400).json({
        error: "Invalid timeframe. Must be one of: 1m, 5m, 10m, 30m"
      });
    }

    const startTimeNum = startTime ? parseInt(startTime as string) : undefined;
    const endTimeNum = endTime ? parseInt(endTime as string) : undefined;

    const whereClause: any = {
      asset: asset as string
    };

    if (startTimeNum && endTimeNum) {
      whereClause.createdAt = {
        gte: new Date(startTimeNum),
        lte: new Date(endTimeNum)
      };
    } else if (startTimeNum) {
      whereClause.createdAt = {
        gte: new Date(startTimeNum)
      };
    } else if (endTimeNum) {
      whereClause.createdAt = {
        lte: new Date(endTimeNum)
      };
    }

    let candles;

    switch (ts) {
      case "1m":
        candles = await prisma.oneMinTicks.findMany({
          where: whereClause,
          orderBy: {
            createdAt: 'asc'
          }
        });
        break;
      case "5m":
        candles = await prisma.fiveMinTicks.findMany({
          where: whereClause,
          orderBy: {
            createdAt: 'asc'
          }
        });
        break;
      case "10m":
        candles = await prisma.tenMinTicks.findMany({
          where: whereClause,
          orderBy: {
            createdAt: 'asc'
          }
        });
        break;
      case "30m":
        candles = await prisma.thirtyMinTicks.findMany({
          where: whereClause,
          orderBy: {
            createdAt: 'asc'
          }
        });
        break;
      default:
        return res.status(400).json({
          error: "Invalid timeframe"
        });
    }

    const formattedCandles = candles.map(candle => ({
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
      startTime: candle.openTime.toString(),
      endTime: candle.closeTime.toString(),
      asset: candle.asset
    }));

    res.json({
      success: true,
      data: formattedCandles,
      count: formattedCandles.length,
      timeframe: ts,
      asset: asset
    });

  } catch (error) {
    console.error("Error fetching candles:", error);
    res.status(500).json({
      error: "Internal server error",
      message: "Failed to fetch candle data"
    });
  }
});

app.get("/api/v1/assets", (req: Request, res: Response) => {
  try {
  } catch (error) {
    console.log(error);
  }
});

const port = process.env.PORT || 8080;

app.listen(port, () => {
  console.log(`Server is running at http://localhost:${port}`);
});
