"use client";

import { useEffect, useRef, useState } from "react";
import {
  createChart,
  ColorType,
  CandlestickSeries,
  type CandlestickData,
  type UTCTimestamp,
} from "lightweight-charts";

const ASSETS = ["BTCUSDT"];
const TIMEFRAMES = ["1m", "5m", "10m", "30m"] as const;

type Timeframe = (typeof TIMEFRAMES)[number];

export default function Charts() {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [asset, setAsset] = useState<string>(ASSETS[0]);
  const [timeframe, setTimeframe] = useState<Timeframe>("1m");

  const barsRef = useRef<CandlestickData[]>([]);
  const lastTimeRef = useRef<UTCTimestamp | null>(null);
  const pollTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const alignTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const [legend, setLegend] = useState<{
    time?: string;
    open?: number;
    high?: number;
    low?: number;
    close?: number;
  }>({});

  useEffect(() => {
    if (!chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
      layout: {
        textColor: "#111827",
        background: { type: ColorType.Solid, color: "#ffffff" },
      },
      rightPriceScale: {
        visible: true,
        borderColor: "#e5e7eb",
      },
      timeScale: {
        rightOffset: 12,
        barSpacing: 6,
        fixLeftEdge: false,
        timeVisible: true,
        secondsVisible: false,
        borderColor: "#e5e7eb",
      },
      grid: {
        vertLines: { color: "#f3f4f6" },
        horzLines: { color: "#f3f4f6" },
      },
      crosshair: {
        mode: 0,
      },
      localization: {
        priceFormatter: (p: number) =>
          p.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 8,
          }),
      },
    });

    const initialWidth = chartContainerRef.current.clientWidth || 800;
    const initialHeight = chartContainerRef.current.clientHeight || 480;
    chart.applyOptions({ width: initialWidth, height: initialHeight });

    const series = chart.addSeries(CandlestickSeries, {
      upColor: "#2563eb",
      downColor: "#ef4444",
      borderVisible: false,
      wickUpColor: "#2563eb",
      wickDownColor: "#ef4444",
      priceLineVisible: false,
    });

    let resizeObserver: ResizeObserver | null = null;
    if (chartContainerRef.current) {
      resizeObserver = new ResizeObserver(() => {
        const { clientWidth, clientHeight } = chartContainerRef.current!;
        chart.applyOptions({ width: clientWidth, height: clientHeight || 480 });
      });
      resizeObserver.observe(chartContainerRef.current);
    }

    const controller = new AbortController();

    const ONE_MIN_MS = 60_000;
    const LAG_MS = 5_000;
    const OVERLAP_BARS = 3;
    const MAX_BARS = 1000;

    function mapCandles(json: any): CandlestickData[] {
      return (json?.data ?? [])
        .map((c: any) => ({
          time: Math.floor(Number(c.startTime) / 1000) as UTCTimestamp,
          open: Number(c.open),
          high: Number(c.high),
          low: Number(c.low),
          close: Number(c.close),
        }))
        .filter((d: any) => Number.isFinite(d.time) && Number.isFinite(d.open))
        .sort((a: any, b: any) => (a.time as number) - (b.time as number));
    }

    function mergeBars(
      existing: CandlestickData[],
      incoming: CandlestickData[]
    ): CandlestickData[] {
      const byTime = new Map<number, CandlestickData>();
      for (const b of existing) byTime.set(b.time as number, b);
      for (const b of incoming) byTime.set(b.time as number, b);
      const merged = Array.from(byTime.values()).sort(
        (a, b) => (a.time as number) - (b.time as number)
      );
      return merged.slice(-MAX_BARS);
    }

    async function initialLoad() {
      setIsLoading(true);
      setError(null);
      try {
        const params = new URLSearchParams({ asset, ts: timeframe });
        const res = await fetch(
          `http://localhost:8080/api/v1/candles?${params.toString()}`,
          { signal: controller.signal }
        );
        if (!res.ok) throw new Error(`Failed to fetch candles: ${res.status}`);
        const json = await res.json();
        const data = mapCandles(json);
        if (data.length > 0) {
          barsRef.current = data.slice(-MAX_BARS);
          series.setData(barsRef.current);
          lastTimeRef.current = barsRef.current[barsRef.current.length - 1]
            .time as UTCTimestamp;
          chart.timeScale().fitContent();
        }
      } catch (e: any) {
        if (e?.name !== "AbortError") setError(e?.message ?? "Unknown error");
      } finally {
        setIsLoading(false);
      }
    }

    async function pollOnce() {
      try {
        const params = new URLSearchParams({ asset, ts: timeframe });
        if (barsRef.current.length > 0) {
          const lastIdx = Math.max(0, barsRef.current.length - OVERLAP_BARS);
          const since = (barsRef.current[lastIdx].time as number) * 1000 - 1;
          params.set("startTime", String(since));
        }
        const res = await fetch(
          `http://localhost:8080/api/v1/candles?${params.toString()}`
        );
        if (!res.ok) return;
        const json = await res.json();
        const incoming = mapCandles(json);
        if (incoming.length === 0) return;
        const merged = mergeBars(barsRef.current, incoming);
        if (
          merged.length === barsRef.current.length + 1 &&
          merged[merged.length - 2]?.time ===
            barsRef.current[barsRef.current.length - 1]?.time
        ) {
          const last = merged[merged.length - 1];
          series.update(last);
          barsRef.current = merged;
          lastTimeRef.current = last.time as UTCTimestamp;
        } else {
          barsRef.current = merged;
          series.setData(barsRef.current);
          lastTimeRef.current = barsRef.current[barsRef.current.length - 1]
            .time as UTCTimestamp;
        }
      } catch {}
    }

    chart.subscribeCrosshairMove((param) => {
      const p = param?.seriesData?.get(series) as CandlestickData | undefined;
      if (p && typeof p.time === "number") {
        const date = new Date((p.time as number) * 1000);
        setLegend({
          time: `${date.toLocaleDateString()} ${date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false })}`,
          open: p.open,
          high: p.high,
          low: p.low,
          close: p.close,
        });
      }
    });

    requestAnimationFrame(() => initialLoad());

    const now = Date.now();
    const nextMinute =
      Math.floor(now / ONE_MIN_MS) * ONE_MIN_MS + ONE_MIN_MS + LAG_MS;
    alignTimerRef.current = setTimeout(
      () => {
        pollOnce();
        pollTimerRef.current = setInterval(() => {
          pollOnce();
        }, ONE_MIN_MS);
      },
      Math.max(0, nextMinute - now)
    );

    return () => {
      controller.abort();
      resizeObserver?.disconnect();
      if (alignTimerRef.current) clearTimeout(alignTimerRef.current);
      if (pollTimerRef.current) clearInterval(pollTimerRef.current);
      chart.remove();
    };
  }, [asset, timeframe]);

  return (
    <section className="w-full min-h-screen bg-white">
      <div className="mx-auto max-w-7xl p-4 space-y-4">
        <header className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <h1 className="text-lg font-semibold text-gray-900">{asset}</h1>
            <span className="text-gray-400">/</span>
            <span className="text-sm text-gray-600">{timeframe}</span>
          </div>
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600">Asset</label>
            <select
              className="h-9 rounded-md border border-gray-300 bg-white px-2 text-sm text-gray-900"
              value={asset}
              onChange={(e) => setAsset(e.target.value)}
            >
              {ASSETS.map((a) => (
                <option key={a} value={a}>
                  {a}
                </option>
              ))}
            </select>
            <label className="text-sm text-gray-600">Timeframe</label>
            <select
              className="h-9 rounded-md border border-gray-300 bg-white px-2 text-sm text-gray-900"
              value={timeframe}
              onChange={(e) => setTimeframe(e.target.value as Timeframe)}
            >
              {TIMEFRAMES.map((tf) => (
                <option key={tf} value={tf}>
                  {tf}
                </option>
              ))}
            </select>
          </div>
        </header>

        <div className="relative w-full h-[480px] sm:h-[600px] rounded-xl border border-gray-200 bg-white shadow-sm">
          {isLoading && (
            <div className="absolute top-2 left-2 text-xs text-gray-500">
              Loading…
            </div>
          )}
          {error && (
            <div className="absolute top-2 right-2 text-xs text-red-700">
              {error}
            </div>
          )}
          <div className="absolute top-2 left-2 mt-6 rounded-md bg-white/90 px-2 py-1 text-[11px] text-gray-700 shadow">
            <div className="flex gap-2 whitespace-nowrap">
              <span className="text-gray-500">Time:</span>
              <span>{legend.time ?? ""}</span>
            </div>
            <div className="flex gap-2 whitespace-nowrap">
              <span className="text-gray-500">O:</span>
              <span>{legend.open ?? ""}</span>
              <span className="text-gray-500">H:</span>
              <span>{legend.high ?? ""}</span>
              <span className="text-gray-500">L:</span>
              <span>{legend.low ?? ""}</span>
              <span className="text-gray-500">C:</span>
              <span>{legend.close ?? ""}</span>
            </div>
          </div>
          <div
            ref={chartContainerRef}
            id="container"
            className="w-full h-full"
          />
        </div>
      </div>
    </section>
  );
}
