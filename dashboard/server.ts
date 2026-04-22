/**
 * Local dashboard: wallet list (PostgreSQL), in-process sync (Data API) + in-process charts (Node).
 *   npm run dashboard
 *   DATABASE_URL in .env
 */
import path from "node:path";
import fs from "node:fs";
import express, { type Request, type Response } from "express";
import { config as loadEnv } from "dotenv";
import { Pool } from "pg";
import { runWalletLastMonthSync } from "../getPositions.js";
import { type ChartClosedSource, renderBtcLast30dChart } from "../lib/btcChartService.js";

const SCRIPTS_DIR = path.resolve(__dirname, "..");
const PUBLIC_DIR = path.join(__dirname, "public");
const GRAPHS_DIR = path.join(PUBLIC_DIR, "graphs");

loadEnv({ path: path.join(SCRIPTS_DIR, ".env") });

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "8kb" }));

if (!fs.existsSync(GRAPHS_DIR)) {
  fs.mkdirSync(GRAPHS_DIR, { recursive: true });
}

const pool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL, max: 4 })
  : null;

const DASHBOARD_PORT = parseInt(process.env.DASHBOARD_PORT || "3333", 10);

function requireDbJson(res: Response): boolean {
  if (!pool) {
    res.status(503).json({ error: "DATABASE_URL is not set" });
    return false;
  }
  return true;
}

function isEthAddress(s: string): boolean {
  return /^0x[a-fA-F0-9]{40}$/i.test(s.trim());
}

function normalizeAddress(s: string): string {
  return s.trim().toLowerCase();
}

app.get("/api/health", (_req: Request, res: Response) => {
  res.json({
    ok: true,
    hasDatabase: Boolean(process.env.DATABASE_URL),
    engine: "in-process (getPositions + btcChartService, no sub-shell)",
  });
});

app.get("/api/wallets", async (_req: Request, res: Response) => {
  if (!requireDbJson(res) || !pool) return;
  try {
    const { rows } = await pool.query<{
      user_address: string;
      since_timestamp_sec: string | null;
      last_month_days: number | null;
      since_iso: string | null;
      window_time_source_note: string | null;
      updated_at: Date;
    }>(
      `SELECT user_address, since_timestamp_sec, last_month_days, since_iso, window_time_source_note, updated_at
       FROM "walletList" ORDER BY updated_at DESC NULLS LAST, user_address ASC`
    );
    res.json(
      rows.map((r) => ({
        userAddress: r.user_address,
        sinceTimestampSec: r.since_timestamp_sec != null ? String(r.since_timestamp_sec) : null,
        lastMonthDays: r.last_month_days,
        sinceIso: r.since_iso,
        windowNote: r.window_time_source_note,
        updatedAt: r.updated_at ? new Date(r.updated_at).toISOString() : null,
      }))
    );
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

app.post("/api/wallets", async (req: Request, res: Response) => {
  if (!requireDbJson(res) || !pool) return;
  const raw =
    req.body && typeof (req.body as { address?: unknown }).address === "string"
      ? (req.body as { address: string }).address
      : "";
  if (!isEthAddress(raw)) {
    res.status(400).json({ error: "Valid 0x + 40 hex address required" });
    return;
  }
  const address = normalizeAddress(raw);
  try {
    await pool.query(
      `INSERT INTO "walletList" (user_address, updated_at) VALUES ($1, now())
       ON CONFLICT (user_address) DO UPDATE SET updated_at = "walletList".updated_at
       RETURNING user_address, updated_at`,
      [address]
    );
    res.json({ ok: true, userAddress: address });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

app.post("/api/refresh", async (req: Request, res: Response) => {
  if (!process.env.DATABASE_URL) {
    res.status(503).end("DATABASE_URL is not set.\n");
    return;
  }
  const raw =
    req.body && typeof (req.body as { address?: unknown }).address === "string"
      ? (req.body as { address: string }).address
      : "";
  if (!isEthAddress(raw)) {
    res.status(400).end("Error: valid 0x + 40 hex address required\n");
    return;
  }
  const address = normalizeAddress(raw);

  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Transfer-Encoding", "chunked");
  res.write(
    "[in-process] runWalletLastMonthSync — Polymarket Data API → PostgreSQL (last 30d)\n\n"
  );
  try {
    await runWalletLastMonthSync(address, (line) => {
      res.write(line + (line.endsWith("\n") ? "" : "\n"));
    });
  } catch (e) {
    res.write(String(e) + "\n");
  }
  res.end();
});

app.post("/api/graph", async (req: Request, res: Response) => {
  if (!process.env.DATABASE_URL || !pool) {
    res.status(503).json({ error: "DATABASE_URL is not set" });
    return;
  }
  const body = req.body as { address?: string; kind?: string; closedFrom?: string };
  if (typeof body.address !== "string" || !isEthAddress(body.address)) {
    res.status(400).json({ error: "Valid 0x + 40 hex address required" });
    return;
  }
  const address = normalizeAddress(body.address);
  const kind = body.kind === "count" ? "count" : "daily";
  const closedFrom: ChartClosedSource = body.closedFrom === "polished" ? "polished" : "api";
  const stamp = "last30d_all";
  const clTag = closedFrom === "polished" ? "polished" : "api";
  const outName = `${kind}_${stamp}_${address.replace(/^0x/, "")}_${clTag}.png`;
  const outPng = path.join(GRAPHS_DIR, outName);
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  const logParts: string[] = [];
  logParts.push(
    `[in-process] renderBtcLast30dChart (closed: ${clTag}) — same rules as analyze.py --all --last-month (all markets, rolling 30d ET)…`
  );
  try {
    const onLog = (line: string) => {
      logParts.push(line);
    };
    await renderBtcLast30dChart(pool, address, kind, outPng, onLog, closedFrom);
  } catch (e) {
    res.status(500).json({ ok: false, log: logParts.join("\n") + "\n" + String(e), error: String(e) });
    return;
  }
  if (fs.existsSync(outPng)) {
    res.json({ ok: true, log: logParts.join("\n") + "\n", imageUrl: `/graphs/${outName}` });
  } else {
    res.status(500).json({ ok: false, log: logParts.join("\n"), error: "output PNG not created" });
  }
});

app.use(
  express.static(PUBLIC_DIR, {
    maxAge: 0,
    index: "index.html",
  })
);

app.listen(DASHBOARD_PORT, "127.0.0.1", () => {
  // eslint-disable-next-line no-console
  console.log(`[dashboard] http://127.0.0.1:${DASHBOARD_PORT}`);
});

process.on("SIGINT", () => {
  if (pool) {
    void pool.end();
  }
  process.exit(0);
});

process.on("SIGTERM", () => {
  if (pool) {
    void pool.end();
  }
  process.exit(0);
});
