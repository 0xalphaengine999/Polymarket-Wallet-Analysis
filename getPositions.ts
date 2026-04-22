import { config as loadEnv } from "dotenv";
loadEnv();

import { writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";
import { Pool } from "pg";
import {
  paramsClosedRow,
  paramsOpenRow,
  paramsPolishedClosedRow,
  paramsTradeRow,
  SQL_INSERT_CLOSED,
  SQL_INSERT_OPEN,
  SQL_INSERT_POLISHED_CLOSED,
  SQL_INSERT_TRADE,
} from "./polymarketDbMap.js";

const DATA_API = "https://data-api.polymarket.com";
/**
 * Data API: array body from GET /positions?user= (each element: open position object as returned by the API).
 * @see https://data-api.polymarket.com/positions
 */
type DataApiPositionsResponse = unknown[];
/**
 * Data API: array body from GET /closed-positions?user=
 * @see https://data-api.polymarket.com/closed-positions
 */
type DataApiClosedPositionsResponse = unknown[];
/**
 * Data API: array body from GET /trades?user= (e.g. takerOnly=false)
 * @see https://data-api.polymarket.com/trades
 */
type DataApiTradesResponse = unknown[];
/** Positions / closed-positions pagination (API default cap). */
const POSITIONS_PAGE_SIZE = 50;
/** Trades: Data API allows up to 10_000 per request (see OpenAPI). */
const TRADES_PAGE_SIZE = 10_000;

/**
 * For `/trades` with a window, we can stop when the last row in a full page is before the cutoff
 * (and only keep in-window rows per page), to avoid many HTTP calls.
 * `/positions` and `/closed-positions` when windowing **do not** use that row-based early stop: the
 * last row in a 50-block may not be globally oldest, or sort order may not match our window time,
 * so we paginate until a short/empty page (or duplicate), then `filterByWindow` once.
 * If the next request returns the **same** JSON page as the previous (offset ignored / stuck), we
 * stop so a bad API loop cannot append duplicate pages forever.
 */
const ASSUME_PAGED_LISTS_NEWEST_FIRST = true;

const SECONDS_PER_DAY = 24 * 60 * 60;

/**
 * In-memory / JSON shape: same three arrays the Data API returns (open positions, closed, trades).
 * `meta` is our own when using --last-month / --since-days.
 */
type PolymarketWalletExportJson = {
  user: string;
  positions: DataApiPositionsResponse;
  closedPositions: DataApiClosedPositionsResponse;
  trades: DataApiTradesResponse;
  /** Set when a time window was applied. */
  meta?: {
    sinceTimestampSec: number;
    sinceIso: string;
    lastMonthDays: number;
    windowTimeSourceNote: string;
  };
};

function normalizeWalletAddress(raw: string): string {
  const t = raw.trim();
  if (!t.startsWith("0x")) {
    throw new Error("Address must start with 0x");
  }
  return t.toLowerCase();
}

function isRecord(x: unknown): x is Record<string, unknown> {
  return typeof x === "object" && x !== null;
}

function normalizeApiUnixSec(n: number): number {
  if (n > 1_000_000_000_000) {
    return Math.floor(n / 1000);
  }
  return n;
}

function getTimestampSec(row: unknown): number | undefined {
  if (!isRecord(row)) return undefined;
  const t = row["timestamp"];
  if (typeof t === "number" && !Number.isNaN(t)) {
    return normalizeApiUnixSec(t);
  }
  if (typeof t === "string" && t.trim() !== "") {
    const n = parseInt(t, 10);
    if (!Number.isNaN(n)) {
      return normalizeApiUnixSec(n);
    }
  }
  return undefined;
}

function getEndDateAsSec(row: unknown): number | undefined {
  if (!isRecord(row)) return undefined;
  const end = row["endDate"];
  if (typeof end !== "string" || end.length < 10) return undefined;
  const head = end.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(head)) return undefined;
  const ms = Date.parse(`${head}T12:00:00.000Z`);
  if (Number.isNaN(ms)) return undefined;
  return Math.floor(ms / 1000);
}

/** e.g. slug `xrp-updown-5m-1771916400` → 1771916400 (start of 5m window). */
function getSlugTailUnixSec(row: unknown): number | undefined {
  if (!isRecord(row)) return undefined;
  for (const k of ["slug", "eventSlug"] as const) {
    const s = String(row[k] ?? "");
    if (!s) continue;
    const p = s.lastIndexOf("-");
    if (p < 0) continue;
    const tail = s.slice(p + 1);
    if (/^\d{9,10}$/.test(tail)) {
      const n = parseInt(tail, 10);
      if (n > 1_000_000_000) return n;
    }
  }
  return undefined;
}

/**
 * Open /positions: use **market** time first so a February market is not included in
 * a April "last 7d" file just because the row lacks `timestamp` or the API last-updated
 * a price in April. Order: endDate → slug tail unix → timestamp.
 */
function getOpenPositionWindowTimeSec(row: unknown): number | undefined {
  return getEndDateAsSec(row) ?? getSlugTailUnixSec(row) ?? getTimestampSec(row);
}

/**
 * Trades and closed: API usually provides `timestamp` (fill time / close time). Fallbacks
 * match old rows. Order: timestamp → endDate → slug tail.
 */
function getTradesOrClosedWindowTimeSec(row: unknown): number | undefined {
  return getTimestampSec(row) ?? getEndDateAsSec(row) ?? getSlugTailUnixSec(row);
}

/**
 * Inclusive: row is kept iff `windowTime >= since`.
 * Rows with no resolvable time are **dropped** in windowed mode (strict).
 */
function filterByWindow(rows: unknown[], since: number, path: string): unknown[] {
  return rows.filter((row) => {
    const t =
      path === "/positions"
        ? getOpenPositionWindowTimeSec(row)
        : getTradesOrClosedWindowTimeSec(row);
    if (t === undefined) return false;
    return t >= since;
  });
}

interface FetchPagedOptions {
  minTimestampSec?: number;
  /**
   * For /trades with a window: filter per row + trim pagination (default true when windowing).
   * (Unused when not windowing or when not /trades.)
   */
  newestFirstTrades?: boolean;
  /** If false, do not log each page (for tests). */
  verbose?: boolean;
  /** In-process progress (e.g. dashboard). Defaults to stderr. */
  logLine?: (line: string) => void;
}

function errLine(line: string): void {
  console.error(line);
}

function logPageProgress(
  logLineFn: (line: string) => void,
  path: string,
  offset: number,
  pageLen: number,
  kind: string,
  pageMs: number,
  sinceStartMs: number,
  extra: string
): void {
  const t = new Date().toISOString().slice(11, 19);
  logLineFn(
    `[getPositions][${t}] ${path} offset=${offset} +${pageLen} rows → ${kind} (page ${(pageMs / 1000).toFixed(1)}s, elapsed ${(sinceStartMs / 1000).toFixed(0)}s)${extra}`
  );
}

function fingerprintPagedResponse(page: unknown[]): string {
  return JSON.stringify(page);
}

async function fetchPagedArray(
  path: string,
  user: string,
  pageSize: number,
  extraParams?: Record<string, string>,
  options?: FetchPagedOptions
): Promise<unknown[]> {
  const { minTimestampSec, newestFirstTrades, verbose = true, logLine: logLineOpt } = options ?? {};
  const logLine = logLineOpt ?? errLine;
  const aggregated: unknown[] = [];
  let offset = 0;
  let nRequests = 0;
  let lastPageFingerprint: string | undefined;
  const t0 = Date.now();

  for (;;) {
    const tReq = Date.now();
    const url = new URL(path, DATA_API);
    url.searchParams.set("user", user);
    url.searchParams.set("limit", String(pageSize));
    url.searchParams.set("offset", String(offset));
    if (extraParams) {
      for (const [k, v] of Object.entries(extraParams)) {
        url.searchParams.set(k, v);
      }
    }

    const res = await fetch(url);
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`${path} failed ${res.status}: ${body}`);
    }

    const page = (await res.json()) as unknown;
    if (!Array.isArray(page)) {
      throw new Error(`${path}: expected JSON array, got ${typeof page}`);
    }

    nRequests += 1;
    const pageMs = Date.now() - tReq;
    const sinceStart = Date.now() - t0;

    const pageFingerprint = page.length > 0 ? fingerprintPagedResponse(page) : undefined;
    if (
      pageFingerprint !== undefined &&
      lastPageFingerprint !== undefined &&
      pageFingerprint === lastPageFingerprint
    ) {
      if (verbose) {
        const t = new Date().toISOString().slice(11, 19);
        logLine(
          `[getPositions][${t}] ${path} offset=${offset} duplicate page (same JSON as previous request); stopping`
        );
      }
      break;
    }
    if (pageFingerprint !== undefined) {
      lastPageFingerprint = pageFingerprint;
    }

    let willStop = false;
    if (minTimestampSec !== undefined && path === "/trades" && newestFirstTrades) {
      for (const row of page) {
        const ts = getTradesOrClosedWindowTimeSec(row);
        if (ts === undefined) {
          continue;
        }
        if (ts >= minTimestampSec) {
          aggregated.push(row);
        }
      }
      if (page.length < pageSize) {
        willStop = true;
      } else {
        if (ASSUME_PAGED_LISTS_NEWEST_FIRST) {
          const last = page[page.length - 1] as unknown;
          const tLast = getTradesOrClosedWindowTimeSec(last);
          if (tLast !== undefined && tLast < minTimestampSec) {
            willStop = true;
          }
        }
      }
      if (verbose) {
        const msg =
          page.length === 0
            ? `done (empty); kept ${aggregated.length} in-window trade rows`
            : `kept ${aggregated.length} in-window trade rows (running total in window)`;
        logPageProgress(logLine, path, offset, page.length, msg, pageMs, sinceStart, willStop ? " [stop]" : "");
      }
      if (page.length === 0) {
        break;
      }
      if (page.length < pageSize) {
        break;
      }
      if (ASSUME_PAGED_LISTS_NEWEST_FIRST) {
        const last = page[page.length - 1] as unknown;
        const tLast = getTradesOrClosedWindowTimeSec(last);
        if (tLast !== undefined && tLast < minTimestampSec) {
          break;
        }
      }
    } else if (minTimestampSec !== undefined) {
      for (const row of page) {
        aggregated.push(row);
      }
      if (page.length < pageSize) {
        willStop = true;
      }
      if (verbose) {
        const msg =
          page.length === 0
            ? "done (empty); will filter to window"
            : `raw cumulative ${aggregated.length} rows (client filter after final page)`;
        logPageProgress(logLine, path, offset, page.length, msg, pageMs, sinceStart, willStop ? " [stop]" : "");
      }
      if (page.length === 0) {
        break;
      }
      if (page.length < pageSize) {
        break;
      }
    } else {
      aggregated.push(...page);
      if (verbose) {
        logPageProgress(
          logLine,
          path,
          offset,
          page.length,
          `cumulative ${aggregated.length} rows (all-time)`,
          pageMs,
          sinceStart,
          page.length < pageSize ? " — last page" : ""
        );
      }
      if (page.length < pageSize) {
        break;
      }
    }
    offset += pageSize;
  }

  if (verbose) {
    const totalMs = Date.now() - t0;
    logLine(
      `[getPositions] ${path} done: ${(totalMs / 1000).toFixed(1)}s, ${nRequests} HTTP request(s) for this path`
    );
  }

  if (minTimestampSec !== undefined && path !== "/trades") {
    return filterByWindow(aggregated, minTimestampSec, path);
  }
  if (minTimestampSec !== undefined && path === "/trades" && !newestFirstTrades) {
    return filterByWindow(aggregated, minTimestampSec, "/trades");
  }
  return aggregated;
}

function parseArgs(argv: string[]): {
  user: string;
  sinceTimestampSec: number | undefined;
  lastMonthDays: number | undefined;
} {
  const out: { user: string; sinceTimestampSec: number | undefined; lastMonthDays: number | undefined } = {
    user: "",
    sinceTimestampSec: undefined,
    lastMonthDays: undefined,
  };
  for (const a of argv) {
    if (a === "--last-month" || a === "--last-30d") {
      if (out.lastMonthDays === undefined) {
        out.lastMonthDays = 30;
      }
    } else if (a.startsWith("--since-days=")) {
      const d = parseInt(a.slice("--since-days=".length), 10);
      if (!Number.isNaN(d) && d > 0) {
        out.lastMonthDays = d;
      }
    }
  }
  const posArgs = argv.filter(
    (a) =>
      a !== "--last-month" &&
      a !== "--last-30d" &&
      !a.startsWith("--since-days=") &&
      a !== "--quiet" &&
      a !== "-q" &&
      a !== "--json"
  );
  out.user = (posArgs[0] ?? "").trim();
  if (out.lastMonthDays != null) {
    const n = out.lastMonthDays;
    out.sinceTimestampSec = Math.floor(Date.now() / 1000) - n * SECONDS_PER_DAY;
  }
  return out;
}

async function ensurePolymarketSchema(pool: Pool): Promise<void> {
  // Matches polymarket_schema.sql: one table row = one Data API object (no JSONB blobs).
  await pool.query(`CREATE TABLE IF NOT EXISTS "walletList" (
    user_address TEXT PRIMARY KEY,
    since_timestamp_sec BIGINT,
    last_month_days INT,
    since_iso TEXT,
    window_time_source_note TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS positions (
    id BIGSERIAL PRIMARY KEY,
    user_address TEXT NOT NULL REFERENCES "walletList" (user_address) ON DELETE CASCADE,
    proxy_wallet TEXT,
    asset TEXT,
    condition_id TEXT,
    initial_value DOUBLE PRECISION,
    current_value DOUBLE PRECISION,
    cash_pnl DOUBLE PRECISION,
    percent_pnl DOUBLE PRECISION,
    total_bought DOUBLE PRECISION,
    realized_pnl DOUBLE PRECISION,
    percent_realized_pnl DOUBLE PRECISION,
    cur_price DOUBLE PRECISION,
    avg_price DOUBLE PRECISION,
    title TEXT,
    slug TEXT,
    icon TEXT,
    event_id TEXT,
    event_slug TEXT,
    outcome TEXT,
    outcome_index INT,
    opposite_outcome TEXT,
    opposite_asset TEXT,
    end_date TEXT,
    timestamp_sec BIGINT,
    mergeable BOOL,
    redeemable BOOL,
    negative_risk BOOL
  )`);
  await pool.query(`CREATE INDEX IF NOT EXISTS positions_user_address_idx ON positions (user_address)`);
  await pool.query(
    `CREATE INDEX IF NOT EXISTS positions_condition_id_idx ON positions (condition_id)`
  );
  await pool.query(`CREATE TABLE IF NOT EXISTS "closedPositions" (
    id BIGSERIAL PRIMARY KEY,
    user_address TEXT NOT NULL REFERENCES "walletList" (user_address) ON DELETE CASCADE,
    proxy_wallet TEXT,
    asset TEXT,
    condition_id TEXT,
    avg_price DOUBLE PRECISION,
    total_bought DOUBLE PRECISION,
    realized_pnl DOUBLE PRECISION,
    cur_price DOUBLE PRECISION,
    title TEXT,
    slug TEXT,
    icon TEXT,
    event_slug TEXT,
    outcome TEXT,
    outcome_index INT,
    opposite_outcome TEXT,
    opposite_asset TEXT,
    end_date TEXT,
    timestamp_sec BIGINT
  )`);
  await pool.query(
    `CREATE INDEX IF NOT EXISTS "closedPositions_user_address_idx" ON "closedPositions" (user_address)`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS "closedPositions_condition_id_idx" ON "closedPositions" (condition_id)`
  );
  await pool.query(`CREATE TABLE IF NOT EXISTS "polishedClosedPositions" (
    id BIGSERIAL PRIMARY KEY,
    user_address TEXT NOT NULL REFERENCES "walletList" (user_address) ON DELETE CASCADE,
    proxy_wallet TEXT,
    asset TEXT,
    condition_id TEXT,
    avg_price DOUBLE PRECISION,
    total_bought DOUBLE PRECISION,
    realized_pnl DOUBLE PRECISION,
    cur_price DOUBLE PRECISION,
    title TEXT,
    slug TEXT,
    icon TEXT,
    event_slug TEXT,
    outcome TEXT,
    outcome_index INT,
    opposite_outcome TEXT,
    opposite_asset TEXT,
    end_date TEXT,
    timestamp_sec BIGINT
  )`);
  await pool.query(
    `CREATE INDEX IF NOT EXISTS "polishedClosedPositions_user_address_idx" ON "polishedClosedPositions" (user_address)`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS "polishedClosedPositions_condition_id_idx" ON "polishedClosedPositions" (condition_id)`
  );
  await pool.query(`CREATE TABLE IF NOT EXISTS trades (
    id BIGSERIAL PRIMARY KEY,
    user_address TEXT NOT NULL REFERENCES "walletList" (user_address) ON DELETE CASCADE,
    condition_id TEXT,
    outcome_index INT,
    side TEXT,
    size DOUBLE PRECISION,
    price DOUBLE PRECISION,
    timestamp_sec BIGINT,
    title TEXT,
    slug TEXT,
    event_slug TEXT,
    asset TEXT,
    proxy_wallet TEXT,
    icon TEXT
  )`);
  await pool.query(`CREATE INDEX IF NOT EXISTS trades_user_address_idx ON trades (user_address)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS trades_condition_id_idx ON trades (condition_id)`);
  await pool.query(
    `CREATE INDEX IF NOT EXISTS "walletList_updated_at_idx" ON "walletList" (updated_at DESC)`
  );
}

async function saveWalletToPostgres(
  pool: Pool,
  user: string,
  out: PolymarketWalletExportJson
): Promise<void> {
  const client = await pool.connect();
  const m = out.meta;
  const sinceSec = m?.sinceTimestampSec ?? null;
  const lastDays = m?.lastMonthDays ?? null;
  const sinceIso = m?.sinceIso ?? null;
  const winNote = m?.windowTimeSourceNote ?? null;
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO "walletList" (user_address, since_timestamp_sec, last_month_days, since_iso, window_time_source_note, updated_at)
       VALUES ($1, $2, $3, $4, $5, now())
       ON CONFLICT (user_address) DO UPDATE SET
         since_timestamp_sec = EXCLUDED.since_timestamp_sec,
         last_month_days = EXCLUDED.last_month_days,
         since_iso = EXCLUDED.since_iso,
         window_time_source_note = EXCLUDED.window_time_source_note,
         updated_at = now()`,
      [user, sinceSec, lastDays, sinceIso, winNote]
    );
    await client.query("DELETE FROM positions WHERE user_address = $1", [user]);
    await client.query('DELETE FROM "closedPositions" WHERE user_address = $1', [user]);
    await client.query('DELETE FROM "polishedClosedPositions" WHERE user_address = $1', [user]);
    await client.query("DELETE FROM trades WHERE user_address = $1", [user]);
    for (const row of out.positions) {
      if (!isRecord(row)) continue;
      await client.query(SQL_INSERT_OPEN, paramsOpenRow(user, row));
    }
    for (const row of out.closedPositions) {
      if (!isRecord(row)) continue;
      await client.query(SQL_INSERT_CLOSED, paramsClosedRow(user, row));
      await client.query(SQL_INSERT_POLISHED_CLOSED, paramsPolishedClosedRow(user, row));
    }
    for (const row of out.trades) {
      if (!isRecord(row)) continue;
      await client.query(SQL_INSERT_TRADE, paramsTradeRow(user, row));
    }
    await client.query("COMMIT");
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {
      /* ignore */
    }
    throw e;
  } finally {
    client.release();
  }
}

/** In-process last-30d sync (dashboard). Same API + DB behavior as `npx tsx getPositions.ts <user> --last-month`. */
export type WalletSyncLog = (line: string) => void;

export async function runWalletLastMonthSync(
  user: string,
  onLog: WalletSyncLog
): Promise<{ user: string; open: number; closed: number; trades: number }> {
  const dsn = process.env.DATABASE_URL;
  if (!dsn) {
    throw new Error(
      "Set DATABASE_URL to store the export in PostgreSQL (\"walletList\", positions, \"closedPositions\", \"polishedClosedPositions\", trades)."
    );
  }
  const u = normalizeWalletAddress(user);
  const lastMonthDays = 30;
  const sinceTimestampSec = Math.floor(Date.now() / 1000) - lastMonthDays * SECONDS_PER_DAY;
  const logLine: WalletSyncLog = (line) => {
    onLog(line);
  };
  const verbose = true;
  const pagedBase: FetchPagedOptions = {
    verbose,
    minTimestampSec: sinceTimestampSec,
    newestFirstTrades: true,
    logLine,
  };
  const pool = new Pool({ connectionString: dsn });
  try {
    await ensurePolymarketSchema(pool);
  } catch (e) {
    await pool.end();
    throw e;
  }
  if (verbose) {
    logLine(
      `[getPositions] start ${u} — time window: since ${new Date(sinceTimestampSec * 1000).toISOString()} (parallel: /positions, /closed-positions, /trades)`
    );
    logLine(
      '[getPositions] DB: relational rows in "walletList" + positions + "closedPositions" + "polishedClosedPositions" + trades — fetches in parallel, then one transaction per wallet sync.'
    );
  }
  const [positions, closedPositions, trades] = await Promise.all([
    fetchPagedArray("/positions", u, POSITIONS_PAGE_SIZE, undefined, pagedBase),
    fetchPagedArray("/closed-positions", u, POSITIONS_PAGE_SIZE, undefined, pagedBase),
    fetchPagedArray("/trades", u, TRADES_PAGE_SIZE, { takerOnly: "false" }, pagedBase),
  ]);
  const out: PolymarketWalletExportJson = {
    user: u,
    positions,
    closedPositions,
    trades,
  };
  out.meta = {
    sinceTimestampSec,
    sinceIso: new Date(sinceTimestampSec * 1000).toISOString(),
    lastMonthDays: 30,
    windowTimeSourceNote:
      "Open /positions: compare by market endDate, else trailing unix in slug (e.g. *-5m-TS), else " +
      "row timestamp, so a February 2026 market is not included in a April 2026 7d window when " +
      "the API omits `timestamp` on the position. /closed-positions and /trades: " +
      "row timestamp, else endDate, else slug. Rows with no resolvable time are dropped.",
  };
  await saveWalletToPostgres(pool, u, out);
  await pool.end();
  logLine(
    `Wrote ${positions.length} open + ${closedPositions.length} closed + ${trades.length} trades (since ${out.meta?.sinceIso}, last 30d) → PostgreSQL`
  );
  return { user: u, open: positions.length, closed: closedPositions.length, trades: trades.length };
}

function isGetPositionsEntryPoint(): boolean {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }
  const b = basename(entry);
  return b === "getPositions.ts" || b === "getPositions.js";
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const verbose = !argv.includes("--quiet") && !argv.includes("-q");
  const { user: rawUser, sinceTimestampSec, lastMonthDays } = parseArgs(argv);
  if (!rawUser) {
    console.error(
      "Usage: npx tsx getPositions.ts <user-address> [options]\n" +
        "  Set DATABASE_URL — upserts \"walletList\" (meta columns) and replaces all rows in positions, \"closedPositions\" + same-count \"polishedClosedPositions\" (realized PnL from price math), trades (one row per API object, no JSONB).\n" +
        "  --json           Also write <user>.json (or <user>_last<N>d.json) to the current directory.\n" +
        "  If DATABASE_URL is unset, you must pass --json to write a file only.\n" +
        "  --last-month     Only data from the last 30 days (trades, closed, positions by timestamp).\n" +
        "  --since-days=N   Same, but N days (overrides 30 if both given).\n" +
        "  --quiet, -q      No per-page progress (progress goes to stderr by default).\n" +
        "Omit time options for a full (all-time) export."
    );
    process.exit(1);
  }

  const user = normalizeWalletAddress(rawUser);
  const windowed = sinceTimestampSec !== undefined;

  const pagedBase = { verbose, ...(windowed ? { minTimestampSec: sinceTimestampSec! } : {}) } as {
    minTimestampSec?: number;
    newestFirstTrades?: boolean;
    verbose: boolean;
  };
  if (windowed) {
    pagedBase.newestFirstTrades = true;
  }

  if (verbose) {
    const msg = windowed
      ? `time window: since ${new Date((sinceTimestampSec ?? 0) * 1000).toISOString()} (parallel: /positions, /closed-positions, /trades)`
      : "full history (parallel: /positions, /closed-positions, /trades)";
    console.error(`[getPositions] start ${user} — ${msg}`);
  }

  const writeJson = argv.includes("--json");
  const dsn = process.env.DATABASE_URL;
  if (!dsn && !writeJson) {
    console.error(
      "Set DATABASE_URL to store the export in PostgreSQL (\"walletList\", positions, \"closedPositions\", \"polishedClosedPositions\", trades), or pass --json to write a .json file only."
    );
    process.exit(1);
  }

  let pool: Pool | null = null;
  if (dsn) {
    pool = new Pool({ connectionString: dsn });
    try {
      await ensurePolymarketSchema(pool);
    } catch (e) {
      await pool.end();
      throw e;
    }
    if (verbose) {
      console.error(
        '[getPositions] DB: relational rows (not JSONB) in "walletList" + positions + "closedPositions" + "polishedClosedPositions" + trades — fetches in parallel, then one transaction per wallet sync.'
      );
    }
  }

  const [positions, closedPositions, trades] = await Promise.all([
    fetchPagedArray("/positions", user, POSITIONS_PAGE_SIZE, undefined, pagedBase),
    fetchPagedArray("/closed-positions", user, POSITIONS_PAGE_SIZE, undefined, pagedBase),
    fetchPagedArray("/trades", user, TRADES_PAGE_SIZE, { takerOnly: "false" }, pagedBase),
  ]);

  const out: PolymarketWalletExportJson = {
    user,
    positions,
    closedPositions,
    trades,
  };
  if (windowed && sinceTimestampSec !== undefined) {
    out.meta = {
      sinceTimestampSec,
      sinceIso: new Date(sinceTimestampSec * 1000).toISOString(),
      lastMonthDays: lastMonthDays ?? 30,
      windowTimeSourceNote:
        "Open /positions: compare by market endDate, else trailing unix in slug (e.g. *-5m-TS), else " +
        "row timestamp, so a February 2026 market is not included in a April 2026 7d window when " +
        "the API omits `timestamp` on the position. /closed-positions and /trades: " +
        "row timestamp, else endDate, else slug. Rows with no resolvable time are dropped.",
    };
  }

  const dest: string[] = [];
  if (pool) {
    await saveWalletToPostgres(pool, user, out);
    dest.push("PostgreSQL:\"walletList\"+positions+\"closedPositions\"+\"polishedClosedPositions\"+trades");
    await pool.end();
  }
  if (writeJson) {
    const base = lastMonthDays != null ? `${user}_last${lastMonthDays}d` : user;
    const filename = `${base}.json`;
    const filepath = resolve(process.cwd(), filename);
    writeFileSync(filepath, `${JSON.stringify(out, null, 2)}\n`, "utf8");
    dest.push(filename);
  }

  const w = windowed
    ? ` (since ${out.meta?.sinceIso ?? "window"}, last ${out.meta?.lastMonthDays ?? "N"}d)`
    : " (all-time)";
  console.log(
    `Wrote ${positions.length} open + ${closedPositions.length} closed + ${trades.length} trades${w} → ${dest.join(" + ")}`
  );
}

if (isGetPositionsEntryPoint()) {
  main().catch((err: unknown) => {
    console.error(err);
    process.exit(1);
  });
}
