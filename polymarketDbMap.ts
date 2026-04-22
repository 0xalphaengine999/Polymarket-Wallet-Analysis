/**
 * Map Polymarket Data API objects (camelCase) → PostgreSQL row parameters (snake_case columns).
 * No JSONB: one table row = one API object.
 */

export function isRecord(x: unknown): x is Record<string, unknown> {
  return typeof x === "object" && x !== null;
}

function str(x: unknown): string | null {
  if (x == null) return null;
  if (typeof x === "string") return x;
  if (typeof x === "number" && !Number.isNaN(x)) return String(x);
  if (typeof x === "boolean") return x ? "true" : "false";
  return null;
}

function num(x: unknown): number | null {
  if (x == null) return null;
  if (typeof x === "number" && !Number.isNaN(x)) return x;
  if (typeof x === "string" && x.trim() !== "") {
    const n = parseFloat(x);
    return Number.isNaN(n) ? null : n;
  }
  return null;
}

function int(x: unknown): number | null {
  const n = num(x);
  if (n == null) return null;
  return Math.trunc(n);
}

/** `timestamp` from the API: seconds or milliseconds → seconds for `timestamp_sec` columns. */
function timestampSec(x: unknown): number | null {
  const n = num(x);
  if (n == null) return null;
  if (n > 1_000_000_000_000) {
    return Math.trunc(n / 1000);
  }
  return Math.trunc(n);
}

function bool(x: unknown): boolean | null {
  if (x == null) return null;
  if (typeof x === "boolean") return x;
  return null;
}

/** GET /closed-positions — one object per row. */
export function paramsClosedRow(user: string, o: Record<string, unknown>): unknown[] {
  return [
    user,
    str(o.proxyWallet),
    str(o.asset),
    str(o.conditionId),
    num(o.avgPrice),
    num(o.totalBought),
    num(o.realizedPnl),
    num(o.curPrice),
    str(o.title),
    str(o.slug),
    str(o.icon),
    str(o.eventSlug),
    str(o.outcome),
    int(o.outcomeIndex),
    str(o.oppositeOutcome),
    str(o.oppositeAsset),
    str(o.endDate),
    timestampSec(o.timestamp),
  ];
}

export const SQL_INSERT_CLOSED = `INSERT INTO "closedPositions" (
  user_address, proxy_wallet, asset, condition_id, avg_price, total_bought, realized_pnl, cur_price,
  title, slug, icon, event_slug, outcome, outcome_index, opposite_outcome, opposite_asset, end_date, timestamp_sec
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`;

/**
 * PnL from notional: totalBought = share size, prices are per share (USDC in [0,1] for typical Yes).
 * realizedPnl = shares * (cur - avg) = (value if marked at cur) - cost at avg.
 */
export function polishedRealizedPnlFromClosedApiRow(o: Record<string, unknown>): number | null {
  const totalBought = num(o.totalBought);
  const avgPrice = num(o.avgPrice);
  const curPrice = num(o.curPrice);
  if (totalBought == null || avgPrice == null || curPrice == null) {
    return null;
  }
  return totalBought * (curPrice - avgPrice);
}

export function paramsPolishedClosedRow(user: string, o: Record<string, unknown>): unknown[] {
  return [
    user,
    str(o.proxyWallet),
    str(o.asset),
    str(o.conditionId),
    num(o.avgPrice),
    num(o.totalBought),
    polishedRealizedPnlFromClosedApiRow(o),
    num(o.curPrice),
    str(o.title),
    str(o.slug),
    str(o.icon),
    str(o.eventSlug),
    str(o.outcome),
    int(o.outcomeIndex),
    str(o.oppositeOutcome),
    str(o.oppositeAsset),
    str(o.endDate),
    timestampSec(o.timestamp),
  ];
}

export const SQL_INSERT_POLISHED_CLOSED = `INSERT INTO "polishedClosedPositions" (
  user_address, proxy_wallet, asset, condition_id, avg_price, total_bought, realized_pnl, cur_price,
  title, slug, icon, event_slug, outcome, outcome_index, opposite_outcome, opposite_asset, end_date, timestamp_sec
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`;

/** GET /positions — open position object. */
export function paramsOpenRow(user: string, o: Record<string, unknown>): unknown[] {
  return [
    user,
    str(o.proxyWallet),
    str(o.asset),
    str(o.conditionId),
    num(o.initialValue),
    num(o.currentValue),
    num(o.cashPnl),
    num(o.percentPnl),
    num(o.totalBought),
    num(o.realizedPnl),
    num(o.percentRealizedPnl),
    num(o.curPrice),
    num(o.avgPrice),
    str(o.title),
    str(o.slug),
    str(o.icon),
    str(o.eventId),
    str(o.eventSlug),
    str(o.outcome),
    int(o.outcomeIndex),
    str(o.oppositeOutcome),
    str(o.oppositeAsset),
    str(o.endDate),
    timestampSec(o.timestamp),
    bool(o.mergeable),
    bool(o.redeemable),
    o.negativeRisk === undefined || o.negativeRisk === null ? null : Boolean(o.negativeRisk),
  ];
}

export const SQL_INSERT_OPEN = `INSERT INTO positions (
  user_address, proxy_wallet, asset, condition_id,
  initial_value, current_value, cash_pnl, percent_pnl, total_bought, realized_pnl, percent_realized_pnl, cur_price, avg_price,
  title, slug, icon, event_id, event_slug, outcome, outcome_index, opposite_outcome, opposite_asset, end_date, timestamp_sec,
  mergeable, redeemable, negative_risk
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)`;

/** GET /trades — one fill. */
export function paramsTradeRow(user: string, o: Record<string, unknown>): unknown[] {
  return [
    user,
    str(o.conditionId),
    int(o.outcomeIndex),
    str(o.side),
    num(o.size),
    num(o.price),
    timestampSec(o.timestamp),
    str(o.title),
    str(o.slug),
    str(o.eventSlug),
    str(o.asset),
    str(o.proxyWallet),
    str(o.icon),
  ];
}

export const SQL_INSERT_TRADE = `INSERT INTO trades (
  user_address, condition_id, outcome_index, side, size, price, timestamp_sec, title, slug, event_slug, asset, proxy_wallet, icon
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`;
