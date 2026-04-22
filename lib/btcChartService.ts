/**
 * In-process wallet charts (ET) matching analyze.py: default scope "all" markets, rolling 30d ET.
 * Renders PNG via sharp (SVG), no python subprocess.
 */
import type { Pool } from "pg";
import sharp from "sharp";
import { DateTime } from "luxon";

const ET = "America/New_York";
const SLOTS_PER_DAY = 288;
const SECONDS_PER_DAY = 24 * 60 * 60;

/** `api` = "closedPositions" (Data API PnL); `polished` = "polishedClosedPositions" (recomputed PnL). */
export type ChartClosedSource = "api" | "polished";

const BTC_UPDOWN_5M_PREFIX = "btc-updown-5m-";
const BTC_UPDOWN_PREFIX = "btc-updown-";
const BITCOIN_HOURLY_SLUG_RE =
  /^bitcoin-up-or-down-([a-z]+)-(\d{1,2})-(\d{4})-(\d{1,2})(am|pm)-et$/i;
const _MONTH_NAME_TO_NUM: Record<string, number> = {
  january: 1,
  february: 2,
  march: 3,
  april: 4,
  may: 5,
  june: 6,
  july: 7,
  august: 8,
  september: 9,
  october: 10,
  november: 11,
  december: 12,
};

type Row = Record<string, unknown>;

function isRecord(x: unknown): x is Row {
  return typeof x === "object" && x !== null;
}

function str(x: unknown): string {
  if (x == null) return "";
  return String(x);
}

function eventSlugTs(eventSlug: string): number | undefined {
  if (!eventSlug.startsWith(BTC_UPDOWN_5M_PREFIX)) {
    return undefined;
  }
  const tail = eventSlug.slice(BTC_UPDOWN_5M_PREFIX.length);
  const n = parseInt(tail, 10);
  if (Number.isNaN(n) || n <= 0) {
    return undefined;
  }
  return n;
}

function isBtcUpdown5m(row: Row): boolean {
  return (
    (str(row.slug) ?? "").startsWith(BTC_UPDOWN_5M_PREFIX) ||
    (str(row.eventSlug) ?? "").startsWith(BTC_UPDOWN_5M_PREFIX)
  );
}

function isBtcTrade(row: Row): boolean {
  const s = (str(row.slug) ?? "").toLowerCase();
  const e = (str(row.eventSlug) ?? "").toLowerCase();
  return s.startsWith("btc-") || e.startsWith("btc-") || s.startsWith("bitcoin-") || e.startsWith("bitcoin-");
}

function btcUpdownTrailingUnix(s: string): number | undefined {
  const t = s.trim();
  if (!t.startsWith(BTC_UPDOWN_PREFIX)) {
    return undefined;
  }
  const tail = t.split("-").at(-1) ?? "";
  if (!/^\d+$/.test(tail)) {
    return undefined;
  }
  const n = parseInt(tail, 10);
  if (n <= 1_000_000_000) {
    return undefined;
  }
  return n;
}

function hour12To24(h12: number, ap: string): number {
  const a = ap.toLowerCase();
  if (a === "am") {
    return h12 === 12 ? 0 : h12;
  }
  return h12 === 12 ? 12 : h12 + 12;
}

function bitcoinUpOrDownHourlySlugUnix(slugOrEvent: string): number | undefined {
  const t = slugOrEvent.trim();
  const m = t.match(BITCOIN_HOURLY_SLUG_RE);
  if (!m) {
    return undefined;
  }
  const monName = m[1] ?? "";
  const dayS = m[2] ?? "";
  const yearS = m[3] ?? "";
  const hourS = m[4] ?? "";
  const ap = m[5] ?? "";
  const month = _MONTH_NAME_TO_NUM[monName.toLowerCase()] ?? 0;
  if (!month) {
    return undefined;
  }
  const day = parseInt(dayS, 10);
  const year = parseInt(yearS, 10);
  const h12 = parseInt(hourS, 10);
  if (Number.isNaN(day) || Number.isNaN(year) || Number.isNaN(h12) || h12 < 1 || h12 > 12) {
    return undefined;
  }
  const h24 = hour12To24(h12, ap);
  const d = DateTime.fromObject(
    { year, month, day, hour: h24, minute: 0, second: 0 },
    { zone: ET }
  );
  if (!d.isValid) {
    return undefined;
  }
  return Math.floor(d.toSeconds());
}

function unixFromEndDate(row: Row): number | undefined {
  const end = row.endDate;
  if (typeof end !== "string" || end.length < 10) {
    return undefined;
  }
  const head = end.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(head)) {
    return undefined;
  }
  if (end.includes("T")) {
    const t = DateTime.fromISO(end, { setZone: true });
    if (!t.isValid) {
      return undefined;
    }
    return Math.floor(t.toSeconds());
  }
  const d = DateTime.fromISO(`${head}T12:00:00.000Z`, { setZone: true });
  if (!d.isValid) {
    return undefined;
  }
  return Math.floor(d.toSeconds());
}

function rowEventUnixEt(row: Row, only5m: boolean): number | undefined {
  const slug = str(row.slug) ?? "";
  const ev = str(row.eventSlug) ?? "";
  if (only5m) {
    if (!isBtcUpdown5m(row)) {
      return undefined;
    }
    for (const s of [ev, slug]) {
      const t = eventSlugTs(s);
      if (t !== undefined) {
        return t;
      }
    }
    return undefined;
  }
  if (!isBtcTrade(row)) {
    return undefined;
  }
  for (const s of [ev, slug]) {
    const u = btcUpdownTrailingUnix(s);
    if (u !== undefined) {
      return u;
    }
  }
  for (const s of [ev, slug]) {
    const u = bitcoinUpOrDownHourlySlugUnix(s);
    if (u !== undefined) {
      return u;
    }
  }
  const raw = row.timestamp;
  if (raw !== undefined && raw !== null) {
    const t = Math.trunc(Number(raw));
    if (!Number.isNaN(t)) {
      return t;
    }
  }
  return unixFromEndDate(row);
}

function rowEventUnixAllMarkets(row: Row): number | undefined {
  const raw = row.timestamp;
  if (raw !== undefined && raw !== null) {
    const t = Math.trunc(Number(raw));
    if (!Number.isNaN(t)) {
      return t;
    }
  }
  const slug = str(row.slug) ?? "";
  const ev = str(row.eventSlug) ?? "";
  for (const s of [ev, slug]) {
    const u = btcUpdownTrailingUnix(s);
    if (u !== undefined) {
      return u;
    }
  }
  for (const s of [ev, slug]) {
    const u = bitcoinUpOrDownHourlySlugUnix(s);
    if (u !== undefined) {
      return u;
    }
  }
  return unixFromEndDate(row);
}

function tradeFillCashFlowUsd(row: Row): number | undefined {
  if (row.side !== "BUY" && row.side !== "SELL") {
    return undefined;
  }
  let sz: number;
  let pr: number;
  try {
    sz = Number(row.size);
    pr = Number(row.price);
  } catch {
    return undefined;
  }
  if (Number.isNaN(sz) || Number.isNaN(pr)) {
    return undefined;
  }
  const notional = sz * pr;
  return row.side === "BUY" ? -notional : notional;
}

function marketKey(row: Row): [string, number] | undefined {
  const cid = row.conditionId;
  if (typeof cid !== "string") {
    return undefined;
  }
  const oi = row.outcomeIndex;
  if (typeof oi === "boolean") {
    return undefined;
  }
  if (oi == null) {
    return [cid.toLowerCase(), -1];
  }
  if (typeof oi === "number" && !Number.isNaN(oi)) {
    return [cid.toLowerCase(), Math.trunc(oi)];
  }
  return [cid.toLowerCase(), -1];
}

function positionCoverKeys(posRows: Row[]): Set<string> {
  const s = new Set<string>();
  for (const r of posRows) {
    const k = marketKey(r);
    if (k) {
      s.add(`${k[0]}\0${k[1]}`);
    }
  }
  return s;
}

function tradeMatchesScope(trade: Row, scope: string): boolean {
  if (scope === "all") {
    return true;
  }
  if (scope === "5m") {
    return isBtcUpdown5m(trade);
  }
  return isBtcTrade(trade);
}

function maxTradeTsByCondition(trades: Row[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const t of trades) {
    if (typeof t.conditionId !== "string") {
      continue;
    }
    const raw = t.timestamp;
    if (raw === undefined || raw === null) {
      continue;
    }
    const ts = Math.trunc(Number(raw));
    if (Number.isNaN(ts)) {
      continue;
    }
    const k = t.conditionId.toLowerCase();
    if (out[k] === undefined || ts > out[k]!) {
      out[k] = ts;
    }
  }
  return out;
}

function positionPnl(row: Row): number {
  if (row.cashPnl != null) {
    return Number(row.cashPnl);
  }
  if (row.realizedPnl != null) {
    return Number(row.realizedPnl);
  }
  const f = tradeFillCashFlowUsd(row);
  return f != null && !Number.isNaN(f) ? f : 0;
}

function resolveEventUnix(
  row: Row,
  scope: string,
  tradeTsByCid: Record<string, number>
): number | undefined {
  const ts = scope === "all" ? rowEventUnixAllMarkets(row) : rowEventUnixEt(row, scope === "5m");
  if (ts !== undefined) {
    return ts;
  }
  const cid = row.conditionId;
  if (typeof cid === "string") {
    return tradeTsByCid[cid.toLowerCase()];
  }
  return undefined;
}

function dayKeyInRollingWindow(dayKey: string, sinceDays: number): boolean {
  if (sinceDays <= 0) {
    return true;
  }
  const nowEt = DateTime.now().setZone(ET);
  const sinceTs = Math.floor(nowEt.toSeconds()) - sinceDays * SECONDS_PER_DAY;
  const d = DateTime.fromFormat(dayKey, "yyyy-MM-dd", { zone: ET });
  if (!d.isValid) {
    return false;
  }
  const dayEnd = d.plus({ days: 1 });
  return Math.floor(dayEnd.toSeconds()) > sinceTs;
}

function _addCell(
  counts: Map<string, number>,
  pnlSum: Map<string, number>,
  ts: number,
  pnlDelta: number,
  countDelta: number
): void {
  const d = DateTime.fromSeconds(ts, { zone: "utc" }).setZone(ET);
  const dayKey = d.toFormat("yyyy-MM-dd");
  const slot = Math.trunc((d.hour * 60 + d.minute) / 5);
  if (slot < 0 || slot >= SLOTS_PER_DAY) {
    return;
  }
  const k = `${dayKey}\0${String(slot)}`;
  counts.set(k, (counts.get(k) ?? 0) + countDelta);
  pnlSum.set(k, (pnlSum.get(k) ?? 0) + pnlDelta);
}

function loadCountsAndPnlFromData(
  data: { positions?: unknown[]; closedPositions?: unknown[]; trades?: unknown[] },
  scope: string
): { counts: Map<string, number>; pnl: Map<string, number> } {
  const posRows: Row[] = [];
  for (const key of ["positions", "closedPositions"] as const) {
    const chunk = data[key];
    if (Array.isArray(chunk)) {
      for (const r of chunk) {
        if (isRecord(r)) {
          posRows.push(r);
        }
      }
    }
  }
  const tradesList: Row[] = Array.isArray(data.trades) ? (data.trades as Row[]).filter(isRecord) : [];
  const tradeTs = maxTradeTsByCondition(tradesList);
  const covered = positionCoverKeys(posRows);
  const counts = new Map<string, number>();
  const pnlSum = new Map<string, number>();
  for (const row of posRows) {
    const ts = resolveEventUnix(row, scope, tradeTs);
    if (ts === undefined) {
      continue;
    }
    _addCell(counts, pnlSum, ts, positionPnl(row), 1);
  }
  for (const tr of tradesList) {
    const mk = marketKey(tr);
    if (mk === undefined) {
      continue;
    }
    const s = `${mk[0]}\0${mk[1]}`;
    if (covered.has(s)) {
      continue;
    }
    if (!tradeMatchesScope(tr, scope)) {
      continue;
    }
    const ts = resolveEventUnix(tr, scope, tradeTs);
    if (ts === undefined) {
      continue;
    }
    const flow = tradeFillCashFlowUsd(tr);
    if (flow == null) {
      continue;
    }
    _addCell(counts, pnlSum, ts, flow, 1);
  }
  return { counts, pnl: pnlSum };
}

function filterByRolling(
  counts: Map<string, number>,
  pnl: Map<string, number>,
  sinceDays: number
): { counts: Map<string, number>; pnl: Map<string, number> } {
  const c2 = new Map<string, number>();
  const p2 = new Map<string, number>();
  for (const [k, v] of counts) {
    const dayKey = k.split("\0")[0] ?? "";
    if (dayKeyInRollingWindow(dayKey, sinceDays)) {
      c2.set(k, v);
    }
  }
  for (const [k, v] of pnl) {
    const dayKey = k.split("\0")[0] ?? "";
    if (dayKeyInRollingWindow(dayKey, sinceDays)) {
      p2.set(k, v);
    }
  }
  return { counts: c2, pnl: p2 };
}

function posRowToApi(r: Row): Row {
  return {
    proxyWallet: r.proxy_wallet,
    asset: r.asset,
    conditionId: r.condition_id,
    initialValue: r.initial_value,
    currentValue: r.current_value,
    cashPnl: r.cash_pnl,
    percentPnl: r.percent_pnl,
    totalBought: r.total_bought,
    realizedPnl: r.realized_pnl,
    percentRealizedPnl: r.percent_realized_pnl,
    curPrice: r.cur_price,
    avgPrice: r.avg_price,
    title: r.title,
    slug: r.slug,
    icon: r.icon,
    eventId: r.event_id,
    eventSlug: r.event_slug,
    outcome: r.outcome,
    outcomeIndex: r.outcome_index,
    oppositeOutcome: r.opposite_outcome,
    oppositeAsset: r.opposite_asset,
    endDate: r.end_date,
    timestamp: r.timestamp_sec,
    mergeable: r.mergeable,
    redeemable: r.redeemable,
    negativeRisk: r.negative_risk,
  } as Row;
}

function closedRowToApi(r: Row): Row {
  return {
    proxyWallet: r.proxy_wallet,
    asset: r.asset,
    conditionId: r.condition_id,
    avgPrice: r.avg_price,
    totalBought: r.total_bought,
    realizedPnl: r.realized_pnl,
    curPrice: r.cur_price,
    title: r.title,
    slug: r.slug,
    icon: r.icon,
    eventSlug: r.event_slug,
    outcome: r.outcome,
    outcomeIndex: r.outcome_index,
    oppositeOutcome: r.opposite_outcome,
    oppositeAsset: r.opposite_asset,
    endDate: r.end_date,
    timestamp: r.timestamp_sec,
  } as Row;
}

function tradeRowToApi(r: Row): Row {
  return {
    conditionId: r.condition_id,
    outcomeIndex: r.outcome_index,
    side: r.side,
    size: r.size,
    price: r.price,
    timestamp: r.timestamp_sec,
    title: r.title,
    slug: r.slug,
    eventSlug: r.event_slug,
    asset: r.asset,
    proxyWallet: r.proxy_wallet,
    icon: r.icon,
  } as Row;
}

function closedTableQuoted(closedFrom: ChartClosedSource): "closedPositions" | "polishedClosedPositions" {
  return closedFrom === "polished" ? "polishedClosedPositions" : "closedPositions";
}

function closedSourceChartSuffix(closedFrom: ChartClosedSource): string {
  return closedFrom === "polished"
    ? " — closed: polishedClosedPositions (recomputed PnL)"
    : " — closed: closedPositions (Data API PnL)";
}

export async function loadWalletDictForChart(
  pool: Pool,
  user: string,
  closedFrom: ChartClosedSource = "api"
): Promise<{
  positions: Row[];
  closedPositions: Row[];
  trades: Row[];
}> {
  const u = user.trim().toLowerCase();
  const tClosed = closedTableQuoted(closedFrom);
  const qP = `SELECT proxy_wallet, asset, condition_id, initial_value, current_value, cash_pnl, percent_pnl, total_bought, realized_pnl, percent_realized_pnl, cur_price, avg_price, title, slug, icon, event_id, event_slug, outcome, outcome_index, opposite_outcome, opposite_asset, end_date, timestamp_sec, mergeable, redeemable, negative_risk
    FROM positions WHERE user_address = $1`;
  const qC = `SELECT proxy_wallet, asset, condition_id, avg_price, total_bought, realized_pnl, cur_price, title, slug, icon, event_slug, outcome, outcome_index, opposite_outcome, opposite_asset, end_date, timestamp_sec
    FROM "${tClosed}" WHERE user_address = $1`;
  const qT = `SELECT condition_id, outcome_index, side, size, price, timestamp_sec, title, slug, event_slug, asset, proxy_wallet, icon
    FROM trades WHERE user_address = $1`;
  const [p, c, t] = await Promise.all([
    pool.query<Row>(qP, [u]),
    pool.query<Row>(qC, [u]),
    pool.query<Row>(qT, [u]),
  ]);
  return {
    positions: p.rows.map((r) => posRowToApi(r)),
    closedPositions: c.rows.map((r) => closedRowToApi(r)),
    trades: t.rows.map((r) => tradeRowToApi(r)),
  };
}

function escapeXml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function dayAxisLabelsEt(days: string[]): string[] {
  return days.map((d) => {
    const dt = DateTime.fromFormat(d, "yyyy-MM-dd", { zone: ET });
    if (!dt.isValid) {
      return d;
    }
    return `${dt.toFormat("MMM d, yyyy")}`;
  });
}

function ylOrRd(n: number, m: number): { r: number; g: number; b: number } {
  if (m <= 0) {
    return { r: 255, g: 255, b: 255 };
  }
  const t = n / m;
  const a = Math.min(1, Math.max(0, t));
  if (a < 0.2) {
    return { r: 255, g: 255, b: 204 };
  }
  if (a < 0.45) {
    return { r: 255, g: 255, b: 153 };
  }
  if (a < 0.65) {
    return { r: 255, g: 204, b: 102 };
  }
  if (a < 0.85) {
    return { r: 255, g: 153, b: 51 };
  }
  return { r: 189, g: 0, b: 38 };
}

function modeTitleForScope(scope: "all" | "btc" | "5m"): string {
  if (scope === "5m") {
    return "BTC up/down 5m";
  }
  if (scope === "btc") {
    return "All BTC-tagged";
  }
  return "All markets";
}

function buildDailySvg(
  daysSorted: string[],
  profits: number[],
  nWindow: number | null,
  modeLabel: string
): string {
  const w = Math.max(1000, Math.min(2800, 45 * daysSorted.length + 400));
  const h = 520;
  const marginL = 70;
  const marginR = 40;
  const marginB = 160;
  const marginT = 100;
  const n = daysSorted.length;
  const yMin = Math.min(0, ...profits) * 1.12;
  const yMax = Math.max(0, ...profits) * 1.12;
  const ySpan = Math.max(1e-9, yMax - yMin);
  const plotH = h - marginT - marginB;
  const plotW = w - marginL - marginR;
  const bw = Math.min(48, plotW / Math.max(1, n) * 0.75);
  const wTitle = nWindow != null ? ` (rolling last ${nWindow}d to now, ET days)` : "";
  const bars: string[] = [];
  for (let i = 0; i < n; i++) {
    const p = profits[i] ?? 0;
    const y0 = marginT + ((yMax - 0) / ySpan) * plotH;
    const y1 = marginT + ((yMax - p) / ySpan) * plotH;
    const x0 = marginL + (i + 0.5) * (plotW / Math.max(1, n)) - bw / 2;
    const yTop = Math.min(y0, y1);
    const bH = Math.abs(y1 - y0);
    const fill = p >= 0 ? "#2ca02c" : "#d62728";
    const lx = x0 + bw / 2;
    bars.push(
      `<rect x="${x0}" y="${yTop}" width="${bw}" height="${bH.toFixed(2)}" fill="${fill}" />`
    );
    bars.push(
      `<text x="${lx}" y="${yTop - 4}" text-anchor="middle" font-size="9" fill="#ffffff" font-family="Outfit, sans-serif">$${p.toFixed(2)}</text>`
    );
  }
  const labels: string[] = dayAxisLabelsEt(daysSorted);
  for (let i = 0; i < n; i++) {
    const lx = marginL + (i + 0.5) * (plotW / Math.max(1, n));
    const t = labels[i] ?? daysSorted[i] ?? "";
    bars.push(
      `<text x="${lx}" y="${h - 30}" text-anchor="end" font-size="8" fill="#ffffff" font-family="Outfit, sans-serif" transform="rotate(-35 ${lx} ${h - 30})">${escapeXml(t)}</text>`
    );
  }
  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
<rect width="100%" height="100%" fill="#0a0b10"/>
<line x1="${marginL}" y1="${marginT + (yMax / ySpan) * plotH}" x2="${w - marginR}" y2="${marginT + (yMax / ySpan) * plotH}" stroke="#333" stroke-width="1" />
${bars.join("\n")}
<text x="${w / 2}" y="40" text-anchor="middle" font-size="16" font-weight="600" fill="#e8eaf2" font-family="Outfit, sans-serif">${escapeXml(modeLabel)} — daily sum per calendar day (ET)${escapeXml(wTitle)}</text>
<text x="${w / 2 - 20}" y="${h - 8}" text-anchor="middle" font-size="11" fill="#ffffff" font-family="Outfit, sans-serif">Day (ET)</text>
</svg>`;
}

function nowXEtFr(): { nowEt: DateTime; x01: number } {
  const nowEt = DateTime.now().setZone(ET);
  const x = nowEt.hour + nowEt.minute / 60 + nowEt.second / 3600;
  return { nowEt, x01: (x / 24) * 100 };
}

function buildCountHeatmapSvg(
  days: string[],
  matrix: number[][],
  sinceDays: number,
  modeLabel: string
): { svg: string; w: number; h: number } {
  const nDays = days.length;
  if (nDays === 0) {
    return { svg: '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="80"><text fill="white" x="10" y="40">No data</text></svg>', w: 400, h: 80 };
  }
  const cw = 3;
  const ch = 16;
  const maxC = Math.max(1, ...matrix.flat());
  const marginL = 130;
  const marginT = 100;
  const marginR = 80;
  const marginB = 80;
  const plotW = SLOTS_PER_DAY * cw;
  const plotH = nDays * ch;
  const w = marginL + plotW + marginR;
  const h = marginT + plotH + marginB;
  const { nowEt, x01 } = nowXEtFr();
  const lineX = marginL + (x01 / 100) * plotW;
  const yLabels = dayAxisLabelsEt(days);
  const cells: string[] = [];
  for (let i = 0; i < nDays; i++) {
    for (let slot = 0; slot < SLOTS_PER_DAY; slot++) {
      const v = matrix[i]![slot] ?? 0;
      const c = ylOrRd(v, maxC);
      const x = marginL + slot * cw;
      const y = marginT + (nDays - 1 - i) * ch;
      cells.push(
        `<rect x="${x}" y="${y}" width="${cw - 0.1}" height="${ch - 0.5}" fill="rgb(${c.r},${c.g},${c.b})" stroke="none" />`
      );
    }
  }
  const yTix: string[] = [];
  for (let i = 0; i < nDays; i++) {
    yTix.push(
      `<text x="8" y="${marginT + (nDays - 0.5 - i) * ch + 4}" font-size="7" fill="#ffffff" font-family="Outfit, sans-serif">${escapeXml(yLabels[i] ?? days[i] ?? "")}</text>`
    );
  }
  return {
    svg: `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
<rect width="100%" height="100%" fill="#0a0b10"/>
${cells.join("\n")}
${yTix.join("\n")}
<line x1="${lineX}" y1="${marginT}" x2="${lineX}" y2="${marginT + plotH}" stroke="red" stroke-width="1.2" />
<text x="${w / 2}" y="50" text-anchor="middle" font-size="16" font-weight="600" fill="#e8eaf2" font-family="Outfit, sans-serif">${escapeXml(modeLabel)} — trade count per day × 5-minute slot (ET) (rolling last ${sinceDays}d to now, ET days)</text>
<text x="${w - 40}" y="70" text-anchor="end" font-size="8" fill="#ffffff" font-family="Outfit, sans-serif">now ET ${nowEt.toFormat("h:mm a")}</text>
<text x="${w / 2}" y="${h - 20}" text-anchor="middle" font-size="10" fill="#ffffff" font-family="Outfit, sans-serif">Time of day (ET) — 12am … 12am+1d (red = now)</text>
</svg>`,
    w,
    h,
  };
}

/**
 * Same idea as: analyze.py (daily|count) --user … --all --last-month (default scope: all markets).
 * @param closedFrom Which table supplies closed position rows: API vs recomputed PnL.
 */
export async function renderBtcLast30dChart(
  pool: Pool,
  user: string,
  kind: "daily" | "count",
  outPath: string,
  onLog: (line: string) => void,
  closedFrom: ChartClosedSource = "api"
): Promise<void> {
  const scope: "all" | "btc" | "5m" = "all";
  const modeLabel = modeTitleForScope(scope) + closedSourceChartSuffix(closedFrom);
  const sinceDays = 30;
  onLog(
    `Loading wallet data (closed: ${closedFrom === "polished" ? "polishedClosedPositions" : "closedPositions"})…`
  );
  const data = await loadWalletDictForChart(pool, user, closedFrom);
  const raw = loadCountsAndPnlFromData(data, scope);
  const { counts, pnl } = filterByRolling(raw.counts, raw.pnl, sinceDays);
  if (counts.size === 0) {
    throw new Error(
      "No rows matched (all markets: timestamp, slug unix, or endDate); with trades, fills need a resolvable time. " +
        "Ensure the wallet was refreshed in the last 30d and has data in the rolling window."
    );
  }
  if (kind === "daily") {
    const dailyTotals = new Map<string, number>();
    for (const [k, v] of pnl) {
      const [day] = k.split("\0");
      if (!day) {
        continue;
      }
      dailyTotals.set(day, (dailyTotals.get(day) ?? 0) + v);
    }
    const daysSorted = [...dailyTotals.keys()].sort();
    const profits = daysSorted.map((d) => dailyTotals.get(d) ?? 0);
    const svg = buildDailySvg(daysSorted, profits, sinceDays, modeLabel);
    onLog("Rendering daily PnL chart (in-process)…");
    await sharp(Buffer.from(svg, "utf8"), { density: 120 })
      .png()
      .toFile(outPath);
  } else {
    const daySet = new Set<string>();
    for (const k of counts.keys()) {
      const [d0] = k.split("\0");
      if (d0) {
        daySet.add(d0);
      }
    }
    const days = [...daySet].sort();
    const n = days.length;
    const dayIndex: Record<string, number> = {};
    for (let i = 0; i < n; i++) {
      dayIndex[days[i]!] = i;
    }
    const mat: number[][] = [];
    for (let i = 0; i < n; i++) {
      mat[i] = new Array(SLOTS_PER_DAY).fill(0);
    }
    for (const [k, c] of counts) {
      const [dk, s] = k.split("\0");
      if (dk == null || s == null) {
        continue;
      }
      const si = parseInt(s, 10);
      const j = dayIndex[dk];
      if (j === undefined || Number.isNaN(si)) {
        continue;
      }
      if (j >= 0 && j < n && si >= 0 && si < SLOTS_PER_DAY) {
        const row = mat[j]!;
        row[si] = c;
      }
    }
    const { svg, w, h } = buildCountHeatmapSvg(days, mat, sinceDays, modeLabel);
    onLog("Rendering count heatmap (in-process)…");
    onLog(
      "Layout: day × 5m slot; red = now ET. PNG size follows SVG (" + w + "×" + h + " logical px)."
    );
    await sharp(Buffer.from(svg, "utf8"), { density: 100 }).png().toFile(outPath);
  }
  onLog(`Wrote ${outPath}`);
}
