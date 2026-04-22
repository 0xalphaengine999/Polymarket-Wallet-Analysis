"""
BTC-related positions — charts in US/Eastern.

Commands:
  python analyze.py count  <positions.json>   # heatmap: time × day → trade count
  python analyze.py pnl    <positions.json>   # heatmap: time × day → net PnL
  python analyze.py daily  <positions.json>   # X: day (ET), Y: net profit ($)
  python analyze.py daily  --user 0x...      # same, from DB: walletList + positions + closedPositions + trades

By default all BTC-tagged Polymarket rows are included (slug/eventSlug starting with
btc- or bitcoin-, e.g. btc-updown-5m-*, btc-updown-15m-*, bitcoin-up-or-down-*-et).
Use --5m to restrict to btc-updown-5m-* only. Use --all for every market (time from
timestamp, btc-updown slug unix, hourly bitcoin slug, or endDate).

Input JSON: wallet export from getPositions.ts: "positions", "closedPositions",
"trades". Daily PnL is (1) sum of cashPnl / realizedPnl from positions + closedPositions;
(2) if trades is non-empty, add signed USDC flow only for trades whose
(conditionId, outcomeIndex) is not covered by any position/closed row (avoids
double-counting fills already reflected in portfolio snapshots). Timestamps for
position rows use slug/time/endDate or max trade time per conditionId as fallback.

Options: [-o OUT] [--show] [--5m | --all] [--last-month | --since-days N]

  Rolling window (optional): only ET calendar days that overlap the last N 24h ending now
  (--last-month = 30). Example:  python analyze.py daily export.json --all --last-month --show

Default outputs: graphs/<stem>_btc_count.png | ... (see -o; folder created if missing)
With --5m: graphs/<stem>_5m_*.png   With --all: graphs/<stem>_all_*.png
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

from dotenv import load_dotenv

load_dotenv()
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axes import Axes
from matplotlib.colors import TwoSlopeNorm
from matplotlib.figure import Figure

BTC_UPDOWN_5M_PREFIX = "btc-updown-5m-"
BTC_UPDOWN_PREFIX = "btc-updown-"  # 5m, 15m, etc. — trailing segment is unix seconds
# e.g. bitcoin-up-or-down-april-16-2026-2am-et (1h window, ET encoded in slug)
BITCOIN_HOURLY_SLUG_RE = re.compile(
    r"^bitcoin-up-or-down-([a-z]+)-(\d{1,2})-(\d{4})-(\d{1,2})(am|pm)-et$",
    re.IGNORECASE,
)
_MONTH_NAME_TO_NUM = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
ET = ZoneInfo("America/New_York")
SECONDS_PER_DAY = 24 * 60 * 60
SLOTS_PER_DAY = 24 * 60 // 5  # 288 five-minute slots from 00:00–23:55
# PnL heatmap: same color for all profits ≥ this ($) and all losses ≤ −this ($)
PNL_COLOR_CLAMP_USD = 250.0
GRAPHS_DIR = Path("graphs")


def default_graph_png_path(filename: str) -> Path:
    GRAPHS_DIR.mkdir(parents=True, exist_ok=True)
    return GRAPHS_DIR / filename


def event_slug_ts(event_slug: str) -> int | None:
    if not event_slug.startswith(BTC_UPDOWN_5M_PREFIX):
        return None
    tail = event_slug[len(BTC_UPDOWN_5M_PREFIX) :]
    try:
        n = int(tail)
    except ValueError:
        return None
    return n if n > 0 else None


def is_btc_updown_5m(row: dict) -> bool:
    slug = row.get("slug") or ""
    ev = row.get("eventSlug") or ""
    return slug.startswith(BTC_UPDOWN_5M_PREFIX) or ev.startswith(BTC_UPDOWN_5M_PREFIX)


def is_btc_trade(row: dict) -> bool:
    """Any Polymarket BTC bucket we can place on the chart (slug / eventSlug)."""
    s = (row.get("slug") or "").lower()
    e = (row.get("eventSlug") or "").lower()
    return s.startswith("btc-") or e.startswith("btc-") or s.startswith("bitcoin-") or e.startswith("bitcoin-")


def btc_updown_trailing_unix(slug_or_event: str) -> int | None:
    """btc-updown-{5m|15m|...}-<unix> → unix; else None."""
    s = slug_or_event.strip()
    if not s.startswith(BTC_UPDOWN_PREFIX):
        return None
    tail = s.rsplit("-", 1)[-1]
    if not tail.isdigit():
        return None
    n = int(tail)
    return n if n > 1_000_000_000 else None


def _hour_12_to_24(h12: int, am_pm: str) -> int:
    ap = am_pm.lower()
    if ap == "am":
        return 0 if h12 == 12 else h12
    return 12 if h12 == 12 else h12 + 12


def bitcoin_up_or_down_hourly_slug_unix(slug_or_event: str) -> int | None:
    """bitcoin-up-or-down-{month}-{day}-{year}-{hour}{am|pm}-et → unix (instant in ET)."""
    s = slug_or_event.strip()
    m = BITCOIN_HOURLY_SLUG_RE.match(s)
    if not m:
        return None
    mon_name, day_s, year_s, hour_s, ap = m.groups()
    month = _MONTH_NAME_TO_NUM.get(mon_name.lower())
    if month is None:
        return None
    try:
        day = int(day_s)
        year = int(year_s)
        h12 = int(hour_s)
    except ValueError:
        return None
    if not (1 <= h12 <= 12):
        return None
    h24 = _hour_12_to_24(h12, ap)
    try:
        dt = datetime(year, month, day, h24, 0, 0, tzinfo=ET)
    except ValueError:
        return None
    return int(dt.timestamp())


def row_event_unix_et(row: dict, only_5m: bool) -> int | None:
    """Unix seconds for placing row on day × time grid (ET derived from this instant)."""
    slug = row.get("slug") or ""
    ev = row.get("eventSlug") or ""

    if only_5m:
        if not is_btc_updown_5m(row):
            return None
        for s in (ev, slug):
            t = event_slug_ts(s)
            if t is not None:
                return t
        return None

    if not is_btc_trade(row):
        return None
    for s in (ev, slug):
        u = btc_updown_trailing_unix(s)
        if u is not None:
            return u
    for s in (ev, slug):
        u = bitcoin_up_or_down_hourly_slug_unix(s)
        if u is not None:
            return u
    raw = row.get("timestamp")
    if raw is not None:
        try:
            return int(float(raw))
        except (TypeError, ValueError):
            pass
    return unix_from_end_date(row)


def unix_from_end_date(row: dict) -> int | None:
    """When there is no timestamp: use endDate (date-only uses noon UTC on that day)."""
    end = row.get("endDate")
    if not isinstance(end, str) or len(end) < 10:
        return None
    head = end[:10]
    if not (len(head) == 10 and head[4] == "-" and head[7] == "-"):
        return None
    try:
        y, mo, d = int(head[0:4]), int(head[5:7]), int(head[8:10])
    except ValueError:
        return None
    if "T" in end:
        try:
            dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            return None
    dt = datetime(y, mo, d, 12, 0, 0, tzinfo=timezone.utc)
    return int(dt.timestamp())


def row_event_unix_all_markets(row: dict) -> int | None:
    """Any row: timestamp, else btc-updown-* unix, else bitcoin-up-or-down hourly slug, else endDate."""
    raw = row.get("timestamp")
    if raw is not None:
        try:
            return int(float(raw))
        except (TypeError, ValueError):
            pass
    slug = row.get("slug") or ""
    ev = row.get("eventSlug") or ""
    for s in (ev, slug):
        u = btc_updown_trailing_unix(s)
        if u is not None:
            return u
    for s in (ev, slug):
        u = bitcoin_up_or_down_hourly_slug_unix(s)
        if u is not None:
            return u
    return unix_from_end_date(row)


def trade_fill_cash_flow_usd(row: dict) -> float | None:
    """Data API /trades rows: signed USDC notional (BUY = outflow, SELL = inflow)."""
    side = row.get("side")
    if side not in ("BUY", "SELL"):
        return None
    try:
        sz = float(row["size"])
        pr = float(row["price"])
    except (KeyError, TypeError, ValueError):
        return None
    notional = sz * pr
    return -notional if side == "BUY" else notional


def market_key(row: dict) -> tuple[str, int] | None:
    """(conditionId lower, outcomeIndex) for matching positions to trades."""
    cid = row.get("conditionId")
    if not isinstance(cid, str):
        return None
    oi = row.get("outcomeIndex")
    if isinstance(oi, bool):
        return None
    if isinstance(oi, (int, float)):
        return (cid.lower(), int(oi))
    return (cid.lower(), -1)


def position_cover_keys(position_rows: list[dict]) -> set[tuple[str, int]]:
    """Markets (condition + outcome) that have a position or closed snapshot."""
    s: set[tuple[str, int]] = set()
    for r in position_rows:
        k = market_key(r)
        if k is not None:
            s.add(k)
    return s


def trade_matches_scope(trade: dict, scope: str) -> bool:
    if scope == "all":
        return True
    if scope == "5m":
        return is_btc_updown_5m(trade)
    return is_btc_trade(trade)


def max_trade_ts_by_condition(trades: list[dict]) -> dict[str, int]:
    """Latest trade timestamp per conditionId (lowercase key) for time attribution."""
    out: dict[str, int] = {}
    for t in trades:
        cid = t.get("conditionId")
        if not isinstance(cid, str):
            continue
        raw = t.get("timestamp")
        if raw is None:
            continue
        try:
            ts = int(float(raw))
        except (TypeError, ValueError):
            continue
        k = cid.lower()
        if k not in out or ts > out[k]:
            out[k] = ts
    return out


def position_pnl(row: dict) -> float:
    """Portfolio rows: cashPnl / realizedPnl; trade-only rows: signed fill flow."""
    if "cashPnl" in row and row["cashPnl"] is not None:
        return float(row["cashPnl"])
    v = row.get("realizedPnl")
    if v is not None:
        return float(v)
    flow = trade_fill_cash_flow_usd(row)
    return float(flow) if flow is not None else 0.0


def extract_trades_list(data: dict) -> list[dict]:
    raw = data.get("trades")
    if isinstance(raw, list):
        return [t for t in raw if isinstance(t, dict)]
    return []


def wallet_rows_and_trade_ts(
    data: dict,
) -> tuple[list[dict], dict[str, int], list[dict]]:
    """
    Only positions + closed rows (never trades). Trades returned separately for residual PnL.
    """
    trade_list = extract_trades_list(data)
    trade_ts_by_cid = max_trade_ts_by_condition(trade_list)

    rows: list[dict] = []
    for key in ("positions", "closedPositions"):
        chunk = data.get(key)
        if isinstance(chunk, list):
            rows.extend(r for r in chunk if isinstance(r, dict))

    return rows, trade_ts_by_cid, trade_list


def position_rows_from_json(data: object) -> list[dict]:
    """Root array of rows, or wallet dump (positions + closed; trades-only fallback)."""
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict):
        if not any(
            k in data for k in ("positions", "closedPositions", "trades")
        ):
            raise SystemExit(
                'JSON object must include "positions", "closedPositions", and/or '
                '"trades" (wallet export), or use a root array of position objects.'
            )
        rows, _, _ = wallet_rows_and_trade_ts(data)
        return rows
    raise SystemExit(
        "JSON root must be an array of position objects, or a wallet export object."
    )


def resolve_event_unix(
    row: dict,
    scope: str,
    trade_ts_by_condition: dict[str, int],
) -> int | None:
    """Position time from slug/timestamp/endDate, else max trade time for conditionId."""
    if scope == "all":
        ts = row_event_unix_all_markets(row)
    else:
        ts = row_event_unix_et(row, only_5m=(scope == "5m"))
    if ts is not None:
        return ts
    cid = row.get("conditionId")
    if isinstance(cid, str):
        return trade_ts_by_condition.get(cid.lower())
    return None


def _add_cell(
    counts: dict[tuple[str, int], int],
    pnl_sum: dict[tuple[str, int], float],
    ts: int,
    pnl_delta: float,
    count_delta: int,
) -> None:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(ET)
    day_key = dt.strftime("%Y-%m-%d")
    slot = (dt.hour * 60 + dt.minute) // 5
    if not 0 <= slot < SLOTS_PER_DAY:
        return
    key = (day_key, slot)
    counts[key] += count_delta
    pnl_sum[key] += pnl_delta


def _jsonb_array_to_list(value: object) -> list:
    """Legacy: wallet exports used a single `api_response` array per user; not used for relational DB."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _pg_pos_row_to_api(r: dict) -> dict:
    """Map `positions` table (snake_case) → Data API / JSON export shape (camelCase)."""
    return {
        "proxyWallet": r.get("proxy_wallet"),
        "asset": r.get("asset"),
        "conditionId": r.get("condition_id"),
        "initialValue": r.get("initial_value"),
        "currentValue": r.get("current_value"),
        "cashPnl": r.get("cash_pnl"),
        "percentPnl": r.get("percent_pnl"),
        "totalBought": r.get("total_bought"),
        "realizedPnl": r.get("realized_pnl"),
        "percentRealizedPnl": r.get("percent_realized_pnl"),
        "curPrice": r.get("cur_price"),
        "avgPrice": r.get("avg_price"),
        "title": r.get("title"),
        "slug": r.get("slug"),
        "icon": r.get("icon"),
        "eventId": r.get("event_id"),
        "eventSlug": r.get("event_slug"),
        "outcome": r.get("outcome"),
        "outcomeIndex": r.get("outcome_index"),
        "oppositeOutcome": r.get("opposite_outcome"),
        "oppositeAsset": r.get("opposite_asset"),
        "endDate": r.get("end_date"),
        "timestamp": r.get("timestamp_sec"),
        "mergeable": r.get("mergeable"),
        "redeemable": r.get("redeemable"),
        "negativeRisk": r.get("negative_risk"),
    }


def _pg_closed_row_to_api(r: dict) -> dict:
    return {
        "proxyWallet": r.get("proxy_wallet"),
        "asset": r.get("asset"),
        "conditionId": r.get("condition_id"),
        "avgPrice": r.get("avg_price"),
        "totalBought": r.get("total_bought"),
        "realizedPnl": r.get("realized_pnl"),
        "curPrice": r.get("cur_price"),
        "title": r.get("title"),
        "slug": r.get("slug"),
        "icon": r.get("icon"),
        "eventSlug": r.get("event_slug"),
        "outcome": r.get("outcome"),
        "outcomeIndex": r.get("outcome_index"),
        "oppositeOutcome": r.get("opposite_outcome"),
        "oppositeAsset": r.get("opposite_asset"),
        "endDate": r.get("end_date"),
        "timestamp": r.get("timestamp_sec"),
    }


def _pg_trade_row_to_api(r: dict) -> dict:
    return {
        "conditionId": r.get("condition_id"),
        "outcomeIndex": r.get("outcome_index"),
        "side": r.get("side"),
        "size": r.get("size"),
        "price": r.get("price"),
        "timestamp": r.get("timestamp_sec"),
        "title": r.get("title"),
        "slug": r.get("slug"),
        "eventSlug": r.get("event_slug"),
        "asset": r.get("asset"),
        "proxyWallet": r.get("proxy_wallet"),
        "icon": r.get("icon"),
    }


def load_wallet_dict_from_path(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return data
    return {"positions": position_rows_from_json(data), "closedPositions": [], "trades": []}


def load_wallet_dict_from_postgres(user: str) -> dict:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as e:
        raise SystemExit("Install psycopg: pip install 'psycopg[binary]'") from e
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set (required for --user).")
    u = user.strip().lower()
    if not u.startswith("0x"):
        raise SystemExit("Address must start with 0x")
    with psycopg.connect(dsn) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """SELECT since_timestamp_sec, last_month_days, since_iso, window_time_source_note
                   FROM "walletList" WHERE user_address = %s""",
                (u,),
            )
            wrow = cur.fetchone()
            if wrow is None:
                raise SystemExit(
                    f'No row in "walletList" for {u} (run npx tsx getPositions.ts with DATABASE_URL).',
                )
            q_pos = """
                SELECT proxy_wallet, asset, condition_id, initial_value, current_value, cash_pnl, percent_pnl, total_bought, realized_pnl, percent_realized_pnl, cur_price, avg_price, title, slug, icon, event_id, event_slug, outcome, outcome_index, opposite_outcome, opposite_asset, end_date, timestamp_sec, mergeable, redeemable, negative_risk
                FROM positions WHERE user_address = %s
            """
            cur.execute(q_pos, (u,))
            pos = [_pg_pos_row_to_api(dict(r)) for r in cur.fetchall()]
            q_cl = """
                SELECT proxy_wallet, asset, condition_id, avg_price, total_bought, realized_pnl, cur_price, title, slug, icon, event_slug, outcome, outcome_index, opposite_outcome, opposite_asset, end_date, timestamp_sec
                FROM "closedPositions" WHERE user_address = %s
            """
            cur.execute(q_cl, (u,))
            closed = [_pg_closed_row_to_api(dict(r)) for r in cur.fetchall()]
            q_tr = """
                SELECT condition_id, outcome_index, side, size, price, timestamp_sec, title, slug, event_slug, asset, proxy_wallet, icon
                FROM trades WHERE user_address = %s
            """
            cur.execute(q_tr, (u,))
            tr = [_pg_trade_row_to_api(dict(r)) for r in cur.fetchall()]

    out: dict = {
        "user": u,
        "positions": pos,
        "closedPositions": closed,
        "trades": tr,
    }
    if (
        wrow.get("since_timestamp_sec") is not None
        or wrow.get("last_month_days") is not None
        or wrow.get("since_iso") is not None
        or wrow.get("window_time_source_note") is not None
    ):
        meta: dict = {}
        if wrow.get("since_timestamp_sec") is not None:
            meta["sinceTimestampSec"] = int(wrow["since_timestamp_sec"])
        if wrow.get("last_month_days") is not None:
            meta["lastMonthDays"] = int(wrow["last_month_days"])
        if wrow.get("since_iso") is not None:
            meta["sinceIso"] = wrow["since_iso"]
        if wrow.get("window_time_source_note") is not None:
            meta["windowTimeSourceNote"] = wrow["window_time_source_note"]
        out["meta"] = meta
    return out


def load_counts_and_pnl_by_day_slot_from_data(
    data: dict,
    scope: str,
) -> tuple[dict[tuple[str, int], int], dict[tuple[str, int], float]]:
    """scope: 'all' | 'btc' | '5m'. Positions PnL + residual trades (see module doc)."""
    if isinstance(data, dict):
        pos_rows, trade_ts_by_cid, trades_list = wallet_rows_and_trade_ts(data)
        covered = position_cover_keys(pos_rows)
    else:
        pos_rows = position_rows_from_json(data)
        trade_ts_by_cid = {}
        trades_list = []
        covered = position_cover_keys(pos_rows)

    counts: dict[tuple[str, int], int] = defaultdict(int)
    pnl_sum: dict[tuple[str, int], float] = defaultdict(float)

    for row in pos_rows:
        if not isinstance(row, dict):
            continue
        ts = resolve_event_unix(row, scope, trade_ts_by_cid)
        if ts is None:
            continue
        _add_cell(counts, pnl_sum, ts, position_pnl(row), 1)

    if trades_list:
        for tr in trades_list:
            if not isinstance(tr, dict):
                continue
            mk = market_key(tr)
            if mk is None or mk in covered:
                continue
            if not trade_matches_scope(tr, scope):
                continue
            ts = resolve_event_unix(tr, scope, trade_ts_by_cid)
            if ts is None:
                continue
            flow = trade_fill_cash_flow_usd(tr)
            if flow is None:
                continue
            _add_cell(counts, pnl_sum, ts, flow, 1)

    return counts, pnl_sum


def load_counts_and_pnl_by_day_slot(
    path: Path,
    scope: str,
) -> tuple[dict[tuple[str, int], int], dict[tuple[str, int], float]]:
    return load_counts_and_pnl_by_day_slot_from_data(load_wallet_dict_from_path(path), scope)


def day_key_in_rolling_window(day_key: str, since_days: int) -> bool:
    """
    True if the ET calendar day YYYY-MM-DD overlaps (now - since_days×24h, +inf).
    Matches a rolling "last N days" window ending at the current instant.
    """
    if since_days <= 0:
        return True
    now_et = datetime.now(ET)
    since_ts = int(now_et.timestamp()) - since_days * SECONDS_PER_DAY
    try:
        day_start = datetime.strptime(day_key, "%Y-%m-%d").replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
            tzinfo=ET,
        )
    except ValueError:
        return False
    day_end = day_start + timedelta(days=1)
    return int(day_end.timestamp()) > since_ts


def filter_by_rolling_window(
    counts: dict[tuple[str, int], int],
    pnl_by_cell: dict[tuple[str, int], float],
    since_days: int,
) -> tuple[dict[tuple[str, int], int], dict[tuple[str, int], float]]:
    """Keep only cells whose ET day_key is inside the rolling window."""
    c2 = {k: v for k, v in counts.items() if day_key_in_rolling_window(k[0], since_days)}
    p2 = {k: v for k, v in pnl_by_cell.items() if day_key_in_rolling_window(k[0], since_days)}
    return c2, p2


def format_time_ampm_et(dt: datetime) -> str:
    h12 = dt.hour % 12
    if h12 == 0:
        h12 = 12
    ampm = "AM" if dt.hour < 12 else "PM"
    return f"{h12}:{dt.minute:02d}:{dt.second:02d} {ampm}"


def extent_for_days(n_days: int) -> list[float]:
    return [0.0, 24.0, -0.5, n_days - 0.5]


def day_axis_labels(days: list[str]) -> list[str]:
    labels = []
    for d in days:
        dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=ET)
        labels.append(f"{dt.strftime('%b')} {dt.day}, {dt.year}")
    return labels


def decorate_xt_time(ax: Axes) -> None:
    ax.set_xticks([0, 6, 12, 18, 24])
    ax.set_xticklabels(["12am", "6am", "12pm", "6pm", "12am+1d"])
    ax.set_xlim(0, 24)


def decorate_y_days(ax: Axes, n_days: int, labels: list[str]) -> None:
    ax.set_yticks(list(range(n_days)))
    ax.set_yticklabels(labels, fontsize=8)


def now_x_et() -> tuple[datetime, float]:
    now_et = datetime.now(ET)
    now_x = (
        now_et.hour
        + now_et.minute / 60.0
        + now_et.second / 3600.0
        + now_et.microsecond / 3_600_000_000.0
    )
    return now_et, now_x


def add_now_line(ax: Axes, now_x: float) -> None:
    ax.axvline(
        now_x,
        color="red",
        linewidth=1.5,
        linestyle="-",
        zorder=10,
    )


def row_figure_height(n_days: int) -> float:
    return max(3.0, min(11.0, 0.32 * n_days + 2))


def save_figure(fig: Figure, out_path: Path, show: bool) -> None:
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Wrote {out_path.resolve()}")
    if show:
        plt.show()
    else:
        plt.close(fig)


def main() -> None:
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output PNG path (default: graphs/<stem>_… per command)",
    )
    parent.add_argument(
        "--show",
        action="store_true",
        help="Open an interactive window after saving (blocks until closed)",
    )
    scope_grp = parent.add_mutually_exclusive_group()
    scope_grp.add_argument(
        "--5m",
        dest="btc_updown_5m_only",
        action="store_true",
        help="Only btc-updown-5m-* markets",
    )
    scope_grp.add_argument(
        "--all",
        dest="all_markets",
        action="store_true",
        help="Every market (time from timestamp, btc-updown slug unix, or endDate)",
    )
    win_grp = parent.add_argument_group("time window (optional, all subcommands)")
    win_grp.add_argument(
        "--last-month",
        action="store_true",
        help="Only include data whose ET day overlaps the last 30*24h ending now (same as --since-days=30)",
    )
    win_grp.add_argument(
        "--since-days",
        type=int,
        default=None,
        metavar="N",
        help="Only include data whose ET calendar day overlaps the last N*24h ending now (overrides --last-month)",
    )

    parser = argparse.ArgumentParser(
        description="Polymarket position charts (ET): BTC default, --5m, or --all",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    for cmd, helpt in (
        ("count", "Trade-count heatmap only"),
        ("pnl", "Net PnL heatmap only"),
        ("daily", "Daily net PnL: X = day (ET), Y = profit ($)"),
    ):
        sp = sub.add_parser(cmd, parents=[parent], help=helpt)
        sp.add_argument(
            "json_path",
            type=Path,
            nargs="?",
            default=None,
            help="Wallet JSON file (omit if using --user)",
        )
        sp.add_argument(
            "--user",
            type=str,
            default=None,
            metavar="ADDR",
            help="0x... load walletList + positions + closedPositions + trades (DATABASE_URL)",
        )

    args = parser.parse_args()
    user_arg = args.user.strip() if args.user else None
    if (args.json_path is None) == (user_arg is None):
        print("Provide exactly one of: json_path (positional) or --user", file=sys.stderr)
        sys.exit(1)
    json_path: Path | None = args.json_path
    if json_path is not None and not json_path.is_file():
        print(f"File not found: {json_path}", file=sys.stderr)
        sys.exit(1)
    if user_arg is not None and not user_arg.lower().startswith("0x"):
        print("--user must be a 0x-prefixed address", file=sys.stderr)
        sys.exit(1)
    db_user: str | None = user_arg.lower() if user_arg is not None else None

    if args.since_days is not None:
        n_window: int | None = args.since_days
    elif args.last_month:
        n_window = 30
    else:
        n_window = None
    if n_window is not None and n_window <= 0:
        print("--since-days N requires N > 0", file=sys.stderr)
        sys.exit(1)

    if getattr(args, "all_markets", False):
        scope = "all"
    elif getattr(args, "btc_updown_5m_only", False):
        scope = "5m"
    else:
        scope = "btc"

    if db_user is not None:
        data = load_wallet_dict_from_postgres(db_user)
        counts, pnl_by_cell = load_counts_and_pnl_by_day_slot_from_data(data, scope)
    else:
        assert json_path is not None
        counts, pnl_by_cell = load_counts_and_pnl_by_day_slot(json_path, scope)
    if n_window is not None:
        counts, pnl_by_cell = filter_by_rolling_window(counts, pnl_by_cell, n_window)
    if not counts:
        hints = {
            "all": "matching rows (timestamp / slug time / endDate); with trades JSON, fills need time",
            "btc": "BTC-related rows (slug time or timestamp); trades include both slug styles",
            "5m": "btc-updown-5m rows with parseable 5m slug unix",
        }
        print(f"No rows matched {hints[scope]}.", file=sys.stderr)
        sys.exit(1)

    stem = db_user if db_user is not None else (json_path.stem if json_path else "wallet")
    tag = {"all": "all", "btc": "btc", "5m": "5m"}[scope]
    mode_title = {
        "all": "All markets",
        "btc": "All BTC-tagged",
        "5m": "BTC up/down 5m",
    }[scope]
    window_suffix = f"_last{n_window}d" if n_window is not None else ""
    window_title = (
        f" (rolling last {n_window}d to now, ET days)"
        if n_window is not None
        else ""
    )

    if args.command == "daily":
        daily_totals: dict[str, float] = defaultdict(float)
        for (day_key, _), v in pnl_by_cell.items():
            daily_totals[day_key] += v
        days_sorted = sorted(daily_totals.keys())
        n = len(days_sorted)
        profits = [daily_totals[d] for d in days_sorted]
        x_labels = day_axis_labels(days_sorted)

        out_path = args.output or default_graph_png_path(
            f"{stem}_{tag}{window_suffix}_daily_profit.png"
        )
        fig_w = max(10.0, min(28.0, 0.45 * n + 4))
        fig, ax = plt.subplots(figsize=(fig_w, 6))
        x = np.arange(n)
        bar_colors = ["#2ca02c" if p >= 0 else "#d62728" for p in profits]
        bars = ax.bar(x, profits, color=bar_colors, width=0.72, edgecolor="none")
        ax.axhline(0.0, color="black", linewidth=0.6, zorder=1)
        ax.bar_label(
            bars,
            labels=[f"${v:,.2f}" for v in profits],
            fontsize=7,
            padding=3,
            color="black",
        )
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8)
        ax.set_xlabel("Day (ET)")
        ax.set_ylabel("Profit ($)")
        ax.set_title(f"{mode_title} — daily sum per calendar day (ET){window_title}")
        ax.margins(y=0.12)
        save_figure(fig, out_path, args.show)
        return

    days = sorted({d for (d, _) in counts.keys()})
    n_days = len(days)
    day_index = {d: i for i, d in enumerate(days)}

    matrix_count = np.zeros((n_days, SLOTS_PER_DAY), dtype=float)
    matrix_pnl = np.zeros((n_days, SLOTS_PER_DAY), dtype=float)
    for (day_key, slot), c in counts.items():
        i = day_index[day_key]
        matrix_count[i, slot] = c
        matrix_pnl[i, slot] = pnl_by_cell[(day_key, slot)]

    y_labels = day_axis_labels(days)
    extent = extent_for_days(n_days)
    now_et, now_x = now_x_et()
    pnl_norm = TwoSlopeNorm(
        vmin=-PNL_COLOR_CLAMP_USD,
        vcenter=0.0,
        vmax=PNL_COLOR_CLAMP_USD,
    )

    if args.command == "count":
        out_path = args.output or default_graph_png_path(
            f"{stem}_{tag}{window_suffix}_count.png"
        )
        row_h = row_figure_height(n_days)
        fig, ax = plt.subplots(figsize=(14, row_h + 0.5))
        im = ax.imshow(
            matrix_count,
            aspect="auto",
            cmap="YlOrRd",
            interpolation="nearest",
            origin="lower",
            extent=extent,
        )
        ax.set_ylabel("Day (ET, calendar date)")
        ax.set_xlabel("Time of day (ET, midnight → end of day)")
        ax.set_title(
            f"{mode_title} — trade count per day × 5-minute slot (ET){window_title}"
        )
        decorate_xt_time(ax)
        decorate_y_days(ax, n_days, y_labels)
        add_now_line(ax, now_x)
        ax.legend(
            handles=[
                plt.Line2D(
                    [0],
                    [0],
                    color="red",
                    linewidth=1.5,
                    label=f"Now ET ({format_time_ampm_et(now_et)})",
                ),
            ],
            loc="upper right",
            fontsize=8,
            framealpha=0.9,
        )
        fig.colorbar(im, ax=ax, fraction=0.02, pad=0.02).set_label("Trades (count)")
        save_figure(fig, out_path, args.show)

    elif args.command == "pnl":
        out_path = args.output or default_graph_png_path(
            f"{stem}_{tag}{window_suffix}_pnl.png"
        )
        row_h = row_figure_height(n_days)
        fig, ax = plt.subplots(figsize=(14, row_h + 0.5))
        im = ax.imshow(
            matrix_pnl,
            aspect="auto",
            cmap="RdYlGn",
            interpolation="nearest",
            origin="lower",
            extent=extent,
            norm=pnl_norm,
        )
        ax.set_ylabel("Day (ET, calendar date)")
        ax.set_xlabel("Time of day (ET, midnight → end of day)")
        ax.set_title(
            f"{mode_title} — net PnL per cell (green = profit, red = loss; "
            f"≥${PNL_COLOR_CLAMP_USD:.0f} / ≤−${PNL_COLOR_CLAMP_USD:.0f} same hue){window_title}"
        )
        decorate_xt_time(ax)
        decorate_y_days(ax, n_days, y_labels)
        add_now_line(ax, now_x)
        ax.legend(
            handles=[
                plt.Line2D(
                    [0],
                    [0],
                    color="red",
                    linewidth=1.5,
                    label=f"Now ET ({format_time_ampm_et(now_et)})",
                ),
            ],
            loc="upper right",
            fontsize=8,
            framealpha=0.9,
        )
        fig.colorbar(im, ax=ax, fraction=0.02, pad=0.02).set_label(
            f"Net PnL (sum, $; saturates at ±{PNL_COLOR_CLAMP_USD:.0f})"
        )
        save_figure(fig, out_path, args.show)


if __name__ == "__main__":
    main()
