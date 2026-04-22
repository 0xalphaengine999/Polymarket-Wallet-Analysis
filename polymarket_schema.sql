-- No JSONB on positions / "closedPositions" / trades: columns match the Polymarket Data API object
-- field names in snake_case (proxy_wallet ↔ proxyWallet, etc.). walletList stores sync metadata
-- in plain columns, not JSONB.

CREATE TABLE IF NOT EXISTS "walletList" (
  user_address TEXT PRIMARY KEY,
  since_timestamp_sec BIGINT,
  last_month_days INT,
  since_iso TEXT,
  window_time_source_note TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- GET /positions: one row per open position
CREATE TABLE IF NOT EXISTS positions (
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
);
CREATE INDEX IF NOT EXISTS positions_user_address_idx ON positions (user_address);
CREATE INDEX IF NOT EXISTS positions_condition_id_idx ON positions (condition_id);

-- GET /closed-positions: one row per object (e.g. proxyWallet, asset, conditionId, avgPrice, …)
CREATE TABLE IF NOT EXISTS "closedPositions" (
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
);
CREATE INDEX IF NOT EXISTS "closedPositions_user_address_idx" ON "closedPositions" (user_address);
CREATE INDEX IF NOT EXISTS "closedPositions_condition_id_idx" ON "closedPositions" (condition_id);

-- Same row shape as "closedPositions", but realized_pnl = total_bought (shares) * (cur_price - avg_price).
-- Kept in sync in the app whenever "closedPositions" is replaced for a wallet.
CREATE TABLE IF NOT EXISTS "polishedClosedPositions" (
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
);
CREATE INDEX IF NOT EXISTS "polishedClosedPositions_user_address_idx" ON "polishedClosedPositions" (user_address);
CREATE INDEX IF NOT EXISTS "polishedClosedPositions_condition_id_idx" ON "polishedClosedPositions" (condition_id);

-- GET /trades: one row per fill
CREATE TABLE IF NOT EXISTS trades (
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
);
CREATE INDEX IF NOT EXISTS trades_user_address_idx ON trades (user_address);
CREATE INDEX IF NOT EXISTS trades_condition_id_idx ON trades (condition_id);
CREATE INDEX IF NOT EXISTS "walletList_updated_at_idx" ON "walletList" (updated_at DESC);
