/* global fetch */

const $ = (s, p = document) => p.querySelector(s);

const logEl = /** @type {HTMLPreElement} */ ($("#logPanel"));
const logState = /** @type {HTMLSpanElement} */ ($("#logState"));
const walletListEl = /** @type {HTMLDivElement} */ ($("#walletList"));
const emptyEl = /** @type {HTMLDivElement} */ ($("#empty"));
const healthEl = /** @type {HTMLDivElement} */ ($("#health"));

function appendLog(text) {
  logEl.textContent += text;
  logEl.parentElement && logEl.scrollTo(0, logEl.scrollHeight);
}
function setLogState(text, run) {
  if (!logState) return;
  logState.textContent = text;
  logState.classList.toggle("run", Boolean(run));
}
function logLine(s) {
  const t = new Date().toISOString().slice(11, 19);
  appendLog(`[${t}] ${s}\n`);
}

async function health() {
  try {
    const r = await fetch("/api/health");
    const d = await r.json();
    if (healthEl) {
      healthEl.hidden = false;
      healthEl.className = "health" + (d.hasDatabase ? " ok" : "");
      healthEl.textContent = d.hasDatabase ? "● DB" : "○ no DATABASE_URL";
    }
  } catch {
    if (healthEl) {
      healthEl.hidden = false;
      healthEl.textContent = "—";
    }
  }
}

async function loadWallets() {
  const r = await fetch("/api/wallets");
  if (!r.ok) {
    const t = await r.text();
    throw new Error(t || "Failed to list wallets");
  }
  return await r.json();
}

/**
 * @param {string} address
 * @param {HTMLButtonElement} btn
 */
/**
 * @param {string} address
 * @param {HTMLButtonElement} [btn]
 */
async function doDeleteWallet(address, btn) {
  const ok = window.confirm(
    "Remove this wallet and delete all stored data for it?\n\n" +
      "This deletes:\n" +
      "• walletList row\n" +
      "• positions, closedPositions, polishedClosedPositions, trades (CASCADE)\n\n" +
      `${address}\n\n` +
      "This cannot be undone."
  );
  if (!ok) return;
  if (btn) btn.disabled = true;
  setLogState("Deleting…", true);
  logLine(
    `Delete ${address} from database (CASCADE: positions, closedPositions, polishedClosedPositions, trades)`
  );
  try {
    const res = await fetch(
      "/api/wallets?address=" + encodeURIComponent(address),
      { method: "DELETE" }
    );
    const d = await res.json().catch(() => ({}));
    if (!res.ok) {
      logLine(`Delete failed: ${d.error || res.statusText}`);
      setLogState("Error", false);
      if (btn) btn.disabled = false;
      return;
    }
    logLine(`Removed ${d.userAddress || address}`);
    setLogState("Ready", false);
    if (btn) btn.disabled = false;
    void renderWallets();
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    logLine(`Delete error: ${msg}`);
    setLogState("Error", false);
    if (btn) btn.disabled = false;
  }
}

/**
 * @param {string} address
 * @param {HTMLButtonElement} btn
 */
async function doRefresh(address, btn) {
  if (btn) btn.disabled = true;
  setLogState("Refreshing…", true);
  logLine(`Refresh ${address} (in-process sync, last 30d → PostgreSQL)`);
  const res = await fetch("/api/refresh", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ address }),
  });
  if (!res.ok || !res.body) {
    const t = await res.text();
    appendLog(t + "\n");
    setLogState("Error", false);
    if (btn) btn.disabled = false;
    return;
  }
  const dec = new TextDecoder();
  const reader = res.body.getReader();
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    appendLog(dec.decode(value, { stream: true }));
  }
  setLogState("Ready", false);
  if (btn) btn.disabled = false;
  void renderWallets();
}

function getGraphClosedFrom() {
  const r = document.querySelector('input[name="graphClosedFrom"]:checked');
  if (r && /** @type {HTMLInputElement} */ (r).value === "polished") {
    return "polished";
  }
  return "api";
}

/**
 * @param {string} address
 * @param {"daily"|"count"} kind
 * @param {"api"|"polished"} closedFrom
 */
async function doGraph(address, kind, closedFrom) {
  setLogState("analyze.py…", true);
  const src =
    closedFrom === "polished" ? "polishedClosedPositions (recomputed PnL)" : "closedPositions (Data API PnL)";
  logLine(
    `Graph ${kind} for ${address} (closed: ${src}; in-process, all markets, rolling 30d ET — like analyze.py --all --last-month)`
  );
  const res = await fetch("/api/graph", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ address, kind, closedFrom }),
  });
  const data = await res.json();
  if (!res.ok) {
    appendLog((data.log || JSON.stringify(data)) + "\n");
    setLogState("Error", false);
    return;
  }
  appendLog((data.log || "") + "\n");
  setLogState("Ready", false);
  const m = /** @type {HTMLDivElement} */ ($("#resultModal"));
  const im = /** @type {HTMLImageElement} */ ($("#resultImage"));
  const w = /** @type {HTMLDivElement} */ ($("#resultImageWrap"));
  const lg = /** @type {HTMLPreElement} */ ($("#resultLog"));
  const a = /** @type {HTMLAnchorElement} */ ($("#resultOpen"));
  if (data.ok && data.imageUrl) {
    if (im) im.src = data.imageUrl;
    if (a) {
      a.href = data.imageUrl;
      a.hidden = false;
    }
    if (w) w.hidden = false;
  } else {
    if (a) a.hidden = true;
    if (w) w.hidden = true;
  }
  if (lg) lg.textContent = data.log || (data.error ? String(data.error) : "");
  if (m) m.hidden = false;
}

function closeResultModal() {
  const m = /** @type {HTMLDivElement} */ ($("#resultModal"));
  if (m) m.hidden = true;
  const w = /** @type {HTMLDivElement} */ ($("#resultImageWrap"));
  if (w) w.hidden = true;
}

let graphAddr = null;

function openGraphModal(address) {
  graphAddr = address;
  const m = /** @type {HTMLDivElement} */ ($("#graphModal"));
  const forLine = /** @type {HTMLParagraphElement} */ ($("#graphModalFor"));
  if (forLine) forLine.textContent = address;
  if (m) m.hidden = false;
}
function closeGraphModal() {
  const m = /** @type {HTMLDivElement} */ ($("#graphModal"));
  if (m) m.hidden = true;
  graphAddr = null;
}

/**
 * @param {Array<{ userAddress: string; updatedAt: string | null }>} rows
 */
function displayRows(rows) {
  walletListEl.innerHTML = "";
  emptyEl.hidden = rows.length > 0;
  for (const w of rows) {
    const row = document.createElement("div");
    row.className = "row";
    const addr = document.createElement("span");
    addr.className = "addr";
    addr.textContent = w.userAddress;
    const up = document.createElement("span");
    up.className = "updated";
    up.textContent = w.updatedAt
      ? new Date(w.updatedAt).toLocaleString()
      : "— (no sync yet)";
    const act = document.createElement("div");
    act.className = "col-actions";
    const br = document.createElement("button");
    br.className = "btn btn-ghost";
    br.type = "button";
    br.textContent = "Refresh";
    br.addEventListener("click", () => {
      void doRefresh(w.userAddress, br);
    });
    const go = document.createElement("button");
    go.className = "btn btn-primary";
    go.type = "button";
    go.textContent = "Launch";
    go.addEventListener("click", () => openGraphModal(w.userAddress));
    const del = document.createElement("button");
    del.className = "btn btn-ghost btn-sm btn-danger";
    del.type = "button";
    del.title = "Remove wallet and all its positions, closed, polished, and trades in the DB";
    del.textContent = "Delete";
    del.addEventListener("click", () => {
      void doDeleteWallet(w.userAddress, del);
    });
    act.append(br, go, del);
    row.append(addr, up, act);
    walletListEl.appendChild(row);
  }
}

async function renderWallets() {
  try {
    const rows = await loadWallets();
    displayRows(rows);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    logLine(`List error: ${msg}`);
  }
}

function setup() {
  health();
  void renderWallets();
  setInterval(health, 20000);

  /** @type {HTMLButtonElement | null} */ ($("#reloadWallets"))?.addEventListener("click", () => {
    void renderWallets();
  });
  /** @type {HTMLButtonElement | null} */ ($("#clearLog"))?.addEventListener("click", () => {
    logEl.textContent = "";
  });
  /** @type {HTMLButtonElement | null} */ ($("#addBtn"))?.addEventListener("click", async () => {
    const inp = /** @type {HTMLInputElement} */ ($("#newWallet"));
    if (!inp) return;
    const a = inp.value.trim();
    if (!/^0x[0-9a-fA-F]{40}$/.test(a)) {
      logLine("Add wallet: need 0x + 40 hex");
      return;
    }
    const r = await fetch("/api/wallets", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ address: a }),
    });
    const d = await r.json();
    if (!r.ok) {
      logLine(`Add failed: ${d.error || r.statusText}`);
      return;
    }
    logLine(`Added: ${d.userAddress}`);
    inp.value = "";
    void renderWallets();
  });
  for (const b of [$("#choiceDaily"), $("#choiceCount")].filter(Boolean)) {
    b.addEventListener("click", () => {
      const k = b.getAttribute("data-kind");
      if (!k || (k !== "daily" && k !== "count") || !graphAddr) return;
      const addr = graphAddr;
      const closedFrom = getGraphClosedFrom();
      closeGraphModal();
      void doGraph(addr, k, closedFrom);
    });
  }
  /** @type {HTMLButtonElement | null} */ ($("#graphModalCancel"))?.addEventListener("click", () => {
    closeGraphModal();
  });
  /** @type {HTMLButtonElement | null} */ ($("#resultClose"))?.addEventListener("click", () => {
    closeResultModal();
  });
  /** @type {HTMLDivElement | null} */ ($("#graphModal"))?.addEventListener("click", (ev) => {
    if (ev.target && /** @type {Element} */ (ev.target).id === "graphModal") closeGraphModal();
  });
  /** @type {HTMLDivElement | null} */ ($("#resultModal"))?.addEventListener("click", (ev) => {
    if (ev.target && /** @type {Element} */ (ev.target).id === "resultModal") closeResultModal();
  });
}

document.addEventListener("DOMContentLoaded", setup);
