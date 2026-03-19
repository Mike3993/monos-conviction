/* ── MONOS Dashboard — frontend logic ─────────────────────────── */

let lastExample = null;
let lastMulti   = null;
let lastRaw     = null;   // full JSON for export

/* ── helpers ──────────────────────────────────────────────────── */

function $(id) { return document.getElementById(id); }
function show(id) { $(id).classList.remove("hidden"); }
function hide(id) { $(id).classList.add("hidden"); }

function getTicker() {
  return ($("ticker").value || "SPY").toUpperCase().trim();
}

function setStatus(msg, ok) {
  const el = $("status");
  el.textContent = msg;
  el.className = "status " + (ok ? "ok" : "error");
  show("status");
}
function clearStatus() { hide("status"); }

function setLoading(on) {
  const sp = $("spinner");
  if (on) sp.classList.remove("hidden");
  else    sp.classList.add("hidden");
  document.querySelectorAll(".input-bar button").forEach(b => b.disabled = on);
}

function fmt(v, d) {
  if (v == null) return "—";
  return typeof v === "number" ? v.toFixed(d != null ? d : 2) : String(v);
}

function pctClass(v) {
  if (v == null) return "";
  return v > 0 ? "pos" : v < 0 ? "neg" : "neu";
}

async function post(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return res.json();
}

/* ── summary cards ───────────────────────────────────────────── */

function renderSummary(d) {
  const cards = [
    ["Ticker",         d.ticker],
    ["Mode",           d.mode],
    ["Hold Days",      d.hold_days],
    ["Total Trades",   d.total_trades],
    ["Skipped",        d.skipped_trades],
    ["Win Rate",       fmt(d.win_rate, 1) + "%"],
    ["Total Return",   fmt(d.total_return, 4) + "%"],
    ["Wgt Return",     fmt(d.weighted_total_return, 4) + "%"],
    ["MSA Wgt Return", fmt(d.msa_weighted_total_return, 4) + "%"],
    ["MR Trades",      d.mean_reversion_trades],
    ["HC Trades",      d.high_conviction_trades],
    ["HC Win Rate",    fmt(d.high_conviction_win_rate, 1) + "%"],
  ];
  let html = "";
  cards.forEach(([label, value]) => {
    let valStr = String(value);
    let cls = "";
    if (valStr.includes("%")) {
      const n = parseFloat(valStr);
      if (!isNaN(n)) cls = pctClass(n);
    }
    html += `<div class="card"><div class="label">${label}</div><div class="value ${cls}">${valStr}</div></div>`;
  });
  $("summary-cards").innerHTML = html;
  show("summary-section");
}

/* ── trades table ────────────────────────────────────────────── */

function renderTrades(trades) {
  if (!trades || !trades.length) { hide("trades-section"); return; }
  let html = "";
  trades.forEach(t => {
    const modeClass = t.trade_mode === "MEAN_REVERSION" ? "mode-mr" : "";
    html += `<tr>
      <td>${t.entry_date}</td>
      <td>${t.exit_date}</td>
      <td>${t.signal}</td>
      <td class="${modeClass}">${t.trade_mode}</td>
      <td>${t.structure}</td>
      <td>${fmt(t.confidence, 1)}</td>
      <td>${t.msa_state}</td>
      <td class="${pctClass(t.underlying_return_pct)}">${fmt(t.underlying_return_pct, 2)}%</td>
      <td class="${pctClass(t.option_return_pct)}">${fmt(t.option_return_pct, 2)}%</td>
      <td>${t.exit_reason}</td>
      <td>${t.hold_days}</td>
      <td class="${t.win ? 'win-y' : 'win-n'}">${t.win ? 'Y' : 'N'}</td>
    </tr>`;
  });
  $("trades-body").innerHTML = html;
  show("trades-section");
}

/* ── confidence buckets ──────────────────────────────────────── */

function renderConfidence(buckets) {
  if (!buckets || !buckets.length) { hide("conf-section"); return; }
  let html = "";
  buckets.forEach(b => {
    html += `<tr>
      <td>${b.bucket}</td>
      <td>${b.trades}</td>
      <td>${fmt(b.win_rate, 1)}%</td>
      <td class="${pctClass(b.avg_return)}">${fmt(b.avg_return, 4)}%</td>
    </tr>`;
  });
  $("conf-body").innerHTML = html;
  show("conf-section");
}

/* ── multi-hold table ────────────────────────────────────────── */

function renderMultiHold(data) {
  if (!data || !Object.keys(data).length) { hide("multi-section"); return; }
  const keys = Object.keys(data).sort((a, b) => {
    return parseInt(a) - parseInt(b);
  });
  let html = "";
  keys.forEach(k => {
    const r = data[k];
    html += `<tr>
      <td>${k}</td>
      <td>${r.total_trades}</td>
      <td>${fmt(r.win_rate, 1)}%</td>
      <td class="${pctClass(r.total_return)}">${fmt(r.total_return, 4)}%</td>
      <td class="${pctClass(r.weighted_total_return)}">${fmt(r.weighted_total_return, 4)}%</td>
      <td class="${pctClass(r.msa_weighted_total_return)}">${fmt(r.msa_weighted_total_return, 4)}%</td>
      <td class="${pctClass(r.avg_return)}">${fmt(r.avg_return, 4)}%</td>
    </tr>`;
  });
  $("multi-body").innerHTML = html;
  show("multi-section");
}

/* ── JSON viewer ─────────────────────────────────────────────── */

function renderJSON(obj) {
  lastRaw = obj;
  $("json-content").textContent = JSON.stringify(obj, null, 2);
  show("json-section");
}

function toggleJSON() {
  const wrap = $("json-wrap");
  const arrow = $("json-toggle");
  wrap.classList.toggle("hidden");
  arrow.classList.toggle("open");
}

function copyJSON() {
  if (!lastRaw) return;
  navigator.clipboard.writeText(JSON.stringify(lastRaw, null, 2));
  setStatus("JSON copied to clipboard", true);
}

/* ── copy summary ────────────────────────────────────────────── */

function copySummary() {
  if (!lastExample && !lastMulti) return;

  let lines = [];
  if (lastExample) {
    const d = lastExample;
    lines.push(`Ticker: ${d.ticker}`);
    lines.push(`Mode: ${d.mode}`);
    lines.push(`Hold Days: ${d.hold_days}`);
    lines.push(`Trades: ${d.total_trades}`);
    lines.push(`Win Rate: ${fmt(d.win_rate, 1)}%`);
    lines.push(`Total Return: ${fmt(d.total_return, 4)}%`);
    lines.push(`Weighted Return: ${fmt(d.weighted_total_return, 4)}%`);
    lines.push(`MSA Weighted: ${fmt(d.msa_weighted_total_return, 4)}%`);
    lines.push(`Mean Reversion Trades: ${d.mean_reversion_trades}`);
    lines.push(`MR Win Rate: ${fmt(d.mean_reversion_win_rate, 1)}%`);
    lines.push(`High Conviction Trades: ${d.high_conviction_trades}`);
    lines.push(`HC Win Rate: ${fmt(d.high_conviction_win_rate, 1)}%`);
    lines.push(`Skipped: ${d.skipped_trades} (Conf: ${d.confidence_filtered_trades}, MSA: ${d.msa_filtered_trades}, Ext: ${d.extension_filtered_trades}, Shock: ${d.shock_filtered_trades}, Trend: ${d.trend_filtered_trades}, Timing: ${d.timing_filtered_trades})`);
    lines.push(`Filters Bypassed: ${d.filters_skipped_by_mode || 0}`);
  }
  if (lastMulti) {
    lines.push("");
    lines.push("Best Multi-Hold:");
    const keys = Object.keys(lastMulti).sort((a,b) => parseInt(a) - parseInt(b));
    keys.forEach(k => {
      const r = lastMulti[k];
      lines.push(`  ${k} -> Win Rate ${fmt(r.win_rate,1)}%, Wgt ${fmt(r.weighted_total_return,4)}%, MSA Wgt ${fmt(r.msa_weighted_total_return,4)}%`);
    });
  }

  const text = lines.join("\n");
  navigator.clipboard.writeText(text);
  setStatus("Summary copied to clipboard", true);
}

/* ── download JSON ───────────────────────────────────────────── */

function downloadJSON() {
  if (!lastRaw) return;
  const blob = new Blob([JSON.stringify(lastRaw, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `monos_${getTicker()}_${Date.now()}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

/* ── API calls ───────────────────────────────────────────────── */

async function runExample() {
  clearStatus();
  setLoading(true);
  try {
    const res = await post("/api/run-example", { ticker: getTicker() });
    if (!res.ok) { setStatus("Error: " + res.error, false); return; }
    lastExample = res.data;
    renderSummary(res.data);
    renderTrades(res.data.trades);
    renderConfidence(res.data.confidence_analysis);
    hide("multi-section");
    renderJSON(res.data);
    setStatus(`Example backtest complete for ${res.data.ticker}`, true);
  } catch (e) {
    setStatus("Request failed: " + e.message, false);
  } finally {
    setLoading(false);
  }
}

async function runMultiHold() {
  clearStatus();
  setLoading(true);
  try {
    const res = await post("/api/run-multi-hold", { ticker: getTicker() });
    if (!res.ok) { setStatus("Error: " + res.error, false); return; }
    lastMulti = res.data;
    renderMultiHold(res.data);
    hide("summary-section");
    hide("trades-section");
    hide("conf-section");
    renderJSON(res.data);
    setStatus(`Multi-hold comparison complete for ${getTicker()}`, true);
  } catch (e) {
    setStatus("Request failed: " + e.message, false);
  } finally {
    setLoading(false);
  }
}

async function runBoth() {
  clearStatus();
  setLoading(true);
  try {
    const res = await post("/api/run-both", { ticker: getTicker() });
    if (!res.ok) { setStatus("Error: " + res.error, false); return; }
    lastExample = res.data.example;
    lastMulti   = res.data.multi_hold;
    renderSummary(res.data.example);
    renderTrades(res.data.example.trades);
    renderConfidence(res.data.example.confidence_analysis);
    renderMultiHold(res.data.multi_hold);
    renderJSON(res.data);
    setStatus(`Full backtest complete for ${res.data.example.ticker}`, true);
  } catch (e) {
    setStatus("Request failed: " + e.message, false);
  } finally {
    setLoading(false);
  }
}

/* ── keyboard shortcut ───────────────────────────────────────── */
$("ticker").addEventListener("keydown", e => {
  if (e.key === "Enter") runBoth();
});

/* ================================================================
   BATCH RUNNER
   ================================================================ */

let lastBatch = null;

function setBatchLoading(on, msg) {
  const sp = $("batch-spinner");
  const btn = $("btn-batch");
  if (on) { sp.classList.remove("hidden"); btn.disabled = true; }
  else    { sp.classList.add("hidden");    btn.disabled = false; }
  $("batch-progress").textContent = msg || "";
}

function modeShort(mode) {
  const map = {
    "TACTICAL": "Tactical",
    "HYBRID": "Hybrid",
    "CONVEX": "Convex",
    "MEAN_REVERSION": "MeanRev",
  };
  return map[mode] || mode;
}

function rowColorClass(wgt) {
  if (wgt > 5) return "row-green";
  if (wgt >= 1) return "row-yellow";
  if (wgt < 0) return "row-red";
  return "";
}

function strengthBadge(s) {
  const cls = s === "HIGH" ? "badge-high" : s === "MEDIUM" ? "badge-medium" : "badge-low";
  return `<span class="badge ${cls}">${s}</span>`;
}

function renderBatch(results, errors) {
  let html = "";
  results.forEach(r => {
    const rc = rowColorClass(r.weighted_return);
    html += `<tr class="${rc}">
      <td><strong>${r.ticker}</strong></td>
      <td class="${r.mode === 'MEAN_REVERSION' ? 'mode-mr' : ''}">${modeShort(r.mode)}</td>
      <td>${r.best_hold}d</td>
      <td>${fmt(r.win_rate, 1)}%</td>
      <td class="${pctClass(r.weighted_return)}">${fmt(r.weighted_return, 2)}%</td>
      <td>${r.trades}</td>
      <td>${r.mr_trades || 0}</td>
      <td>${r.hc_trades || 0}</td>
      <td>${strengthBadge(r.strength)}</td>
    </tr>`;
  });
  $("batch-body").innerHTML = html;

  // Render errors
  let errHtml = "";
  if (errors && errors.length) {
    errHtml = errors.map(e => `<div>Failed: ${e.ticker} — ${e.error}</div>`).join("");
  }
  $("batch-errors").innerHTML = errHtml;

  show("batch-section");
}

function copyBatchResults() {
  if (!lastBatch || !lastBatch.length) return;
  let lines = ["=== MONOS BATCH RESULTS ==="];
  lastBatch.forEach(r => {
    const mode = modeShort(r.mode).padEnd(8);
    lines.push(`${r.ticker.padEnd(4)} | ${mode} | ${r.best_hold}d  | WR: ${fmt(r.win_rate, 0)}% | WGT: ${fmt(r.weighted_return, 2)}% | Str: ${r.strength}`);
  });
  lines.push("");
  lines.push("Generated by MONOS Conviction Engine");
  navigator.clipboard.writeText(lines.join("\n"));
  setStatus("Batch results copied to clipboard", true);
}

function downloadBatchJSON() {
  if (!lastBatch) return;
  const blob = new Blob([JSON.stringify(lastBatch, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `monos_batch_${Date.now()}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

/* ── top trades rendering ─────────────────────────────────────── */

let lastTopTrades = null;
let lastTopFormatted = null;

function renderTopTrades(trades) {
  if (!trades || !trades.length) { hide("top-trades-section"); return; }
  lastTopTrades = trades;
  let html = "";
  trades.forEach(t => {
    const dirClass = t.direction === "LONG" ? "pos" : "neg";
    const strClass = t.strength === "HIGH" ? "badge-high" : t.strength === "MEDIUM" ? "badge-medium" : "badge-low";
    html += `
    <div class="trade-card rank-${t.rank}">
      <div class="trade-card-header">
        <span class="trade-card-ticker">${t.ticker}</span>
        <span class="trade-card-rank">#${t.rank}</span>
      </div>
      <div class="trade-card-body">
        <div><span class="tc-label">Direction</span><div class="tc-value ${dirClass}">${t.direction}</div></div>
        <div><span class="tc-label">Mode</span><div class="tc-value">${t.mode}</div></div>
        <div><span class="tc-label">Structure</span><div class="tc-value">${t.structure}</div></div>
        <div><span class="tc-label">Hold</span><div class="tc-value">${t.hold}</div></div>
        <div><span class="tc-label">Sizing</span><div class="tc-value">${t.sizing}</div></div>
        <div><span class="tc-label">Win Rate</span><div class="tc-value">${fmt(t.win_rate, 1)}%</div></div>
        <div><span class="tc-label">Wgt Return</span><div class="tc-value ${pctClass(t.weighted_return)}">${fmt(t.weighted_return, 2)}%</div></div>
        <div><span class="tc-label">Strength</span><div class="tc-value"><span class="badge ${strClass}">${t.strength}</span></div></div>
      </div>
      <div class="trade-card-rationale">${t.rationale}</div>
    </div>`;
  });
  $("top-trades-cards").innerHTML = html;
  show("top-trades-section");
}

function copyTopTrades() {
  if (lastTopFormatted) {
    navigator.clipboard.writeText(lastTopFormatted);
    setStatus("Top trades copied to clipboard", true);
  }
}

async function runBatch() {
  clearStatus();
  const raw = $("batch-tickers").value || "SPY, QQQ, IWM, SMH, GLD, SLV";
  const tickers = raw.split(",").map(t => t.trim().toUpperCase()).filter(Boolean);

  setBatchLoading(true, `Running ${tickers.length} tickers...`);
  try {
    const res = await post("/api/run-batch", { tickers });
    if (!res.ok) {
      setStatus("Batch error: " + (res.error || "unknown"), false);
      return;
    }
    lastBatch = res.results;
    renderBatch(res.results, res.errors);
    // Render top trades if present
    if (res.top_trades) {
      lastTopFormatted = res.top_trades_formatted || null;
      renderTopTrades(res.top_trades);
    }
    setStatus(`Batch complete — ${res.results.length} tickers, ${(res.top_trades || []).length} top trades`, true);
  } catch (e) {
    setStatus("Batch request failed: " + e.message, false);
  } finally {
    setBatchLoading(false);
  }
}

/* ── batch keyboard shortcut ─────────────────────────────────── */
$("batch-tickers").addEventListener("keydown", e => {
  if (e.key === "Enter") runBatch();
});

/* ================================================================
   TAB NAVIGATION
   ================================================================ */

function switchTab(name) {
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  document.querySelectorAll(".tab-content").forEach(t => t.classList.remove("active"));
  document.querySelector(`.tab-btn[onclick="switchTab('${name}')"]`).classList.add("active");
  $("tab-" + name).classList.add("active");

  // When switching to execution tab, populate top trades if available
  if (name === "execution" && lastTopTrades) {
    renderExecTopPanel(lastTopTrades);
    refreshLedger();
  }
  // When switching to PnL tab, auto-load and start refresh
  if (name === "pnl") {
    refreshPnL();
  } else {
    // Stop auto-refresh when leaving PnL tab
    stopPnLAuto();
  }
  // When switching to dialogue tab, auto-load if trades available
  if (name === "dialogue" && lastTopTrades) {
    loadDialogue();
  }
  // When switching to EVA tab, auto-load data
  if (name === "eva") {
    loadEVA();
  }
}

/* ================================================================
   EXECUTION + LEDGER TAB
   ================================================================ */

let lastExecSpecs = null;

function setExecStatus(msg, ok) {
  const el = $("exec-status");
  el.textContent = msg;
  el.className = "status " + (ok ? "ok" : "error");
  el.classList.remove("hidden");
}

/* ── Section 1: Top Trades Panel (with Add to Ledger) ─────────── */

function renderExecTopPanel(trades) {
  if (!trades || !trades.length) {
    $("exec-top-cards").innerHTML = '<p class="placeholder-text">No top trades yet. Run a batch first.</p>';
    return;
  }
  let html = "";
  trades.forEach((t, idx) => {
    const dirClass = t.direction === "LONG" ? "pos" : "neg";
    const strClass = t.strength === "HIGH" ? "badge-high" : t.strength === "MEDIUM" ? "badge-medium" : "badge-low";
    const dataAttr = `data-idx="${idx}"`;
    html += `
    <div class="trade-card rank-${t.rank}">
      <div class="trade-card-header">
        <span class="trade-card-ticker">${t.ticker}</span>
        <span class="trade-card-rank">#${t.rank}</span>
      </div>
      <div class="trade-card-body">
        <div><span class="tc-label">Direction</span><div class="tc-value ${dirClass}">${t.direction}</div></div>
        <div><span class="tc-label">Mode</span><div class="tc-value">${t.mode}</div></div>
        <div><span class="tc-label">Structure</span><div class="tc-value">${t.structure}</div></div>
        <div><span class="tc-label">Hold</span><div class="tc-value">${t.hold}</div></div>
        <div><span class="tc-label">Sizing</span><div class="tc-value">${t.sizing}</div></div>
        <div><span class="tc-label">Win Rate</span><div class="tc-value">${fmt(t.win_rate, 1)}%</div></div>
        <div><span class="tc-label">Wgt Return</span><div class="tc-value ${pctClass(t.weighted_return)}">${fmt(t.weighted_return, 2)}%</div></div>
        <div><span class="tc-label">Strength</span><div class="tc-value"><span class="badge ${strClass}">${t.strength}</span></div></div>
      </div>
      <div class="trade-card-rationale">${t.rationale}</div>
      <div style="margin-top:12px">
        <button onclick="addToLedgerFromTop(${idx})" style="width:100%">Add to Ledger</button>
      </div>
    </div>`;
  });
  $("exec-top-cards").innerHTML = html;
}

async function addToLedgerFromTop(idx) {
  if (!lastTopTrades || !lastTopTrades[idx]) return;
  const t = lastTopTrades[idx];
  const holdMatch = (t.hold || "").match(/(\d+)/);
  const holdNum = holdMatch ? parseInt(holdMatch[1]) : 5;

  // Open modal immediately with basic data
  const prefill = {
    ticker: t.ticker,
    direction: t.direction,
    mode: t.mode,
    structure: t.structure,
    hold_days: holdNum,
    confidence: t.win_rate,
    expected_return: t.weighted_return,
  };
  openTradeModal(prefill);

  // Then fetch live chain in background
  show("m-pricing-loading");
  hide("m-pricing-banner");
  try {
    const res = await post("/api/options/prefill", {
      ticker: t.ticker,
      mode: t.mode,
      direction: t.direction,
      structure: t.structure,
    });
    if (res.ok && res.contract) {
      const c = res.contract;
      // Populate pricing banner
      $("m-contract-sym").textContent = c.contract_symbol || "";
      $("m-bid").textContent = "$" + fmt(c.bid, 2);
      $("m-ask").textContent = "$" + fmt(c.ask, 2);
      $("m-mid").textContent = "$" + fmt(c.mid, 2);
      $("m-spread").textContent = "$" + fmt(c.spread, 2);
      $("m-suggested").textContent = "$" + fmt(c.suggested_entry_price, 2);
      $("m-dte").textContent = (c.dte || "—") + "d";
      show("m-pricing-banner");

      // Prefill form fields from live data
      if (c.expiration) $("m-expiration").value = c.expiration;
      if (c.strike) $("m-strike").value = c.strike;
      if (c.suggested_entry_price) $("m-entry").value = c.suggested_entry_price;

      // Store in hidden fields
      $("m-contract-symbol").value = c.contract_symbol || "";
      $("m-quoted-bid").value = c.bid || "";
      $("m-quoted-ask").value = c.ask || "";
      $("m-quoted-mid").value = c.mid || "";
      $("m-suggested-price").value = c.suggested_entry_price || "";
      // Delta / moneyness / candidates for ledger commit
      window._prefillDelta = c.strike_delta || null;
      window._prefillMoneyness = c.moneyness_pct || null;
      window._prefillCandidates = c.strike_candidates || null;
    }
  } catch (e) {
    // Live data unavailable — user fills manually
  } finally {
    hide("m-pricing-loading");
  }
}

function copyExecTopTrades() {
  if (lastTopFormatted) {
    navigator.clipboard.writeText(lastTopFormatted);
    setExecStatus("Top trades copied", true);
  }
}

/* ── Section 2: Trade Generator / Specs ───────────────────────── */

async function generateExecSpecs() {
  if (!lastTopTrades || !lastTopTrades.length) {
    setExecStatus("No top trades available. Run a batch first.", false);
    return;
  }
  try {
    const res = await post("/api/generate-exec", { top_trades: lastTopTrades });
    if (!res.ok) { setExecStatus("Error: " + (res.error || "unknown"), false); return; }
    lastExecSpecs = res.specs;
    renderExecSpecs(res.specs);
    setExecStatus(`Generated ${res.specs.length} trade specs`, true);
  } catch (e) {
    setExecStatus("Request failed: " + e.message, false);
  }
}

function renderExecSpecs(specs) {
  if (!specs || !specs.length) { hide("exec-specs-section"); return; }
  let html = "";
  specs.forEach(s => {
    html += `
    <div class="spec-card">
      <div class="spec-card-ticker">${s.ticker}</div>
      <div class="spec-card-body">
        <div class="sc-row"><span class="sc-label">Action</span><span class="sc-val">${s.action}</span></div>
        <div class="sc-row"><span class="sc-label">DTE</span><span class="sc-val">${s.dte}</span></div>
        <div class="sc-row"><span class="sc-label">Strike</span><span class="sc-val">${s.strike}</span></div>
        <div class="sc-row"><span class="sc-label">Sizing</span><span class="sc-val">${s.sizing}</span></div>
        <div class="sc-row"><span class="sc-label">Hold</span><span class="sc-val">${s.hold}</span></div>
        <div class="sc-row"><span class="sc-label">Note</span><span class="sc-val">${s.expiry_note}</span></div>
        <div class="sc-row"><span class="sc-label">Win Rate</span><span class="sc-val">${fmt(s.win_rate, 1)}%</span></div>
        <div class="sc-row"><span class="sc-label">Strength</span><span class="sc-val"><span class="badge ${s.strength === 'HIGH' ? 'badge-high' : s.strength === 'MEDIUM' ? 'badge-medium' : 'badge-low'}">${s.strength}</span></span></div>
      </div>
    </div>`;
  });
  $("exec-specs-cards").innerHTML = html;
  show("exec-specs-section");
}

function copyExecSpecs() {
  if (!lastExecSpecs || !lastExecSpecs.length) return;
  let lines = ["=== MONOS TRADE SPECS ===", ""];
  lastExecSpecs.forEach(s => {
    lines.push(`${s.ticker}:`);
    lines.push(`  ${s.action}`);
    lines.push(`  ${s.dte}`);
    lines.push(`  Strike: ${s.strike}`);
    lines.push(`  Sizing: ${s.sizing}`);
    lines.push(`  Hold: ${s.hold}`);
    lines.push(`  ${s.expiry_note}`);
    lines.push("");
  });
  lines.push("Generated by MONOS Conviction Engine");
  navigator.clipboard.writeText(lines.join("\n"));
  setExecStatus("Trade specs copied", true);
}

/* ── Section 3: Trade Ledger — Modal-based ────────────────────── */

function todayStr() {
  return new Date().toISOString().split("T")[0];
}

function addDaysStr(days) {
  const d = new Date();
  d.setDate(d.getDate() + (days || 5));
  return d.toISOString().split("T")[0];
}

function openTradeModal(prefill) {
  const p = prefill || {};
  $("m-date").value        = p.date_open  || todayStr();
  $("m-ticker").value      = p.ticker     || "";
  $("m-direction").value   = p.direction  || "LONG";
  $("m-mode").value        = p.mode       || "TACTICAL";
  $("m-structure").value   = p.structure  || "LONG_CALL";
  $("m-expiration").value  = p.expiration || addDaysStr(p.hold_days || 14);
  $("m-strike").value      = p.strike     || "ATM";
  $("m-entry").value       = p.entry_price || "";
  $("m-contracts").value   = p.contracts  || 1;
  $("m-hold").value        = p.hold_days  || "";
  $("m-conf").value        = p.confidence != null ? p.confidence : "";
  $("m-expected").value    = p.expected_return != null ? p.expected_return : "";
  $("m-notes").value       = p.notes      || "";
  // Reset hidden pricing fields
  $("m-contract-symbol").value = "";
  $("m-quoted-bid").value = "";
  $("m-quoted-ask").value = "";
  $("m-quoted-mid").value = "";
  $("m-suggested-price").value = "";
  hide("m-pricing-banner");
  hide("m-pricing-loading");
  $("modal-title").textContent = p.ticker ? `Add ${p.ticker} to Ledger` : "Add Trade to Ledger";
  show("trade-modal-overlay");
}

function closeTradeModal(e) {
  if (e && e.target !== $("trade-modal-overlay")) return;
  hide("trade-modal-overlay");
}

async function commitTrade() {
  const body = {
    date_open:             $("m-date").value,
    ticker:                $("m-ticker").value,
    direction:             $("m-direction").value,
    mode:                  $("m-mode").value,
    structure:             $("m-structure").value,
    contract_symbol:       $("m-contract-symbol").value || "",
    expiration:            $("m-expiration").value,
    strike:                $("m-strike").value,
    actual_entry_price:    parseFloat($("m-entry").value) || null,
    contracts:             parseInt($("m-contracts").value) || 1,
    hold_days:             parseInt($("m-hold").value) || null,
    confidence:            parseFloat($("m-conf").value) || null,
    expected_return:       parseFloat($("m-expected").value) || null,
    quoted_bid_open:       parseFloat($("m-quoted-bid").value) || null,
    quoted_ask_open:       parseFloat($("m-quoted-ask").value) || null,
    quoted_mid_open:       parseFloat($("m-quoted-mid").value) || null,
    suggested_entry_price: parseFloat($("m-suggested-price").value) || null,
    strike_delta:          window._prefillDelta || null,
    moneyness_pct:         window._prefillMoneyness || null,
    strike_candidates:     window._prefillCandidates || null,
    notes:                 $("m-notes").value,
  };
  try {
    const res = await post("/api/ledger/add", body);
    if (res.ok) {
      const sbTag = res.supabase_synced ? " [Supabase ✓]" : " [local only]";
      setExecStatus(`Trade #${res.entry.id} committed: ${res.entry.ticker} ${res.entry.direction} ${res.entry.structure}${sbTag}`, true);
      hide("trade-modal-overlay");
      refreshLedger();
    } else {
      setExecStatus("Error committing trade", false);
    }
  } catch (e) {
    setExecStatus("Failed: " + e.message, false);
  }
}

/* ── Close Trade Modal ────────────────────────────────────────── */

async function openCloseModal(id) {
  $("cm-id").value = id;
  $("cm-id-label").textContent = id;
  $("cm-exit").value = "";
  $("cm-date-close").value = todayStr();
  $("cm-close-notes").value = "";
  $("cm-contract-symbol").value = "";
  hide("cm-pricing-banner");
  hide("cm-pricing-loading");
  show("close-modal-overlay");

  // Find the ledger entry to get its contract symbol
  try {
    const ledgerRes = await (await fetch("/api/ledger")).json();
    if (ledgerRes.ok) {
      const entry = ledgerRes.ledger.find(e => e.id === id);
      if (entry && entry.contract_symbol) {
        $("cm-contract-symbol").value = entry.contract_symbol;
        show("cm-pricing-loading");

        const res = await post("/api/options/quote", { contract_symbol: entry.contract_symbol });
        if (res.ok && res.quote) {
          const q = res.quote;
          $("cm-contract-sym").textContent = q.contract_symbol || "";
          $("cm-bid").textContent = "$" + fmt(q.bid, 2);
          $("cm-ask").textContent = "$" + fmt(q.ask, 2);
          $("cm-mid").textContent = "$" + fmt(q.mid, 2);
          $("cm-spread").textContent = "$" + fmt(q.spread, 2);
          $("cm-suggested").textContent = "$" + fmt(q.suggested_exit_price, 2);
          show("cm-pricing-banner");
          // Prefill exit price with suggested
          if (q.suggested_exit_price) $("cm-exit").value = q.suggested_exit_price;
        }
        hide("cm-pricing-loading");
      }
    }
  } catch (e) { hide("cm-pricing-loading"); }
}

function closeCloseModal(e) {
  if (e && e.target !== $("close-modal-overlay")) return;
  hide("close-modal-overlay");
}

async function submitCloseTrade() {
  // Read live pricing from banner if available
  const bidText = $("cm-bid").textContent.replace("$", "");
  const askText = $("cm-ask").textContent.replace("$", "");
  const midText = $("cm-mid").textContent.replace("$", "");
  const sugText = $("cm-suggested").textContent.replace("$", "");

  // Capture exit engine state at close time from PnL data
  const tradeId = parseInt($("cm-id").value);
  let exitDecision = null, exitUrg = null, exitTags = null;
  if (lastPnLData && lastPnLData.trades) {
    const pnlTrade = lastPnLData.trades.find(t => t.id === tradeId);
    if (pnlTrade) {
      exitDecision = pnlTrade.exit_state;
      exitUrg = pnlTrade.exit_urgency;
      exitTags = pnlTrade.exit_rule_tags;
    }
  }

  const body = {
    id:                   tradeId,
    actual_exit_price:    parseFloat($("cm-exit").value) || null,
    date_close:           $("cm-date-close").value,
    quoted_bid_close:     parseFloat(bidText) || null,
    quoted_ask_close:     parseFloat(askText) || null,
    quoted_mid_close:     parseFloat(midText) || null,
    suggested_exit_price: parseFloat(sugText) || null,
    close_notes:          $("cm-close-notes").value,
    exit_decision:        exitDecision,
    exit_urgency:         exitUrg,
    exit_rule_tags:       exitTags,
  };
  try {
    const res = await post("/api/ledger/close", body);
    if (res.ok) {
      const e = res.entry;
      const retStr = e.actual_return != null ? ` | Return: ${fmt(e.actual_return, 2)}%` : "";
      const pnlStr = e.pnl != null ? ` | PnL: $${fmt(e.pnl, 2)}` : "";
      const sbTag = res.supabase_synced ? " [Supabase ✓]" : "";
      setExecStatus(`Trade #${e.id} closed${retStr}${pnlStr}${sbTag}`, true);
      hide("close-modal-overlay");
      refreshLedger();
    } else {
      setExecStatus(res.error || "Error closing trade", false);
    }
  } catch (e) {
    setExecStatus("Failed: " + e.message, false);
  }
}

/* ── Ledger refresh + render ──────────────────────────────────── */

async function refreshLedger() {
  try {
    const res = await (await fetch("/api/ledger")).json();
    if (!res.ok) return;
    renderLedger(res.ledger);
    const stats = await (await fetch("/api/ledger/stats")).json();
    if (stats.ok) renderPerfAndEdge(stats);
  } catch (e) { /* silently fail */ }
}

function renderLedger(ledger) {
  if (!ledger || !ledger.length) {
    $("ledger-body").innerHTML = '<tr><td colspan="15" style="color:var(--text-dim);text-align:center;padding:20px">No trades logged yet</td></tr>';
    return;
  }
  let html = "";
  ledger.forEach(e => {
    const statusCls = e.status === "OPEN" ? "status-open" : "status-closed";
    const winCell = e.win === true ? '<span class="win-y">W</span>' :
                    e.win === false ? '<span class="win-n">L</span>' : '';
    const pnlCell = e.pnl != null ? `<span class="${pctClass(e.pnl)}">$${fmt(e.pnl, 2)}</span>` : '';
    const retCell = e.actual_return != null ? `<span class="${pctClass(e.actual_return)}">${fmt(e.actual_return, 2)}%</span>` : '';
    const actionCell = e.status === "OPEN"
      ? `<button class="btn-close-trade" onclick="openCloseModal(${e.id})">Close</button>`
      : '';
    html += `<tr>
      <td>${e.id}</td>
      <td>${e.date_open || e.date || ''}</td>
      <td><strong>${e.ticker}</strong></td>
      <td class="${e.direction === 'LONG' ? 'pos' : 'neg'}">${e.direction || ''}</td>
      <td>${e.mode}</td>
      <td>${e.structure}</td>
      <td>${e.strike || ''}</td>
      <td>${e.entry_price != null ? fmt(e.entry_price, 2) : ''}</td>
      <td>${e.exit_price != null ? fmt(e.exit_price, 2) : ''}</td>
      <td>${e.contracts || 1}</td>
      <td>${pnlCell}</td>
      <td>${retCell}</td>
      <td>${winCell}</td>
      <td class="${statusCls}">${e.status}</td>
      <td>${actionCell}</td>
    </tr>`;
  });
  $("ledger-body").innerHTML = html;
}

/* ── Section 4 + 5: Performance + Edge Diagnostics ────────────── */

function renderPerfAndEdge(stats) {
  // Perf summary cards
  const perfCards = [
    ["Total Trades", stats.total_trades],
    ["Win Rate", fmt(stats.win_rate, 1) + "%"],
    ["Avg Return", fmt(stats.avg_return, 4) + "%"],
  ];
  // Add mode breakdown
  if (stats.mode_stats) {
    for (const [mode, ms] of Object.entries(stats.mode_stats)) {
      perfCards.push([mode, `${ms.trades} trades, ${fmt(ms.win_rate, 0)}% WR, ${fmt(ms.avg_return, 2)}% avg`]);
    }
  }

  let perfHtml = "";
  if (stats.total_trades === 0) {
    perfHtml = '<p class="placeholder-text">Close some trades to see performance stats.</p>';
  } else {
    perfCards.forEach(([label, value]) => {
      let cls = "";
      const s = String(value);
      if (s.includes("%")) {
        const n = parseFloat(s);
        if (!isNaN(n)) cls = pctClass(n);
      }
      perfHtml += `<div class="card"><div class="label">${label}</div><div class="value ${cls}">${value}</div></div>`;
    });
  }
  $("exec-perf-cards").innerHTML = perfHtml;

  // Edge diagnostics
  if (stats.total_trades > 0) {
    const edgeCards = [
      ["Best Ticker", stats.best_ticker || "—"],
      ["Worst Ticker", stats.worst_ticker || "—"],
      ["Best Mode", stats.best_mode || "—"],
      ["Worst Mode", stats.worst_mode || "—"],
      ["Avg Slippage Open", stats.avg_slippage_open != null ? "$" + fmt(stats.avg_slippage_open, 4) : "—"],
      ["Avg Slippage Close", stats.avg_slippage_close != null ? "$" + fmt(stats.avg_slippage_close, 4) : "—"],
      ["Expected vs Actual", stats.avg_expected_vs_actual != null ? fmt(stats.avg_expected_vs_actual, 2) + "%" : "—"],
      ["Open Trades", stats.open_trades || 0],
    ];
    let edgeHtml = "";
    edgeCards.forEach(([label, value]) => {
      edgeHtml += `<div class="card"><div class="label">${label}</div><div class="value">${value}</div></div>`;
    });
    $("exec-edge-cards").innerHTML = edgeHtml;
    show("exec-edge-section");
  } else {
    hide("exec-edge-section");
  }
}

/* ================================================================
   OPEN PnL + EXIT RECOMMENDATIONS TAB
   ================================================================ */

let _pnlAutoTimer = null;
let _pnlAutoOn = false;
let lastPnLData = null;

function setPnLStatus(msg, ok) {
  const el = $("pnl-status");
  el.textContent = msg;
  el.className = "status " + (ok ? "ok" : "error");
  el.classList.remove("hidden");
}

async function refreshPnL() {
  try {
    const res = await (await fetch("/api/ledger/open-pnl")).json();
    if (!res.ok) { setPnLStatus("Error: " + (res.error || "unknown"), false); return; }
    lastPnLData = res;
    renderPnLSummary(res);
    renderUrgentActions(res.trades);
    renderPnLTable(res.trades);
    $("pnl-last-update").textContent = "Updated: " + new Date().toLocaleTimeString();
    if (res.total_open > 0) {
      const actionCount = res.trades.filter(t => t.exit_state !== "HOLD").length;
      setPnLStatus(`Monitoring ${res.total_open} open trades — ${actionCount} action(s) flagged`, true);
    }
  } catch (e) {
    setPnLStatus("Failed: " + e.message, false);
  }
}

function togglePnLAuto() { _pnlAutoOn ? stopPnLAuto() : startPnLAuto(); }

function startPnLAuto() {
  _pnlAutoOn = true;
  $("btn-pnl-auto").textContent = "Auto-Refresh: ON";
  $("btn-pnl-auto").style.background = "var(--green)";
  $("pnl-auto-indicator").className = "pnl-auto-on";
  $("pnl-auto-indicator").textContent = "● LIVE";
  _pnlAutoTimer = setInterval(refreshPnL, 45000);
}

function stopPnLAuto() {
  _pnlAutoOn = false;
  if (_pnlAutoTimer) { clearInterval(_pnlAutoTimer); _pnlAutoTimer = null; }
  const btn = $("btn-pnl-auto");
  if (btn) { btn.textContent = "Auto-Refresh: OFF"; btn.style.background = ""; }
  const ind = $("pnl-auto-indicator");
  if (ind) { ind.className = "pnl-auto-off"; ind.textContent = ""; }
}

/* ── Summary panel ───────────────────────────────────────────── */

function renderPnLSummary(data) {
  if (!data || data.total_open === 0) {
    $("pnl-summary-cards").innerHTML = '<p class="placeholder-text">No open trades. Add trades in the Execution tab first.</p>';
    hide("pnl-table-section");
    hide("pnl-urgent-section");
    return;
  }

  // Count exit states
  const counts = {};
  data.trades.forEach(t => { counts[t.exit_state] = (counts[t.exit_state] || 0) + 1; });

  const cards = [
    ["Open Trades", data.total_open],
    ["Total PnL", "$" + fmt(data.total_unrealized_pnl, 2)],
    ["Avg Return", fmt(data.avg_unrealized_return, 2) + "%"],
    ["HOLD", counts["HOLD"] || 0],
    ["TAKE PROFIT", (counts["TAKE_PROFIT"] || 0) + (counts["TAKE_PROFIT_SOON"] || 0)],
    ["SCALE OUT", (counts["SCALE_OUT"] || 0) + (counts["TRIM_HOLD"] || 0)],
    ["CUT LOSS", (counts["CUT_LOSS"] || 0) + (counts["DANGER"] || 0)],
    ["TIME EXIT", (counts["TIME_EXIT"] || 0) + (counts["REVIEW"] || 0)],
  ];

  let html = "";
  cards.forEach(([label, value]) => {
    let cls = "";
    const s = String(value);
    if (s.startsWith("$")) { const n = parseFloat(s.replace("$","")); if (!isNaN(n)) cls = pctClass(n); }
    else if (s.includes("%")) { const n = parseFloat(s); if (!isNaN(n)) cls = pctClass(n); }
    else if (label === "CUT LOSS" && value > 0) cls = "neg";
    else if (label === "TAKE PROFIT" && value > 0) cls = "pos";
    else if (label === "SCALE OUT" && value > 0) cls = "pos";
    html += `<div class="card"><div class="label">${label}</div><div class="value ${cls}">${value}</div></div>`;
  });
  $("pnl-summary-cards").innerHTML = html;
}

/* ── Urgent actions panel ────────────────────────────────────── */

function renderUrgentActions(trades) {
  const urgent = trades.filter(t =>
    ["CUT_LOSS","DANGER","TAKE_PROFIT","TAKE_PROFIT_SOON","TIME_EXIT","SCALE_OUT","TRIM_HOLD","REVIEW"].includes(t.exit_state)
  );

  if (!urgent.length) { hide("pnl-urgent-section"); return; }

  // Sort: HIGH urgency first
  const urgOrder = {"HIGH":0,"MEDIUM":1,"MANUAL":2,"LOW":3};
  urgent.sort((a,b) => (urgOrder[a.exit_urgency]||4) - (urgOrder[b.exit_urgency]||4));

  let html = "";
  urgent.forEach(t => {
    const urgCls = t.exit_urgency === "HIGH" ? "urg-high" : t.exit_urgency === "MEDIUM" ? "urg-medium" : "urg-manual";
    const stateCls = t.exit_state.includes("CUT") || t.exit_state === "DANGER" ? "neg" :
                     t.exit_state.includes("TAKE") || t.exit_state === "SCALE_OUT" || t.exit_state === "TRIM_HOLD" ? "pos" : "neu";
    const showPartial = (t.contracts || 1) > 1;

    html += `
    <div class="urgent-card ${urgCls}">
      <div class="urgent-ticker">${t.ticker}</div>
      <div class="urgent-info">
        <div class="urgent-state ${stateCls}">${exitBadge(t.exit_state)} <span class="urgency-badge urgency-${t.exit_urgency.toLowerCase()}">${t.exit_urgency}</span></div>
        <div class="urgent-explain">${t.exit_explanation || t.exit_action || ''}</div>
        <div class="urgent-action-text">→ ${t.exit_action || ''}</div>
      </div>
      <div style="text-align:right;font-family:var(--mono);font-size:13px;min-width:100px">
        <div class="${pctClass(t.unrealized_return_pct)}" style="font-weight:700">${fmt(t.unrealized_return_pct, 2)}%</div>
        <div class="${pctClass(t.unrealized_pnl)}" style="font-size:11px">$${fmt(t.unrealized_pnl, 2)}</div>
        <div style="color:var(--text-dim);font-size:11px">${t.days_held}d held</div>
      </div>
      <div class="urgent-btns">
        <button class="btn-close-trade" onclick="openCloseModal(${t.id})">Close</button>
        ${showPartial ? `<button class="btn-partial" onclick="openPartialModal(${t.id}, ${t.contracts})">Partial</button>` : ''}
      </div>
    </div>`;
  });

  $("pnl-urgent-list").innerHTML = html;
  show("pnl-urgent-section");
}

/* ── Exit badge helper ───────────────────────────────────────── */

function exitBadge(state) {
  const map = {
    "HOLD":              { cls: "exit-hold",  label: "HOLD" },
    "TAKE_PROFIT":       { cls: "exit-tp",    label: "TAKE PROFIT" },
    "TAKE_PROFIT_SOON":  { cls: "exit-tp",    label: "TP SOON" },
    "SCALE_OUT":         { cls: "exit-scale", label: "SCALE OUT" },
    "TRIM_HOLD":         { cls: "exit-scale", label: "TRIM" },
    "CUT_LOSS":          { cls: "exit-cut",   label: "CUT LOSS" },
    "DANGER":            { cls: "exit-cut",   label: "DANGER" },
    "TIME_EXIT":         { cls: "exit-time",  label: "TIME EXIT" },
    "REVIEW":            { cls: "exit-time",  label: "REVIEW" },
  };
  const m = map[state] || { cls: "exit-hold", label: state };
  return `<span class="exit-badge ${m.cls}">${m.label}</span>`;
}

function urgencyBadge(urg) {
  const cls = urg === "HIGH" ? "urgency-high" : urg === "MEDIUM" ? "urgency-medium" : urg === "MANUAL" ? "urgency-manual" : "urgency-low";
  return `<span class="urgency-badge ${cls}">${urg}</span>`;
}

function tagPills(tags) {
  if (!tags || !tags.length) return "";
  return tags.map(t => `<span class="tag-pill">${t}</span>`).join("");
}

/* ── Full position table ─────────────────────────────────────── */

function renderPnLTable(trades) {
  if (!trades || !trades.length) { hide("pnl-table-section"); return; }

  let html = "";
  trades.forEach(t => {
    const rowClass = t.health === "GREEN" ? "pnl-row-green" :
                     t.health === "YELLOW" ? "pnl-row-yellow" : "pnl-row-red";
    const showPartial = (t.contracts || 1) > 1;

    html += `<tr class="${rowClass}">
      <td>${t.id}</td>
      <td><strong>${t.ticker}</strong></td>
      <td class="${t.direction === 'LONG' ? 'pos' : 'neg'}">${t.direction}</td>
      <td>${t.mode}</td>
      <td>${t.structure}</td>
      <td>${t.contracts || 1}</td>
      <td>$${fmt(t.entry_price, 2)}</td>
      <td>$${fmt(t.current_mid, 2)}</td>
      <td class="${pctClass(t.unrealized_pnl)}"><strong>$${fmt(t.unrealized_pnl, 2)}</strong></td>
      <td class="${pctClass(t.unrealized_return_pct)}"><strong>${fmt(t.unrealized_return_pct, 2)}%</strong></td>
      <td>${t.days_held}d <span style="color:var(--text-dim);font-size:10px">/${t.hold_target}d</span></td>
      <td>${fmt(t.distance_to_tp, 1)}%</td>
      <td>${fmt(t.distance_to_sl, 1)}%</td>
      <td>${exitBadge(t.exit_state)}</td>
      <td>${urgencyBadge(t.exit_urgency || "LOW")}</td>
      <td>${tagPills(t.exit_rule_tags)}</td>
      <td>
        <div class="pnl-btn-group">
          <button class="btn-close-trade" onclick="openCloseModal(${t.id})">Close</button>
          ${showPartial ? `<button class="btn-partial" onclick="openPartialModal(${t.id}, ${t.contracts})">Partial</button>` : ''}
          <button class="btn-refresh-quote" onclick="refreshSingleQuote(${t.id})" title="Refresh quote">↻</button>
        </div>
      </td>
    </tr>`;

    // Explanation sub-row
    if (t.exit_explanation && t.exit_state !== "HOLD") {
      html += `<tr class="pnl-explain-row ${rowClass}">
        <td></td><td colspan="16">${t.exit_explanation}</td>
      </tr>`;
    }
  });
  $("pnl-body").innerHTML = html;
  show("pnl-table-section");
}

/* ── Refresh single quote ────────────────────────────────────── */

async function refreshSingleQuote(id) {
  setPnLStatus(`Refreshing quote for trade #${id}...`, true);
  await refreshPnL();
}

/* ── Partial close modal ─────────────────────────────────────── */

function openPartialModal(id, totalContracts) {
  $("pm-id").value = id;
  $("pm-id-label").textContent = id;
  $("pm-info").textContent = `Total contracts: ${totalContracts}. Close a portion and keep the rest open.`;
  $("pm-contracts").value = Math.floor(totalContracts / 2) || 1;
  $("pm-contracts").max = totalContracts - 1;
  $("pm-exit").value = "";
  $("pm-date").value = todayStr();
  show("partial-modal-overlay");
}

function closePartialModal(e) {
  if (e && e.target !== $("partial-modal-overlay")) return;
  hide("partial-modal-overlay");
}

async function submitPartialClose() {
  const body = {
    id: parseInt($("pm-id").value),
    close_contracts: parseInt($("pm-contracts").value),
    exit_price: parseFloat($("pm-exit").value) || null,
    date_close: $("pm-date").value,
  };
  try {
    const res = await post("/api/ledger/partial-close", body);
    if (res.ok) {
      const cp = res.closed_portion;
      const rm = res.remaining;
      const retStr = cp.actual_return != null ? ` | Return: ${fmt(cp.actual_return, 2)}%` : "";
      const pnlStr = cp.pnl != null ? ` | PnL: $${fmt(cp.pnl, 2)}` : "";
      setPnLStatus(`Partial close: ${cp.contracts} ctrs closed${retStr}${pnlStr}. ${rm.contracts} ctrs remaining.`, true);
      hide("partial-modal-overlay");
      refreshPnL();
    } else {
      setPnLStatus(res.error || "Partial close failed", false);
    }
  } catch (e) {
    setPnLStatus("Failed: " + e.message, false);
  }
}

/* ── Copy / Download ─────────────────────────────────────────── */

function copyExitSummary() {
  if (!lastPnLData || !lastPnLData.trades.length) {
    setPnLStatus("No open trades to copy", false);
    return;
  }
  const d = lastPnLData;
  const urgOrder = {"HIGH":0,"MEDIUM":1,"MANUAL":2,"LOW":3};
  const sorted = [...d.trades].sort((a,b) => (urgOrder[a.exit_urgency]||4) - (urgOrder[b.exit_urgency]||4));

  let lines = ["=== MONOS EXIT RECOMMENDATIONS ===", ""];
  lines.push(`Open Trades: ${d.total_open}`);
  lines.push(`Total PnL: $${fmt(d.total_unrealized_pnl, 2)}`);
  lines.push(`Avg Return: ${fmt(d.avg_unrealized_return, 2)}%`);
  lines.push(`Timestamp: ${new Date().toLocaleString()}`);
  lines.push("");

  sorted.forEach(t => {
    const marker = t.exit_urgency === "HIGH" ? "!!!" : t.exit_urgency === "MEDIUM" ? ">> " : "   ";
    const tags = (t.exit_rule_tags || []).join(", ");
    lines.push(`${marker}${t.ticker.padEnd(4)} | ${t.mode.padEnd(16)} | ${(t.exit_state || "HOLD").padEnd(16)} | ${fmt(t.unrealized_return_pct,1).padStart(6)}% | ${t.days_held}d held | ${t.exit_urgency}${tags ? " | " + tags : ""}`);
  });

  lines.push("");
  lines.push("Actions:");
  sorted.filter(t => t.exit_state !== "HOLD").forEach(t => {
    lines.push(`  ${t.ticker}: ${t.exit_action}`);
  });

  lines.push("");
  lines.push("Generated by MONOS Conviction Engine");
  navigator.clipboard.writeText(lines.join("\n"));
  setPnLStatus("Exit summary copied to clipboard", true);
}

function downloadPnLJSON() {
  if (!lastPnLData) return;
  const blob = new Blob([JSON.stringify(lastPnLData, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `monos_pnl_${Date.now()}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

/* ================================================================
   RULES & DIALOGUE TAB
   ================================================================ */

let lastDialogues = null;

function setDialogueStatus(msg, ok) {
  const el = $("dialogue-status");
  el.textContent = msg;
  el.className = "status " + (ok ? "ok" : "error");
  el.classList.remove("hidden");
}

async function loadDialogue() {
  if (!lastTopTrades || !lastTopTrades.length) {
    $("dialogue-hint").textContent = "Run a batch on the Backtest tab first. Top trades will be analyzed here with full reasoning.";
    $("dialogue-cards").innerHTML = "";
    return;
  }
  hide("dialogue-hint");
  $("dialogue-cards").innerHTML = '<p class="placeholder-text" style="padding:20px">Generating trade dialogue...</p>';

  try {
    const res = await post("/api/trade-dialogue", { top_trades: lastTopTrades });
    if (!res.ok) { setDialogueStatus("Error: " + (res.error || "unknown"), false); return; }
    lastDialogues = res.dialogues;
    renderDialogues(res.dialogues);
    setDialogueStatus(`Generated reasoning for ${res.dialogues.length} trades`, true);
  } catch (e) {
    setDialogueStatus("Failed: " + e.message, false);
  }
}

function renderDialogues(dialogues) {
  if (!dialogues || !dialogues.length) {
    $("dialogue-cards").innerHTML = '<p class="placeholder-text">No trade dialogues generated.</p>';
    return;
  }

  let html = "";
  dialogues.forEach((d, idx) => {
    const dirClass = d.direction === "LONG" ? "pos" : "neg";
    const strClass = d.signal_breakdown.strength === "HIGH" ? "badge-high" :
                     d.signal_breakdown.strength === "MEDIUM" ? "badge-medium" : "badge-low";

    // Signal grid
    const sb = d.signal_breakdown;
    let signalHtml = `
      <div class="dlg-signal-grid">
        <div class="dlg-signal-item"><span class="dlg-signal-label">Confidence</span><span class="dlg-signal-val">${sb.confidence}</span></div>
        <div class="dlg-signal-item"><span class="dlg-signal-label">Direction</span><span class="dlg-signal-val ${dirClass}">${sb.direction}</span></div>
        <div class="dlg-signal-item"><span class="dlg-signal-label">Mode</span><span class="dlg-signal-val">${sb.mode}</span></div>
        <div class="dlg-signal-item"><span class="dlg-signal-label">Structure</span><span class="dlg-signal-val">${sb.structure}</span></div>
        <div class="dlg-signal-item"><span class="dlg-signal-label">Wgt Return</span><span class="dlg-signal-val ${pctClass(parseFloat(sb.weighted_return))}">${sb.weighted_return}</span></div>
        <div class="dlg-signal-item"><span class="dlg-signal-label">Strength</span><span class="dlg-signal-val"><span class="badge ${strClass}">${sb.strength}</span></span></div>
      </div>`;

    // Filters
    let filterHtml = "";
    d.filters.forEach(f => {
      let cls = "dlg-filter-neutral";
      if (f.includes("ACTIVE") || f.includes("aligned") || f.includes("HIGH") || f.includes("solid") || f.includes("boosted")) cls = "dlg-filter-active";
      if (f.includes("SKIPPED") || f.includes("bypassed")) cls = "dlg-filter-skipped";
      filterHtml += `<div class="dlg-filter ${cls}">${f}</div>`;
    });

    // Decision
    const dec = d.decision;
    let decHtml = `
      <div class="dlg-decision">
        <div class="dlg-decision-row"><span class="dlg-decision-label">Selection:</span>${dec.why_selected}</div>
        <div class="dlg-decision-row"><span class="dlg-decision-label">Structure:</span>${dec.why_structure}</div>
        <div class="dlg-decision-row"><span class="dlg-decision-label">Hold:</span>${dec.why_hold}</div>
        <div class="dlg-decision-row"><span class="dlg-decision-label">Sizing:</span>${dec.why_sizing}</div>
      </div>`;

    // Rejected
    let rejHtml = "";
    d.rejected.forEach(r => {
      rejHtml += `
        <div class="dlg-rejected-item">
          <div class="dlg-rejected-alt">X ${r.alternative}</div>
          <div class="dlg-rejected-reason">${r.reason}</div>
        </div>`;
    });

    // Conclusion
    const strength = sb.strength;
    const concText = `${strength === 'HIGH' ? 'High' : strength === 'MEDIUM' ? 'Medium' : 'Low'} conviction ${d.direction.toLowerCase()} opportunity in ${d.mode.toLowerCase()} regime.`;

    html += `
    <div class="dlg-card">
      <div class="dlg-card-header">
        <div>
          <span class="dlg-ticker">${d.ticker}</span>
          <span class="dlg-dir ${dirClass}" style="margin-left:12px;font-size:14px">${d.direction}</span>
        </div>
        <div class="dlg-meta">
          <span class="badge ${strClass}">${sb.strength}</span>
          <span style="color:var(--text-dim)">#${d.rank}</span>
          <button class="dlg-copy-btn" onclick="copySingleDialogue(${idx})">Copy</button>
        </div>
      </div>
      <div class="dlg-body">
        <div class="dlg-section">
          <div class="dlg-section-title">Signal Breakdown</div>
          ${signalHtml}
        </div>
        <div class="dlg-section">
          <div class="dlg-section-title">Filter Status</div>
          ${filterHtml}
        </div>
        <div class="dlg-section">
          <div class="dlg-section-title">Decision Logic</div>
          ${decHtml}
        </div>
        <div class="dlg-section">
          <div class="dlg-section-title">Rejected Alternatives</div>
          <div class="dlg-rejected">${rejHtml}</div>
        </div>
        <div class="dlg-section">
          <div class="dlg-conclusion">${concText}</div>
        </div>
      </div>
    </div>`;
  });

  $("dialogue-cards").innerHTML = html;
}

function copySingleDialogue(idx) {
  if (!lastDialogues || !lastDialogues[idx]) return;
  navigator.clipboard.writeText(lastDialogues[idx].narrative);
  setDialogueStatus(`Copied dialogue for ${lastDialogues[idx].ticker}`, true);
}

function copyAllDialogue() {
  if (!lastDialogues || !lastDialogues.length) {
    setDialogueStatus("No dialogues to copy", false);
    return;
  }
  const full = lastDialogues.map(d => d.narrative).join("\n\n" + "=".repeat(50) + "\n\n");
  const header = "=== MONOS TRADE DIALOGUE REPORT ===\n\n";
  const footer = "\n\nGenerated by MONOS Conviction Engine";
  navigator.clipboard.writeText(header + full + footer);
  setDialogueStatus(`Copied ${lastDialogues.length} trade dialogues`, true);
}

/* ================================================================
   EXPECTED vs ACTUAL TAB
   ================================================================ */

let lastEVA = null;

function setEVAStatus(msg, ok) {
  const el = $("eva-status");
  el.textContent = msg;
  el.className = "status " + (ok ? "ok" : "error");
  el.classList.remove("hidden");
}

async function loadEVA() {
  try {
    const res = await (await fetch("/api/ledger/eva")).json();
    if (!res.ok) { setEVAStatus("Error loading EVA data", false); return; }
    lastEVA = res;
    renderEVAStats(res);
    renderEVAModes(res.mode_stats, res.best_mode, res.worst_mode);
    renderEVATrades(res.trades);
    if (res.trades.length > 0) {
      drawScatter(res.trades);
      drawModeBar(res.mode_stats);
      show("eva-charts-section");
    } else {
      hide("eva-charts-section");
    }
    if (res.total > 0) {
      setEVAStatus(`Loaded ${res.total} closed trades`, true);
    }
    // Also load decision quality
    loadDecisionQuality();
  } catch (e) {
    setEVAStatus("Failed: " + e.message, false);
  }
}

/* ── Stats cards ─────────────────────────────────────────────── */

function renderEVAStats(d) {
  if (!d || d.total === 0) {
    $("eva-stats-cards").innerHTML = '<p class="placeholder-text">Close some trades in the Execution tab to see analysis here.</p>';
    return;
  }
  const cards = [
    ["Closed Trades", d.total],
    ["Win Rate", fmt(d.win_rate, 1) + "%"],
    ["Avg Expected", d.avg_expected != null ? fmt(d.avg_expected, 2) + "%" : "—"],
    ["Avg Actual", d.avg_actual != null ? fmt(d.avg_actual, 2) + "%" : "—"],
    ["Avg Gap", d.avg_gap != null ? fmt(d.avg_gap, 2) + "%" : "—"],
    ["Beat Rate", fmt(d.beat_rate, 1) + "%"],
    ["Best Mode", d.best_mode || "—"],
    ["Worst Mode", d.worst_mode || "—"],
  ];
  let html = "";
  cards.forEach(([label, value]) => {
    let cls = "";
    const s = String(value);
    if (s.includes("%")) {
      const n = parseFloat(s);
      if (!isNaN(n) && label !== "Win Rate" && label !== "Beat Rate") cls = pctClass(n);
    }
    if (label === "Avg Gap") cls = parseFloat(s) >= 0 ? "pos" : "neg";
    html += `<div class="card"><div class="label">${label}</div><div class="value ${cls}">${value}</div></div>`;
  });
  $("eva-stats-cards").innerHTML = html;
}

/* ── Mode breakdown table ────────────────────────────────────── */

function renderEVAModes(modeStats, bestMode, worstMode) {
  if (!modeStats || !Object.keys(modeStats).length) { hide("eva-mode-section"); return; }
  let html = "";
  const modes = Object.entries(modeStats).sort((a, b) => (b[1].avg_actual || 0) - (a[1].avg_actual || 0));
  modes.forEach(([mode, ms]) => {
    const ratingCls = ms.rating === "STRONG" ? "badge-strong" : ms.rating === "OK" ? "badge-ok" : "badge-weak";
    const isBest = mode === bestMode;
    const isWorst = mode === worstMode;
    const highlight = isBest ? "row-green" : isWorst ? "row-red" : "";
    html += `<tr class="${highlight}">
      <td><strong>${mode}</strong>${isBest ? ' ★' : ''}${isWorst ? ' ▼' : ''}</td>
      <td>${ms.trades}</td>
      <td>${ms.avg_expected != null ? fmt(ms.avg_expected, 2) + '%' : '—'}</td>
      <td class="${pctClass(ms.avg_actual)}">${ms.avg_actual != null ? fmt(ms.avg_actual, 2) + '%' : '—'}</td>
      <td class="${ms.avg_gap != null ? (ms.avg_gap >= 0 ? 'gap-pos' : 'gap-neg') : ''}">${ms.avg_gap != null ? fmt(ms.avg_gap, 2) + '%' : '—'}</td>
      <td>${fmt(ms.win_rate, 1)}%</td>
      <td class="${pctClass(ms.total_return)}">${fmt(ms.total_return, 2)}%</td>
      <td><span class="badge ${ratingCls}">${ms.rating}</span></td>
    </tr>`;
  });
  $("eva-mode-body").innerHTML = html;
  show("eva-mode-section");
}

/* ── Trade-by-trade table ────────────────────────────────────── */

function renderEVATrades(trades) {
  if (!trades || !trades.length) { hide("eva-table-section"); return; }
  let html = "";
  trades.forEach(t => {
    const gapCls = t.gap != null ? (t.gap >= 0 ? "gap-pos" : "gap-neg") : "";
    const winCell = t.win === true ? '<span class="win-y">W</span>' :
                    t.win === false ? '<span class="win-n">L</span>' : '—';
    html += `<tr>
      <td>${t.id}</td>
      <td><strong>${t.ticker}</strong></td>
      <td>${t.mode}</td>
      <td>${t.structure}</td>
      <td>${t.expected_return != null ? fmt(t.expected_return, 2) + '%' : '—'}</td>
      <td class="${pctClass(t.actual_return)}">${t.actual_return != null ? fmt(t.actual_return, 2) + '%' : '—'}</td>
      <td class="${gapCls}">${t.gap != null ? (t.gap >= 0 ? '+' : '') + fmt(t.gap, 2) + '%' : '—'}</td>
      <td>${winCell}</td>
    </tr>`;
  });
  $("eva-body").innerHTML = html;
  show("eva-table-section");
}

/* ── Canvas charts (pure vanilla — no library) ────────────────── */

const MODE_COLORS = {
  "TACTICAL":        "#58a6ff",
  "HYBRID":          "#d29922",
  "CONVEX":          "#3fb950",
  "MEAN_REVERSION":  "#db6d28",
};

function getCtx(id) {
  const canvas = $(id);
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);
  return { ctx, w: rect.width, h: rect.height };
}

function drawScatter(trades) {
  const { ctx, w, h } = getCtx("eva-scatter-canvas");
  const pad = { top: 20, right: 20, bottom: 40, left: 55 };
  const pw = w - pad.left - pad.right;
  const ph = h - pad.top - pad.bottom;

  // Filter trades with both values
  const pts = trades.filter(t => t.expected_return != null && t.actual_return != null);
  if (!pts.length) return;

  const allVals = pts.flatMap(t => [t.expected_return, t.actual_return]);
  let mn = Math.min(...allVals, 0);
  let mx = Math.max(...allVals, 0);
  const range = mx - mn || 1;
  mn -= range * 0.1;
  mx += range * 0.1;

  const scX = v => pad.left + ((v - mn) / (mx - mn)) * pw;
  const scY = v => pad.top + ph - ((v - mn) / (mx - mn)) * ph;

  // Clear
  ctx.clearRect(0, 0, w, h);

  // Grid
  ctx.strokeStyle = "#30363d";
  ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {
    const v = mn + ((mx - mn) * i / 4);
    const x = scX(v);
    const y = scY(v);
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(w - pad.right, y); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(x, pad.top); ctx.lineTo(x, h - pad.bottom); ctx.stroke();

    ctx.fillStyle = "#8b949e";
    ctx.font = "10px Consolas, monospace";
    ctx.textAlign = "right";
    ctx.fillText(v.toFixed(1) + "%", pad.left - 6, y + 3);
    ctx.textAlign = "center";
    ctx.fillText(v.toFixed(1) + "%", x, h - pad.bottom + 14);
  }

  // Diagonal (expected = actual)
  ctx.strokeStyle = "#484f58";
  ctx.lineWidth = 1;
  ctx.setLineDash([4, 4]);
  ctx.beginPath();
  ctx.moveTo(scX(mn), scY(mn));
  ctx.lineTo(scX(mx), scY(mx));
  ctx.stroke();
  ctx.setLineDash([]);

  // Points
  pts.forEach(t => {
    const x = scX(t.expected_return);
    const y = scY(t.actual_return);
    const color = MODE_COLORS[t.mode] || "#8b949e";
    ctx.beginPath();
    ctx.arc(x, y, 5, 0, Math.PI * 2);
    ctx.fillStyle = color;
    ctx.globalAlpha = 0.85;
    ctx.fill();
    ctx.globalAlpha = 1;
    ctx.strokeStyle = "#fff";
    ctx.lineWidth = 1;
    ctx.stroke();
  });

  // Axis labels
  ctx.fillStyle = "#8b949e";
  ctx.font = "11px -apple-system, sans-serif";
  ctx.textAlign = "center";
  ctx.fillText("Expected Return %", pad.left + pw / 2, h - 4);
  ctx.save();
  ctx.translate(12, pad.top + ph / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.fillText("Actual Return %", 0, 0);
  ctx.restore();

  // Legend
  const modes = [...new Set(pts.map(t => t.mode))];
  let lx = pad.left;
  ctx.font = "10px Consolas, monospace";
  modes.forEach(m => {
    ctx.fillStyle = MODE_COLORS[m] || "#8b949e";
    ctx.fillRect(lx, 4, 10, 10);
    ctx.fillStyle = "#c9d1d9";
    ctx.textAlign = "left";
    ctx.fillText(m, lx + 14, 13);
    lx += ctx.measureText(m).width + 28;
  });
}

function drawModeBar(modeStats) {
  const { ctx, w, h } = getCtx("eva-bar-canvas");
  const pad = { top: 20, right: 20, bottom: 50, left: 55 };
  const pw = w - pad.left - pad.right;
  const ph = h - pad.top - pad.bottom;

  const entries = Object.entries(modeStats).filter(([, v]) => v.trades > 0);
  if (!entries.length) return;

  ctx.clearRect(0, 0, w, h);

  const barW = Math.min(60, pw / entries.length - 20);
  const gap = (pw - barW * entries.length) / (entries.length + 1);

  // Get max values for scale
  const allExp = entries.map(([, v]) => v.avg_expected || 0);
  const allAct = entries.map(([, v]) => v.avg_actual || 0);
  const allVals = [...allExp, ...allAct];
  let mn = Math.min(...allVals, 0);
  let mx = Math.max(...allVals, 0);
  const range = mx - mn || 1;
  mn -= range * 0.15;
  mx += range * 0.15;

  const scY = v => pad.top + ph - ((v - mn) / (mx - mn)) * ph;
  const zeroY = scY(0);

  // Horizontal grid + zero line
  ctx.strokeStyle = "#30363d";
  ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {
    const v = mn + ((mx - mn) * i / 4);
    const y = scY(v);
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(w - pad.right, y); ctx.stroke();
    ctx.fillStyle = "#8b949e";
    ctx.font = "10px Consolas, monospace";
    ctx.textAlign = "right";
    ctx.fillText(v.toFixed(1) + "%", pad.left - 6, y + 3);
  }
  // Zero line
  ctx.strokeStyle = "#484f58";
  ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(pad.left, zeroY); ctx.lineTo(w - pad.right, zeroY); ctx.stroke();

  // Bars: expected (dim) + actual (bright) side-by-side
  entries.forEach(([mode, v], i) => {
    const x = pad.left + gap + i * (barW + gap);
    const halfBar = barW / 2 - 2;
    const color = MODE_COLORS[mode] || "#8b949e";

    // Expected bar (dimmed)
    const expY = scY(v.avg_expected || 0);
    ctx.globalAlpha = 0.35;
    ctx.fillStyle = color;
    ctx.fillRect(x, Math.min(expY, zeroY), halfBar, Math.abs(expY - zeroY));
    ctx.globalAlpha = 1;

    // Actual bar (full)
    const actY = scY(v.avg_actual || 0);
    ctx.fillStyle = color;
    ctx.fillRect(x + halfBar + 4, Math.min(actY, zeroY), halfBar, Math.abs(actY - zeroY));

    // Mode label
    ctx.fillStyle = "#c9d1d9";
    ctx.font = "10px Consolas, monospace";
    ctx.textAlign = "center";
    ctx.fillText(mode, x + barW / 2, h - pad.bottom + 14);

    // Value labels
    ctx.font = "9px Consolas, monospace";
    ctx.fillStyle = "#8b949e";
    ctx.fillText("E:" + fmt(v.avg_expected, 1), x + halfBar / 2, Math.min(expY, zeroY) - 4);
    ctx.fillStyle = color;
    ctx.fillText("A:" + fmt(v.avg_actual, 1), x + halfBar + 4 + halfBar / 2, Math.min(actY, zeroY) - 4);
  });

  // Legend
  ctx.font = "10px Consolas, monospace";
  ctx.globalAlpha = 0.4;
  ctx.fillStyle = "#8b949e";
  ctx.fillRect(pad.left, h - 14, 10, 10);
  ctx.globalAlpha = 1;
  ctx.fillStyle = "#8b949e";
  ctx.textAlign = "left";
  ctx.fillText("Expected", pad.left + 14, h - 5);
  ctx.fillStyle = "#58a6ff";
  ctx.fillRect(pad.left + 90, h - 14, 10, 10);
  ctx.fillStyle = "#c9d1d9";
  ctx.fillText("Actual", pad.left + 104, h - 5);
}

/* ── Decision Quality ─────────────────────────────────────────── */

let lastDQ = null;

async function loadDecisionQuality() {
  try {
    const res = await (await fetch("/api/ledger/decision-quality")).json();
    if (!res.ok || res.total === 0) { hide("dq-section"); return; }
    lastDQ = res;
    renderDQAccuracy(res.accuracy, res.total);
    renderDQDistribution(res.quality_distribution, res.total);
    renderDQModeTable(res.mode_quality);
    renderDQTrades(res.trades);
    show("dq-section");
  } catch (e) {
    hide("dq-section");
  }
}

function renderDQAccuracy(acc, total) {
  const cards = [
    ["Closed Trades", total],
    ["Win Rate", fmt(acc.win_rate, 1) + "%"],
    ["Good Exit Rate", fmt(acc.good_exit_rate, 1) + "%"],
    ["Exit Precision", fmt(acc.exit_precision, 1) + "%"],
    ["Exit Discipline", fmt(acc.exit_discipline, 1) + "%"],
    ["Beat Rate", fmt(acc.beat_rate, 1) + "%"],
    ["Avg Gap", acc.avg_return_gap != null ? fmt(acc.avg_return_gap, 2) + "%" : "—"],
  ];
  let html = "";
  cards.forEach(([label, value]) => {
    let cls = "";
    const s = String(value);
    if (label === "Avg Gap") cls = parseFloat(s) >= 0 ? "pos" : "neg";
    else if (s.includes("%")) {
      const n = parseFloat(s);
      if (!isNaN(n) && n >= 60) cls = "pos";
      else if (!isNaN(n) && n < 40) cls = "neg";
    }
    html += `<div class="card"><div class="label">${label}</div><div class="value ${cls}">${value}</div></div>`;
  });
  $("dq-accuracy-cards").innerHTML = html;
}

function renderDQDistribution(dist, total) {
  const order = [
    { key: "OPTIMAL", color: "var(--green)", label: "Optimal" },
    { key: "EARLY",   color: "var(--yellow)", label: "Early" },
    { key: "LATE",    color: "var(--red)", label: "Late" },
    { key: "STOPPED", color: "var(--accent)", label: "Stopped" },
    { key: "NEUTRAL", color: "var(--text-dim)", label: "Neutral" },
  ];
  let html = "";
  order.forEach(({ key, color, label }) => {
    const count = dist[key] || 0;
    const pct = total > 0 ? round((count / total) * 100, 1) : 0;
    html += `<div class="dq-bar-row">
      <span class="dq-bar-label">${label}</span>
      <div class="dq-bar-track"><div class="dq-bar-fill" style="width:${pct}%;background:${color}"></div></div>
      <span class="dq-bar-count">${count} (${pct}%)</span>
    </div>`;
  });
  $("dq-dist-bars").innerHTML = html;
}

function round(v, d) { return Math.round(v * Math.pow(10, d)) / Math.pow(10, d); }

function dqBadge(quality) {
  const map = {
    "OPTIMAL": "dq-optimal", "EARLY": "dq-early",
    "LATE": "dq-late", "STOPPED": "dq-stopped", "NEUTRAL": "dq-neutral",
  };
  const cls = map[quality] || "dq-neutral";
  return `<span class="dq-badge ${cls}">${quality || '—'}</span>`;
}

function renderDQModeTable(modeQ) {
  if (!modeQ || !Object.keys(modeQ).length) return;
  let html = "";
  Object.entries(modeQ).sort((a, b) => b[1].trades - a[1].trades).forEach(([mode, ms]) => {
    html += `<tr>
      <td><strong>${mode}</strong></td>
      <td>${ms.trades}</td>
      <td class="pos">${ms.optimal}</td>
      <td class="neu">${ms.early}</td>
      <td class="neg">${ms.late}</td>
      <td>${ms.stopped}</td>
      <td>${fmt(ms.precision, 0)}%</td>
      <td>${fmt(ms.discipline, 0)}%</td>
    </tr>`;
  });
  $("dq-mode-body").innerHTML = html;
}

function renderDQTrades(trades) {
  if (!trades || !trades.length) return;
  let html = "";
  trades.forEach(t => {
    const gapCls = t.return_gap != null ? (t.return_gap >= 0 ? "gap-pos" : "gap-neg") : "";
    const winCell = t.win === true ? '<span class="win-y">W</span>' :
                    t.win === false ? '<span class="win-n">L</span>' : '—';
    html += `<tr>
      <td>${t.id}</td>
      <td><strong>${t.ticker}</strong></td>
      <td>${t.mode}</td>
      <td>${t.expected_return != null ? fmt(t.expected_return, 2) + '%' : '—'}</td>
      <td class="${pctClass(t.actual_return)}">${t.actual_return != null ? fmt(t.actual_return, 2) + '%' : '—'}</td>
      <td class="${gapCls}">${t.return_gap != null ? (t.return_gap >= 0 ? '+' : '') + fmt(t.return_gap, 2) + '%' : '—'}</td>
      <td>${dqBadge(t.exit_quality)}</td>
      <td>${winCell}</td>
    </tr>`;
  });
  $("dq-trade-body").innerHTML = html;
}

function copyDQ() {
  if (!lastDQ || !lastDQ.formatted) {
    setEVAStatus("No decision quality data", false);
    return;
  }
  navigator.clipboard.writeText(lastDQ.formatted);
  setEVAStatus("Decision quality report copied", true);
}

/* ── Copy EVA report ─────────────────────────────────────────── */

function copyEVA() {
  if (!lastEVA || lastEVA.total === 0) {
    setEVAStatus("No data to copy", false);
    return;
  }
  const d = lastEVA;
  let lines = ["=== MONOS EXPECTED vs ACTUAL ===", ""];
  lines.push(`Closed Trades: ${d.total}`);
  lines.push(`Win Rate: ${fmt(d.win_rate, 1)}%`);
  lines.push(`Beat Rate: ${fmt(d.beat_rate, 1)}% (trades that beat expectations)`);
  lines.push(`Avg Expected: ${d.avg_expected != null ? fmt(d.avg_expected, 2) + '%' : '—'}`);
  lines.push(`Avg Actual: ${d.avg_actual != null ? fmt(d.avg_actual, 2) + '%' : '—'}`);
  lines.push(`Avg Gap: ${d.avg_gap != null ? fmt(d.avg_gap, 2) + '%' : '—'}`);
  lines.push(`Best Mode: ${d.best_mode || '—'}`);
  lines.push(`Worst Mode: ${d.worst_mode || '—'}`);
  lines.push("");
  lines.push("Mode Breakdown:");
  for (const [mode, ms] of Object.entries(d.mode_stats)) {
    lines.push(`  ${mode}: ${ms.trades} trades | WR ${fmt(ms.win_rate, 0)}% | Exp ${ms.avg_expected != null ? fmt(ms.avg_expected, 1) : '—'}% | Act ${ms.avg_actual != null ? fmt(ms.avg_actual, 1) : '—'}% | Gap ${ms.avg_gap != null ? fmt(ms.avg_gap, 1) : '—'}% | Tot ${fmt(ms.total_return, 1)}% | ${ms.rating}`);
  }
  lines.push("");
  lines.push("Trades:");
  d.trades.forEach(t => {
    const gapStr = t.gap != null ? (t.gap >= 0 ? '+' : '') + fmt(t.gap, 1) + '%' : '—';
    lines.push(`  #${t.id} ${t.ticker.padEnd(4)} ${t.mode.padEnd(16)} Exp:${t.expected_return != null ? fmt(t.expected_return, 1) + '%' : '—'} Act:${t.actual_return != null ? fmt(t.actual_return, 1) + '%' : '—'} Gap:${gapStr} ${t.win ? 'W' : 'L'}`);
  });
  lines.push("");
  lines.push("Generated by MONOS Conviction Engine");

  navigator.clipboard.writeText(lines.join("\n"));
  setEVAStatus("EVA report copied to clipboard", true);
}
