/** Bumped with index.html `?v=` and `<meta name="nelson4-management-ui-build">` — if this log mismatches DevTools Network, you are not serving this package build. */
console.info("[operator UI] static build v=100");
/** Scheduler rows no longer use this; kept so any cached bundle that still interpolates `${prepareDbBtn}` does not throw. */
var prepareDbBtn = "";

const $ = (sel) => document.querySelector(sel);

async function fetchJSON(url, opts) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 20000);
  try {
    const r = await fetch(url, {
      cache: "no-store",
      ...opts,
      signal: ctrl.signal,
    });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  } finally {
    clearTimeout(t);
  }
}

/** FastAPI-style JSON error body: detail string, list of {msg}, or arbitrary object. */
function formatApiErrorBody(data) {
  if (!data || typeof data !== "object") return "";
  const d = data.detail;
  if (typeof d === "string") return d;
  if (Array.isArray(d)) {
    return d
      .map((x) => (x && typeof x.msg === "string" ? x.msg : JSON.stringify(x)))
      .join("; ");
  }
  if (d != null) return JSON.stringify(d);
  if (data.error) return String(data.error);
  return JSON.stringify(data);
}

async function fetchSchedulerTasksJSON() {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 20000);
  try {
    const r = await fetch("/api/v1/scheduler/tasks", { signal: ctrl.signal });
    const text = await r.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = {};
    }
    if (!r.ok) {
      throw new Error(formatApiErrorBody(data) || `HTTP ${r.status}`);
    }
    return data;
  } finally {
    clearTimeout(t);
  }
}

/** Human-readable scheduler interval; raw seconds in title attribute where used. */
function formatFrequencyHuman(sec) {
  const s = Number(sec);
  if (!Number.isFinite(s) || s < 0) return String(sec);
  if (s < 60) return `${s} s`;
  if (s < 3600) {
    const m = Math.round(s / 60);
    return `${m} min`;
  }
  if (s < 86400) {
    const h = s / 3600;
    return `${h % 1 === 0 ? h : h.toFixed(1)} h`;
  }
  const d = s / 86400;
  return `${d % 1 === 0 ? d : d.toFixed(1)} d`;
}

const CHECK_LABELS = {
  postgres: "PostgreSQL (database connectivity)",
  message_bus: "RabbitMQ (AMQP connectivity)",
  scheduler: "Scheduler (control-plane periodic loop)",
  management_api: "Management API",
  extension_trading_broker: "Trading broker (worker extension probe)",
  extension_market_strategy: "Market strategy (worker extension probe)",
};

function overallLabel(status) {
  if (status === "healthy") return "All systems operational";
  if (status === "degraded") return "Degraded — some checks failing";
  return "Critical — multiple failures";
}

function renderHealth(data) {
  const el = $("#health-panel");
  if (!el) return;
  const overall = data.status || "unhealthy";
  el.innerHTML = `
    <div class="health-overall">
      <div class="traffic-light-stack" data-state="${escapeHtml(overall)}" title="${escapeHtml(overall)}">
        <span class="tl-bulb red"></span>
        <span class="tl-bulb amber"></span>
        <span class="tl-bulb green"></span>
      </div>
      <div class="health-overall-text">
        <div class="health-overall-title">Overall status</div>
        <div class="health-overall-sub">${escapeHtml(overallLabel(overall))}</div>
      </div>
    </div>
    <ul class="health-check-list" role="list">
      ${renderCheckItems(data.checks || {})}
    </ul>
  `;
}

function formatHealthTime(iso) {
  if (!iso) return null;
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso).slice(0, 19) + "Z";
    return d.toLocaleString(undefined, { dateStyle: "short", timeStyle: "medium" });
  } catch {
    return String(iso);
  }
}

/** Stable order; old APIs may omit `scheduler` — we inject a fallback row. */
const HEALTH_CHECK_ORDER = [
  "postgres",
  "message_bus",
  "scheduler",
  "management_api",
  "extension_trading_broker",
  "extension_market_strategy",
];

function renderCheckItems(checks) {
  const c = { ...(checks || {}) };
  if (!c.scheduler) {
    c.scheduler = {
      ok: false,
      error:
        "Scheduler check missing from API — rebuild/restart the Management API container (or hard-refresh the page).",
    };
  }

  return HEALTH_CHECK_ORDER.filter((k) => c[k] != null)
    .map((key) => {
      const v = c[key];
      const ok = Boolean(v && v.ok === true);
      const failed = Boolean(v && v.ok === false);
      /** API uses ``ok: null`` + optional ``skipped`` when a probe is disabled or not applicable. */
      const skipped = Boolean(v && v.ok !== true && v.ok !== false);
      const err = v && v.error;
      const label = CHECK_LABELS[key] || key.replace(/_/g, " ");
      const state = ok ? "up" : skipped ? "skip" : "down";
      const pill = ok
        ? '<span class="health-status-pill ok">OK</span>'
        : skipped
          ? '<span class="health-status-pill skip">N/A</span>'
          : '<span class="health-status-pill bad">FAIL</span>';

      let extraRight = "";
      if (key === "scheduler") {
        const t = formatHealthTime(v && v.last_tick_at);
        const phase = v && v.last_phase;
        const tc = v && v.task_count != null ? Number(v.task_count) : null;
        const tasksPart =
          tc !== null && Number.isFinite(tc)
            ? `<span class="muted">${escapeHtml(String(tc))} task(s) configured</span>`
            : "";
        if (ok) {
          const timePart = t
            ? `<span class="health-last-meta">Loop heartbeat: <strong>${escapeHtml(t)}</strong></span>`
            : `<span class="health-last-meta muted">Loop heartbeat: <strong>pending</strong></span>`;
          const phasePart =
            phase != null && phase !== ""
              ? `<span class="health-phase muted">(${escapeHtml(String(phase))})</span>`
              : "";
          extraRight = `<span class="health-scheduler-extra">${timePart}${tasksPart ? ` · ${tasksPart}` : ""}${phasePart}</span>`;
        }
      }

      if (key.startsWith("extension_") && v && v.detail) {
        extraRight = `<span class="health-last-meta${skipped ? " muted" : ""}">${escapeHtml(
          String(v.detail),
        )}</span>`;
      }

      const errLine =
        failed && err ? `<div class="health-check-error">${escapeHtml(err)}</div>` : "";

      return `<li class="health-check-row ${state}">
        <span class="health-led" aria-hidden="true"></span>
        <div class="health-check-body">
          <div class="health-check-line">
            <span class="health-check-name" title="${escapeHtml(label)}">${escapeHtml(label)}</span>
            <span class="health-check-pill-wrap">${pill}</span>
            <span class="health-check-extra">${extraRight}</span>
          </div>
          ${errLine}
        </div>
      </li>`;
    })
    .join("");
}

function statusBadge(ok) {
  return ok
    ? '<span class="badge ok">ok</span>'
    : '<span class="badge bad">fail</span>';
}

function applyOperatorVersionFromHealth(data) {
  const span = $("#operator-version");
  if (!span) return;
  const op = data && data.operator;
  if (!op || typeof op !== "object") {
    span.textContent = "";
    return;
  }
  const cp = op.control_plane != null && String(op.control_plane).trim() !== "" ? String(op.control_plane).trim() : null;
  const ui = op.management_ui != null && String(op.management_ui).trim() !== "" ? String(op.management_ui).trim() : null;
  const parts = [];
  if (cp) parts.push(`control-plane ${cp}`);
  if (ui) parts.push(`management-ui ${ui}`);
  span.textContent = parts.length ? `(${parts.join(" · ")})` : "";
}

async function loadHealth() {
  const el = $("#health-panel");
  if (!el) return;
  try {
    const data = await fetchJSON("/api/v1/health/detailed");
    applyOperatorVersionFromHealth(data);
    renderHealth(data);
  } catch (e) {
    const msg =
      e.name === "AbortError"
        ? "Request timed out (20s). Is the API reachable?"
        : e.message || String(e);
    el.innerHTML = `<p class="health-error">Could not load health: ${escapeHtml(msg)}</p>`;
  }
}

function renderSchedulerRow(c, sched) {
  const cmds = (sched && sched.commands) || [];
  const opts = cmds.length
    ? cmds
        .map(
          (cmd) =>
            `<option value="${escapeHtml(cmd.id)}" title="${escapeHtml(cmd.description || "")}">${escapeHtml(cmd.label)}</option>`
        )
        .join("")
    : `<option value="status">Status (only)</option>`;
  const st = sched && sched.status ? sched.status : {};
  const last =
    st.last_command_at && st.last_command
      ? `${escapeHtml(st.last_command)} @ ${escapeHtml(String(st.last_command_at).slice(0, 19))}Z`
      : "—";
  const paused = st.paused ? " (paused)" : "";
  return `<li class="component-row scheduler-built-in enabled" data-component-id="${escapeHtml(c.component_id)}">
    <span class="component-led" aria-hidden="true"></span>
    <div class="component-body component-body-wide">
      <div class="component-id">${escapeHtml(c.component_id)} <span class="badge builtin">built-in</span></div>
      <div class="component-meta">
        <span class="component-phase">${escapeHtml(c.phase)}</span>
        <span class="component-flag">${c.enabled ? "Enabled" : "Disabled"}</span>
      </div>
      <div class="scheduler-controls">
        <label class="sr-only" for="scheduler-command-select">Command</label>
        <select id="scheduler-command-select" class="scheduler-select" aria-label="Scheduler command">
          ${opts}
        </select>
        <button type="button" class="scheduler-exec">Execute</button>
        <pre class="scheduler-result" aria-live="polite"></pre>
      </div>
      <p class="muted scheduler-hint">Last: ${last}${escapeHtml(paused)}</p>
    </div>
  </li>`;
}

function renderRegistryRow(c) {
  const catalog = String(c.registry_display || "").toLowerCase() === "catalog";
  const enabled = !!c.enabled;
  const stateClass = catalog
    ? "registry-component--catalog"
    : enabled
      ? "up"
      : "registry-component--disabled";
  const pill = catalog
    ? '<span class="health-status-pill skip">Catalog</span>'
    : enabled
      ? '<span class="health-status-pill ok">Enabled</span>'
      : '<span class="health-status-pill bad">Disabled</span>';
  const phase = String(c.phase || "").replace(/_/g, " ");
  const impl = String(c.implementation_label || "").trim();
  const pkg = String(c.implementation_package || "").trim();
  const pkgTitle = pkg ? escapeAttr(pkg) : "";
  const extId = String(c.extension_id || "").trim();
  const extVer = String(c.extension_version || "").trim();
  const titleBits = [String(c.component_id || "").trim()];
  if (extId) titleBits.push(`IoC: ${extId}`);
  if (pkg) titleBits.push(pkg);
  const title = escapeAttr(titleBits.filter(Boolean).join(" · "));
  const verAfterImpl = extVer
    ? ` <span class="muted registry-component-version" title="TradeStation extension package version">(${escapeHtml(
        extVer
      )})</span>`
    : "";
  let extra = "";
  if (impl) {
    extra = `<span class="registry-impl-wrap" ${pkgTitle ? `title="${pkgTitle}"` : ""}><strong class="registry-impl">${escapeHtml(
      impl
    )}</strong>${verAfterImpl}`;
    if (phase) {
      extra += `<span class="muted registry-phase"> · ${escapeHtml(phase)}</span>`;
    }
    extra += "</span>";
  } else if (phase) {
    extra = `<span class="muted registry-phase">${escapeHtml(phase)}</span>`;
    if (extVer) {
      extra += verAfterImpl;
    }
  } else if (extVer) {
    extra = `<span class="registry-impl-wrap">${verAfterImpl.trimStart()}</span>`;
  }
  return `<li class="health-check-row registry-component ${stateClass}" title="${title}">
    <span class="health-led" aria-hidden="true"></span>
    <div class="health-check-body">
      <div class="health-check-line">
        <span class="health-check-name">${escapeHtml(c.component_id)}</span>
        <span class="health-check-pill-wrap">${pill}</span>
        <span class="health-check-extra">${extra}</span>
      </div>
    </div>
  </li>`;
}

const BUILTIN_SCHEDULER = {
  component_id: "management-scheduler",
  extension_id: null,
  phase: "management",
  enabled: true,
  registered_at: null,
  built_in: true,
};

function isSchedulerComponent(c) {
  if (!c || !c.component_id) return false;
  const id = String(c.component_id).toLowerCase();
  return id === "management-scheduler" || id === "scheduler";
}

/** Scheduler command dropdown + Execute (Scheduler tab). */
async function loadSchedulerManagement() {
  const el = $("#scheduler-management-out");
  if (!el) return;
  let sched = null;
  try {
    sched = await fetchJSON("/api/v1/scheduler");
  } catch (e) {
    sched = null;
  }
  const row = renderSchedulerRow(BUILTIN_SCHEDULER, sched);
  el.innerHTML = `<ul class="component-list" role="list">${row}</ul>`;
}

async function loadComponents() {
  const el = $("#components-out");
  if (!el) return;
  const setListHtml = (list, data) => {
    const filtered = list.filter((c) => c && !isSchedulerComponent(c));
    if (filtered.length === 0) {
      el.innerHTML =
        '<p class="muted">No components in the stub list yet. Extension registry integration is pending.</p>';
      return;
    }
    const rows = filtered.map((c) => renderRegistryRow(c)).join("");
    const note =
      data && data.note
        ? `<p class="muted component-catalog-note" role="note">${escapeHtml(String(data.note))}</p>`
        : "";
    const banner =
      data && data.registry_error
        ? `<p class="component-api-banner" role="status">Registry list partial: ${escapeHtml(
            String(data.registry_error)
          )}</p>`
        : "";
    el.innerHTML = `${note}${banner}<ul class="health-check-list registry-component-list" role="list">${rows}</ul>`;
  };
  try {
    const data = await fetchJSON("/api/v1/components");
    const list = Array.isArray(data.components) ? [...data.components] : [];
    setListHtml(list, data);
  } catch (e) {
    const msg =
      e.name === "AbortError"
        ? "Request timed out (20s). Is the API reachable?"
        : e.message || String(e);
    el.innerHTML = `<p class="component-api-banner warn">Could not load <code>/api/v1/components</code>: ${escapeHtml(
      msg
    )}</p>`;
  }
}

async function executeSchedulerCommand(command, payload) {
  const r = await fetchJSON("/api/v1/scheduler/command", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ command, payload: payload || {}, issued_by: "ui" }),
  });
  return r;
}

async function refreshSchedulerHint() {
  try {
    const r = await fetchJSON("/api/v1/scheduler");
    const row = document.querySelector(".scheduler-built-in");
    const hint = row?.querySelector(".scheduler-hint");
    if (!hint || !r.status) return;
    const st = r.status;
    const last =
      st.last_command_at && st.last_command
        ? `${st.last_command} @ ${String(st.last_command_at).slice(0, 19)}Z`
        : "—";
    const paused = st.paused ? " (paused)" : "";
    hint.textContent = `Last: ${last}${paused}`;
  } catch {
    /* ignore */
  }
}

function escapeHtml(s) {
  if (s == null) return "";
  const d = document.createElement("div");
  d.textContent = String(s);
  return d.innerHTML;
}

function escapeAttr(s) {
  if (s == null) return "";
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;");
}

/** Last successful trading-desk summary (probe); used to redraw live positions without re-parsing the pre). */
let _lastTradingDeskSummary = null;

/** ``setInterval`` id for Trading desk persistence auto-refresh (cleared when leaving tab or disabling). */
let _tdPersistIntervalHandle = null;

/** In-memory store for trading-desk table JSON cells (large payloads only; small cells use data-json-b64). */
const _tdJsonViewStore = new Map();
let _tdJsonViewSeq = 0;

/** Max JSON string length to embed as base64 on the button (avoids huge attributes). */
const _TD_JSON_B64_MAX = 56000;

function _tdJsonEncodeB64(obj) {
  const s = JSON.stringify(obj);
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin);
}

function _tdJsonPayloadFromButton(btn) {
  const b64 = btn.getAttribute("data-json-b64");
  if (b64) {
    try {
      const bin = atob(b64);
      const bytes = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
      return JSON.parse(new TextDecoder().decode(bytes));
    } catch (e) {
      console.error("[operator UI] td-json-b64 decode failed", e);
    }
  }
  const id = btn.getAttribute("data-json-id");
  if (id != null) {
    const p = _tdJsonViewStore.get(id);
    if (p !== undefined) return p;
  }
  return undefined;
}

function ensureTdJsonViewDelegation() {
  if (document.documentElement.dataset.tdJsonViewBound === "1") return;
  document.documentElement.dataset.tdJsonViewBound = "1";
  document.addEventListener(
    "click",
    (e) => {
      const t = e.target;
      const el = t && t.nodeType === Node.TEXT_NODE ? t.parentElement : t;
      const btn = el && el.closest ? el.closest(".td-json-view-btn") : null;
      if (!btn) return;
      e.preventDefault();
      const label = btn.getAttribute("data-json-label") || "JSON";
      const payload = _tdJsonPayloadFromButton(btn);
      if (payload === undefined) {
        console.warn("[operator UI] View JSON: no payload (reload persistence or refresh table)");
        return;
      }
      openTdJsonModal(label, payload);
    },
    true
  );
}

function openTdJsonModal(title, payload) {
  const dlg = $("#dialog-td-json");
  const titleEl = $("#dialog-td-json-title");
  const pre = $("#td-json-modal-pre");
  if (!dlg || !pre) {
    try {
      alert(JSON.stringify(payload, null, 2));
    } catch {
      alert(String(payload));
    }
    return;
  }
  if (titleEl) titleEl.textContent = title || "JSON";
  try {
    pre.textContent = JSON.stringify(payload, null, 2);
  } catch {
    pre.textContent = String(payload);
  }
  try {
    if (typeof dlg.showModal === "function") {
      dlg.showModal();
    } else {
      dlg.setAttribute("open", "");
    }
  } catch (err) {
    console.error("[operator UI] dialog showModal failed", err);
    try {
      alert(JSON.stringify(payload, null, 2));
    } catch {
      alert(String(payload));
    }
  }
}

function tdJsonViewCellHtml(value, columnKey) {
  if (value === null || value === undefined) return "";
  if (typeof value === "string" && value.trim() === "[object Object]") {
    return '<span class="muted" title="Bad string in DB or old UI">(invalid object string)</span>';
  }
  if (typeof value === "object") {
    const raw = JSON.stringify(value);
    if (raw.length <= _TD_JSON_B64_MAX) {
      const b64 = _tdJsonEncodeB64(value);
      return `<button type="button" class="td-json-view-btn td-json-view-btn--primary" data-json-b64="${escapeAttr(
        b64
      )}" data-json-label="${escapeAttr(columnKey)}">View JSON</button>`;
    }
    const id = `tdjv-${++_tdJsonViewSeq}`;
    _tdJsonViewStore.set(id, value);
    return `<button type="button" class="td-json-view-btn td-json-view-btn--primary" data-json-id="${escapeAttr(
      id
    )}" data-json-label="${escapeAttr(columnKey)}">View JSON (large)</button>`;
  }
  if (typeof value === "string") {
    const t = value.trim();
    if (
      (t.startsWith("{") && t.endsWith("}")) ||
      (t.startsWith("[") && t.endsWith("]"))
    ) {
      try {
        const parsed = JSON.parse(value);
        const raw = JSON.stringify(parsed);
        if (raw.length <= _TD_JSON_B64_MAX) {
          const b64 = _tdJsonEncodeB64(parsed);
          return `<button type="button" class="td-json-view-btn td-json-view-btn--primary" data-json-b64="${escapeAttr(
            b64
          )}" data-json-label="${escapeAttr(columnKey)}">View JSON</button>`;
        }
        const id = `tdjv-${++_tdJsonViewSeq}`;
        _tdJsonViewStore.set(id, parsed);
        return `<button type="button" class="td-json-view-btn td-json-view-btn--primary" data-json-id="${escapeAttr(
          id
        )}" data-json-label="${escapeAttr(columnKey)}">View JSON (large)</button>`;
      } catch {
        /* fall through */
      }
    }
  }
  return escapeHtml(String(value));
}

/**
 * @param {object[]} rows
 * @param {HTMLElement | null} containerEl
 * @param {{ skipClear?: boolean, columnLabels?: Record<string, string> }} [options] When rendering two tables in one batch (orders + positions), pass skipClear on the second call after clearing once in the caller. Optional columnLabels overrides header text for matching row keys.
 */
function renderTradingDeskTable(rows, containerEl, options) {
  if (!containerEl) return;
  ensureTdJsonViewDelegation();
  const skipClear = options && options.skipClear;
  const columnLabels = options && options.columnLabels ? options.columnLabels : null;
  if (!skipClear) {
    _tdJsonViewStore.clear();
    _tdJsonViewSeq = 0;
  }
  if (!rows || !rows.length) {
    containerEl.innerHTML = "<p class=\"muted\">No rows.</p>";
    containerEl.className = "td-table-wrap muted";
    return;
  }
  containerEl.className = "td-table-wrap";
  const keys = Object.keys(rows[0]);
  let html =
    '<table class="st-task-table"><thead><tr>' +
    keys
      .map((k) => {
        const label = columnLabels && columnLabels[k] != null ? columnLabels[k] : k;
        return `<th>${escapeHtml(String(label))}</th>`;
      })
      .join("") +
    "</tr></thead><tbody>";
  for (const row of rows) {
    html +=
      "<tr>" +
      keys
        .map((k) => `<td>${tdJsonViewCellHtml(row[k], k)}</td>`)
        .join("") +
      "</tr>";
  }
  html += "</tbody></table>";
  containerEl.innerHTML = html;
}

function renderTradingDeskBrokerPositions(summary) {
  const out = $("#td-broker-positions-out");
  const sel = $("#td-account-id");
  const manual = String($("#td-account-id-manual")?.value || "").trim();
  if (!out) return;
  const accountId = manual || String(sel?.value || "").trim();
  const probe =
    summary &&
    summary.trading_broker_probe &&
    summary.trading_broker_probe.trading_broker &&
    summary.trading_broker_probe.trading_broker.tradestation_brokerage;
  const byAccount = probe && probe.positions_by_account && typeof probe.positions_by_account === "object"
    ? probe.positions_by_account
    : {};
  if (!accountId) {
    out.className = "td-table-wrap muted";
    out.innerHTML = "<p class=\"muted\">Choose an account to view live broker positions.</p>";
    return;
  }
  const accountData = byAccount[accountId];
  if (!accountData || !Array.isArray(accountData.positions)) {
    out.className = "td-table-wrap muted";
    out.innerHTML = `<p class="muted">No live positions returned for account <code>${escapeHtml(accountId)}</code>.</p>`;
    return;
  }
  renderTradingDeskTable(accountData.positions, out, { skipClear: true });
}

function tradingDeskAccountOptionsFromSummary(summary) {
  const probe = summary && summary.trading_broker_probe;
  const rootCandidates = [
    probe &&
      probe.trading_broker &&
      probe.trading_broker.tradestation_brokerage &&
      probe.trading_broker.tradestation_brokerage.accounts,
    probe && probe.tradestation_brokerage && probe.tradestation_brokerage.accounts,
    summary &&
      summary.trading_broker &&
      summary.trading_broker.tradestation_brokerage &&
      summary.trading_broker.tradestation_brokerage.accounts,
    summary && summary.tradestation_brokerage && summary.tradestation_brokerage.accounts,
  ];
  const root = rootCandidates.find((x) => Array.isArray(x));
  if (!Array.isArray(root)) return [];
  return root
    .filter((x) => x && typeof x === "object")
    .map((x) => {
      const accountId =
        x.account_id != null && String(x.account_id).trim() !== ""
          ? String(x.account_id).trim()
          : x.AccountID != null && String(x.AccountID).trim() !== ""
            ? String(x.AccountID).trim()
          : "";
      if (!accountId) return null;
      const type = x.account_type != null ? String(x.account_type).trim() : "";
      const alias = x.alias != null ? String(x.alias).trim() : "";
      const status = x.status != null ? String(x.status).trim() : "";
      const currency = x.currency != null ? String(x.currency).trim() : "";
      const extras = [type, alias, status, currency].filter(Boolean).join(" | ");
      return {
        accountId,
        accountType: type,
        status,
        label: extras ? `${accountId} — ${extras}` : accountId,
      };
    })
    .filter(Boolean);
}

function populateTradingDeskAccountSelect(summary) {
  const sel = $("#td-account-id");
  const help = $("#td-account-help");
  if (!sel) return;
  const prev = String(sel.value || "").trim();
  const opts = tradingDeskAccountOptionsFromSummary(summary);
  if (!opts.length) {
    sel.innerHTML =
      '<option value="">No accounts from probe (enter manual AccountID below)</option>';
    if (help) {
      help.textContent =
        "No brokerage accounts parsed from probe response. You can still enter AccountID manually.";
    }
    return;
  }
  sel.innerHTML = opts
    .map((o) => `<option value="${escapeAttr(o.accountId)}">${escapeHtml(o.label)}</option>`)
    .join("");
  const hasPrev = opts.some((o) => o.accountId === prev);
  if (hasPrev) {
    sel.value = prev;
  } else {
    const preferred = opts.find(
      (o) =>
        String(o.status || "").toLowerCase() === "active" &&
        String(o.accountType || "").toLowerCase() !== "futures"
    );
    sel.value = (preferred || opts[0]).accountId;
  }
  if (help) {
    help.textContent = `Loaded ${opts.length} account(s). Each row shows AccountID and broker metadata (type, alias, status, currency).`;
  }
}

function tradingDeskSummaryDataForRefresh() {
  if (_lastTradingDeskSummary && typeof _lastTradingDeskSummary === "object") {
    return _lastTradingDeskSummary;
  }
  const out = $("#td-summary-out");
  if (!out) return null;
  try {
    return JSON.parse(out.textContent || "{}");
  } catch {
    return null;
  }
}

async function loadTradingDeskSummary() {
  const out = $("#td-summary-out");
  const rk = $("#td-routing-key-out");
  if (out) {
    out.textContent = "Loading…";
    out.className = "td-json-out";
  }
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 60000);
  try {
    const r = await fetch("/api/v1/trading-desk/summary", {
      signal: ctrl.signal,
      cache: "no-store",
    });
    const text = await r.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = {};
    }
    if (!r.ok) {
      if (out) {
        out.textContent = formatApiErrorBody(data) || text || `HTTP ${r.status}`;
        out.className = "td-json-out bad";
      }
      return;
    }
    _lastTradingDeskSummary = data;
    if (rk) rk.textContent = data.place_order_routing_key || "(unknown)";
    populateTradingDeskAccountSelect(data);
    renderTradingDeskBrokerPositions(data);
    if (out) {
      out.className = "td-json-out";
      out.textContent = JSON.stringify(data, null, 2);
    }
  } catch (e) {
    if (out) {
      const msg = e && e.name === "AbortError" ? "timeout (try again)" : e.message;
      out.textContent = "Error: " + msg;
      out.className = "td-json-out bad";
    }
  } finally {
    clearTimeout(t);
  }
}

function tradingDeskPersistenceLimit() {
  const el = $("#td-persist-limit");
  let n = parseInt(String(el?.value ?? "5"), 10);
  if (Number.isNaN(n) || n < 1) n = 5;
  if (n > 500) n = 500;
  if (el) el.value = String(n);
  return n;
}

function tradingDeskPersistenceOffset() {
  const el = $("#td-persist-offset");
  let n = parseInt(String(el?.value ?? "0"), 10);
  if (Number.isNaN(n) || n < 0) n = 0;
  if (n > 1000000) n = 1000000;
  if (el) el.value = String(n);
  return n;
}

function tradingDeskPersistencePollSeconds() {
  const el = $("#td-persist-poll-sec");
  let n = parseInt(String(el?.value ?? "10"), 10);
  if (Number.isNaN(n) || n < 1) n = 10;
  if (n > 3600) n = 3600;
  if (el) el.value = String(n);
  return n;
}

function stopTradingDeskPersistencePoll() {
  if (_tdPersistIntervalHandle != null) {
    clearInterval(_tdPersistIntervalHandle);
    _tdPersistIntervalHandle = null;
  }
}

/** Start auto-refresh when the Trading desk tab is active and the checkbox is on. */
function startTradingDeskPersistencePoll() {
  stopTradingDeskPersistencePoll();
  const panel = $("#panel-trading-desk");
  const auto = $("#td-persist-auto");
  if (!panel || !panel.classList.contains("active")) return;
  if (!auto || !auto.checked) return;
  const sec = tradingDeskPersistencePollSeconds();
  _tdPersistIntervalHandle = setInterval(() => {
    void loadTradingDeskPersistence();
  }, sec * 1000);
}

async function loadTradingDeskPersistence() {
  const strategyEl = $("#td-strategy-orders-out");
  const ordersEl = $("#td-orders-out");
  const posEl = $("#td-positions-out");
  const histEl = $("#td-position-history-out");
  const metaEl = $("#td-persist-meta");
  const lim = tradingDeskPersistenceLimit();
  const off = tradingDeskPersistenceOffset();
  if (strategyEl) strategyEl.textContent = "Loading…";
  if (ordersEl) ordersEl.textContent = "Loading…";
  if (posEl) posEl.textContent = "Loading…";
  if (histEl) histEl.textContent = "Loading…";
  if (metaEl) metaEl.textContent = "";
  try {
    const data = await fetchJSON(
      `/api/v1/trading-desk/persistence?limit=${lim}&offset=${off}`
    );
    if (data && data.ok === false) {
      const err =
        (data.error && String(data.error)) ||
        "Persistence query failed (see control-plane logs).";
      const msg = `Error: ${err}`;
      if (strategyEl) {
        strategyEl.textContent = msg;
        strategyEl.className = "td-table-wrap muted";
      }
      if (ordersEl) {
        ordersEl.textContent = msg;
        ordersEl.className = "td-table-wrap muted";
      }
      if (posEl) {
        posEl.textContent = msg;
        posEl.className = "td-table-wrap muted";
      }
      if (histEl) {
        histEl.textContent = msg;
        histEl.className = "td-table-wrap muted";
      }
      return;
    }
    _tdJsonViewStore.clear();
    _tdJsonViewSeq = 0;
    const strat = Array.isArray(data.strategy_order_requests) ? data.strategy_order_requests : [];
    renderTradingDeskTable(strat, strategyEl, { skipClear: false });
    renderTradingDeskTable(data.orders || [], ordersEl, { skipClear: true });
    renderTradingDeskTable(data.positions || [], posEl, { skipClear: true });
    const hist = Array.isArray(data.position_history) ? data.position_history : [];
    renderTradingDeskTable(hist, histEl, { skipClear: true });
    const pg = data.paging;
    if (metaEl && pg && pg.totals && typeof pg.totals === "object") {
      const t = pg.totals;
      const fmt = (k) =>
        typeof t[k] === "number" ? String(t[k]) : "—";
      metaEl.textContent =
        `limit ${lim}, offset ${off} · row totals — strategy ${fmt("strategy_order_requests")}, ` +
        `orders ${fmt("orders")}, positions ${fmt("positions")}, history ${fmt("position_history")}`;
    }
  } catch (e) {
    const msg = "Error: " + e.message;
    if (strategyEl) {
      strategyEl.textContent = msg;
      strategyEl.className = "td-table-wrap muted";
    }
    if (ordersEl) {
      ordersEl.textContent = msg;
      ordersEl.className = "td-table-wrap muted";
    }
    if (posEl) {
      posEl.textContent = msg;
      posEl.className = "td-table-wrap muted";
    }
    if (histEl) {
      histEl.textContent = msg;
      histEl.className = "td-table-wrap muted";
    }
  }
}

function stockHistoryLimit() {
  const el = $("#sh-limit");
  let n = parseInt(String(el?.value ?? "50"), 10);
  if (Number.isNaN(n) || n < 1) n = 50;
  if (n > 500) n = 500;
  if (el) el.value = String(n);
  return n;
}

/**
 * Derive stock-history table columns from event detail JSON (only non-empty when a matching field exists).
 * @param {unknown} detail
 * @returns {{ position: string, quantity: string, status: string }}
 */
function stockHistoryDerivedFromDetail(detail) {
  const empty = { position: "", quantity: "", status: "" };
  if (!detail || typeof detail !== "object") return empty;
  const d = /** @type {Record<string, unknown>} */ (detail);

  let position = "";
  if (d.long_short != null && String(d.long_short).trim() !== "") {
    position = String(d.long_short);
  } else if (d.side != null && String(d.side).trim() !== "") {
    position = String(d.side);
  } else if (d.position != null && String(d.position).trim() !== "") {
    position = String(d.position);
  } else if (d.trade_action != null && String(d.trade_action).trim() !== "") {
    position = String(d.trade_action);
  }

  let quantity = "";
  if (d.quantity != null && d.quantity !== "") {
    quantity = String(d.quantity);
  } else {
    const req = d.quantity_requested;
    const fil = d.quantity_filled;
    const hasReq = req != null && req !== "";
    const hasFil = fil != null && fil !== "";
    if (hasFil && hasReq) {
      quantity = `${fil} / ${req}`;
    } else if (hasFil) {
      quantity = String(fil);
    } else if (hasReq) {
      quantity = String(req);
    }
  }

  let status = "";
  if (d.status != null && d.status !== "") {
    status = String(d.status);
  }

  return { position, quantity, status };
}

async function loadStockHistoryCorrelationIds() {
  const symEl = $("#sh-symbol");
  const cidEl = $("#sh-correlation-id");
  if (!cidEl) return;
  const previous = String(cidEl.value || "").trim();
  cidEl.innerHTML = '<option value="">— (any)</option>';
  const sym = String(symEl?.value || "").trim().toUpperCase();
  if (!sym) {
    return;
  }
  cidEl.disabled = true;
  try {
    const params = new URLSearchParams({ symbol: sym });
    const data = await fetchJSON(`/api/v1/trading-desk/stock-history/correlation-ids?${params}`);
    if (!data || data.ok !== true || !Array.isArray(data.correlation_ids)) {
      return;
    }
    for (const id of data.correlation_ids) {
      const o = document.createElement("option");
      o.value = String(id);
      o.textContent = String(id);
      cidEl.appendChild(o);
    }
    if (previous && [...cidEl.options].some((op) => op.value === previous)) {
      cidEl.value = previous;
    }
  } catch {
    /* keep — (any) only */
  } finally {
    cidEl.disabled = false;
  }
}

async function loadStockHistorySymbols() {
  const sel = $("#sh-symbol");
  if (!sel) return;
  sel.innerHTML = '<option value="">—</option>';
  try {
    const data = await fetchJSON("/api/v1/trading-desk/stock-history/symbols");
    if (!data || data.ok !== true || !Array.isArray(data.symbols)) {
      sel.innerHTML = '<option value="">(no symbols in DB)</option>';
      void loadStockHistoryCorrelationIds();
      return;
    }
    for (const s of data.symbols) {
      const o = document.createElement("option");
      o.value = String(s);
      o.textContent = String(s);
      sel.appendChild(o);
    }
  } catch {
    sel.innerHTML = '<option value="">(failed to load symbols)</option>';
  }
  void loadStockHistoryCorrelationIds();
}

async function loadStockHistory() {
  const out = $("#sh-out");
  const meta = $("#sh-meta");
  const sym = String($("#sh-symbol")?.value || "").trim().toUpperCase();
  if (!sym) {
    if (out) {
      out.className = "td-table-wrap muted";
      out.textContent = "Choose a symbol from the list.";
    }
    return;
  }
  const cid = String($("#sh-correlation-id")?.value || "").trim();
  const lim = stockHistoryLimit();
  if (out) {
    out.textContent = "Loading…";
    out.className = "td-table-wrap muted";
  }
  if (meta) meta.textContent = "";
  const params = new URLSearchParams({ symbol: sym, limit: String(lim) });
  if (cid) params.set("correlation_id", cid);
  try {
    const data = await fetchJSON(`/api/v1/trading-desk/stock-history?${params}`);
    if (!data || data.ok !== true) {
      const err = (data && data.error) || "Request failed";
      if (out) {
        out.className = "td-table-wrap muted";
        out.textContent = "Error: " + err;
      }
      return;
    }
    const evs = Array.isArray(data.events) ? data.events : [];
    const rows = evs.map((ev) => {
      const derived = stockHistoryDerivedFromDetail(ev.detail);
      return {
        event_time: ev.event_time,
        event_type: ev.event_type,
        position: derived.position,
        quantity: derived.quantity,
        status: derived.status,
        correlation_id: ev.correlation_id != null && ev.correlation_id !== "" ? ev.correlation_id : "—",
        source_id: ev.source_id,
        detail: ev.detail != null ? JSON.stringify(ev.detail) : "",
      };
    });
    if (meta) {
      meta.textContent =
        `total=${data.total != null ? data.total : rows.length} · symbol=${sym}` +
        (cid ? ` · correlation_id=${cid}` : " · latest activity (no correlation filter)");
    }
    renderTradingDeskTable(rows, out, {
      skipClear: false,
      columnLabels: {
        position: "Position",
        quantity: "Quantity",
        status: "Status",
      },
    });
  } catch (e) {
    if (out) {
      out.className = "td-table-wrap muted";
      out.textContent = "Error: " + e.message;
    }
  }
}

function tradingDeskResolvedAccountId() {
  const sel = $("#td-account-id");
  const manual = String($("#td-account-id-manual")?.value || "").trim();
  const selected = String(sel?.value || "").trim();
  return manual || selected || "";
}

async function loadTradingDeskReconcile() {
  const out = $("#td-reconcile-out");
  const accountId = tradingDeskResolvedAccountId();
  if (!accountId) {
    if (out) {
      out.className = "td-json-out bad";
      out.textContent = "Select or enter an Account ID first.";
    }
    return;
  }
  const params = new URLSearchParams({
    account_id: accountId,
    execution_environment: "SIM",
  });
  if (out) {
    out.textContent = "Reconciling… (TradeStation + Postgres)";
    out.className = "td-json-out";
  }
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 120000);
  try {
    const r = await fetch(`/api/v1/trading-desk/reconcile?${params}`, {
      method: "POST",
      cache: "no-store",
      signal: ctrl.signal,
      headers: { "Content-Type": "application/json" },
    });
    const text = await r.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = {};
    }
    if (out) {
      const bad = data.ok === false;
      out.className = bad ? "td-json-out bad" : "td-json-out";
      out.textContent = JSON.stringify(data, null, 2);
    }
    if (data && data.ok === true) {
      void loadTradingDeskPersistence();
    }
  } catch (e) {
    if (out) {
      const msg = e && e.name === "AbortError" ? "timeout (try again)" : e.message;
      out.textContent = "Error: " + msg;
      out.className = "td-json-out bad";
    }
  } finally {
    clearTimeout(t);
  }
}

/** Extensions YAML section name activated by this bus topic (for the Tasks table). */
function schedulerExtensionForTopic(topic, extensionByTopic) {
  const key = String(topic ?? "").trim();
  const fromApi = extensionByTopic && key && extensionByTopic[key];
  if (fromApi) {
    return fromApi;
  }
  if (key === "data.calendar.request") {
    return "engineering_lab.market_calendar";
  }
  if (key === "data.request") {
    return "engineering_lab.equity_data_acquisition";
  }
  if (key === "data.livefeed.request") {
    return "engineering_lab.livefeed_equity_quotes";
  }
  return "—";
}

/** Scheduler task: last run or last connectivity probe (Nelson3 / DA-style pill + optional error line). */
function renderSchedulerOutcomeCell(prefix, t) {
  const atKey = `${prefix}_at`;
  const okKey = `${prefix}_ok`;
  const errKey = `${prefix}_error`;
  const at = t[atKey];
  if (!at) {
    return '<span class="muted">—</span>';
  }
  const errRaw = t[errKey];
  const ok = t[okKey] === true && !(errRaw != null && String(errRaw).length > 0);
  const pillClass = ok ? "da-test-pill da-test-ok" : "da-test-pill da-test-bad";
  const label = ok ? "OK" : "Fail";
  const err = errRaw;
  const tip = escapeHtml(err ? String(err) : "");
  const when = escapeHtml(String(at).slice(0, 19)) + "Z";
  const errLine =
    !ok && err
      ? `<div class="st-err-line" title="${tip}">${escapeHtml(String(err))}</div>`
      : "";
  const line = `<span class="st-outcome-inline"><span class="${pillClass}" title="${tip}">${label}</span><small class="muted st-outcome-time">${when}</small></span>`;
  return errLine ? `<div class="st-outcome-cell">${line}${errLine}</div>` : line;
}

const SVG_CLIPBOARD_ICON = `<svg class="st-copy-icon" xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>`;

/** Copy last Run now correlation id (icon only; full id in tooltip and clipboard). */
function schedulerCopyCorrelationCell(t) {
  const cid = t.last_run_correlation_id;
  if (!cid) {
    return `<span class="st-run-id-row st-run-id-row--empty st-run-id-row--icon-only"><button type="button" class="st-copy-cid st-copy-cid--empty" disabled title="Run now to generate a correlation id" aria-label="No correlation id yet">${SVG_CLIPBOARD_ICON}</button></span>`;
  }
  const raw = String(cid);
  const attr = escapeAttr(raw);
  return `<span class="st-run-id-row st-run-id-row--icon-only"><button type="button" class="st-copy-cid" data-cid="${attr}" title="Copy correlation id (last Run now): ${attr}" aria-label="Copy correlation id">${SVG_CLIPBOARD_ICON}</button></span>`;
}

async function copyTextToClipboard(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    try {
      const ta = document.createElement("textarea");
      ta.value = text;
      ta.setAttribute("readonly", "");
      ta.style.position = "fixed";
      ta.style.left = "-9999px";
      document.body.appendChild(ta);
      ta.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(ta);
      return ok;
    } catch {
      return false;
    }
  }
}

function renderEmbeddedWorkersHtml(data) {
  const workers = (data && data.embedded_workers) || [];
  if (!workers.length) return "";
  const allOk = data && data.embedded_workers_ok;
  const summary = allOk
    ? '<p class="muted da-workers-summary">All <strong>enabled</strong> embedded consumers report healthy.</p>'
    : '<p class="muted da-workers-summary bad">Some <strong>enabled</strong> consumers are not healthy — fix extensions env or enable flags.</p>';
  const rows = workers
    .map((w) => {
      const ok = w.healthy;
      const pill = ok
        ? '<span class="readiness-pill ok">OK</span>'
        : '<span class="readiness-pill bad">Issue</span>';
      let ext = "—";
      if (w.extensions_import_ok === true) ext = "yes";
      if (w.extensions_import_ok === false) ext = "no";
      const hint = w.hint ? `<div class="readiness-detail">${escapeHtml(w.hint)}</div>` : "";
      return `<tr><td><code>${escapeHtml(w.component_id)}</code></td><td>${escapeHtml(
        String(w.state || "")
      )}</td><td>${w.consumer_enabled ? "yes" : "no"}</td><td>${w.thread_alive ? "yes" : "no"}</td><td>${ext}</td><td><code>${escapeHtml(
        String(w.queue || "")
      )}</code></td><td>${pill}${hint}</td></tr>`;
    })
    .join("");
  const thead = `<thead><tr><th>Component</th><th>State</th><th>Consumer on</th><th>Thread alive</th><th><code>extensions</code></th><th>Queue</th><th></th></tr></thead>`;
  return `<h4 class="da-workers-h">Embedded DA consumers</h4><p class="muted small">RabbitMQ <code>data.request</code> subscribers running inside this Management API process.</p>${summary}<table class="da-device-table da-workers-table" role="grid">${thead}<tbody>${rows}</tbody></table>`;
}

function renderReadinessHtml(data) {
  const ready = data && data.ready;
  const checks = (data && data.checks) || {};
  const notes = (data && data.notes) || [];
  const overall = ready
    ? '<div class="readiness-overall ok">Ready — core infrastructure OK for device registration</div>'
    : '<div class="readiness-overall bad">Not ready — fix failing checks before registering devices</div>';
  const rows = Object.keys(checks)
    .map((name) => {
      const c = checks[name];
      const ok = c && c.ok;
      const pill = ok
        ? '<span class="readiness-pill ok">OK</span>'
        : '<span class="readiness-pill bad">Fail</span>';
      const det = c && c.detail ? escapeHtml(String(c.detail)) : "";
      return `<li><span class="readiness-name">${escapeHtml(name)}</span> ${pill}${
        det ? `<span class="readiness-detail">${det}</span>` : ""
      }</li>`;
    })
    .join("");
  const noteList =
    notes.length > 0
      ? `<ul class="readiness-notes">${notes.map((n) => `<li>${escapeHtml(n)}</li>`).join("")}</ul>`
      : "";
  const workersBlock = renderEmbeddedWorkersHtml(data);
  return `${overall}<ul class="readiness-checks" role="list">${rows}</ul>${noteList}${workersBlock}`;
}

async function loadDaReadiness() {
  const el = $("#da-readiness-out");
  if (!el) return;
  el.classList.remove("muted");
  el.innerHTML = "<p>Loading…</p>";
  try {
    const data = await fetchJSON("/api/v1/extensions/data-acquisition/readiness");
    el.innerHTML = renderReadinessHtml(data);
  } catch (e) {
    el.innerHTML = `<p class="bad">Error: ${escapeHtml(e.message)}</p>`;
  }
}

async function loadDaDevices() {
  const el = $("#da-devices-out");
  if (!el) return;
  el.classList.remove("muted");
  el.textContent = "Loading…";
  try {
    const data = await fetchJSON("/api/v1/extensions/data-acquisition/devices");
    const list = Array.isArray(data.devices) ? data.devices : [];
    if (list.length === 0) {
      el.innerHTML = '<p class="muted">No devices registered yet.</p>';
      return;
    }
    const head = `<thead><tr><th>Component ID</th><th>Extension</th><th>Topic</th><th>On</th><th>Registered</th><th>Last test</th><th>Actions</th></tr></thead>`;
    const body = list
      .map((d) => {
        const cid = escapeHtml(d.component_id);
        const rawCid = d.component_id;
        const extRaw =
          d.extension_section != null && String(d.extension_section).trim() !== ""
            ? String(d.extension_section).trim()
            : d.extension_id != null && String(d.extension_id).trim() !== ""
              ? String(d.extension_id).trim()
              : null;
        const extCell =
          extRaw == null
            ? "—"
            : `<code class="st-ext-cell" title="extensions.${escapeHtml(extRaw)} in execution-runtime YAML">${escapeHtml(
                extRaw
              )}</code>`;
        const topicCell =
          d.topic != null && String(d.topic).trim() !== ""
            ? `<code>${escapeHtml(String(d.topic).trim())}</code>`
            : "—";
        const lt = d.last_da_test || {};
        let lastTestHtml = '<span class="muted">—</span>';
        if (lt.at) {
          const ok = lt.ok === true;
          const pillClass = ok ? "da-test-pill da-test-ok" : "da-test-pill da-test-bad";
          const label = ok ? "OK" : "Fail";
          const tip = escapeHtml(
            (lt.detail && lt.detail.message) || (typeof lt.detail === "object" ? JSON.stringify(lt.detail) : "") || ""
          );
          const when = escapeHtml(String(lt.at).slice(0, 19)) + "Z";
          lastTestHtml = `<span class="${pillClass}" title="${tip}">${label}</span><br/><small class="muted">${when}</small>`;
        }
        const testTitle =
          extRaw != null
            ? `GET /v1/extensions/test?section=${extRaw} on execution-runtime (scoped extension probe).`
            : "Extension connectivity test";
        const configBtn =
          extRaw != null
            ? `<button type="button" class="btn-st-config" data-da-config="${escapeAttr(extRaw)}" data-da-cid="${escapeAttr(String(rawCid))}" title="Effective YAML, documentation, manifest, Prepare DB (no scheduler task)">Configuration</button>`
            : "";
        return `<tr><td><code>${cid}</code></td><td>${extCell}</td><td>${topicCell}</td><td>${
          d.enabled ? "Yes" : "No"
        }</td><td>${escapeHtml(
          d.registered_at ? String(d.registered_at).slice(0, 19) + "Z" : "—"
        )}</td><td class="da-last-test">${lastTestHtml}</td><td class="st-task-actions"><button type="button" class="btn-da-test" data-cid="${escapeAttr(
          String(rawCid)
        )}" title="${escapeAttr(testTitle)}">Test</button>${configBtn}</td></tr>`;
      })
      .join("");
    el.innerHTML = `<table class="da-device-table">${head}<tbody>${body}</tbody></table>`;
  } catch (e) {
    el.textContent = "Error: " + e.message;
  }
}

async function loadSchedulerTasks() {
  const el = $("#st-tasks-out");
  if (!el) return;
  el.classList.remove("muted");
  el.textContent = "Loading…";
  try {
    const data = await fetchSchedulerTasksJSON();
    const list = Array.isArray(data.tasks) ? data.tasks : [];
    const extMap =
      data.extension_by_topic && typeof data.extension_by_topic === "object"
        ? data.extension_by_topic
        : null;
    if (list.length === 0) {
      el.innerHTML =
        '<p class="muted">No tasks yet. Add one above (e.g. <code>data.request</code> every 300s).</p>';
      return;
    }
    const head = `<thead><tr><th>ID</th><th>Name</th><th>Extension</th><th>Topic</th><th>Interval</th><th>On</th><th>Last run</th><th>Last test</th><th title="Last Run now correlation id (Loki/Grafana)">Run ID</th><th>Actions</th></tr></thead>`;
    const body = list
      .map((t) => {
        const payload = escapeHtml(JSON.stringify(t.payload_json || {}));
        const extFromTask =
          t.extension_section != null && String(t.extension_section).trim() !== ""
            ? String(t.extension_section).trim()
            : null;
        const ext = extFromTask ?? schedulerExtensionForTopic(t.topic, extMap);
        const extCell =
          ext === "—"
            ? "—"
            : `<code class="st-ext-cell" title="extensions.${escapeHtml(ext)} in execution-runtime YAML">${escapeHtml(ext)}</code>`;
        const lastRun = renderSchedulerOutcomeCell("last_run", t);
        const lastConn = renderSchedulerOutcomeCell("last_connectivity", t);
        const testTitle =
          ext !== "—"
            ? `Checks RabbitMQ publish, then GET /v1/extensions/test?section=${ext} on execution-runtime (this extension only).`
            : "Publishes a connectivity probe to the broker (topic routing key).";
        const configBtn =
          ext !== "—"
            ? `<button type="button" class="btn-st-config" data-st-config="${escapeHtml(ext)}" data-st-config-task="${t.id}" title="Effective YAML, IoC documentation, manifest, and Prepare DB">Configuration</button>`
            : "";
        const copyCell = schedulerCopyCorrelationCell(t);
        const freqSec = Number(t.frequency_seconds);
        const freqVal = Number.isFinite(freqSec) && freqSec >= 1 ? Math.floor(freqSec) : 300;
        // TODO(scheduler): Interval editor — mirror Add task UX (`#st-interval-preset` + `#st-freq`): preset dropdown + seconds field, then PATCH `frequency_seconds` (reuse `data-st-save-freq` / handler below).
        return `<tr>
          <td>${t.id}</td>
          <td>${escapeHtml(t.name)}</td>
          <td>${extCell}</td>
          <td><code>${escapeHtml(t.topic)}</code><br/><small class="muted">${payload}</small></td>
          <td class="st-interval-cell" title="Seconds between automatic runs (control-plane tick)">
            <input type="number" min="1" max="2592000" step="1" class="st-freq-input" data-st-freq="${t.id}" value="${freqVal}" aria-label="Interval seconds" style="width:6.5rem" />
            <button type="button" class="scheduler-exec st-freq-apply" data-st-save-freq="${t.id}">Apply</button>
            <div class="muted st-freq-hint" style="font-size:0.75rem;margin-top:0.2rem">${escapeHtml(formatFrequencyHuman(freqVal))}</div>
          </td>
          <td>${t.enabled ? "Yes" : "No"}</td>
          <td class="st-last-cell">${lastRun}</td>
          <td class="st-last-cell">${lastConn}</td>
          <td class="st-copy-cell">${copyCell}</td>
          <td class="st-task-actions">
            <button type="button" class="btn-da-test" data-st-test="${t.id}" title="${escapeHtml(testTitle)}">Test</button>
            <button type="button" data-st-run="${t.id}">Run now</button>
            ${configBtn}
            <button type="button" data-st-toggle="${t.id}" data-st-next="${t.enabled ? "0" : "1"}">${t.enabled ? "Disable" : "Enable"}</button>
            <button type="button" class="st-danger" data-st-del="${t.id}">Delete</button>
          </td>
        </tr>`;
      })
      .join("");
    el.innerHTML = `<table class="st-task-table">${head}<tbody>${body}</tbody></table>`;
  } catch (e) {
    el.textContent = "Error: " + e.message;
  }
}

/** Public Grafana URL from a /api/v1/settings row (`value` is usually `{ value: "http://..." }`). */
function grafanaEmbedUrlFromRow(row) {
  if (!row || row.value == null) return "";
  const v = row.value;
  if (typeof v === "string") return v;
  if (typeof v === "object") {
    if (typeof v.value === "string") return v.value;
    if (typeof v.url === "string") return v.url;
  }
  return "";
}

function applyGrafanaEmbedFromSettings(data) {
  const g = data.settings.find((x) => x.key === "ui.public_grafana_url");
  const url = grafanaEmbedUrlFromRow(g);
  const frame = $("#grafana-frame");
  const statusEl = $("#grafana-embed-status");
  const openEl = $("#grafana-open-external");
  if (statusEl) {
    if (url) {
      statusEl.textContent = "Embedding: " + url;
      statusEl.classList.remove("warn");
    } else {
      statusEl.textContent =
        "No URL loaded. Add ui.public_grafana_url on the Settings tab (JSON {\"value\":\"http://localhost:3000\"}) or set PUBLIC_GRAFANA_URL on control-plane and restart.";
      statusEl.classList.add("warn");
    }
  }
  if (openEl) {
    if (url) {
      openEl.href = url;
      openEl.hidden = false;
    } else {
      openEl.hidden = true;
      openEl.removeAttribute("href");
    }
  }
  if (frame) {
    frame.src = url || "about:blank";
  }
}

function applyRabbitMqEmbedFromSettings(data) {
  const row = data.settings.find((x) => x.key === "ui.public_rabbitmq_management_url");
  const url = grafanaEmbedUrlFromRow(row);
  const frame = $("#rabbitmq-frame");
  const statusEl = $("#rabbitmq-embed-status");
  const openEl = $("#rabbitmq-open-external");
  if (statusEl) {
    if (url) {
      statusEl.textContent = "Embedding: " + url;
      statusEl.classList.remove("warn");
    } else {
      statusEl.textContent =
        "No URL loaded. Add ui.public_rabbitmq_management_url on the Settings tab (JSON {\"value\":\"http://localhost:15672\"}) or set PUBLIC_RABBITMQ_MANAGEMENT_URL on control-plane and restart.";
      statusEl.classList.add("warn");
    }
  }
  if (openEl) {
    if (url) {
      openEl.href = url;
      openEl.hidden = false;
    } else {
      openEl.hidden = true;
      openEl.removeAttribute("href");
    }
  }
  if (frame) {
    frame.src = url || "about:blank";
  }
}

function fillBusTopologyEditor(data) {
  const ta = $("#bus-topology-json");
  const st = $("#bus-topology-status");
  if (!ta) return;
  const row = data.settings.find((x) => x.key === "bus.topology");
  const v = row && row.value != null ? row.value : {};
  ta.value = JSON.stringify(v, null, 2);
  if (st) st.textContent = "";
}

async function loadSettings() {
  const el = $("#settings-list");
  if (!el) return;
  try {
    const data = await fetchJSON("/api/v1/settings");
    el.innerHTML = data.settings
      .map(
        (s) =>
          `<div class="setting-row"><strong>${escapeHtml(s.key)}</strong> ${statusBadge(true)}<br/><code>${escapeHtml(JSON.stringify(s.value))}</code><br/><small class="muted">updated: ${s.updated_at || "—"} by ${escapeHtml(s.updated_by || "—")}</small></div>`
      )
      .join("");
    applyGrafanaEmbedFromSettings(data);
    applyRabbitMqEmbedFromSettings(data);
    fillBusTopologyEditor(data);
  } catch (e) {
    if (el) el.textContent = "Error: " + e.message;
  }
}

// Load dashboard data first so a bug in button handlers cannot leave health on "Loading…"
loadHealth();
loadComponents();
loadSchedulerManagement();
loadSettings();
// Detailed health hits execution-runtime extension probes; keep interval moderate (see control-plane scoped probes).
setInterval(loadHealth, 60000);

const btnReload = $("#btn-reload");
if (btnReload) {
  btnReload.addEventListener("click", async () => {
    try {
      const r = await fetchJSON("/api/v1/system/reload-config", { method: "POST" });
      alert(r.message || "OK");
      await loadSettings();
    } catch (e) {
      alert(e.message);
    }
  });
}

$("#btn-settings-export-yaml")?.addEventListener("click", async () => {
  const st = $("#settings-yaml-status");
  try {
    const r = await fetch("/api/v1/settings/export.yaml");
    if (!r.ok) throw new Error(await r.text());
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "nelson-control-plane-settings.yaml";
    a.click();
    URL.revokeObjectURL(url);
    if (st) st.textContent = "Downloaded nelson-control-plane-settings.yaml";
  } catch (e) {
    if (st) st.textContent = "Export failed: " + (e.message || e);
  }
});

$("#btn-settings-import-yaml")?.addEventListener("click", () => {
  $("#input-settings-import-yaml")?.click();
});

$("#input-settings-import-yaml")?.addEventListener("change", async (ev) => {
  const st = $("#settings-yaml-status");
  const input = ev.target;
  const file = input.files && input.files[0];
  if (!file) return;
  try {
    const text = await file.text();
    const r = await fetch("/api/v1/settings/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ yaml: text, updated_by: "ui-import" }),
    });
    const data = r.ok ? await r.json() : {};
    if (!r.ok) throw new Error(formatApiErrorBody(data) || (await r.text()));
    if (st) st.textContent = `Imported ${data.imported} key(s).`;
    await loadSettings();
  } catch (e) {
    if (st) st.textContent = "Import failed: " + (e.message || e);
  } finally {
    input.value = "";
  }
});

$("#btn-bus-topology-save")?.addEventListener("click", async () => {
  const ta = $("#bus-topology-json");
  const st = $("#bus-topology-status");
  if (!ta) return;
  let value;
  try {
    value = JSON.parse(ta.value);
  } catch {
    if (st) st.textContent = "Invalid JSON";
    return;
  }
  try {
    const r = await fetch("/api/v1/settings/bus.topology", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value, updated_by: "ui" }),
    });
    if (!r.ok) throw new Error(await r.text());
    if (st) st.textContent = "Saved.";
    await loadSettings();
  } catch (e) {
    if (st) st.textContent = String(e.message || e);
  }
});

$("#btn-bus-topology-copy-env")?.addEventListener("click", async () => {
  const ta = $("#bus-topology-json");
  const st = $("#bus-topology-status");
  if (!ta) return;
  let line;
  try {
    line = JSON.stringify(JSON.parse(ta.value));
  } catch {
    if (st) st.textContent = "Fix JSON before copying";
    return;
  }
  try {
    await navigator.clipboard.writeText(line);
    if (st) st.textContent = "Copied one-line JSON for NELSON_BUS_TOPOLOGY_JSON";
  } catch {
    if (st) st.textContent = "Copy failed — select JSON and copy manually";
  }
});

const formSetting = $("#form-setting");
if (formSetting) {
  formSetting.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const fd = new FormData(ev.target);
    const key = fd.get("key").trim();
    let value;
    try {
      value = JSON.parse(fd.get("value"));
    } catch {
      alert("Value must be valid JSON");
      return;
    }
    try {
      const r = await fetch(`/api/v1/settings/${encodeURIComponent(key)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value, updated_by: "ui" }),
      });
      if (!r.ok) throw new Error(await r.text());
      ev.target.reset();
      await loadSettings();
      alert("Saved");
    } catch (e) {
      alert(e.message);
    }
  });
}

const RMQ_TAP_MAX_MESSAGES = 200;
const RMQ_TAP_STATES_LS = "rmq_tap_states_v1";
const RMQ_TAP_ACTIVE_LS = "rmq_tap_active_id_v1";
// tap_id -> { tapId, bufferMsgs, ws, routing_key, exchange, exchange_type, queue_name }
let rmqTapsById = new Map();
let rmqActiveTapId = null;
let mbusCatalogLoaded = false;

function stopRmqTapWs(tapId) {
  const st = rmqTapsById.get(tapId);
  if (!st) return;
  if (st.ws) {
    try {
      st.ws.close();
    } catch (_) {
      /* ignore */
    }
  }
  st.ws = null;
}

function stopAllRmqTapWs() {
  for (const tapId of rmqTapsById.keys()) {
    stopRmqTapWs(tapId);
  }
}

function tapWsUrl(tapId) {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}/api/v1/message-bus/tap/${encodeURIComponent(
    tapId
  )}/ws`;
}

/** Normalize tap buffer entry: `{ receivedAt, body }` or legacy raw `body`. */
function rmqTapNormalizeEntry(entry) {
  if (entry && typeof entry === "object" && "body" in entry) {
    return { receivedAt: entry.receivedAt || null, body: entry.body };
  }
  return { receivedAt: null, body: entry };
}

function rmqTapPayloadDataOnly(body) {
  if (body && typeof body === "object" && body !== null && "payload" in body) {
    return body.payload;
  }
  return body;
}

/** Topics always offered in the tap dropdown (scheduler + livefeed); merged with broker bindings. */
const RMQ_TAP_SYSTEM_TOPICS = [
  "data.request",
  "data.calendar.request",
  "model.trigger",
  "data.livefeed.equity_quote",
  "execution.trading_broker.place_order.command.v1",
  "execution.trading_broker.place_order.response.v1",
  "execution.trading_broker.order_lifecycle.v1",
];

/** SVG: code / full JSON (Lucide-style). */
const RMQ_TAP_ICON_JSON = `<svg class="rmq-tap-act-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>`;

/** SVG: clipboard (copy data). */
const RMQ_TAP_ICON_COPY = `<svg class="rmq-tap-act-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/><rect x="8" y="2" width="8" height="4" rx="1" ry="1"/></svg>`;

/**
 * Single table column: show `payload` JSON, or full message for `data.request` (no separate app-level data node).
 * Full envelope: JSON button / modal.
 */
function rmqTapDataColumnText(body) {
  if (body == null) return "—";
  if (typeof body !== "object") {
    return escapeHtml(JSON.stringify(body, null, 2) || String(body));
  }
  if ("payload" in body) {
    const rk =
      body.routing_key != null && body.routing_key !== undefined ? String(body.routing_key) : "";
    if (rk === "data.request") {
      try {
        return escapeHtml(JSON.stringify(body, null, 2));
      } catch {
        return escapeHtml(String(body));
      }
    }
    const d = body.payload;
    if (d === undefined || d === null) return "—";
    if (typeof d === "object" && !Array.isArray(d) && Object.keys(d).length === 0) return "{ }";
    try {
      return escapeHtml(typeof d === "object" ? JSON.stringify(d, null, 2) : String(d));
    } catch {
      return escapeHtml(String(d));
    }
  }
  try {
    return escapeHtml(JSON.stringify(body, null, 2));
  } catch {
    return escapeHtml(String(body));
  }
}

function formatTapReceivedAt(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return `${d.toLocaleString(undefined, { hour12: false })}.${String(d.getMilliseconds()).padStart(3, "0")}`;
}

function openRmqTapJsonDialog(body) {
  const pre = $("#rmq-tap-json-pre");
  const dlg = $("#dialog-rmq-tap-json");
  if (pre) {
    try {
      pre.textContent = JSON.stringify(body, null, 2);
    } catch {
      pre.textContent = String(body);
    }
  }
  if (dlg && typeof dlg.showModal === "function") dlg.showModal();
}

function rmqTapRenderBuffer() {
  const tbody = $("#rmq-tap-tbody");
  if (!tbody) return;
  const st = rmqTapsById.get(rmqActiveTapId);
  const rows = st?.bufferMsgs || [];
  if (rows.length === 0) {
    tbody.innerHTML =
      '<tr class="rmq-tap-empty-row"><td colspan="4" class="muted">No messages yet — activate a tap and stream over WebSocket.</td></tr>';
    return;
  }
  tbody.innerHTML = rows
    .map((raw, i) => {
      const { receivedAt, body } = rmqTapNormalizeEntry(raw);
      const dataText = rmqTapDataColumnText(body);
      const fullIso = receivedAt ? escapeAttr(receivedAt) : "";
      return `<tr>
        <td class="rmq-tap-cell-idx">${i + 1}</td>
        <td class="rmq-tap-cell-received" title="${fullIso}">${escapeHtml(formatTapReceivedAt(receivedAt))}</td>
        <td><pre class="rmq-tap-cell-payload rmq-tap-cell-data">${dataText}</pre></td>
        <td class="rmq-tap-cell-actions">
          <button type="button" class="rmq-tap-btn-json st-copy-cid" data-rmq-json="${i}" title="Full message (JSON)" aria-label="Full message JSON">${RMQ_TAP_ICON_JSON}</button>
          <button type="button" class="rmq-tap-btn-copy-payload st-copy-cid" data-rmq-copy-payload="${i}" title="Copy data payload" aria-label="Copy data payload">${RMQ_TAP_ICON_COPY}</button>
        </td>
      </tr>`;
    })
    .join("");
  const scroll = document.querySelector(".rmq-tap-table-scroll");
  if (scroll) scroll.scrollTop = scroll.scrollHeight;
}

function rmqTapConnectWs(tapId) {
  const st = $("#rmq-tap-status");
  if (!tapId) return;
  const tap = rmqTapsById.get(tapId);
  if (!tap) return;
  stopRmqTapWs(tapId);
  tap.bufferMsgs = tap.bufferMsgs || [];

  // Only render buffer if this tap is currently selected.
  if (tapId === rmqActiveTapId) rmqTapRenderBuffer();

  const ws = new WebSocket(tapWsUrl(tapId));
  if (st && rmqActiveTapId === tapId) st.textContent = `WS connecting… tap_id=${tapId}`;
  tap.ws = ws;

  ws.onopen = () => {
    if (st && rmqActiveTapId === tapId) st.textContent = `WS connected. tap_id=${tapId}`;
  };

  ws.onmessage = (ev) => {
    let msg;
    try {
      msg = JSON.parse(ev.data);
    } catch (e) {
      msg = { _parse_error: "invalid_json_from_ws", _raw: String(ev.data).slice(0, 2000) };
    }
    tap.bufferMsgs.push({
      receivedAt: new Date().toISOString(),
      body: msg,
    });
    if (tap.bufferMsgs.length > RMQ_TAP_MAX_MESSAGES) {
      tap.bufferMsgs = tap.bufferMsgs.slice(-RMQ_TAP_MAX_MESSAGES);
    }
    if (tapId === rmqActiveTapId) {
      rmqTapRenderBuffer();
      if (st) st.textContent = `WS received ${tap.bufferMsgs.length} msg(s). tap_id=${tapId}`;
    }
  };

  ws.onerror = () => {
    if (st && rmqActiveTapId === tapId) st.textContent = `WS error. tap_id=${tapId}`;
  };

  ws.onclose = () => {
    if (st && rmqActiveTapId === tapId) st.textContent = `WS closed. tap_id=${tapId}`;
    tap.ws = null;
  };
}

function rmqTapPersistStatesToSession() {
  const meta = [...rmqTapsById.values()].map((t) => ({
    tapId: t.tapId,
    routing_key: t.routing_key,
    exchange: t.exchange,
    exchange_type: t.exchange_type,
    queue_name: t.queue_name,
  }));
  sessionStorage.setItem(RMQ_TAP_STATES_LS, JSON.stringify(meta));
  sessionStorage.setItem(RMQ_TAP_ACTIVE_LS, rmqActiveTapId || "");
}

function rmqTapSetActive(tapId) {
  if (!tapId || !rmqTapsById.has(tapId)) return;
  rmqActiveTapId = tapId;
  const t = rmqTapsById.get(tapId);
  const st = $("#rmq-tap-status");
  if (st) {
    st.textContent = `Active tap: queue ${t.queue_name} → exchange ${t.exchange} / routing_key ${t.routing_key}`;
  }
  rmqTapRenderTabs();
  rmqTapRenderBuffer();
  rmqTapPersistStatesToSession();
}

function rmqTapShortLabel(s) {
  const x = String(s ?? "");
  if (x.length <= 34) return x || "—";
  return x.slice(0, 18) + "…" + x.slice(-12);
}

function rmqTapRenderTabs() {
  const cont = $("#rmq-tap-tabs");
  if (!cont) return;
  if (rmqTapsById.size === 0) {
    cont.classList.add("muted");
    cont.textContent = "No taps open.";
    return;
  }
  cont.classList.remove("muted");
  cont.innerHTML = "";
  for (const [tapId, t] of rmqTapsById.entries()) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.dataset.tapId = tapId;
    btn.textContent = rmqTapShortLabel(t.routing_key);
    btn.className = "scheduler-exec";
    btn.style.padding = "0.25rem 0.5rem";
    btn.style.fontSize = "0.8rem";
    if (tapId === rmqActiveTapId) {
      btn.style.border = "2px solid #2c7efb";
    }
    cont.appendChild(btn);
  }
}

function syncRmqTapTopicPresetFromInput() {
  const sel = $("#rmq-tap-topic-preset");
  const rkEl = $("#rmq-tap-rk");
  if (!sel || !rkEl) return;
  const cur = String(rkEl.value || "").trim();
  if (!cur) {
    sel.value = "";
    return;
  }
  const match = [...sel.options].some((o) => o.value === cur);
  sel.value = match ? cur : "";
}

async function loadRmqTapTopicPresets() {
  const sel = $("#rmq-tap-topic-preset");
  const rkEl = $("#rmq-tap-rk");
  if (!sel) return;
  const cur = rkEl ? String(rkEl.value || "").trim() : "";
  sel.innerHTML = '<option value="">Custom (type routing key below)</option>';
  const merged = new Set(RMQ_TAP_SYSTEM_TOPICS);
  try {
    const d = await fetchJSON("/api/v1/message-bus/tap/defaults");
    const def = d && typeof d.default_livefeed_routing_key === "string" ? d.default_livefeed_routing_key.trim() : "";
    if (def) merged.add(def);
  } catch {
    /* ignore */
  }
  try {
    const r = await fetchJSON("/api/v1/message-bus/exchange-bindings");
    if (!r.ok || !Array.isArray(r.bindings)) {
      sel.title = r.error || "Could not load topics from RabbitMQ bindings";
    } else {
      sel.title = "";
      for (const b of r.bindings) {
        const rk = String(b.routing_key ?? "").trim();
        if (rk) merged.add(rk);
      }
    }
  } catch (e) {
    sel.title = (sel.title || "") + (e.message || String(e));
  }
  const topics = [...merged].sort((a, b) => a.localeCompare(b));
  const seen = new Set(topics);
  for (const rk of topics) {
    const opt = document.createElement("option");
    opt.value = rk;
    opt.textContent = rk;
    sel.appendChild(opt);
  }
  if (cur && seen.has(cur)) sel.value = cur;
  else syncRmqTapTopicPresetFromInput();
}

async function loadTapDefaults() {
  const meta = $("#rmq-tap-meta");
  const rk = $("#rmq-tap-rk");
  try {
    const d = await fetchJSON("/api/v1/message-bus/tap/defaults");
    if (!d.exchange) {
      if (meta) meta.textContent = "Could not load tap defaults.";
      return;
    }
    if (meta) {
      meta.innerHTML = `Publish path: exchange <code>${escapeHtml(d.exchange)}</code> (<code>${escapeHtml(
        d.exchange_type || "topic"
      )}</code>). <strong>Topic</strong> list = <em>known system topics</em> (scheduler defaults, livefeed) <strong>plus</strong> routing keys seen in <strong>RabbitMQ</strong> (queues already bound to this exchange). Topics that exist only in config/IoC but have <strong>no queue binding yet</strong> will not appear from the broker — type them manually or create a consumer binding first. Default livefeed: <code>${escapeHtml(
        d.default_livefeed_routing_key || ""
      )}</code>. <strong>Activate tap</strong> adds a separate tap queue (messages duplicated for inspection; workers unchanged).`;
    }
    if (rk && !String(rk.value || "").trim()) {
      rk.value = d.default_livefeed_routing_key || "data.livefeed.equity_quote";
    }
  } catch (e) {
    if (meta) meta.textContent = e.message || String(e);
  }
}

function restoreTapUiFromStorage() {
  const stop = $("#btn-rmq-tap-stop");

  let meta = [];
  try {
    meta = JSON.parse(sessionStorage.getItem(RMQ_TAP_STATES_LS) || "[]");
  } catch {
    meta = [];
  }
  if (!Array.isArray(meta)) meta = [];

  // Clear any current runtime state; this is best-effort only.
  rmqTapsById = new Map();
  rmqActiveTapId = sessionStorage.getItem(RMQ_TAP_ACTIVE_LS) || null;
  for (const m of meta) {
    if (!m || typeof m !== "object") continue;
    if (!m.tapId) continue;
    rmqTapsById.set(m.tapId, {
      tapId: m.tapId,
      bufferMsgs: [],
      ws: null,
      routing_key: m.routing_key || "",
      exchange: m.exchange || "",
      exchange_type: m.exchange_type || "topic",
      queue_name: m.queue_name || "",
    });
  }

  if (rmqActiveTapId && !rmqTapsById.has(rmqActiveTapId)) rmqActiveTapId = null;
  if (!rmqActiveTapId && rmqTapsById.size > 0) rmqActiveTapId = rmqTapsById.keys().next().value;

  rmqTapRenderTabs();
  if (rmqActiveTapId) rmqTapRenderBuffer();
  if (stop) stop.disabled = rmqTapsById.size === 0;

  // Reconnect WS for restored taps (buffers repopulate as messages arrive).
  for (const tapId of rmqTapsById.keys()) rmqTapConnectWs(tapId);
}

async function rmqTapDeleteById(tid) {
  if (!tid) return;
  stopRmqTapWs(tid);
  rmqTapsById.delete(tid);
  if (rmqActiveTapId === tid) rmqActiveTapId = null;

  try {
    await fetch(`/api/v1/message-bus/tap/${encodeURIComponent(tid)}`, { method: "DELETE" });
  } catch (_) {
    /* ignore */
  }

  if (!rmqActiveTapId && rmqTapsById.size > 0) rmqActiveTapId = rmqTapsById.keys().next().value;
  rmqTapRenderTabs();
  rmqTapRenderBuffer();
  rmqTapPersistStatesToSession();
}

async function rmqTapStart() {
  const rkEl = $("#rmq-tap-rk");
  const st = $("#rmq-tap-status");
  const v = rkEl && String(rkEl.value || "").trim();
  if (!v) {
    alert("Enter a routing key");
    return;
  }
  try {
    const r = await fetchJSON("/api/v1/message-bus/tap", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ routing_key: v }),
    });
    if (!r.tap_id) throw new Error(r.error || r.detail || "tap start failed");
    const tapId = r.tap_id;
    rmqTapsById.set(tapId, {
      tapId,
      bufferMsgs: [],
      ws: null,
      routing_key: r.routing_key || v,
      exchange: r.exchange || "",
      exchange_type: r.exchange_type || "topic",
      queue_name: r.queue_name || "",
    });
    rmqTapPersistStatesToSession();
    rmqTapSetActive(tapId);
    const stop = $("#btn-rmq-tap-stop");
    if (stop) stop.disabled = false;
    rmqTapConnectWs(r.tap_id);
  } catch (e) {
    if (st) st.textContent = e.message || String(e);
  }
}

async function rmqTapStop() {
  const st = $("#rmq-tap-status");
  const stop = $("#btn-rmq-tap-stop");
  const tid = rmqActiveTapId;
  if (!tid) {
    if (st) st.textContent = "No active tap to stop.";
    if (stop) stop.disabled = true;
    return;
  }
  if (st) st.textContent = `Stopping tap ${tid}…`;
  await rmqTapDeleteById(tid);
  if (st) st.textContent = rmqTapsById.size ? "Tap stopped; select another tab." : "All taps stopped.";
  if (stop) stop.disabled = rmqTapsById.size === 0;
}

function fmtRate(v) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return Number(v).toFixed(3);
}

/** First-open load for Queue traffic: broker overview line + topic tap UI. */
async function loadQueueTrafficTab() {
  const ovEl = $("#rmq-overview");
  try {
    const oRes = await fetchJSON("/api/v1/message-bus/overview");
    if (oRes.ok && oRes.overview && ovEl) {
      const o = oRes.overview;
      ovEl.innerHTML = `Cluster <strong>${escapeHtml(o.cluster_name || "—")}</strong> · publish <code>${fmtRate(o.publish_rate_per_sec)}</code>/s · deliver_get <code>${fmtRate(o.deliver_get_rate_per_sec)}</code>/s · <span class="muted">mgmt</span> <code>${escapeHtml(oRes.management_url || "")}</code>`;
    } else if (ovEl) {
      ovEl.textContent = oRes.error ? `Overview: ${oRes.error}` : "";
    }
  } catch (e) {
    if (ovEl) ovEl.textContent = e.message || String(e);
  }
  await loadTapDefaults();
  restoreTapUiFromStorage();
  await loadRmqTapTopicPresets();
}

$("#rmq-tap-topic-preset")?.addEventListener("change", (ev) => {
  const v = ev.target && ev.target.value;
  const rkEl = $("#rmq-tap-rk");
  if (rkEl && v) rkEl.value = v;
});

$("#rmq-tap-rk")?.addEventListener("input", () => syncRmqTapTopicPresetFromInput());

$("#btn-rmq-tap-start")?.addEventListener("click", () => rmqTapStart());
$("#btn-rmq-tap-stop")?.addEventListener("click", () => rmqTapStop());
$("#btn-rmq-tap-clear")?.addEventListener("click", () => {
  const t = rmqTapsById.get(rmqActiveTapId);
  const st = $("#rmq-tap-status");
  if (!t) {
    if (st) st.textContent = "No active tap to clear.";
    return;
  }
  t.bufferMsgs = [];
  rmqTapRenderBuffer();
  if (st) st.textContent = "Tap buffer cleared.";
});

$("#btn-rmq-tap-copy-payloads")?.addEventListener("click", async () => {
  const tap = rmqTapsById.get(rmqActiveTapId);
  if (!tap || !tap.bufferMsgs.length) {
    alert("No messages in the buffer.");
    return;
  }
  const parts = tap.bufferMsgs.map((raw) => {
    const { body } = rmqTapNormalizeEntry(raw);
    const dataOnly = rmqTapPayloadDataOnly(body);
    try {
      return JSON.stringify(dataOnly, null, 2);
    } catch {
      return String(dataOnly);
    }
  });
  const ok = await copyTextToClipboard(parts.join("\n\n---\n\n"));
  if (!ok) alert("Could not copy to clipboard.");
});

$("#btn-rmq-tap-json-close")?.addEventListener("click", () => {
  $("#dialog-rmq-tap-json")?.close();
});

$("#btn-td-json-close")?.addEventListener("click", () => {
  $("#dialog-td-json")?.close();
});

$("#rmq-tap-buffer-wrap")?.addEventListener("click", async (ev) => {
  const jsonBtn = ev.target.closest("button[data-rmq-json]");
  if (jsonBtn) {
    const i = parseInt(jsonBtn.getAttribute("data-rmq-json") || "", 10);
    const tap = rmqTapsById.get(rmqActiveTapId);
    const raw = tap?.bufferMsgs?.[i];
    const { body } = rmqTapNormalizeEntry(raw);
    openRmqTapJsonDialog(body);
    return;
  }
  const copyBtn = ev.target.closest("button[data-rmq-copy-payload]");
  if (copyBtn) {
    const i = parseInt(copyBtn.getAttribute("data-rmq-copy-payload") || "", 10);
    const tap = rmqTapsById.get(rmqActiveTapId);
    const raw = tap?.bufferMsgs?.[i];
    const { body } = rmqTapNormalizeEntry(raw);
    const dataOnly = rmqTapPayloadDataOnly(body);
    let text;
    try {
      text = JSON.stringify(dataOnly, null, 2);
    } catch {
      text = String(dataOnly);
    }
    const ok = await copyTextToClipboard(text);
    if (ok) {
      copyBtn.classList.add("st-copy-cid--done");
      setTimeout(() => copyBtn.classList.remove("st-copy-cid--done"), 1400);
    } else {
      alert("Could not copy to clipboard.");
    }
  }
});

$("#rmq-tap-tabs")?.addEventListener("click", (ev) => {
  const target = ev?.target;
  const btn = target && target.closest ? target.closest("button[data-tap-id]") : null;
  if (!btn) return;
  const tapId = btn.dataset.tapId;
  rmqTapSetActive(tapId);
});

document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
    btn.classList.add("active");
    const panel = $("#panel-" + btn.dataset.tab);
    if (panel) panel.classList.add("active");
    if (btn.dataset.tab === "grafana" || btn.dataset.tab === "rabbitmq-management") loadSettings();
    if (btn.dataset.tab === "data-acquisition") {
      loadDaReadiness();
      loadDaDevices();
    }
    if (btn.dataset.tab === "scheduler-tasks") {
      loadSchedulerManagement();
      loadSchedulerTasks();
    }
    if (btn.dataset.tab === "trading-desk") {
      void loadTradingDeskSummary();
      void loadTradingDeskPersistence();
      startTradingDeskPersistencePoll();
    } else {
      stopTradingDeskPersistencePoll();
    }
    if (btn.dataset.tab === "stock-history") {
      void loadStockHistorySymbols();
    }
    if (btn.dataset.tab === "rmq-traffic") {
      if (!mbusCatalogLoaded) {
        mbusCatalogLoaded = true;
        void loadQueueTrafficTab();
      }
    }
  });
});

$("#btn-da-readiness")?.addEventListener("click", () => {
  loadDaReadiness();
});

$("#btn-td-summary")?.addEventListener("click", () => {
  void loadTradingDeskSummary();
});

$("#btn-td-live-positions-probe")?.addEventListener("click", () => {
  void loadTradingDeskSummary();
});

$("#btn-td-live-positions-redraw")?.addEventListener("click", () => {
  const data = tradingDeskSummaryDataForRefresh();
  if (data) {
    renderTradingDeskBrokerPositions(data);
  } else {
    const wrap = $("#td-broker-positions-out");
    if (wrap) {
      wrap.className = "td-table-wrap muted";
      wrap.innerHTML =
        '<p class="muted">No cached probe yet. Use <strong>Refresh broker</strong> or <strong>Refresh live positions (probe)</strong> first.</p>';
    }
  }
});

$("#td-account-id")?.addEventListener("change", () => {
  const data = tradingDeskSummaryDataForRefresh();
  if (data) renderTradingDeskBrokerPositions(data);
});

$("#td-account-id-manual")?.addEventListener("input", () => {
  const data = tradingDeskSummaryDataForRefresh();
  if (data) renderTradingDeskBrokerPositions(data);
});

$("#btn-td-probe-toggle")?.addEventListener("click", () => {
  const btn = $("#btn-td-probe-toggle");
  const wrap = $("#td-probe-wrap");
  if (!btn || !wrap) return;
  const open = wrap.hidden === false;
  wrap.hidden = open;
  btn.textContent = open ? "Show probe details" : "Hide probe details";
  btn.setAttribute("aria-expanded", open ? "false" : "true");
});

$("#btn-td-persistence")?.addEventListener("click", () => {
  void loadTradingDeskPersistence();
});

$("#btn-sh-load")?.addEventListener("click", () => {
  void loadStockHistory();
});

$("#sh-symbol")?.addEventListener("change", () => {
  void loadStockHistoryCorrelationIds();
});

$("#btn-td-persist-prev")?.addEventListener("click", () => {
  const lim = tradingDeskPersistenceLimit();
  const el = $("#td-persist-offset");
  if (!el) return;
  let n = parseInt(String(el.value ?? "0"), 10);
  if (Number.isNaN(n) || n < 0) n = 0;
  n = Math.max(0, n - lim);
  el.value = String(n);
  void loadTradingDeskPersistence();
});

$("#btn-td-persist-next")?.addEventListener("click", () => {
  const lim = tradingDeskPersistenceLimit();
  const el = $("#td-persist-offset");
  if (!el) return;
  let n = parseInt(String(el.value ?? "0"), 10);
  if (Number.isNaN(n) || n < 0) n = 0;
  n = Math.min(1000000, n + lim);
  el.value = String(n);
  void loadTradingDeskPersistence();
});

$("#td-persist-offset")?.addEventListener("change", () => {
  tradingDeskPersistenceOffset();
});

$("#td-persist-limit")?.addEventListener("change", () => {
  const o = $("#td-persist-offset");
  if (o) o.value = "0";
  tradingDeskPersistenceLimit();
});

$("#td-persist-auto")?.addEventListener("change", () => {
  startTradingDeskPersistencePoll();
});

$("#td-persist-poll-sec")?.addEventListener("change", () => {
  tradingDeskPersistencePollSeconds();
  startTradingDeskPersistencePoll();
});

$("#btn-td-reconcile")?.addEventListener("click", () => {
  void loadTradingDeskReconcile();
});

const formTdPlace = $("#form-td-place-order");
if (formTdPlace) {
  formTdPlace.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const res = $("#td-place-result");
    const fd = new FormData(ev.target);
    const selectedAccount = String(fd.get("account_id") || "").trim();
    const manualAccount = String($("#td-account-id-manual")?.value || "").trim();
    const accountId = manualAccount || selectedAccount;
    const body = {
      account_id: accountId,
      symbol: String(fd.get("symbol") || "MSFT").trim(),
      quantity: parseInt(String(fd.get("quantity") || "1"), 10) || 1,
      trade_action: String(fd.get("trade_action") || "BUY"),
      tif: String(fd.get("tif") || "GTC"),
      gtd_date: String(fd.get("gtd_date") || "").trim(),
      omit_asset_type: fd.get("omit_asset_type") === "on",
    };
    if (!body.account_id) {
      if (res) {
        res.className = "da-register-result bad";
        res.textContent = "Account ID required.";
      }
      return;
    }
    if (res) {
      res.className = "da-register-result";
      res.textContent = "Publishing…";
    }
    try {
      const r = await fetch("/api/v1/trading-desk/place-order", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const text = await r.text();
      let data = {};
      try {
        data = text ? JSON.parse(text) : {};
      } catch {
        data = {};
      }
      if (!r.ok) {
        if (res) {
          res.className = "da-register-result bad";
          res.textContent = formatApiErrorBody(data) || text;
        }
        return;
      }
      if (res) {
        res.className = data.ok ? "da-register-result ok" : "da-register-result bad";
        res.textContent = JSON.stringify(data, null, 2);
      }
      if (data.ok) void loadTradingDeskPersistence();
    } catch (e) {
      if (res) {
        res.className = "da-register-result bad";
        res.textContent = "Error: " + e.message;
      }
    }
  });
}

const formDa = $("#form-da-device");
if (formDa) {
  formDa.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const out = $("#da-register-result");
    const fd = new FormData(ev.target);
    const device_id = String(fd.get("device_id") || "")
      .trim()
      .toLowerCase();
    const extension_id = String(fd.get("extension_id") || "").trim();
    const enabled = $("#da-enabled")?.checked === true;
    if (!device_id) {
      if (out) {
        out.className = "da-register-result bad";
        out.textContent = "Enter a device ID.";
      }
      return;
    }
    if (out) {
      out.className = "da-register-result";
      out.textContent = "Registering…";
    }
    try {
      const r = await fetchJSON("/api/v1/extensions/data-acquisition/devices", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          device_id,
          extension_id: extension_id || null,
          enabled,
          issued_by: "ui",
        }),
      });
      if (r.ok) {
        if (out) {
          out.className = "da-register-result ok";
          out.textContent = `Registered ${r.component_id} (extension: ${r.extension_id}).`;
        }
        ev.target.reset();
        await loadDaDevices();
        await loadComponents();
      } else {
        if (out) {
          out.className = "da-register-result bad";
          out.textContent = r.error || JSON.stringify(r);
        }
      }
    } catch (e) {
      if (out) {
        out.className = "da-register-result bad";
        out.textContent = "Error: " + e.message;
      }
    }
  });
}

const formSt = $("#form-st-task");
if (formSt) {
  formSt.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const out = $("#st-form-result");
    const name = String($("#st-name")?.value || "").trim();
    const topic = String($("#st-topic")?.value || "").trim();
    let payload_json = {};
    const raw = String($("#st-payload")?.value || "").trim();
    if (raw) {
      try {
        payload_json = JSON.parse(raw);
      } catch {
        if (out) {
          out.className = "da-register-result bad";
          out.textContent = "Payload must be valid JSON.";
        }
        return;
      }
    }
    const frequency_seconds = parseInt(String($("#st-freq")?.value || "300"), 10);
    const enabled = $("#st-enabled")?.checked === true;
    if (out) {
      out.className = "da-register-result";
      out.textContent = "Saving…";
    }
    try {
      await fetchJSON("/api/v1/scheduler/tasks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          topic,
          payload_json,
          frequency_seconds,
          enabled,
        }),
      });
      if (out) {
        out.className = "da-register-result ok";
        out.textContent = "Task created.";
      }
      ev.target.reset();
      const fq = $("#st-freq");
      if (fq) fq.value = "300";
      const preset = $("#st-interval-preset");
      if (preset) preset.value = "";
      const enb = $("#st-enabled");
      if (enb) enb.checked = true;
      await loadSchedulerTasks();
    } catch (e) {
      if (out) {
        out.className = "da-register-result bad";
        out.textContent = "Error: " + e.message;
      }
    }
  });
}

const stPreset = $("#st-interval-preset");
const stFreqInput = $("#st-freq");
if (stPreset && stFreqInput) {
  stPreset.addEventListener("change", () => {
    const v = stPreset.value;
    if (v) stFreqInput.value = v;
  });
  stFreqInput.addEventListener("input", () => {
    const n = String(stFreqInput.value).trim();
    const opt = Array.from(stPreset.options).find((o) => o.value === n);
    stPreset.value = opt ? n : "";
  });
}

/** First top-level `key: value` in extension YAML block (best-effort; worker snippet). */
function parseExtensionYamlScalar(yamlText, key) {
  if (!yamlText || typeof yamlText !== "string") return null;
  const esc = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(`^\\s*${esc}:\\s*(.+)$`, "m");
  const mm = yamlText.match(re);
  if (!mm) return null;
  return mm[1]
    .trim()
    .replace(/^["']|["']$/g, "")
    .replace(/\s+#.*$/, "");
}

/** Worker may return YAML text or a parsed object; manifest “current value” uses either. */
function getExtensionEffectiveScalar(effectiveYaml, key) {
  if (effectiveYaml != null && typeof effectiveYaml === "object" && !Array.isArray(effectiveYaml)) {
    if (!Object.prototype.hasOwnProperty.call(effectiveYaml, key)) return null;
    const v = effectiveYaml[key];
    if (v == null) return null;
    if (typeof v === "object") return JSON.stringify(v);
    return String(v);
  }
  if (typeof effectiveYaml === "string") return parseExtensionYamlScalar(effectiveYaml, key);
  return null;
}

/**
 * Best-effort YAML text for plain JSON (extension config shape).
 * Used when `effective_extension_yaml` is still an object after JSON parse
 * (e.g. older control-plane or direct worker-shaped payloads).
 */
function plainObjectToYaml(value, indent = 0) {
  const pad = (d) => "  ".repeat(d);
  if (value === null) return "null";
  if (value === undefined) return "null";
  const ty = typeof value;
  if (ty === "string") {
    if (value === "" || /^[\w#./:@~-]+$/.test(value)) return value;
    return JSON.stringify(value);
  }
  if (ty === "number" || ty === "boolean") return String(value);
  if (Array.isArray(value)) {
    if (value.length === 0) return "[]";
    const lines = [];
    for (const el of value) {
      if (el !== null && typeof el === "object") {
        const block = plainObjectToYaml(el, indent + 1);
        lines.push(`${pad(indent)}-\n${block}`);
      } else {
        lines.push(`${pad(indent)}- ${plainObjectToYaml(el, 0)}`);
      }
    }
    return lines.join("\n");
  }
  if (ty === "object") {
    const keys = Object.keys(value);
    if (keys.length === 0) return "{}";
    const lines = [];
    for (const k of keys) {
      const child = value[k];
      if (child !== null && typeof child === "object") {
        const inner = plainObjectToYaml(child, indent + 1);
        lines.push(`${pad(indent)}${k}:\n${inner}`);
      } else {
        lines.push(`${pad(indent)}${k}: ${plainObjectToYaml(child, 0)}`);
      }
    }
    return lines.join("\n");
  }
  return String(value);
}

/** Effective tab: YAML string from API, or YAML rendered from a plain object. */
function formatIocEffectiveExtensionYamlForDisplay(v) {
  if (v == null) return "—";
  if (typeof v === "string") return v;
  if (typeof v === "object") return plainObjectToYaml(v, 0);
  return String(v);
}

/** When the worker does not return effective YAML, show manifest routing defaults (not live effective config). */
function effectiveFallbackFromManifest(m) {
  if (!m || typeof m !== "object") return "—";
  const lines = [];
  lines.push(
    "# Effective extension YAML is not available from the worker (unreachable, missing section, or error)."
  );
  lines.push(
    "# Below: default subscribe/publish routing keys from the extension manifest. " +
      "Authoritative wiring is execution-runtime YAML (extensions.<section>)."
  );
  lines.push("");
  const bus = m.bus_topics || {};
  const subs = Array.isArray(bus.subscribe_routing_keys) ? bus.subscribe_routing_keys : [];
  const pubs = Array.isArray(bus.publish_routing_keys) ? bus.publish_routing_keys : [];
  lines.push(`default_subscribe_routing_keys:`);
  lines.push(subs.length ? subs.map((k) => `  - ${k}`).join("\n") : "  []");
  lines.push(`default_publish_routing_keys:`);
  lines.push(pubs.length ? pubs.map((k) => `  - ${k}`).join("\n") : "  []");
  return lines.join("\n");
}

function effectiveYamlOrManifestFallback(effYaml, m) {
  if (effYaml != null) return formatIocEffectiveExtensionYamlForDisplay(effYaml);
  if (m && typeof m === "object") return effectiveFallbackFromManifest(m);
  return "—";
}

function manifestSectionHtml(title, bodyHtml) {
  return `<section class="ext-manifest-section"><h4 class="ext-manifest-h">${escapeHtml(title)}</h4>${bodyHtml}</section>`;
}

/** Structured manifest view (no raw JSON in main body). */
function renderManifestStructuredHtml(m, effectiveYaml) {
  if (!m || typeof m !== "object") {
    return '<p class="muted">No manifest data.</p>';
  }
  const parts = [];
  if (m.summary) {
    parts.push(`<p class="ext-manifest-summary muted">${escapeHtml(String(m.summary))}</p>`);
  }
  const p = m.persistency;
  let persistHtml = "";
  if (p) {
    const need =
      p.persistency_required === true ? "Required" : p.persistency_required === false ? "Optional" : "—";
    const handleEff = getExtensionEffectiveScalar(effectiveYaml, "persistency");
    const current =
      handleEff != null
        ? `<code>${escapeHtml(handleEff)}</code>`
        : '<span class="muted">Not available (worker unreachable or key missing in YAML)</span>';
    persistHtml = `<dl class="ext-manifest-dl">
      <dt>Need persistency handle</dt><dd><strong>${escapeHtml(need)}</strong></dd>
      <dt>YAML key</dt><dd>${p.yaml_key ? `<code>${escapeHtml(String(p.yaml_key))}</code>` : "—"}</dd>
      <dt>Expected config type</dt><dd>${
        p.expected_config
          ? `<code class="ext-manifest-long">${escapeHtml(String(p.expected_config))}</code>`
          : "—"
      }</dd>
      <dt>Current value (worker)</dt><dd>${current}</dd>
    </dl>`;
    if (p.missing_message) {
      persistHtml += `<p class="ext-manifest-note muted">${escapeHtml(String(p.missing_message))}</p>`;
    }
  } else {
    persistHtml = '<p class="muted">No persistency metadata for this extension.</p>';
  }
  parts.push(manifestSectionHtml("Persistency", persistHtml));

  const entRows = [];
  if (Array.isArray(m.persisted_entity_refs) && m.persisted_entity_refs.length) {
    entRows.push(
      `<dt>Contract entity types</dt><dd><ul class="ext-manifest-ul">${m.persisted_entity_refs
        .map((x) => `<li><code>${escapeHtml(String(x))}</code></li>`)
        .join("")}</ul></dd>`
    );
  }
  if (p && Array.isArray(p.required_sql_tables) && p.required_sql_tables.length) {
    entRows.push(
      `<dt>Required SQL tables</dt><dd><ul class="ext-manifest-ul">${p.required_sql_tables
        .map((x) => `<li><code>${escapeHtml(String(x))}</code></li>`)
        .join("")}</ul></dd>`
    );
  }
  if (p && Array.isArray(p.deferred_sql_tables) && p.deferred_sql_tables.length) {
    entRows.push(
      `<dt>Deferred SQL tables</dt><dd><ul class="ext-manifest-ul">${p.deferred_sql_tables
        .map((x) => `<li><code>${escapeHtml(String(x))}</code></li>`)
        .join("")}</ul></dd>`
    );
  }
  const entitiesUsed =
    entRows.length > 0
      ? `<dl class="ext-manifest-dl">${entRows.join("")}</dl>`
      : '<p class="muted">—</p>';
  parts.push(manifestSectionHtml("Entities & tables (uses)", entitiesUsed));

  const msg = m.messaging;
  let pubEnt = "";
  if (msg && (msg.entity_type || msg.orm_relation)) {
    pubEnt = '<dl class="ext-manifest-dl">';
    if (msg.entity_type) {
      pubEnt += `<dt>Bus payload entity type</dt><dd><code>${escapeHtml(String(msg.entity_type))}</code></dd>`;
    }
    if (msg.orm_relation) {
      pubEnt += `<dt>ORM relation</dt><dd><code>${escapeHtml(String(msg.orm_relation))}</code></dd>`;
    }
    if (msg.publish_routing_key_default) {
      pubEnt += `<dt>Default publish routing key</dt><dd><code>${escapeHtml(
        String(msg.publish_routing_key_default)
      )}</code></dd>`;
    }
    const pk = parseExtensionYamlScalar(effectiveYaml, "publish_routing_key");
    if (pk) {
      pubEnt += `<dt>Current publish routing key (worker)</dt><dd><code>${escapeHtml(pk)}</code></dd>`;
    }
    pubEnt += "</dl>";
    if (msg.transport_note) {
      pubEnt += `<p class="muted ext-manifest-note">${escapeHtml(String(msg.transport_note))}</p>`;
    }
  } else {
    pubEnt =
      '<p class="muted">This extension does not publish application payloads to the message bus.</p>';
  }
  parts.push(manifestSectionHtml("Entities on the bus (publish)", pubEnt));

  const bus = m.bus_topics || {};
  const subs = Array.isArray(bus.subscribe_routing_keys) ? bus.subscribe_routing_keys : [];
  const pubs = Array.isArray(bus.publish_routing_keys) ? bus.publish_routing_keys : [];
  let topicsHtml = '<dl class="ext-manifest-dl">';
  topicsHtml += `<dt>Subscribe (routing keys)</dt><dd>${
    subs.length
      ? `<ul class="ext-manifest-ul">${subs
          .map((k) => `<li><code>${escapeHtml(String(k))}</code></li>`)
          .join("")}</ul>`
      : '<span class="muted">—</span>'
  }</dd>`;
  topicsHtml += `<dt>Publish (defaults)</dt><dd>${
    pubs.length
      ? `<ul class="ext-manifest-ul">${pubs
          .map((k) => `<li><code>${escapeHtml(String(k))}</code></li>`)
          .join("")}</ul>`
      : '<span class="muted">—</span>'
  }</dd>`;
  topicsHtml += "</dl>";
  if (bus.note) {
    topicsHtml += `<p class="muted ext-manifest-note">${escapeHtml(String(bus.note))}</p>`;
  }
  parts.push(manifestSectionHtml("Topics (message bus)", topicsHtml));

  return `<div class="ext-manifest-wrap">${parts.join("")}</div>`;
}

const _EXT_CONFIG_TABS = ["effective", "documentation", "manifest"];

function setExtensionConfigTab(which) {
  const dlg = $("#dialog-extension-config");
  if (!dlg) return;
  const w = _EXT_CONFIG_TABS.includes(which) ? which : "effective";
  for (const t of _EXT_CONFIG_TABS) {
    const btn = dlg.querySelector(`[data-ext-config-tab="${t}"]`);
    const panel = document.getElementById(`ext-config-panel-${t}`);
    const active = t === w;
    if (btn) {
      btn.classList.toggle("ioc-tab--active", active);
      btn.setAttribute("aria-selected", active ? "true" : "false");
    }
    if (panel) panel.hidden = !active;
  }
}

const _IOC_EFFECTIVE_KEYS = new Set([
  "effective_extension_yaml",
  "effective_config_file",
  "effective_redacted",
  "effective_detail",
  "effective_source",
  "effective_error",
]);

/** IoC API merges docs + worker YAML; show resolved values first, schema docs second. */
function formatIocDialogPayload(ioc) {
  if (!ioc || typeof ioc !== "object") return ioc;
  const effective = {
    extension_yaml: ioc.effective_extension_yaml,
    config_file: ioc.effective_config_file,
    redacted: ioc.effective_redacted,
    detail: ioc.effective_detail,
    worker_admin_url: ioc.effective_source,
    error: ioc.effective_error,
  };
  const documentation = {};
  for (const k of Object.keys(ioc)) {
    if (!_IOC_EFFECTIVE_KEYS.has(k)) documentation[k] = ioc[k];
  }
  return { effective, documentation };
}

function openExtensionConfigDialog(section, taskId, daComponentId) {
  const dlg = $("#dialog-extension-config");
  const title = $("#dialog-extension-config-title");
  const meta = $("#ext-config-effective-meta");
  const effPre = $("#ext-config-effective-pre");
  const docPre = $("#ext-config-doc-pre");
  const manBody = $("#ext-config-manifest-body");
  const rawPre = $("#ext-config-manifest-raw-pre");
  const provisionWrap = $("#ext-config-provision-wrap");
  const provisionBtn = $("#btn-ext-config-provision");
  const provisionHint = $("#ext-config-provision-hint");
  if (!dlg || !effPre || !docPre || !manBody) return;

  const tid = taskId != null && String(taskId).trim() !== "" ? String(taskId).trim() : "";
  const daCid =
    daComponentId != null && String(daComponentId).trim() !== "" ? String(daComponentId).trim() : "";
  if (provisionWrap && provisionBtn) {
    if (tid) {
      provisionWrap.hidden = false;
      provisionBtn.dataset.provisionTaskId = tid;
      delete provisionBtn.dataset.provisionDaComponentId;
      if (provisionHint) {
        provisionHint.textContent =
          "Schema only: POST execution-runtime /v1/extensions/provision (DDL) for this scheduler task's extension. When persistency.ensure_tables_on_sync is true, the worker may already create tables on startup.";
      }
    } else if (daCid) {
      provisionWrap.hidden = false;
      provisionBtn.dataset.provisionDaComponentId = daCid;
      delete provisionBtn.dataset.provisionTaskId;
      if (provisionHint) {
        provisionHint.textContent =
          "Schema only: POST execution-runtime /v1/extensions/provision (DDL) for this data-acquisition device's extension. When persistency.ensure_tables_on_sync is true, the worker may already create tables on startup.";
      }
    } else {
      provisionWrap.hidden = true;
      delete provisionBtn.dataset.provisionTaskId;
      delete provisionBtn.dataset.provisionDaComponentId;
    }
  }

  setExtensionConfigTab("effective");
  if (title) title.textContent = "Configuration";
  if (meta) meta.innerHTML = "";
  effPre.textContent = "Loading…";
  docPre.textContent = "Loading…";
  manBody.innerHTML = '<p class="muted">Loading…</p>';
  if (rawPre) rawPre.textContent = "";
  dlg.showModal();

  const manUrl = `/api/v1/extensions/manifest?section=${encodeURIComponent(section)}`;
  const iocUrl = `/api/v1/extensions/ioc?section=${encodeURIComponent(section)}`;

  Promise.allSettled([fetchJSON(manUrl), fetchJSON(iocUrl)]).then((results) => {
    const manOk = results[0].status === "fulfilled" ? results[0].value : null;
    const iocOk = results[1].status === "fulfilled" ? results[1].value : null;
    const manErr = results[0].status === "rejected" ? results[0].reason : null;
    const iocErr = results[1].status === "rejected" ? results[1].reason : null;

    const m = manOk && manOk.manifest;
    const ioc = iocOk && iocOk.ioc;
    const extTitle =
      (m && m.extension_section) || (ioc && ioc.extension_section) || section;
    if (title) title.textContent = `Configuration — ${extTitle}`;

    const effYaml = ioc && ioc.effective_extension_yaml;

    if (m) {
      manBody.innerHTML = renderManifestStructuredHtml(m, effYaml != null ? effYaml : null);
      if (rawPre) rawPre.textContent = JSON.stringify(m, null, 2);
    } else {
      const msg = manErr && manErr.message ? String(manErr.message) : "Manifest unavailable.";
      manBody.innerHTML = `<p class="bad">${escapeHtml(msg)}</p>`;
      if (rawPre) rawPre.textContent = "";
    }

    if (ioc) {
      const metaLines = [];
      if (ioc.effective_error) {
        metaLines.push(`<span class="bad">Worker: ${escapeHtml(String(ioc.effective_error))}</span>`);
      } else {
        if (ioc.effective_config_file) {
          metaLines.push(`Config: <code>${escapeHtml(String(ioc.effective_config_file))}</code>`);
        }
        if (ioc.effective_redacted != null) {
          metaLines.push(`Redacted: ${ioc.effective_redacted ? "yes" : "no"}`);
        }
        if (ioc.effective_source) {
          metaLines.push(`Source: <code>${escapeHtml(String(ioc.effective_source))}</code>`);
        }
      }
      if (meta) meta.innerHTML = metaLines.length ? metaLines.join(" · ") : "";
      effPre.textContent = effectiveYamlOrManifestFallback(effYaml, m);

      const payload = formatIocDialogPayload(ioc);
      docPre.textContent = JSON.stringify(payload.documentation || {}, null, 2);
    } else {
      const msg = iocErr && iocErr.message ? String(iocErr.message) : "IoC unavailable.";
      if (meta) meta.innerHTML = `<span class="bad">${escapeHtml(msg)}</span>`;
      effPre.textContent = m ? effectiveFallbackFromManifest(m) : "—";
      docPre.textContent = msg;
    }
  });
}

$("#dialog-extension-config")?.addEventListener("click", (ev) => {
  const tabBtn = ev.target.closest("[data-ext-config-tab]");
  if (!tabBtn || !$("#dialog-extension-config")?.contains(tabBtn)) return;
  const t = tabBtn.getAttribute("data-ext-config-tab");
  if (t) setExtensionConfigTab(t);
});

$("#btn-extension-config-close")?.addEventListener("click", () => {
  $("#dialog-extension-config")?.close();
});

async function runSchedulerTaskProvision(taskId, button) {
  if (!taskId) return;
  if (button) button.disabled = true;
  try {
    const r = await fetch(`/api/v1/scheduler/tasks/${taskId}/provision`, { method: "POST" });
    const raw = await r.text();
    let data = {};
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch {
      data = {};
    }
    if (!r.ok) {
      const msg =
        formatApiErrorBody(data) ||
        (raw && raw.length < 2000 ? raw : "") ||
        "(empty response)";
      alert(`Prepare DB failed (HTTP ${r.status})\n${msg}`);
    } else {
      alert(`Prepare DB\n\n${JSON.stringify(data, null, 2)}`);
    }
  } catch (e) {
    alert(e.message);
  } finally {
    if (button) button.disabled = false;
  }
}

async function runDaDeviceProvision(componentId, button) {
  if (!componentId) return;
  if (button) button.disabled = true;
  try {
    const r = await fetch(
      `/api/v1/extensions/data-acquisition/devices/${encodeURIComponent(componentId)}/provision`,
      { method: "POST" }
    );
    const raw = await r.text();
    let data = {};
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch {
      data = {};
    }
    if (!r.ok) {
      const msg =
        formatApiErrorBody(data) ||
        (raw && raw.length < 2000 ? raw : "") ||
        "(empty response)";
      alert(`Prepare DB failed (HTTP ${r.status})\n${msg}`);
    } else {
      alert(`Prepare DB\n\n${JSON.stringify(data, null, 2)}`);
    }
  } catch (e) {
    alert(e.message);
  } finally {
    if (button) button.disabled = false;
  }
}

$("#btn-ext-config-provision")?.addEventListener("click", async () => {
  const btn = $("#btn-ext-config-provision");
  const daCid = btn && btn.dataset.provisionDaComponentId;
  if (daCid) {
    await runDaDeviceProvision(daCid, btn);
    return;
  }
  const id = btn && btn.dataset.provisionTaskId;
  if (!id) return;
  await runSchedulerTaskProvision(id, btn);
});

$("#st-tasks-out")?.addEventListener("click", async (ev) => {
  const cfgBtn = ev.target.closest("button[data-st-config]");
  if (cfgBtn) {
    const section = cfgBtn.getAttribute("data-st-config");
    const taskId = cfgBtn.getAttribute("data-st-config-task");
    if (section) openExtensionConfigDialog(section, taskId || null);
    return;
  }

  const copyCidBtn = ev.target.closest("button.st-copy-cid");
  if (copyCidBtn && copyCidBtn.dataset.cid) {
    ev.preventDefault();
    const ok = await copyTextToClipboard(copyCidBtn.dataset.cid);
    if (ok) {
      copyCidBtn.classList.add("st-copy-cid--done");
      setTimeout(() => copyCidBtn.classList.remove("st-copy-cid--done"), 1400);
    } else {
      alert("Could not copy to clipboard.");
    }
    return;
  }

  const testBtn = ev.target.closest("button[data-st-test]");
  if (testBtn) {
    const id = testBtn.getAttribute("data-st-test");
    testBtn.disabled = true;
    try {
      const r = await fetch(`/api/v1/scheduler/tasks/${id}/test`, { method: "POST" });
      const raw = await r.text();
      let data = {};
      try {
        data = raw ? JSON.parse(raw) : {};
      } catch {
        data = {};
      }
      if (!r.ok) {
        const msg =
          formatApiErrorBody(data) ||
          (raw && raw.length < 2000 ? raw : "") ||
          "(empty response)";
        alert(`Test failed (HTTP ${r.status})\n${msg}`);
      } else if (!data.ok) {
        alert(`Connectivity failed\n\n${data.error || formatApiErrorBody(data) || JSON.stringify(data)}`);
      } else {
        alert(`Connectivity OK\n\n${data.detail || "Probe published."}`);
      }
      await loadSchedulerTasks();
    } catch (e) {
      alert(e.message);
    } finally {
      testBtn.disabled = false;
    }
    return;
  }

  const run = ev.target.closest("button[data-st-run]");
  if (run) {
    const id = run.getAttribute("data-st-run");
    run.disabled = true;
    try {
      const r = await fetch(`/api/v1/scheduler/tasks/${id}/run`, { method: "POST" });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) {
        alert(`Run failed (HTTP ${r.status})\n${JSON.stringify(data)}`);
      } else if (!data.ok) {
        alert(`Run failed\n\n${data.error || JSON.stringify(data)}`);
      } else {
        const d = data.detail ? `\n\n${data.detail}` : "";
        alert(`Run published to RabbitMQ.${d}\n\nUse the clipboard icon in the Run ID column to copy the correlation id for Loki/Grafana.`);
      }
      await loadSchedulerTasks();
    } catch (e) {
      alert(e.message);
    } finally {
      run.disabled = false;
    }
    return;
  }
  // TODO(scheduler): When task rows get preset + seconds UI, keep PATCH body `{ frequency_seconds }` unchanged.
  const saveFreq = ev.target.closest("button[data-st-save-freq]");
  if (saveFreq) {
    const id = saveFreq.getAttribute("data-st-save-freq");
    const row = saveFreq.closest("tr");
    const input = row?.querySelector(`input[data-st-freq="${id}"]`);
    const n = input ? parseInt(String(input.value || "").trim(), 10) : NaN;
    if (!id || !Number.isFinite(n) || n < 1 || n > 2592000) {
      alert("Interval must be an integer between 1 and 2592000 seconds.");
      return;
    }
    saveFreq.disabled = true;
    try {
      await fetchJSON(`/api/v1/scheduler/tasks/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ frequency_seconds: n }),
      });
      await loadSchedulerTasks();
    } catch (e) {
      alert(e.message);
    } finally {
      saveFreq.disabled = false;
    }
    return;
  }
  const toggle = ev.target.closest("button[data-st-toggle]");
  if (toggle) {
    const id = toggle.getAttribute("data-st-toggle");
    const en = toggle.getAttribute("data-st-next") === "1";
    toggle.disabled = true;
    try {
      await fetchJSON(`/api/v1/scheduler/tasks/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: en }),
      });
      await loadSchedulerTasks();
    } catch (e) {
      alert(e.message);
    } finally {
      toggle.disabled = false;
    }
    return;
  }
  const del = ev.target.closest("button[data-st-del]");
  if (del) {
    const id = del.getAttribute("data-st-del");
    if (!confirm("Delete task " + id + "?")) return;
    del.disabled = true;
    try {
      await fetchJSON(`/api/v1/scheduler/tasks/${id}`, { method: "DELETE" });
      await loadSchedulerTasks();
    } catch (e) {
      alert(e.message);
    } finally {
      del.disabled = false;
    }
  }
});

$("#da-devices-out")?.addEventListener("click", async (ev) => {
  const cfgBtn = ev.target.closest("button[data-da-config]");
  if (cfgBtn) {
    const section = cfgBtn.getAttribute("data-da-config");
    const daCid = cfgBtn.getAttribute("data-da-cid");
    if (section) openExtensionConfigDialog(section, null, daCid || null);
    return;
  }

  const testBtn = ev.target.closest("button.btn-da-test");
  if (testBtn) {
    const rawCid = testBtn.dataset.cid;
    if (!rawCid) return;
    testBtn.disabled = true;
    try {
      const r = await fetchJSON(`/api/v1/extensions/data-acquisition/test`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ component_id: rawCid, issued_by: "ui" }),
      });
      const title = r.ok ? "Connectivity OK" : "Connectivity failed";
      alert(`${title}\n\n${JSON.stringify(r, null, 2)}`);
      await loadDaDevices();
    } catch (e) {
      alert(e.message);
    } finally {
      testBtn.disabled = false;
    }
  }
});

$("#scheduler-management-out")?.addEventListener("click", async (ev) => {
  const btn = ev.target.closest("button.scheduler-exec");
  if (!btn) return;
  const row = btn.closest(".component-row");
  const sel = row?.querySelector(".scheduler-select");
  const out = row?.querySelector(".scheduler-result");
  const cmd = sel?.value;
  if (!cmd || !sel) return;
  btn.disabled = true;
  if (out) out.textContent = "Running…";
  try {
    const res = await executeSchedulerCommand(cmd, {});
    if (out) {
      out.textContent = res.ok
        ? JSON.stringify(res, null, 2)
        : res.error || JSON.stringify(res);
    }
    await loadHealth();
    await refreshSchedulerHint();
  } catch (e) {
    if (out) out.textContent = "Error: " + e.message;
  } finally {
    btn.disabled = false;
  }
});
