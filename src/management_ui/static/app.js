/** Bumped with index.html `?v=` and `<meta name="nelson4-management-ui-build">` — if this log mismatches DevTools Network, you are not serving this package build. */
console.info("[Nelson4 operator UI] static build v=35");

const $ = (sel) => document.querySelector(sel);

async function fetchJSON(url, opts) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 20000);
  try {
    const r = await fetch(url, { ...opts, signal: ctrl.signal });
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
  postgres: "PostgreSQL",
  rabbitmq: "RabbitMQ",
  scheduler: "Scheduler (market hours)",
  management_api: "Management API",
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
const HEALTH_CHECK_ORDER = ["postgres", "rabbitmq", "scheduler", "management_api"];

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
      const ok = v && v.ok;
      const err = v && v.error;
      const label = CHECK_LABELS[key] || key.replace(/_/g, " ");
      const state = ok ? "up" : "down";
      const pill = ok
        ? '<span class="health-status-pill ok">OK</span>'
        : '<span class="health-status-pill bad">FAIL</span>';

      let extraRight = "";
      if (key === "scheduler") {
        const t = formatHealthTime(v && v.last_tick_at);
        const phase = v && v.last_phase;
        if (ok) {
          const timePart = t
            ? `<span class="health-last-meta">Last trigger: <strong>${escapeHtml(t)}</strong></span>`
            : `<span class="health-last-meta muted">Last trigger: <strong>never</strong></span>`;
          const phasePart =
            phase != null && phase !== ""
              ? `<span class="health-phase muted">(${escapeHtml(String(phase))})</span>`
              : "";
          extraRight = `<span class="health-scheduler-extra">${timePart}${phasePart}</span>`;
        }
      }

      const errLine =
        !ok && err
          ? `<div class="health-check-error">${escapeHtml(err)}</div>`
          : "";

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

async function loadHealth() {
  const el = $("#health-panel");
  if (!el) return;
  try {
    const data = await fetchJSON("/api/v1/health/detailed");
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
  return `<li class="component-row ${c.enabled ? "enabled" : "disabled"}">
    <span class="component-led" aria-hidden="true"></span>
    <div class="component-body">
      <div class="component-id">${escapeHtml(c.component_id)}</div>
      <div class="component-meta">
        <span class="component-phase">${escapeHtml(c.phase)}</span>
        <span class="component-flag">${c.enabled ? "Enabled" : "Disabled"}</span>
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

/** Normalized so scheduler row always gets command UI (dropdown + Execute). */
function renderComponentRow(c, sched) {
  if (isSchedulerComponent(c)) {
    const merged = { ...BUILTIN_SCHEDULER, ...c, component_id: BUILTIN_SCHEDULER.component_id };
    return renderSchedulerRow(merged, sched);
  }
  return renderRegistryRow(c);
}

async function loadComponents() {
  const el = $("#components-out");
  if (!el) return;
  let sched = null;
  try {
    sched = await fetchJSON("/api/v1/scheduler");
  } catch (e) {
    sched = null;
  }
  const setListHtml = (list, data) => {
    if (list.length === 0) {
      el.innerHTML =
        '<p class="muted">No components. Add extensions later or use the built-in scheduler above.</p>';
      return;
    }
    const rows = list.map((c) => renderComponentRow(c, sched)).join("");
    const banner =
      data && data.registry_error
        ? `<p class="component-api-banner" role="status">Registry list partial: ${escapeHtml(
            String(data.registry_error)
          )}</p>`
        : "";
    el.innerHTML = `${banner}<ul class="component-list" role="list">${rows}</ul>`;
  };
  try {
    const data = await fetchJSON("/api/v1/components");
    let list = Array.isArray(data.components) ? [...data.components] : [];
    if (!list.some(isSchedulerComponent)) {
      list.unshift(BUILTIN_SCHEDULER);
    }
    setListHtml(list, data);
  } catch (e) {
    const msg =
      e.name === "AbortError"
        ? "Request timed out (20s). Is the API reachable?"
        : e.message || String(e);
    const row = renderSchedulerRow(BUILTIN_SCHEDULER, sched);
    el.innerHTML = `<p class="component-api-banner warn">Could not load <code>/api/v1/components</code>: ${escapeHtml(
      msg
    )}. Built-in scheduler controls are shown below.</p><ul class="component-list" role="list">${row}</ul>`;
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
  return `<span class="${pillClass}" title="${tip}">${label}</span><br/><small class="muted">${when}</small>${errLine}`;
}

const SVG_CLIPBOARD_ICON = `<svg class="st-copy-icon" xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>`;

/** Copy button for last Run now correlation id (Grafana/Loki). */
function schedulerCopyCorrelationCell(t) {
  const cid = t.last_run_correlation_id;
  if (!cid) {
    return `<button type="button" class="st-copy-cid st-copy-cid--empty" disabled title="Run now to generate a correlation id" aria-label="No correlation id yet">${SVG_CLIPBOARD_ICON}</button>`;
  }
  const safe = escapeHtml(String(cid));
  return `<button type="button" class="st-copy-cid" data-cid="${safe}" title="Copy correlation id (last Run now)" aria-label="Copy correlation id">${SVG_CLIPBOARD_ICON}</button>`;
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
    const head = `<thead><tr><th>Component ID</th><th>Extension</th><th>Enabled</th><th>Registered</th><th>Last test</th><th>Actions</th></tr></thead>`;
    const body = list
      .map((d) => {
        const cid = escapeHtml(d.component_id);
        const rawCid = d.component_id;
        const toggleLabel = d.enabled ? "Disable" : "Enable";
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
        return `<tr><td><code>${cid}</code></td><td>${escapeHtml(
          d.extension_id || "—"
        )}</td><td>${d.enabled ? "Yes" : "No"}</td><td>${escapeHtml(
          d.registered_at ? String(d.registered_at).slice(0, 19) + "Z" : "—"
        )}</td><td class="da-last-test">${lastTestHtml}</td><td class="da-actions"><button type="button" class="btn-da-test" data-cid="${escapeHtml(
          rawCid
        )}" title="Same Polygon request as the worker (v2 aggs + DA_POLYGON_LIVE_TIMESPAN)">Test</button> <button type="button" class="btn-da-toggle" data-cid="${cid}">${toggleLabel}</button></td></tr>`;
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
        const manifestBtn =
          ext !== "—"
            ? `<button type="button" class="btn-st-manifest" data-st-manifest="${escapeHtml(ext)}" title="YAML section requirements, persistency, entity refs (from engineering-lab manifest)">Manifest</button>`
            : "";
        const iocBtn =
          ext !== "—"
            ? `<button type="button" class="btn-st-ioc" data-st-ioc="${escapeHtml(ext)}" title="Resolved extensions.* YAML from the worker (effective first); operator schema docs below">IoC</button>`
            : "";
        const prepareDbBtn =
          ext !== "—"
            ? `<button type="button" class="btn-st-provision" data-st-provision="${t.id}" title="Schema only: POST execution-runtime /v1/extensions/provision (DDL). When persistency.ensure_tables_on_sync is true, the worker also creates tables on startup / first sync — use this if that flag is false or you need DDL before the worker runs.">Prepare DB</button>`
            : "";
        const copyCell = schedulerCopyCorrelationCell(t);
        return `<tr>
          <td>${t.id}</td>
          <td>${escapeHtml(t.name)}</td>
          <td>${extCell}</td>
          <td><code>${escapeHtml(t.topic)}</code><br/><small class="muted">${payload}</small></td>
          <td title="${escapeHtml(String(t.frequency_seconds))} s">${escapeHtml(formatFrequencyHuman(t.frequency_seconds))}</td>
          <td>${t.enabled ? "Yes" : "No"}</td>
          <td class="st-last-cell">${lastRun}</td>
          <td class="st-last-cell">${lastConn}</td>
          <td class="st-copy-cell">${copyCell}</td>
          <td class="st-task-actions">
            <button type="button" class="btn-da-test" data-st-test="${t.id}" title="${escapeHtml(testTitle)}">Test</button>
            ${manifestBtn}
            ${iocBtn}
            ${prepareDbBtn}
            <button type="button" data-st-run="${t.id}">Run now</button>
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
    const g = data.settings.find((x) => x.key === "ui.public_grafana_url");
    if (g && g.value && g.value.value) {
      const frame = $("#grafana-frame");
      if (frame) frame.src = g.value.value;
    }
  } catch (e) {
    if (el) el.textContent = "Error: " + e.message;
  }
}

// Load dashboard data first so a bug in button handlers cannot leave health on "Loading…"
loadHealth();
loadComponents();
loadSettings();
setInterval(loadHealth, 10000);

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

document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
    btn.classList.add("active");
    const panel = $("#panel-" + btn.dataset.tab);
    if (panel) panel.classList.add("active");
    if (btn.dataset.tab === "grafana") loadSettings();
    if (btn.dataset.tab === "data-acquisition") {
      loadDaReadiness();
      loadDaDevices();
    }
    if (btn.dataset.tab === "scheduler-tasks") loadSchedulerTasks();
  });
});

$("#btn-da-readiness")?.addEventListener("click", () => {
  loadDaReadiness();
});

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

function openExtensionManifestDialog(section) {
  const dlg = $("#dialog-manifest");
  const pre = $("#dialog-manifest-pre");
  const title = $("#dialog-manifest-title");
  if (!dlg || !pre) return;
  if (title) {
    title.textContent = "Extension manifest";
  }
  pre.textContent = "Loading…";
  dlg.showModal();
  const url = `/api/v1/extensions/manifest?section=${encodeURIComponent(section)}`;
  fetchJSON(url)
    .then((data) => {
      const m = data && data.manifest;
      if (title && m && m.extension_section) {
        title.textContent = String(m.extension_section);
      }
      pre.textContent = JSON.stringify(m != null ? m : data, null, 2);
    })
    .catch((e) => {
      pre.textContent = e.message || String(e);
    });
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

function openExtensionIocDialog(section) {
  const dlg = $("#dialog-ioc");
  const pre = $("#dialog-ioc-pre");
  const title = $("#dialog-ioc-title");
  if (!dlg || !pre) return;
  if (title) {
    title.textContent = "Extension IoC (effective YAML)";
  }
  pre.textContent = "Loading…";
  dlg.showModal();
  const url = `/api/v1/extensions/ioc?section=${encodeURIComponent(section)}`;
  fetchJSON(url)
    .then((data) => {
      const ioc = data && data.ioc;
      if (title && ioc && ioc.extension_section) {
        title.textContent = `IoC — ${String(ioc.extension_section)}`;
      }
      const payload = ioc != null ? formatIocDialogPayload(ioc) : data;
      pre.textContent = JSON.stringify(payload, null, 2);
    })
    .catch((e) => {
      pre.textContent = e.message || String(e);
    });
}

$("#btn-manifest-close")?.addEventListener("click", () => {
  $("#dialog-manifest")?.close();
});

$("#btn-ioc-close")?.addEventListener("click", () => {
  $("#dialog-ioc")?.close();
});

$("#st-tasks-out")?.addEventListener("click", async (ev) => {
  const manBtn = ev.target.closest("button[data-st-manifest]");
  if (manBtn) {
    const section = manBtn.getAttribute("data-st-manifest");
    if (section) openExtensionManifestDialog(section);
    return;
  }

  const iocBtn = ev.target.closest("button[data-st-ioc]");
  if (iocBtn) {
    const section = iocBtn.getAttribute("data-st-ioc");
    if (section) openExtensionIocDialog(section);
    return;
  }

  const provBtn = ev.target.closest("button[data-st-provision]");
  if (provBtn) {
    const id = provBtn.getAttribute("data-st-provision");
    provBtn.disabled = true;
    try {
      const r = await fetch(`/api/v1/scheduler/tasks/${id}/provision`, { method: "POST" });
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
      provBtn.disabled = false;
    }
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
  const testBtn = ev.target.closest("button.btn-da-test");
  if (testBtn) {
    const rawCid = testBtn.dataset.cid;
    if (!rawCid) return;
    testBtn.disabled = true;
    try {
      const r = await fetchJSON(
        `/api/v1/extensions/data-acquisition/devices/${encodeURIComponent(rawCid)}/test`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ issued_by: "ui" }),
        }
      );
      const title = r.ok ? "Connectivity OK" : "Connectivity failed";
      alert(`${title}\n\n${JSON.stringify(r, null, 2)}`);
      await loadDaDevices();
    } catch (e) {
      alert(e.message);
    } finally {
      testBtn.disabled = false;
    }
    return;
  }

  const btn = ev.target.closest("button.btn-da-toggle");
  if (!btn) return;
  const rawCid = btn.dataset.cid;
  if (!rawCid) return;
  const wantEnabled = btn.textContent.trim() === "Enable";
  btn.disabled = true;
  try {
    await fetchJSON(
      `/api/v1/extensions/data-acquisition/devices/${encodeURIComponent(rawCid)}`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: wantEnabled, issued_by: "ui" }),
      }
    );
    await loadDaDevices();
    await loadComponents();
  } catch (e) {
    alert(e.message);
  } finally {
    btn.disabled = false;
  }
});

$("#components-out")?.addEventListener("click", async (ev) => {
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
