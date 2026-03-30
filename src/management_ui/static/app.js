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
    const data = await fetchJSON("/api/v1/scheduler/tasks");
    const list = Array.isArray(data.tasks) ? data.tasks : [];
    if (list.length === 0) {
      el.innerHTML =
        '<p class="muted">No tasks yet. Add one above (e.g. <code>data.request</code> every 300s).</p>';
      return;
    }
    const head = `<thead><tr><th>ID</th><th>Name</th><th>Topic</th><th>Freq (s)</th><th>On</th><th>Last run</th><th>Actions</th></tr></thead>`;
    const body = list
      .map((t) => {
        const payload = escapeHtml(JSON.stringify(t.payload_json || {}));
        const last = t.last_run_at ? escapeHtml(String(t.last_run_at).slice(0, 19)) + "Z" : "—";
        return `<tr>
          <td>${t.id}</td>
          <td>${escapeHtml(t.name)}</td>
          <td><code>${escapeHtml(t.topic)}</code><br/><small class="muted">${payload}</small></td>
          <td>${t.frequency_seconds}</td>
          <td>${t.enabled ? "Yes" : "No"}</td>
          <td>${last}</td>
          <td class="st-task-actions">
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

$("#btn-calendar-sync")?.addEventListener("click", async () => {
  const out = $("#calendar-sync-result");
  if (out) {
    out.className = "da-register-result";
    out.textContent = "Syncing…";
  }
  try {
    const r = await fetchJSON("/api/v1/calendar/sync-polygon", { method: "POST" });
    if (out) {
      out.className = "da-register-result ok";
      out.textContent = `Upserted ${r.upserted} row(s). API items: ${r.fetched_from_api}. Skipped: ${r.skipped}.`;
    }
  } catch (e) {
    if (out) {
      out.className = "da-register-result bad";
      out.textContent = "Error: " + e.message;
    }
  }
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
    const topic = String($("#st-topic")?.value || "");
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

$("#st-tasks-out")?.addEventListener("click", async (ev) => {
  const run = ev.target.closest("button[data-st-run]");
  if (run) {
    const id = run.getAttribute("data-st-run");
    run.disabled = true;
    try {
      await fetchJSON(`/api/v1/scheduler/tasks/${id}/run`, { method: "POST" });
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
