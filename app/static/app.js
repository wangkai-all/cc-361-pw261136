/* Sealed Wire Vault — console (no framework, no build step) */
"use strict";

const state = {
  data: null,
  view: "sources",
  selectedObject: null,
  selectedRun: null,
  compareA: null,
  compareB: null,
  objectDetail: null,
  timer: null,
};

/* ------------------------------ utilities ------------------------------ */

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

function short(value, n = 12) {
  if (!value) return "—";
  return value.length <= n ? value : value.slice(0, n) + "…";
}

function bytes(n) {
  if (n == null) return "—";
  if (n < 1024) return n + " B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KiB";
  return (n / 1024 / 1024).toFixed(2) + " MiB";
}

function when(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (isNaN(d)) return iso;
  const pad = (x) => String(x).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
         `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function ago(iso) {
  if (!iso) return "";
  const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 5) return "just now";
  if (s < 60) return `${Math.floor(s)}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
}

function toast(message, kind = "") {
  const wrap = $("#toast-wrap");
  const el = document.createElement("div");
  el.className = "toast " + kind;
  el.textContent = message;
  wrap.appendChild(el);
  setTimeout(() => { el.style.opacity = "0"; el.style.transition = "opacity .3s"; }, 3800);
  setTimeout(() => el.remove(), 4300);
}

function banner(message, kind = "") {
  const el = $("#banner");
  if (!message) { el.hidden = true; el.textContent = ""; return; }
  el.hidden = false;
  el.className = "banner " + kind;
  el.textContent = message;
}

async function api(path, { method = "GET", body } = {}) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  const ctype = res.headers.get("Content-Type") || "";
  let payload = null;
  if (ctype.includes("application/json")) {
    payload = await res.json().catch(() => null);
  } else {
    payload = await res.text();
  }
  if (!res.ok) {
    const msg = (payload && payload.error) || res.statusText || "request failed";
    const err = new Error(msg);
    err.status = res.status;
    err.detail = payload && payload.detail;
    throw err;
  }
  return payload;
}

async function downloadPost(path, body, fallbackName) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) {
    let msg = res.statusText;
    try { msg = (await res.json()).error || msg; } catch (_) {}
    throw new Error(msg);
  }
  const blob = await res.blob();
  const disp = res.headers.get("Content-Disposition") || "";
  const m = /filename="([^"]+)"/.exec(disp);
  const name = m ? m[1] : fallbackName || "download.bin";
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 4000);
  return name;
}

async function act(fn, successMessage) {
  try {
    const result = await fn();
    if (successMessage) toast(successMessage, "ok");
    await refresh({ quiet: true });
    return result;
  } catch (err) {
    toast(err.message, "error");
    throw err;
  }
}

/* ------------------------------ data load ------------------------------ */

async function refresh({ quiet = false } = {}) {
  try {
    const data = await api("/api/state");
    state.data = data;
    renderAll();
    if (!quiet) banner("");
  } catch (err) {
    if (!quiet) banner("Cannot reach the vault API: " + err.message, "error");
  }
}

function renderAll() {
  if (!state.data) return;
  renderStatus();
  renderSources();
  renderObjects();
  renderRuns();
  renderRulePanel();
  renderWorkers();
  if (state.view === "events") loadEvents();
  $("#data-dir").textContent = state.data.demo.data_dir;
  refreshLiveDetail();
}

let liveDetailBusy = false;

async function refreshLiveDetail() {
  // Keep an open run detail in sync while it is queued/running/paused, and keep
  // an open object detail in sync while a run over it is active.
  if (liveDetailBusy) return;
  const runId = state.selectedRun;
  const run = (state.data.runs || []).find((r) => r.id === runId);
  const runActive = run && ["queued", "running", "paused"].includes(run.status);
  const objRuns = state.selectedObject
    ? (state.data.runs || []).filter((r) => r.object_id === state.selectedObject)
    : [];
  const objActive = objRuns.some((r) => ["queued", "running"].includes(r.status));
  if (state.view === "runs" && runId && runActive && !$("#run-detail").hidden) {
    liveDetailBusy = true;
    try {
      const data = await api(`/api/runs/${encodeURIComponent(runId)}/view`);
      if (state.selectedRun === runId) renderRunDetail(data);
    } catch (_) { /* keep existing view */ }
    finally { liveDetailBusy = false; }
  } else if (state.view === "objects" && state.selectedObject && objActive) {
    liveDetailBusy = true;
    try {
      const data = await api(`/api/objects/${encodeURIComponent(state.selectedObject)}`);
      if (state.selectedObject === data.object.id) renderObjectDetail(data);
    } catch (_) { /* keep existing view */ }
    finally { liveDetailBusy = false; }
  }
}

/* ------------------------------ status bar ------------------------------ */

function renderStatus() {
  const d = state.data;
  const c = d.counts;
  const cap = d.workers.capture_worker;
  const fet = d.workers.fetch_worker;
  const sea = d.workers.seal_worker;
  const par = d.workers.parse_worker;
  const pipe = d.pipeline || {};
  const items = [
    { dot: cap.running ? "ok" : "", label: `capture worker ${cap.running ? "up" : "down"}` },
    { dot: fet.running ? "ok" : "", label: `fetch ${fet.running ? "up" : "down"}` },
    { dot: sea.running ? "ok" : "", label: `seal ${sea.running ? "up" : "down"}` },
    { dot: par.running ? "ok" : "", label: `parse worker ${par.running ? "up" : "down"}` },
    { dot: pipe.staged_files && pipe.staged_files.length ? "warn" : "info",
      label: `${pipe.staged_files ? pipe.staged_files.length : 0} staged · ${pipe.fetched_waiting_seal || 0} awaiting seal` },
    { dot: c.damaged_chunks ? "bad" : "ok", label: `${c.damaged_chunks} damaged chunks` },
    { dot: "info", label: `${c.objects} objects` },
    { dot: "info", label: `${c.chunks} chunks · ${bytes(c.chunk_bytes)}` },
  ];
  $("#status-cluster").innerHTML = items.map((it) =>
    `<span class="chip"><span class="dot ${it.dot}"></span>${esc(it.label)}</span>`
  ).join("");
}

/* ------------------------------ sources ------------------------------ */

function renderSources() {
  const grid = $("#source-grid");
  const sources = state.data.sources;
  if (!sources.length) {
    grid.innerHTML = `<div class="empty">No sources registered. Use “Register fixtures &amp; rules”, or POST /api/demo/seed.</div>`;
    return;
  }
  grid.innerHTML = sources.map((s) => {
    const withdrawn = s.status === "withdrawn";
    const latest = s.latest_capture;
    const statusBadge = withdrawn
      ? `<span class="badge bad">withdrawn</span>`
      : `<span class="badge ok">active</span>`;
    const latestLine = latest
      ? `<span>last capture <b>${esc(latest.status)}</b> · ${when(latest.started_at)}</span>`
      : `<span>never captured</span>`;
    const job = (state.data.jobs || []).find(
      (j) => j.source_id === s.id && ["queued", "running", "fetched"].includes(j.status)
    );
    return `
      <article class="card" data-source="${esc(s.id)}">
        <div class="card-head">
          <h3>${esc(s.name)} ${statusBadge}</h3>
          <span class="badge">${esc(s.mode)}</span>
        </div>
        <div class="mono-block">${esc(s.url)}</div>
        <div class="meta">
          <span>${esc(s.origin)}</span>
          <span>${s.objects} sealed object${s.objects === 1 ? "" : "s"}</span>
          ${latestLine}
        </div>
        <div class="actions">
          <button class="btn btn-primary btn-sm" data-act="capture" data-pipeline="combined" ${withdrawn ? "disabled" : ""}>Capture &amp; seal</button>
          <button class="btn btn-sm" data-act="capture" data-pipeline="split" ${withdrawn ? "disabled" : ""}>Fetch → seal (split)</button>
          ${job ? `<button class="btn btn-danger btn-sm" data-act="cancel" data-job="${esc(job.id)}">Cancel ${esc(job.status)}</button>` : ""}
          <button class="btn btn-sm" data-act="bump" ${withdrawn ? "disabled" : ""}>Mutate fixture</button>
          <button class="btn btn-sm ${withdrawn ? "" : "btn-danger"}" data-act="toggle-withdraw">
            ${withdrawn ? "Reactivate" : "Withdraw"}
          </button>
        </div>
        ${withdrawn ? `<p class="hint small">Withdrawn ${esc(when(s.withdrawn_at))}. New captures are refused; already-sealed objects and their parse results remain readable and exportable.</p>` : ""}
      </article>`;
  }).join("");

  grid.querySelectorAll("[data-act]").forEach((btn) => {
    btn.addEventListener("click", () => sourceAction(btn));
  });
}

async function sourceAction(btn) {
  const card = btn.closest("[data-source]");
  const sourceId = card.dataset.source;
  const actName = btn.dataset.act;
  const source = state.data.sources.find((s) => s.id === sourceId);
  btn.disabled = true;
  try {
    if (actName === "capture") {
      const pipeline = btn.dataset.pipeline || "combined";
      const res = await act(
        () => api(`/api/sources/${encodeURIComponent(sourceId)}/capture`,
          { method: "POST", body: { pipeline } }),
        `Capture queued for ${source.name} (${pipeline})`
      );
      if (res && res.workers) {
        const started = Object.entries(res.workers)
          .filter(([, w]) => w && w.started)
          .map(([name, w]) => `${name} pid ${w.pid}`);
        if (started.length) toast(`worker started: ${started.join(", ")}`);
      } else if (res && res.worker && res.worker.started) {
        toast(`capture worker started (pid ${res.worker.pid})`);
      }
      setTimeout(() => refresh({ quiet: true }), 900);
    } else if (actName === "cancel") {
      await act(
        () => api(`/api/jobs/${encodeURIComponent(btn.dataset.job)}/cancel`, { method: "POST", body: {} }),
        "Cancel requested"
      );
    } else if (actName === "bump") {
      const res = await act(
        () => api(`/api/sources/${encodeURIComponent(sourceId)}/bump`, { method: "POST", body: {} }),
        null
      );
      toast(`Fixture ${source.name} advanced to revision ${res.revision}. Previous sealed objects are unchanged.`, "ok");
    } else if (actName === "toggle-withdraw") {
      const withdrawn = source.status !== "withdrawn";
      const res = await act(
        () => api(`/api/sources/${encodeURIComponent(sourceId)}/${withdrawn ? "withdraw" : "reactivate"}`,
          { method: "POST", body: { reason: "demo retraction from console" } }),
        null
      );
      toast(res.status === "withdrawn"
        ? `${source.name} withdrawn. Existing objects stay readable.`
        : `${source.name} reactivated.`, "ok");
    }
  } catch (_) { /* toast already shown */ }
  btn.disabled = false;
  refresh({ quiet: true });
}

/* ------------------------------ objects ------------------------------ */

function renderObjects() {
  const list = $("#object-list");
  const objects = state.data.objects;
  if (!objects.length) {
    list.innerHTML = `<div class="empty">No sealed objects yet. Capture a source to create one.</div>`;
    return;
  }
  list.innerHTML = objects.map((o) => {
    const runCount = state.data.runs.filter((r) => r.object_id === o.id).length;
    const sel = state.selectedObject === o.id ? " is-selected" : "";
    return `
      <div class="row-card${sel}" data-object="${esc(o.id)}">
        <div>
          <div class="row-title">${esc(o.source_name || o.source_id)}
            <span class="badge info">${bytes(o.total_bytes)}</span>
            <span class="badge">${o.chunk_count} chunks</span>
          </div>
          <div class="row-sub">
            <span class="mono">${esc(o.id)}</span>
            <span>${when(o.created_at)}</span>
            <span>${runCount} run${runCount === 1 ? "" : "s"}</span>
          </div>
        </div>
        <div class="row-sub">
          <span class="mono" title="manifest sha256">manifest ${esc(short(o.manifest_digest, 16))}</span>
          <span class="mono" title="stream sha256">stream ${esc(short(o.stream_digest || "", 16))}</span>
          <span>${o.sharing ? `${o.sharing.shared_chunks} shared chunks · ${bytes(o.sharing.shared_bytes)} saved` : ""}</span>
        </div>
        <div class="row-actions">
          <button class="btn btn-sm btn-primary" data-obj-act="open">Open</button>
        </div>
      </div>`;
  }).join("");
  list.querySelectorAll("[data-object]").forEach((row) => {
    row.addEventListener("click", () => openObject(row.dataset.object));
  });
}

async function openObject(objectKey) {
  state.selectedObject = objectKey;
  state.view = "objects";
  setView("objects");
  renderObjects();
  const detail = $("#object-detail");
  detail.hidden = false;
  detail.innerHTML = `<div class="panel"><span class="spinner"></span> loading object…</div>`;
  try {
    const data = await api(`/api/objects/${encodeURIComponent(objectKey)}`);
    state.objectDetail = data;
    renderObjectDetail(data);
  } catch (err) {
    detail.innerHTML = `<div class="banner error">${esc(err.message)}</div>`;
  }
}

function renderObjectDetail(data) {
  const o = data.object;
  const m = data.manifest;
  const detail = $("#object-detail");
  const chunks = data.chunks;
  const damaged = chunks.filter((c) => c.status === "damaged");
  const chunkStrip = chunks.map((c) => {
    const cls = ["chunk", c.status === "ok" ? "ok" : "damaged", c.refcount > 1 ? "shared" : ""].join(" ");
    return `<span class="${cls}" title="seq ${c.seq} · ${c.length} B · ${esc(c.digest)} · shared by ${c.refcount} object(s)"
      data-chunk-seq="${c.seq}">#${c.seq}</span>`;
  }).join("");

  detail.innerHTML = `
    <div class="detail-head">
      <div>
        <h2>${esc(o.id)} <span class="badge ${damaged.length ? "bad" : "ok"}">${damaged.length ? "damage localised" : "intact"}</span></h2>
        <p class="hint">Source <b>${esc(m.source.name)}</b> · captured ${esc(m.capture.started_at)} · ${bytes(o.total_bytes)} in ${o.chunk_count} chunks</p>
      </div>
      <div class="row-actions">
        <button class="btn btn-sm" data-obj-act="verify">Verify all chunks</button>
        <button class="btn btn-sm" data-obj-act="bytes">Download .orig</button>
        <button class="btn btn-sm btn-primary" data-obj-act="export">Export package (.zip)</button>
      </div>
    </div>

    <div class="provenance">
      <div class="prov"><div class="k">object id</div><div class="v">${esc(o.id)}</div></div>
      <div class="prov"><div class="k">manifest sha256</div><div class="v">${esc(o.manifest_digest)}</div></div>
      <div class="prov"><div class="k">stream sha256</div><div class="v">${esc(m.stream.sha256)}</div></div>
      <div class="prov"><div class="k">merkle root (chunks)</div><div class="v">${esc(m.root_digest)}</div></div>
      <div class="prov"><div class="k">chunking</div><div class="v">${esc(m.chunking.algorithm)} min ${m.chunking.min} / avg ${m.chunking.avg} / max ${m.chunking.max}</div></div>
      <div class="prov"><div class="k">capture</div><div class="v">${esc(m.capture.http_status ?? "—")} · ${esc(m.capture.content_type || "—")}<br>${esc(m.capture.fetched_url)}</div></div>
    </div>

    ${damaged.length ? `<div class="blocked-note">
      <b>Localised damage:</b> chunk${damaged.length === 1 ? "" : "s"}
      ${damaged.map((c) => `#${c.seq}`).join(", ")}
      ${damaged.length === 1 ? "is" : "are"} unreadable. Only records whose byte spans overlap ${damaged.length === 1 ? "it" : "them"} are blocked;
      every other record and every other object stays readable. Restore is available below for demo chunks.
    </div>` : ""}

    <div class="panel">
      <h3>Chunks <span class="muted small">(${data.chunk_availability.distinct} distinct · ${data.chunk_availability.locally_stored} slots · ${chunks.filter(c => c.refcount > 1).length} shared)</span></h3>
      <p class="hint small">Click a chunk to fetch its exact bytes, or to run the demo damage/restore action. A shared chunk damages only if you target it.</p>
      <div class="chunk-strip" id="chunk-strip">${chunkStrip}</div>
    </div>

    <div class="panel">
      <h3>Parse runs for this object</h3>
      <div id="obj-runs"></div>
      <div class="search" style="margin-top:10px">
        <select id="rule-select">
          ${(state.data.rules || []).map((r) =>
            `<option value="${esc(r.id)}" ${r.is_active ? "selected" : ""}>${esc(r.id)} — ${esc(r.description)}</option>`
          ).join("")}
        </select>
        <button class="btn btn-primary btn-sm" data-obj-act="parse">Start parse run</button>
      </div>
    </div>
  `;

  const runBox = $("#obj-runs", detail);
  if (!data.runs.length) {
    runBox.innerHTML = `<div class="empty">No runs yet for this object.</div>`;
  } else {
    runBox.innerHTML = `<table class="table"><thead><tr>
      <th>run</th><th>rule</th><th>status</th><th>records</th><th>result sha256</th><th>cursor</th><th></th>
    </tr></thead><tbody>${data.runs.map((r) => `
      <tr>
        <td class="mono">${esc(short(r.id, 14))}</td>
        <td>${esc(r.rule_id)}</td>
        <td><span class="badge ${runBadge(r.status)}">${esc(r.status)}</span></td>
        <td>${r.result_count ?? 0}</td>
        <td class="mono">${esc(short(r.result_digest, 16))}</td>
        <td class="mono">${bytes(r.cursor)} / ${bytes(o.total_bytes)}</td>
        <td class="nowrap">
          <button class="btn btn-sm" data-run-open="${esc(r.id)}">View</button>
          ${r.status === "done" ? `<button class="btn btn-sm" data-run-pick="${esc(r.id)}">Compare</button>` : ""}
        </td>
      </tr>`).join("")}</tbody></table>`;
  }

  detail.querySelectorAll("[data-obj-act]").forEach((btn) => {
    btn.addEventListener("click", () => objectAction(btn, o, data));
  });
  detail.querySelectorAll("[data-run-open]").forEach((btn) => {
    btn.addEventListener("click", () => openRun(btn.dataset.runOpen));
  });
  detail.querySelectorAll("[data-run-pick]").forEach((btn) => {
    btn.addEventListener("click", () => pickForCompare(btn.dataset.runPick));
  });
  detail.querySelectorAll("[data-chunk-seq]").forEach((el) => {
    el.addEventListener("click", () => chunkDialog(o, data, Number(el.dataset.chunkSeq)));
  });
}

function runBadge(status) {
  return { done: "ok", running: "info", paused: "warn", queued: "info",
           failed: "bad", canceled: "bad", interrupted: "warn" }[status] || "";
}

async function objectAction(btn, o, data) {
  const kind = btn.dataset.objAct;
  btn.disabled = true;
  try {
    if (kind === "verify") {
      const report = await act(() => api(`/api/objects/${encodeURIComponent(o.id)}/verify`), null);
      if (report.status === "ok") {
        toast(`All ${report.chunks_total} chunks verified; stream digest matches.`, "ok");
      } else {
        toast(`Verification: ${report.status}. ${report.damaged.length} chunk(s) damaged; ${report.affected_records} record(s) blocked across ${report.affected_runs.length} run(s).`, "error");
      }
      await openObject(o.id);
    } else if (kind === "bytes") {
      // Direct byte download, no zip: this is the exact captured stream.
      const res = await fetch(`/api/objects/${encodeURIComponent(o.id)}/bytes`);
      if (!res.ok) {
        const p = await res.json().catch(() => ({}));
        throw new Error(p.error || "download failed");
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url; a.download = `${o.id}.orig`;
      document.body.appendChild(a); a.click(); a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 3000);
      toast("Exact original bytes downloaded.", "ok");
    } else if (kind === "export") {
      const name = await downloadPost(`/api/objects/${encodeURIComponent(o.id)}/export`,
        { partial: false }, `sealed-${o.id}.zip`);
      toast(`Downloaded ${name}. Verify offline with: python3 verify_package.py .`, "ok");
    } else if (kind === "parse") {
      const ruleId = $("#rule-select").value;
      const res = await act(
        () => api(`/api/objects/parse`, { method: "POST", body: { object: o.id, rule_id: ruleId } }),
        `Run queued with ${ruleId}`
      );
      if (res.worker && res.worker.started) toast(`parse worker started (pid ${res.worker.pid})`);
      setTimeout(() => openObject(o.id), 1100);
    }
  } catch (err) {
    if (kind === "bytes" || kind === "export") toast(err.message, "error");
  }
  btn.disabled = false;
  if (kind !== "parse" && kind !== "verify") refresh({ quiet: true });
}

async function chunkDialog(o, data, seq) {
  const chunk = data.chunks.find((c) => c.seq === seq);
  if (!chunk) return;
  const body = `
    <div class="mono-block">digest ${esc(chunk.digest)}
offset ${chunk.offset} · length ${chunk.length} B · shared by ${chunk.refcount} object(s)</div>
    <div class="search" style="margin-top:10px;flex-wrap:wrap">
      <button class="btn btn-sm" id="dl-chunk">Fetch bytes</button>
      <button class="btn btn-sm btn-danger" id="dmg-chunk" ${chunk.status === "damaged" ? "disabled" : ""}>Simulate corruption</button>
      <button class="btn btn-sm btn-danger" id="miss-chunk" ${chunk.status === "damaged" ? "disabled" : ""}>Simulate loss</button>
      <button class="btn btn-sm" id="fix-chunk" ${chunk.status === "damaged" ? "" : "disabled"}>Restore from quarantine</button>
    </div>
    <div id="chunk-preview" class="mono-block" style="margin-top:10px;max-height:200px;overflow:auto"></div>`;
  const modal = showModal(`Chunk #${seq}`, body);
  const root = modal.el;
  $("#dl-chunk", root).addEventListener("click", async () => {
    const res = await fetch(`/api/objects/${encodeURIComponent(o.id)}/chunks/${seq}`);
    if (!res.ok) {
      const p = await res.json().catch(() => ({}));
      $("#chunk-preview", root).textContent = "ERROR: " + (p.error || res.statusText);
      return;
    }
    const text = await res.text();
    $("#chunk-preview", root).textContent = text.slice(0, 4000) +
      (text.length > 4000 ? `\n… (${text.length} chars total)` : "");
  });
  const doDamage = async (mode) => {
    try {
      await api(`/api/objects/${encodeURIComponent(o.id)}/damage`,
        { method: "POST", body: { seq, mode, force: true } });
      modal.close();
      toast(`Chunk #${seq} ${mode === "missing" ? "removed" : "corrupted"} (demo). Quarantined original kept for restore.`, "error");
      await openObject(o.id);
      refresh({ quiet: true });
    } catch (err) { toast(err.message, "error"); }
  };
  $("#dmg-chunk", root).addEventListener("click", () => doDamage("corrupt"));
  $("#miss-chunk", root).addEventListener("click", () => doDamage("missing"));
  $("#fix-chunk", root).addEventListener("click", async () => {
    try {
      await api(`/api/chunks/${encodeURIComponent(chunk.digest)}`, { method: "POST", body: {} });
      modal.close();
      toast("Chunk restored. Records that depended on it are readable again.", "ok");
      await openObject(o.id);
      refresh({ quiet: true });
    } catch (err) { toast(err.message, "error"); }
  });
}

/* ------------------------------ runs ------------------------------ */

function renderRulePanel() {
  const panel = $("#rule-panel");
  if (!state.data.rules.length) {
    panel.innerHTML = `<h3>Parse rules</h3><div class="empty">No rules registered yet.</div>`;
    return;
  }
  const byName = {};
  state.data.rules.forEach((r) => { (byName[r.name] = byName[r.name] || []).push(r); });
  panel.innerHTML = `<h3>Parse rules <span class="muted small">(switch the version used by new runs; old runs keep their pinned version)</span></h3>
    ${Object.entries(byName).map(([name, versions]) => `
      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:8px 0">
        <b class="mono">${esc(name)}</b>
        <select data-family="${esc(name)}">
          ${versions.map((v) => `<option value="${esc(v.id)}" ${v.is_active ? "selected" : ""}>
            ${esc(v.id)} — ${esc(v.description)}</option>`).join("")}
        </select>
        <button class="btn btn-sm" data-switch-family="${esc(name)}">Make active</button>
        <span class="muted small">source sha256 ${esc(short(versions[0].source_sha256, 16))}</span>
      </div>`).join("")}`;
  panel.querySelectorAll("[data-switch-family]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const family = btn.dataset.switchFamily;
      const select = $(`select[data-family="${family}"]`, panel);
      try {
        await api("/api/rules/switch", { method: "POST", body: { family, rule_id: select.value } });
        toast(`Active rule for ${family} is now ${select.value}`, "ok");
        refresh({ quiet: true });
      } catch (err) { toast(err.message, "error"); }
    });
  });
}

function renderRuns() {
  const list = $("#run-list");
  const runs = state.data.runs;
  if (!runs.length) {
    list.innerHTML = `<div class="empty">No parse runs yet. Open an object and start one.</div>`;
    return;
  }
  list.innerHTML = runs.map((r) => {
    const sel = state.selectedRun === r.id ? " is-selected" : "";
    const pct = r.total_chunks ? Math.min(100, Math.round((r.processed_chunks / r.total_chunks) * 100)) : 0;
    const active = ["queued", "running", "paused"].includes(r.status);
    return `
      <div class="row-card${sel}" data-run="${esc(r.id)}">
        <div>
          <div class="row-title mono">${esc(short(r.id, 18))}
            <span class="badge ${runBadge(r.status)}">${esc(r.status)}</span>
            <span class="badge">${esc(r.rule_id)}</span>
          </div>
          <div class="row-sub">
            <span>object ${esc(short(r.object_id, 16))}</span>
            <span>${r.result_count ?? 0} records</span>
            <span>${r.status === "done" ? "result " + esc(short(r.result_digest, 16)) : `${pct}% checkpointed`}</span>
            ${r.stopped_by ? `<span class="badge warn">stopped: ${esc(r.stopped_by)}</span>` : ""}
          </div>
          ${active ? `<div class="progress" style="margin-top:8px"><i style="width:${pct}%"></i></div>` : ""}
        </div>
        <div class="row-sub">
          <span>${when(r.started_at || r.created_at)}</span>
          <span class="mono">cursor ${bytes(r.cursor)}</span>
        </div>
        <div class="row-actions">
          <button class="btn btn-sm" data-run-act="view">View results</button>
          ${active ? `<button class="btn btn-sm" data-run-act="resume">Resume</button>` : ""}
          ${r.status === "done" ? `<button class="btn btn-sm" data-run-act="compare">Compare</button>` : ""}
        </div>
      </div>`;
  }).join("");
  list.querySelectorAll("[data-run]").forEach((row) => {
    row.addEventListener("click", (ev) => {
      if (ev.target.closest("[data-run-act]")) return;
      openRun(row.dataset.run);
    });
  });
  list.querySelectorAll("[data-run-act]").forEach((btn) => {
    const runId = btn.closest("[data-run]").dataset.run;
    btn.addEventListener("click", async () => {
      if (btn.dataset.runAct === "view") return openRun(runId);
      if (btn.dataset.runAct === "resume") {
        try {
          await api(`/api/runs/${encodeURIComponent(runId)}/resume`, { method: "POST", body: {} });
          toast("Resume requested; a parse worker will pick it up.", "ok");
          setTimeout(() => openRun(runId), 900);
        } catch (err) { toast(err.message, "error"); }
      }
      if (btn.dataset.runAct === "compare") return pickForCompare(runId);
    });
  });
}

function pickForCompare(runId) {
  const runs = state.data.runs;
  if (!state.compareA || state.compareA === runId) {
    state.compareA = runId;
    toast("Picked as run A. Choose another run to compare.");
    return;
  }
  state.compareB = runId;
  openCompare(state.compareA, state.compareB);
  state.compareA = state.compareB = null;
}

async function openRun(runId) {
  state.selectedRun = runId;
  setView("runs");
  const detail = $("#run-detail");
  const compareDetail = $("#compare-detail");
  compareDetail.hidden = true;
  detail.hidden = false;
  detail.innerHTML = `<div class="panel"><span class="spinner"></span> loading run…</div>`;
  try {
    const data = await api(`/api/runs/${encodeURIComponent(runId)}/view`);
    renderRunDetail(data);
  } catch (err) {
    detail.innerHTML = `<div class="banner error">${esc(err.message)}</div>`;
  }
}

function renderRunDetail(data) {
  const run = data.run;
  const records = data.records;
  const detail = $("#run-detail");
  const active = ["queued", "running", "paused"].includes(run.status);
  detail.innerHTML = `
    <div class="detail-head">
      <div>
        <h2 class="mono">${esc(run.id)} <span class="badge ${runBadge(run.status)}">${esc(run.status)}</span></h2>
        <p class="hint">
          rule <b>${esc(run.rule_id)}</b> over object <b>${esc(run.object_id)}</b>.
          ${data.blocked_records ? `<b style="color:var(--bad)">${data.blocked_records}</b> of ${data.total_records} records are blocked by damaged chunks; the rest are served.` :
            `All ${data.total_records} records readable.`}
        </p>
      </div>
      <div class="row-actions">
        ${active ? `<button class="btn btn-sm" data-runctl="stop">Stop (graceful)</button>
                    <button class="btn btn-sm btn-danger" data-runctl="kill">Stop &amp; kill worker</button>
                    <button class="btn btn-sm btn-primary" data-runctl="resume">Resume</button>` : ""}
        ${run.status === "done" ? `<button class="btn btn-sm" data-runctl="compare">Compare…</button>
          <button class="btn btn-sm" data-runctl="export">Export package</button>` : ""}
      </div>
    </div>
    <div class="provenance">
      <div class="prov"><div class="k">result sha256</div><div class="v">${esc(run.result_digest || "—")}</div></div>
      <div class="prov"><div class="k">checkpoint cursor</div><div class="v">${bytes(run.cursor)}</div></div>
      <div class="prov"><div class="k">stopped by</div><div class="v">${esc(run.stopped_by || "—")}</div></div>
      <div class="prov"><div class="k">started / finished</div><div class="v">${esc(when(run.started_at))}<br>${esc(when(run.finished_at))}</div></div>
    </div>
    ${run.error ? `<div class="banner ${run.status === "failed" ? "error" : ""}">${esc(run.error)}</div>` : ""}
    <div class="panel">
      <h3>Records <span class="muted small">(${records.length} · ${data.blocked_records} blocked · ${data.gaps.length} gap${data.gaps.length === 1 ? "" : "s"})</span></h3>
      ${data.gaps.map((g) => `<div class="gap">Gap ${g.start}–${g.end}: ${esc(g.reason)}. No records were invented for these bytes.</div>`).join("")}
      <div class="scroll"><div style="padding:8px">${records.map(recordHtml).join("") || `<div class="empty">No records yet.</div>`}</div></div>
    </div>
  `;
  detail.querySelectorAll("[data-runctl]").forEach((btn) => {
    btn.addEventListener("click", () => runControl(btn.dataset.runctl, run, records));
  });
}

function recordHtml(rec) {
  const v = rec.value || {};
  const fields = Object.entries(v)
    .filter(([k]) => k !== "id")
    .map(([k, val]) => `<span class="f"><span class="fname">${esc(k)}</span><span>${esc(val)}</span></span>`)
    .join("");
  const blockedBy = rec.blocked_by && rec.blocked_by.length
    ? `<div class="blocked-note small">Blocked: depends on damaged chunk${rec.blocked_by.length > 1 ? "s" : ""}
        ${rec.blocked_by.map((d) => `<span class="mono">${esc(short(d, 12))}</span>`).join(", ")}</div>`
    : "";
  return `
    <div class="record ${rec.blocked ? "blocked" : ""}">
      <div class="record-head">
        <b>${esc(v.id ?? "record " + rec.ordinal)}</b>
        ${rec.blocked ? `<span class="badge bad">blocked</span>` : `<span class="badge ok">served</span>`}
      </div>
      <div class="record-fields">${fields}</div>
      <div class="record-foot">bytes ${rec.span.start}–${rec.span.end} · chunk #${rec.chunk.seq} ${esc(short(rec.chunk.digest, 16))} · deps ${rec.deps.length}</div>
      ${blockedBy}
    </div>`;
}

async function runControl(kind, run, records) {
  try {
    if (kind === "stop") {
      await api(`/api/runs/${encodeURIComponent(run.id)}/stop`, { method: "POST", body: { kill_worker: false } });
      toast("Graceful stop requested. The worker checkpoints and exits as paused.", "ok");
    } else if (kind === "kill") {
      await api(`/api/runs/${encodeURIComponent(run.id)}/stop`, { method: "POST", body: { kill_worker: true } });
      toast("Parse worker SIGKILLed. Run stays resumable from its last checkpoint.", "error");
    } else if (kind === "resume") {
      await api(`/api/runs/${encodeURIComponent(run.id)}/resume`, { method: "POST", body: {} });
      toast("Resume requested.", "ok");
    } else if (kind === "compare") {
      return pickForCompare(run.id);
    } else if (kind === "export") {
      const name = await downloadPost(`/api/objects/${encodeURIComponent(run.object_id)}/export`,
        { run_ids: [run.id] }, `sealed-${run.object_id}.zip`);
      toast(`Exported ${name} with this run's result pinned.`, "ok");
      return;
    }
    setTimeout(() => openRun(run.id), 800);
    refresh({ quiet: true });
  } catch (err) { toast(err.message, "error"); }
}

async function openCompare(a, b) {
  setView("runs");
  $("#run-detail").hidden = true;
  const el = $("#compare-detail");
  el.hidden = false;
  el.innerHTML = `<div class="panel"><span class="spinner"></span> comparing…</div>`;
  try {
    const data = await api(`/api/compare?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`);
    renderCompare(el, data);
  } catch (err) {
    el.innerHTML = `<div class="banner error">${esc(err.message)}</div>`;
  }
}

function renderCompare(el, data) {
  el.innerHTML = `
    <div class="detail-head">
      <div>
        <h2>Rule comparison</h2>
        <p class="hint">Same sealed object, two rule versions. Changed fields point into the exact bytes both runs read.</p>
      </div>
      <button class="btn btn-sm btn-quiet" id="close-compare">Close</button>
    </div>
    <div class="compare-grid">
      <div class="prov"><div class="k">run A</div><div class="v">${esc(data.run_a.id)}<br>${esc(data.run_a.rule_id)} · ${data.run_a.count} records<br>${esc(data.run_a.digest || "")}</div></div>
      <div class="prov"><div class="k">run B</div><div class="v">${esc(data.run_b.id)}<br>${esc(data.run_b.rule_id)} · ${data.run_b.count} records<br>${esc(data.run_b.digest || "")}</div></div>
    </div>
    ${data.same_object ? "" : `<div class="banner error">These runs are over different objects; the comparison is positional only.</div>`}
    <div class="panel">
      <h3>Summary</h3>
      <p class="small">${data.summary.changed} changed · ${data.summary.only_in_a} only in A · ${data.summary.only_in_b} only in B · ${data.unchanged} unchanged</p>
    </div>
    <div class="panel">
      <h3>Changed records (${data.changed.length})</h3>
      <div class="scroll"><div style="padding:8px">
        ${data.changed.map((ch) => `
          <div class="record">
            <div class="record-head"><b>${esc(ch.key)}</b></div>
            ${Object.entries(ch.fields).map(([f, pair]) => `
              <div class="diff-field">${esc(f)}: <span class="a">${esc(JSON.stringify(pair.a))}</span>
                → <span class="b">${esc(JSON.stringify(pair.b))}</span></div>`).join("")}
            <div class="record-foot">span A ${ch.span_a.start}–${ch.span_a.end} · span B ${ch.span_b.start}–${ch.span_b.end}</div>
          </div>`).join("") || `<div class="empty">No changed fields.</div>`}
      </div></div>
    </div>
  `;
  $("#close-compare", el).addEventListener("click", () => { el.hidden = true; });
}

/* ------------------------------ workers ------------------------------ */

function renderWorkers() {
  const grid = $("#worker-grid");
  const workers = state.data.workers;
  const items = [
    { key: "capture", name: "Capture worker (combined)", mod: "app.capture_worker",
      info: workers.capture_worker,
      note: "Fetches and seals in one process. Simplest mode; still separate from the parser." },
    { key: "fetch", name: "Fetch worker", mod: "app.fetch_worker", info: workers.fetch_worker,
      note: "The only process that talks to a source. Writes a staging file + length + SHA-256, nothing else." },
    { key: "seal", name: "Seal worker", mod: "app.seal_worker", info: workers.seal_worker,
      note: "Reads staging only, re-hashes it, then chunks and commits the sealed object." },
    { key: "parse", name: "Parse worker", mod: "app.parse_worker", info: workers.parse_worker,
      note: "Claims parse runs, reads only sealed chunks, checkpoints each batch." },
  ];
  grid.innerHTML = items.map((it) => `
    <article class="card">
      <div class="card-head">
        <h3><span class="dot ${it.info.running ? "ok" : ""}"></span>${it.name}</h3>
        <span class="badge ${it.info.running ? "ok" : ""}">${it.info.running ? "pid " + it.info.pid : "stopped"}</span>
      </div>
      <p class="hint small">${it.note}</p>
      <div class="mono-block">PYTHONPATH=. python3 -m ${it.mod}</div>
      <div class="actions">
        <button class="btn btn-sm btn-primary" data-worker="${it.key}" data-worker-act="start">Start</button>
        <button class="btn btn-sm" data-worker="${it.key}" data-worker-act="stop">Stop (SIGTERM)</button>
        <button class="btn btn-sm btn-danger" data-worker="${it.key}" data-worker-act="kill">Kill (SIGKILL)</button>
      </div>
    </article>`).join("");
  grid.querySelectorAll("[data-worker-act]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const worker = btn.dataset.worker;
      const action = btn.dataset.workerAct;
      try {
        if (action === "start") {
          await api("/api/workers", { method: "POST", body: { action: "start", worker } });
          toast(`${worker} worker started.`, "ok");
        } else {
          const res = await api("/api/workers", {
            method: "POST", body: { action: "stop", worker, hard: action === "kill" },
          });
          toast(res.stopped ? `${worker} worker stopped (${res.signal || "SIGTERM"}).` : "Worker was not running.",
            action === "kill" ? "error" : "ok");
        }
        refresh({ quiet: true });
      } catch (err) { toast(err.message, "error"); }
    });
  });
  $("#worker-notes").innerHTML = [
    "All four workers are separate OS processes; any of them can be started here or from a shell.",
    "Capture (combined) does fetch+seal in one process. Fetch and Seal split those stages: only the fetch worker touches a source, only the seal worker writes the vault.",
    "The parse worker never opens a network connection; its only input is the sealed manifest.",
    "SIGKILL mid-run is safe: committed batches are already durable, the lease expires, and any worker resumes from the checkpoint.",
    "Graceful stop finishes the current batch, then writes status=paused and its cursor.",
  ].map((t) => `<li>${esc(t)}</li>`).join("");
}

/* ------------------------------ events ------------------------------ */

async function loadEvents() {
  try {
    const data = await api("/api/events?limit=120");
    const tbody = $("#event-table tbody");
    tbody.innerHTML = data.events.map((e) => `
      <tr>
        <td class="mono nowrap">${esc(when(e.at))}</td>
        <td><span class="badge">${esc(e.kind)}</span></td>
        <td class="mono">${esc(e.entity || "")} ${esc(short(e.entity_id || "", 16))}</td>
        <td>${esc(e.actor)}</td>
        <td class="mono">${esc(JSON.stringify(e.detail))}</td>
      </tr>`).join("") || `<tr><td colspan="5" class="empty">No events.</td></tr>`;
  } catch (err) {
    toast(err.message, "error");
  }
}

/* ------------------------------ modal ------------------------------ */

function showModal(title, html) {
  const wrap = document.createElement("div");
  wrap.className = "modal-backdrop";
  wrap.innerHTML = `<div class="modal"><div class="card-head"><h3>${esc(title)}</h3>
    <button class="btn btn-sm btn-quiet" data-close>Close</button></div>${html}</div>`;
  document.body.appendChild(wrap);
  const close = () => wrap.remove();
  $("[data-close]", wrap).addEventListener("click", close);
  wrap.addEventListener("click", (ev) => { if (ev.target === wrap) close(); });
  return { el: wrap, close };
}

/* ------------------------------ nav / init ------------------------------ */

function setView(view) {
  state.view = view;
  $$(".tab").forEach((t) => t.classList.toggle("is-active", t.dataset.view === view));
  $$(".view").forEach((v) => v.classList.toggle("is-active", v.id === "view-" + view));
  if (view === "events") loadEvents();
}

function init() {
  $$(".tab").forEach((tab) => tab.addEventListener("click", () => setView(tab.dataset.view)));

  $("#btn-seed").addEventListener("click", async () => {
    try {
      await act(() => api("/api/demo/seed", { method: "POST", body: {} }), "Fixtures and rules registered.");
    } catch (_) {}
  });

  $("#btn-find-object").addEventListener("click", () => {
    const key = $("#object-search").value.trim();
    if (key) openObject(key);
  });
  $("#object-search").addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") $("#btn-find-object").click();
  });
  $("#btn-refresh-events").addEventListener("click", loadEvents);

  refresh();
  state.timer = setInterval(() => refresh({ quiet: true }), 4000);
}

document.addEventListener("DOMContentLoaded", init);
