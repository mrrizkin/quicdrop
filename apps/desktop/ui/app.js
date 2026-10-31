/* QuicDrop desktop UI — talks to the daemon through the `api` Tauri command. */

const { invoke } = window.__TAURI__.core;
const { listen } = window.__TAURI__.event;

const state = {
  info: {},
  serverPeers: [],
  manual: [],
  savedRouteStates: {},
  devices: [],
  sections: { lan: [], mesh: [], saved: [] },
  transfers: [],
  approvals: [],
  trust: [],
  config: { out_dir: "", policy: "ask" },
  fileQuery: "",
  selectedPeer: null,
};

Object.defineProperty(state, "peers", {
  get() {
    return flattenSections(this.sections);
  },
  set(_) {},
  configurable: true,
  enumerable: true,
});

const $ = (id) => document.getElementById(id);

const icon = (name) => `<svg class="i"><use href="#i-${name}"/></svg>`;


const esc = (value) =>
  String(value ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[c]);

function fmtBytes(n) {
  n = Number(n) || 0;
  if (n < 1024) return `${n} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let value = n;
  let i = -1;
  do {
    value /= 1024;
    i += 1;
  } while (value >= 1024 && i < units.length - 1);
  return `${value.toFixed(value >= 100 ? 0 : 1)} ${units[i]}`;
}

function timeAgo(unixSecs) {
  if (!unixSecs) return "just now";
  const secs = Math.max(0, Math.floor(Date.now() / 1000) - unixSecs);
  if (secs < 60) return "just now";
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

function initial(name) {
  const clean = String(name || "?").trim();
  return esc(clean ? clean[0].toUpperCase() : "?");
}

function shortId(id) {
  return esc(String(id || "").slice(0, 8));
}

/* Stable, distinct avatar colour per device id. */
function hueFor(id) {
  let h = 0;
  for (const ch of String(id || "")) h = (h * 31 + ch.charCodeAt(0)) % 360;
  return h;
}

function fmtRate(bytesPerSec) {
  if (!bytesPerSec || !Number.isFinite(bytesPerSec)) return "";
  return `${fmtBytes(bytesPerSec)}/s`;
}

function fmtEta(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) return "";
  if (seconds < 60) return `${Math.ceil(seconds)}s left`;
  if (seconds < 3600) return `${Math.ceil(seconds / 60)}m left`;
  return `${Math.ceil(seconds / 3600)}h left`;
}

function fmtDuration(ms) {
  if (!Number.isFinite(ms) || ms < 0) return "";
  const sec = ms / 1000;
  if (sec < 59.95) return `${sec.toFixed(1)} s`;
  const totalSec = Math.round(sec);
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  if (m < 60) return s > 0 ? `${m}m ${s}s` : `${m}m`;
  const h = Math.floor(m / 60);
  const remM = m % 60;
  return remM > 0 ? `${h}h ${remM}m` : `${h}h`;
}

/* Throughput samples only advance when bytes do; redraws do not decay the rate. */
const speedSamples = new Map();
const RATE_WINDOW_MS = 250;
const RATE_IDLE_MS = 3000;

function rateFor(transfer) {
  if (transfer.status !== "active") {
    speedSamples.delete(transfer.id);
    return 0;
  }
  const now = performance.now();
  const bytes = transfer.transferred || 0;
  const prev = speedSamples.get(transfer.id);
  if (!prev || bytes < prev.bytes) {
    speedSamples.set(transfer.id, { bytes, at: now, rate: 0 });
    return 0;
  }
  if (bytes === prev.bytes) {
    if (now - prev.at >= RATE_IDLE_MS && prev.rate > 0) {
      prev.rate *= 0.9;
      prev.at = now;
    }
    return prev.rate;
  }
  const elapsed = (now - prev.at) / 1000;
  if (elapsed * 1000 < RATE_WINDOW_MS) return prev.rate;
  const instant = (bytes - prev.bytes) / elapsed;
  const rate = prev.rate > 0 ? prev.rate * 0.7 + instant * 0.3 : instant;
  speedSamples.set(transfer.id, { bytes, at: now, rate });
  return rate;
}


const PREFS_KEY = "quicdrop-prefs";
const prefs = {
  theme: "system",
  density: "comfortable",
  motion: true,
  openAfter: false,
  addresses: [],
};

function loadPrefs() {
  try {
    Object.assign(prefs, JSON.parse(localStorage.getItem(PREFS_KEY) || "{}"));
  } catch (_) {
    /* Corrupt or unavailable storage falls back to defaults. */
  }
  applyPrefs();
}

function savePrefs() {
  try {
    localStorage.setItem(PREFS_KEY, JSON.stringify(prefs));
  } catch (_) {
    /* Private mode or a full quota must not block the UI. */
  }
  applyPrefs();
}

function resolvedTheme() {
  if (prefs.theme !== "system") return prefs.theme;
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function applyPrefs() {
  const root = document.documentElement;
  root.dataset.theme = resolvedTheme();
  root.dataset.density = prefs.density;
  root.dataset.motion = prefs.motion ? "on" : "off";
}

function toggleTheme() {
  const order = ["system", "light", "dark"];
  prefs.theme = order[(order.indexOf(prefs.theme) + 1) % order.length];
  savePrefs();
}

let toastTimer;
function toast(message) {
  const el = $("toast");
  el.textContent = message;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (el.hidden = true), 4200);
}

async function api(method, path, body) {
  return invoke("api", { method, path, body: body ?? null });
}


async function refresh() {
  try {
    const [status, peers, transfers, trust, config, approvals] = await Promise.all([
      api("GET", "/api/status"),
      api("GET", "/api/peers"),
      api("GET", "/api/transfers?limit=50"),
      api("GET", "/api/trust"),
      api("GET", "/api/config"),
      api("GET", "/api/approvals"),
    ]);
    state.info = status;
    state.transfers = mergeTransfers(state.transfers, transfers);
    state.trust = trust;
    state.config = config;
    state.approvals = approvals;
    seedSavedRouteStates(transfers);
    mergePeers(peers);
    renderAll();
  } catch (error) {
    toast(`Cannot reach daemon: ${error}`);
  }
}

function peerKey(peer) {
  return peer.key || peer.device_id || peer.addr;
}

function recomputeDevices() {
  state.devices = buildDevices(state.serverPeers, state.manual, state.savedRouteStates);
  applyTrust();
  state.sections = assignRouteSections(state.devices);
}

function updateSavedRouteLiveState(addr, routeState) {
  if (!addr) return;
  if (state.savedRouteStates[addr] !== routeState) {
    state.savedRouteStates[addr] = routeState;
    recomputeDevices();
    schedulePeers();
  }
}

function seedSavedRouteStates(transfers) {
  if (!Array.isArray(transfers)) return;
  for (const saved of state.manual) {
    if (state.savedRouteStates[saved.addr]) continue;
    const recent = transfers.find(
      (t) =>
        t.direction === "out" &&
        t.peer_id === saved.addr &&
        (t.status === "done" || t.status === "failed"),
    );
    if (recent) {
      state.savedRouteStates[saved.addr] = recent.status === "done" ? "live" : "lost";
    }
  }
}

function manualPeer(addr, label = "") {
  const clean = String(label || "").trim();
  return {
    device_id: "",
    name: clean || addr,
    label: clean,
    platform: "",
    addr,
    source: "address",
    trusted: false,
    manual: true,
    last_seen: 0,
  };
}

function withDefaultPort(target, port) {
  if (!port) return target;
  if (target.startsWith("[")) {
    return target.includes("]:") ? target : `${target.replace(/\]$/, "")}]:${port}`;
  }
  const colons = (target.match(/:/g) || []).length;
  if (colons === 0) return `${target}:${port}`;
  if (colons === 1) return /:\d+$/.test(target) ? target : `${target}:${port}`;
  return `[${target}]:${port}`;
}

function loadManualPeers() {
  const port = state.info.quic_port || 53320;
  const saved = Array.isArray(prefs.addresses) ? prefs.addresses : [];
  state.manual = saved
    .map((entry) => {
      const raw = typeof entry === "string" ? entry : entry?.addr;
      if (!raw) return null;
      const label = typeof entry === "string" ? "" : entry.label || "";
      return manualPeer(withDefaultPort(raw, port), label);
    })
    .filter(Boolean);
  saveManualPrefs();
  recomputeDevices();
}

function saveManualPrefs() {
  prefs.addresses = state.manual.map((peer) => ({ addr: peer.addr, label: peer.label || "" }));
  savePrefs();
}

/* Discovered peers and saved addresses coexist as independent routes. */
function mergePeers(serverPeers) {
  state.serverPeers = Array.isArray(serverPeers) ? serverPeers : [];
  recomputeDevices();
  return state.peers;
}

function addManualPeer(rawAddress, label, port) {
  const addr = withDefaultPort(String(rawAddress || "").trim(), port || state.info.quic_port || 53320);
  if (!addr) return "";
  if (state.manual.some((peer) => peer.addr === addr)) {
    toast(`${addr} is already in the list.`);
    return "";
  }
  const peer = manualPeer(addr, label);
  state.manual.push(peer);
  recomputeDevices();
  saveManualPrefs();
  return addr;
}

function removeManualPeer(addr) {
  state.manual = state.manual.filter((peer) => peer.addr !== addr);
  delete state.savedRouteStates[addr];
  const savedKey = `saved:${addr}`;
  if (state.selectedPeer === savedKey || state.selectedPeer === addr) {
    state.selectedPeer = null;
  }
  recomputeDevices();
  saveManualPrefs();
  renderPeers();
}

function upsertPeer(peer) {
  const index = state.serverPeers.findIndex(
    (p) => (p.device_id && p.device_id === peer.device_id) || p.addr === peer.addr,
  );
  if (index >= 0) state.serverPeers[index] = peer;
  else state.serverPeers.push(peer);
  recomputeDevices();
}

function removeDiscoveredPeer(deviceId) {
  const lost = state.serverPeers.find((p) => p.device_id === deviceId);
  if (!lost) return;
  const lanKey = `lan:${deviceId}`;
  if (
    state.selectedPeer === lanKey ||
    state.selectedPeer === deviceId ||
    state.selectedPeer === lost.addr
  ) {
    state.selectedPeer = null;
  }
  state.serverPeers = state.serverPeers.filter((p) => p.device_id !== deviceId);
  recomputeDevices();
  schedulePeers();
}

const TERMINAL_STATUS = new Set(["done", "failed", "cancelled", "declined"]);

const FILE_PHASES = ["active", "queue", "done", "failed"];
const FILE_PHASE_LABELS = {
  active: "In progress",
  queue: "Queued",
  done: "Done",
  failed: "Failed",
};

function filePhase(file) {
  if (file.ok === true) return "done";
  if (file.ok === false) return "failed";
  return (file.done || 0) > 0 ? "active" : "queue";
}

/* Files still moving rank before byte-complete files awaiting verification. */
function fileRank(file) {
  if (file.ok === true || file.ok === false) return 2;
  return file.size > 0 && (file.done || 0) >= file.size ? 1 : 0;
}

function sortFileIndices(files, indices) {
  return [...indices].sort((a, b) => fileRank(files[a]) - fileRank(files[b]) || a - b);
}

function filePhases(transfer) {
  const counts = { active: 0, queue: 0, done: 0, failed: 0 };
  (transfer.files || []).forEach((file) => {
    counts[filePhase(file)] += 1;
  });
  return counts;
}

/* Folder shares keep their root as the first path segment of every file. */
function folderRoot(transfer) {
  const files = transfer.files || [];
  if (files.length === 0) return null;
  const roots = new Set();
  for (const file of files) {
    const name = String(file.name || "");
    const slash = name.indexOf("/");
    if (slash <= 0) return null;
    roots.add(name.slice(0, slash));
  }
  const unique = [...roots];
  return { name: unique.length === 1 ? unique[0] : `${unique.length} folders` };
}

/* Keep the furthest progress and terminal status from a late HTTP snapshot. */
function mergeTransferView(current, incoming) {
  if (!current) return incoming;
  const files = (incoming.files || []).map((file, index) => {
    const old = current.files?.[index];
    if (!old) return file;
    return {
      ...file,
      done: Math.max(old.done || 0, file.done || 0),
      ok: file.ok === true || file.ok === false ? file.ok : old.ok,
    };
  });
  const total =
    current.transferred > 0 && current.total_bytes > 0
      ? current.total_bytes
      : incoming.total_bytes || current.total_bytes || 0;
  const settled = TERMINAL_STATUS.has(current.status);
  return {
    ...incoming,
    status: settled ? current.status : incoming.status,
    transferred: Math.max(current.transferred || 0, incoming.transferred || 0),
    total_bytes: total,
    files,
    error: incoming.error || current.error,
    finished_at: incoming.finished_at || current.finished_at,
    duration_ms: settled ? current.duration_ms : (incoming.duration_ms ?? current.duration_ms),
    avg_bps: settled ? current.avg_bps : (incoming.avg_bps ?? current.avg_bps),
  };
}

function mergeTransfers(current, incoming) {
  const local = new Map(current.map((transfer) => [transfer.id, transfer]));
  return incoming
    .map((transfer) => mergeTransferView(local.get(transfer.id), transfer))
    .sort(compareTransfers);
}

/* Bytes-complete transfers still verifying sit below transfers that are moving. */
function transferRank(transfer) {
  const status = transferStatus(transfer);
  if (status === "active") return 0;
  if (status === "verifying" || status === "waiting") return 1;
  return 2;
}

function compareTransfers(a, b) {
  return transferRank(a) - transferRank(b) || (b.started_at || 0) - (a.started_at || 0);
}

function upsertTransfer(transfer) {
  const index = state.transfers.findIndex((t) => t.id === transfer.id);
  if (index >= 0) state.transfers[index] = mergeTransferView(state.transfers[index], transfer);
  else state.transfers.unshift(transfer);
  state.transfers.sort(compareTransfers);
  if (state.transfers.length > 50) state.transfers.length = 50;
}

function applyTrust() {
  const trusted = new Set(state.trust.map((p) => p.device_id));
  state.devices.forEach((d) => {
    d.trusted = Boolean(d.device_id && trusted.has(d.device_id));
  });
}


function renderAll() {
  renderDevice();
  applyTrust();
  renderPeers();
  renderTrust();
  renderTransfers(true);
  renderApproval();
}

function renderDevice() {
  const { name, short_id: shortIdValue, device_id } = state.info;
  $("device-name").textContent = name || "this device";
  const short = shortIdValue || String(device_id || "").slice(0, 8);
  $("device-id").textContent = short ? `id ${short}` : "";
  $("device-id").title = device_id ? `Device id: ${device_id}` : "";
  $("device-dot").classList.add("on");
  const avatar = $("self-avatar");
  if (avatar) {
    avatar.textContent = initial(name);
    avatar.style.setProperty("--hue", hueFor(device_id || shortIdValue));
  }
}

const SECTION_DEFS = [
  { id: "lan", label: "LAN" },
  { id: "mesh", label: "Mesh" },
  { id: "saved", label: "Saved addresses" },
];

function renderPeers() {
  const totalCount = flattenSections(state.sections).length;
  $("peer-count").textContent = totalCount;
  const list = $("peer-list");
  $("peers-hint").textContent =
    totalCount === 0
      ? "Looking for devices on your network…"
      : "Pick a device, then send files or drop them here.";

  if (state.selectedPeer && !peerByKey(state.selectedPeer)) {
    state.selectedPeer = null;
  }
  const selected = Boolean(state.selectedPeer);
  $("btn-send").disabled = !selected;
  $("btn-send-folder").disabled = !selected;

  if (totalCount === 0) {
    list.innerHTML = `
      <li class="peer-empty">
        <div class="radar"></div>
        <div>No devices nearby.</div>
      </li>`;
    return;
  }

  const html = [];
  for (const def of SECTION_DEFS) {
    const items = state.sections[def.id] || [];
    if (items.length === 0) continue;

    html.push(`
      <li class="peer-section-head" role="separator" aria-label="${esc(def.label)}">
        <span>${esc(def.label)}</span>
        <span class="count">${items.length}</span>
      </li>`);

    for (const item of items) {
      const isSelected = state.selectedPeer === item.key;
      const title = item.manual
        ? (item.label ? `Send to ${item.label} (${item.addr})` : `Send to ${item.addr}`)
        : (item.device_id ? `${item.name} · via ${item.addr} · id ${shortId(item.device_id)}` : `${item.name} · via ${item.addr}`);
      const platform = item.platform
        ? `<span class="peer-os" title="Platform">${esc(item.platform)}</span>`
        : "";
      const idElem = item.device_id
        ? `<span class="peer-id" title="device id">${shortId(item.device_id)}</span>`
        : "";

      html.push(`
        <li class="peer ${isSelected ? "selected" : ""}" data-key="${esc(item.key)}"
            tabindex="0" role="button" aria-selected="${isSelected ? "true" : "false"}"
            title="${esc(title)}">
          <span class="peer-check">✓</span>
          <div class="avatar" style="--hue:${hueFor(item.device_id || item.addr)}">${initial(item.name)}</div>
          <div class="peer-lines">
            <div class="peer-row">
              <span class="peer-label">${esc(item.name)}</span>
              <span class="peer-tags">
                ${item.trusted ? `<span title="Trusted">${icon("shield")}</span>` : ""}
              </span>
            </div>
            <div class="peer-row">
              <span class="peer-meta-left">
                ${idElem}
                <span class="peer-route" title="via ${esc(item.addr)}">via ${esc(item.addr)}</span>
                ${platform}
              </span>
              <span class="peer-actions">
                ${item.manual ? `<button class="icon-btn peer-action" data-remove="${esc(item.addr)}" title="Remove">${icon("trash")}</button>` : ""}
                <button class="icon-btn peer-action" data-send-files="${esc(item.key)}" title="Send files">${icon("upload")}</button>
                <button class="icon-btn peer-action" data-send-folders="${esc(item.key)}" title="Send folders">${icon("folder")}</button>
              </span>
            </div>
          </div>
        </li>`);
    }
  }

  list.innerHTML = html.join("");

  list.querySelectorAll(".peer").forEach((row) => {
    row.addEventListener("click", (event) => {
      if (event.target.closest("[data-send-files]") || event.target.closest("[data-send-folders]") || event.target.closest("[data-remove]")) return;
      selectPeer(row.dataset.key);
    });
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        if (event.target !== row) return;
        event.preventDefault();
        selectPeer(row.dataset.key);
      }
    });
  });
  list.querySelectorAll("[data-remove]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      removeManualPeer(button.dataset.remove);
    });
  });
  list.querySelectorAll("[data-send-files]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const peer = peerByKey(button.dataset.sendFiles);
      if (peer) sendFilesToPeer(peer);
    });
  });
  list.querySelectorAll("[data-send-folders]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const peer = peerByKey(button.dataset.sendFolders);
      if (peer) sendFoldersToPeer(peer);
    });
  });
}

function selectPeer(key) {
  state.selectedPeer = state.selectedPeer === key ? null : key;
  renderPeers();
}

function peerByKey(key) {
  return findRouteRow(state.sections, key);
}

function renderTrust() {
  $("trust-count").textContent = state.trust.length;
  const list = $("trust-list");
  if (state.trust.length === 0) {
    list.innerHTML = `<li class="hint" style="padding:4px 6px">None</li>`;
    return;
  }
  list.innerHTML = state.trust
    .map(
      (peer) => `
      <li class="trust-item">
        <div class="avatar" style="--hue:${hueFor(peer.device_id)}">${initial(peer.name)}</div>
        <span class="name" title="${esc(peer.name)}">${esc(peer.name)}</span>
        <span class="peer-id" title="device id">${shortId(peer.device_id)}</span>
        <span class="when">${timeAgo(peer.last_seen)}</span>
        <button class="icon-btn" data-forget="${esc(peer.device_id)}" title="Forget">${icon("trash")}</button>
      </li>`,
    )
    .join("");
  list.querySelectorAll("[data-forget]").forEach((button) => {
    button.addEventListener("click", () => forget(button.dataset.forget));
  });
}

const FILE_ROWS_INITIAL = 100;
const FILE_ROWS_MAX = 500;
const expandedFiles = new Set();
const fileGroupOpen = new Map();
const DEFAULT_OPEN_PHASES = new Set(["active"]);

function openPhasesFor(transfer) {
  let phases = fileGroupOpen.get(transfer.id);
  if (!phases) {
    phases = new Set(DEFAULT_OPEN_PHASES);
    fileGroupOpen.set(transfer.id, phases);
  }
  return phases;
}
const transferRows = new Map();
const dirtyTransfers = new Set();
const dirtyFiles = new Map();
let allTransfersDirty = false;
let transfersRenderQueued = false;

function transferStatus(transfer) {
  if (
    transfer.status === "active" &&
    transfer.total_bytes > 0 &&
    (transfer.transferred || 0) >= transfer.total_bytes
  ) {
    return transfer.direction === "in" ? "verifying" : "waiting";
  }
  return transfer.status;
}

function filesKey(transfer) {
  const files = transfer.files || [];
  const folder = folderRoot(transfer);
  const phases = folder ? files.map(filePhase).join("") : "";
  const ranks = files.map(fileRank).join("");
  const open = folder ? [...openPhasesFor(transfer)].sort().join(",") : "";
  return `${files.length}|${state.fileQuery}|${expandedFiles.has(transfer.id) ? "all" : "part"}|${phases}|${ranks}|${open}`;
}

/* Peers and trust change on every beacon; rebuild their lists at most once a frame. */
let peersRenderQueued = false;
let trustRenderQueued = false;

function schedulePeers() {
  if (peersRenderQueued) return;
  peersRenderQueued = true;
  const run = () => {
    peersRenderQueued = false;
    recomputeDevices();
    renderPeers();
  };
  if (typeof requestAnimationFrame === "function") requestAnimationFrame(run);
  else setTimeout(run, 32);
}

function scheduleTrust() {
  if (trustRenderQueued) return;
  trustRenderQueued = true;
  const run = () => {
    trustRenderQueued = false;
    renderTrust();
  };
  if (typeof requestAnimationFrame === "function") requestAnimationFrame(run);
  else setTimeout(run, 32);
}

function scheduleTransfers() {
  if (transfersRenderQueued) return;
  transfersRenderQueued = true;
  const run = () => {
    transfersRenderQueued = false;
    renderTransfers();
  };
  if (typeof requestAnimationFrame === "function") requestAnimationFrame(run);
  else setTimeout(run, 32);
}

function markTransferDirty(id, allFiles = false) {
  if (id) {
    dirtyTransfers.add(id);
    if (allFiles) {
      const transfer = state.transfers.find((item) => item.id === id);
      if (transfer) {
        let indices = dirtyFiles.get(id);
        if (!indices) {
          indices = new Set();
          dirtyFiles.set(id, indices);
        }
        (transfer.files || []).forEach((_, index) => indices.add(index));
      }
    }
  } else {
    allTransfersDirty = true;
  }
  scheduleTransfers();
}

function markFileDirty(id, index) {
  let indices = dirtyFiles.get(id);
  if (!indices) {
    indices = new Set();
    dirtyFiles.set(id, indices);
  }
  indices.add(index);
  dirtyTransfers.add(id);
  scheduleTransfers();
}

function invalidateTransfers() {
  allTransfersDirty = true;
  scheduleTransfers();
}

function createTransferView(transfer) {
  const el = document.createElement("li");
  el.className = "transfer";
  el.dataset.transfer = transfer.id;
  el.innerHTML = `
    <div class="dir in"></div>
    <div class="t-main">
      <div class="transfer-top">
        <div class="transfer-title"></div>
        <button class="icon-btn" type="button" title="Cancel">${icon("stop")}</button>
      </div>
      <div class="transfer-sub"></div>
      <div class="t-counts" hidden></div>
      <div class="bar"><span></span></div>
      <ul class="file-rows"></ul>
    </div>
    <div class="t-side">
      <span class="status"></span>
      <span class="t-pct"></span>
    </div>`;
  const view = {
    el,
    dir: el.querySelector(".dir"),
    title: el.querySelector(".transfer-title"),
    cancel: el.querySelector("[title='Cancel']"),
    sub: el.querySelector(".transfer-sub"),
    counts: el.querySelector(".t-counts"),
    bar: el.querySelector(".bar > span"),
    files: el.querySelector(".file-rows"),
    status: el.querySelector(".status"),
    pct: el.querySelector(".t-pct"),
    fileNodes: new Map(),
    filesKey: "",
  };
  view.cancel.addEventListener("click", () => cancelTransfer(transfer.id));
  return view;
}

function updateTransferView(view, transfer) {
  const files = transfer.files || [];
  const total = transfer.total_bytes || 0;
  const done = transfer.transferred || 0;
  const pct = total > 0 ? Math.min(100, Math.floor((done / total) * 100)) : 0;
  const status = transferStatus(transfer);
  const summary =
    files.length === 0
      ? "no files"
      : files.length === 1
        ? files[0].name
        : `${files[0].name} +${files.length - 1}`;
  const rate = rateFor(transfer);
  const eta = rate > 0 ? fmtEta((total - done) / rate) : "";
  const showAvg =
    transfer.status === "done" &&
    transfer.avg_bps != null &&
    transfer.duration_ms != null &&
    done > 0;
  const bytesDesc = showAvg
    ? `${fmtBytes(done)} in ${fmtDuration(transfer.duration_ms)}`
    : `${fmtBytes(done)} / ${fmtBytes(total)}`;
  const rateDesc =
    transfer.status === "active" && rate > 0
      ? fmtRate(rate)
      : showAvg
        ? `avg ${fmtRate(transfer.avg_bps)}`
        : "";
  const etaDesc = transfer.status === "active" ? eta : "";
  const bits = [
    esc(transfer.peer_name),
    bytesDesc,
    rateDesc,
    etaDesc,
    timeAgo(transfer.finished_at || transfer.started_at),
  ].filter(Boolean);
  if (transfer.error) bits.push(esc(transfer.error));

  const folder = folderRoot(transfer);
  const counts = folder ? filePhases(transfer) : null;
  view.el.className = `transfer ${transfer.status}`;
  view.dir.className = `dir ${transfer.direction}`;
  view.dir.innerHTML = icon(transfer.direction === "in" ? "download" : "upload");
  view.title.textContent = folder ? folder.name : summary;
  view.title.title = folder ? folder.name : summary;
  view.sub.innerHTML = bits.join(" · ");
  if (counts) {
    view.counts.innerHTML = FILE_PHASES.map(
      (phase) =>
        `<span class="${phase}">${counts[phase]} ${FILE_PHASE_LABELS[phase].toLowerCase()}</span>`,
    ).join("");
    view.counts.hidden = false;
  } else {
    view.counts.innerHTML = "";
    view.counts.hidden = true;
  }
  view.bar.style.width = `${pct}%`;
  view.status.className = `status ${status}`;
  view.status.textContent = status;
  view.pct.textContent =
    transfer.status === "active"
      ? status === "verifying"
        ? "verify"
        : status === "waiting"
          ? "wait"
          : `${pct}%`
      : "";
  view.cancel.hidden = transfer.status !== "active";
}

function updateFileRow(view, file) {
  const done = file.done || 0;
  const pct = file.size > 0 ? Math.min(100, Math.floor((done / file.size) * 100)) : 0;
  const mark = file.ok === true ? "✓" : file.ok === false ? "✕" : "";
  const pending = !mark && file.size > 0 && done >= file.size;
  view.name.textContent = file.name;
  view.name.title = file.name;
  view.bytes.textContent = `${fmtBytes(done)} / ${fmtBytes(file.size)}`;
  view.pct.textContent = mark || (pending ? "verify" : `${pct}%`);
  view.pct.classList.toggle("pending", Boolean(pending));
  view.pct.classList.toggle("failed", file.ok === false);
  view.bar.style.width = `${pct}%`;
}

function createFileRow(index, file) {
  const el = document.createElement("li");
  el.className = "file-row";
  el.dataset.fileIndex = String(index);
  el.innerHTML = `
    <span class="file-name"></span>
    <span class="file-bytes"></span>
    <span class="file-pct"></span>
    <span class="file-bar"><span></span></span>`;
  const view = {
    el,
    name: el.querySelector(".file-name"),
    bytes: el.querySelector(".file-bytes"),
    pct: el.querySelector(".file-pct"),
    bar: el.querySelector(".file-bar > span"),
  };
  updateFileRow(view, file);
  return view;
}

function rebuildFileRows(view, transfer) {
  const files = transfer.files || [];
  const query = state.fileQuery.trim().toLowerCase();
  const expanded = expandedFiles.has(transfer.id);
  const limit = expanded ? FILE_ROWS_MAX : FILE_ROWS_INITIAL;
  const folder = folderRoot(transfer);
  const matches = [];
  files.forEach((file, index) => {
    if (!query || file.name.toLowerCase().includes(query)) matches.push(index);
  });

  view.files.replaceChildren();
  view.fileNodes = new Map();
  view.files.hidden = files.length === 0 || (files.length === 1 && !folder);
  if (view.files.hidden) {
    view.filesKey = filesKey(transfer);
    return;
  }

  if (!folder) {
    const ordered = sortFileIndices(files, matches);
    ordered.slice(0, limit).forEach((index) => {
      const row = createFileRow(index, files[index]);
      view.fileNodes.set(index, row);
      view.files.appendChild(row.el);
    });

    if (matches.length === 0) {
      const empty = document.createElement("li");
      empty.className = "file-more";
      empty.textContent = "No file matches.";
      view.files.appendChild(empty);
    } else if (ordered.length > limit) {
      view.files.appendChild(fileMore(transfer, ordered.length - limit, expanded));
    }
    view.filesKey = filesKey(transfer);
    return;
  }

  const open = openPhasesFor(transfer);
  const searching = query.length > 0;
  const byPhase = { active: [], queue: [], done: [], failed: [] };
  matches.forEach((index) => byPhase[filePhase(files[index])].push(index));

  FILE_PHASES.forEach((phase) => {
    const indices = sortFileIndices(files, byPhase[phase]);
    if (indices.length === 0) return;
    const isOpen = searching || open.has(phase);
    const head = document.createElement("li");
    head.className = `file-group ${phase}`;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "file-group-head";
    button.setAttribute("aria-expanded", String(isOpen));
    button.innerHTML = `<span class="chev">${isOpen ? "▾" : "▸"}</span><span>${FILE_PHASE_LABELS[phase]}</span><span class="count">${indices.length}</span>`;
    if (!searching) {
      button.addEventListener("click", () => {
        if (open.has(phase)) open.delete(phase);
        else open.add(phase);
        markTransferDirty(transfer.id);
      });
    }
    head.appendChild(button);
    view.files.appendChild(head);
    if (!isOpen) return;

    indices.slice(0, limit).forEach((index) => {
      const row = createFileRow(index, files[index]);
      row.el.classList.add("grouped");
      view.fileNodes.set(index, row);
      view.files.appendChild(row.el);
    });
    if (indices.length > limit) {
      view.files.appendChild(fileMore(transfer, indices.length - limit, expanded));
    }
  });

  if (matches.length === 0) {
    const empty = document.createElement("li");
    empty.className = "file-more";
    empty.textContent = "No file matches.";
    view.files.appendChild(empty);
  }
  view.filesKey = filesKey(transfer);
}

function fileMore(transfer, remaining, expanded) {
  const more = document.createElement("li");
  more.className = "file-more";
  if (!expanded) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "link-btn";
    button.textContent = `Show ${remaining} more`;
    button.addEventListener("click", () => {
      expandedFiles.add(transfer.id);
      markTransferDirty(transfer.id);
    });
    more.appendChild(button);
  } else {
    more.textContent = `+${remaining} more`;
  }
  return more;
}

function renderTransfers(force = false) {
  const list = $("transfer-list");
  const forceAll = force || allTransfersDirty;
  const dirty = new Set(dirtyTransfers);
  const dirtyFileIndices = new Map(dirtyFiles);
  allTransfersDirty = false;
  dirtyTransfers.clear();
  dirtyFiles.clear();

  const active = state.transfers.filter((t) => t.status === "active").length;
  $("transfer-count").textContent =
    state.transfers.length === 0
      ? ""
      : active > 0
        ? `${active} active`
        : `${state.transfers.length} recent`;

  if (state.transfers.length === 0) {
    list.innerHTML = `<li class="empty">Nothing yet.</li>`;
    transferRows.clear();
    return;
  }

  let cursor = list.firstElementChild;
  state.transfers.forEach((transfer) => {
    let view = transferRows.get(transfer.id);
    if (!view) {
      view = createTransferView(transfer);
      transferRows.set(transfer.id, view);
      updateTransferView(view, transfer);
      rebuildFileRows(view, transfer);
    } else {
      if (forceAll || dirty.has(transfer.id)) updateTransferView(view, transfer);
      if (view.filesKey !== filesKey(transfer)) {
        rebuildFileRows(view, transfer);
      } else {
        const indices = dirtyFileIndices.get(transfer.id);
        if (indices) {
          indices.forEach((index) => {
            const row = view.fileNodes.get(index);
            const file = transfer.files?.[index];
            if (row && file) updateFileRow(row, file);
          });
        }
      }
    }

    if (view.el === cursor) {
      cursor = cursor.nextElementSibling;
    } else {
      list.insertBefore(view.el, cursor);
    }
  });

  while (cursor) {
    const next = cursor.nextElementSibling;
    if (cursor.dataset.transfer) {
      transferRows.delete(cursor.dataset.transfer);
      fileGroupOpen.delete(cursor.dataset.transfer);
    }
    cursor.remove();
    cursor = next;
  }
}

async function cancelTransfer(id) {
  try {
    await api("POST", `/api/transfers/${encodeURIComponent(id)}/cancel`);
    toast("Transfer cancelled. Sending the same files again resumes it.");
  } catch (error) {
    toast(`Could not cancel: ${error}`);
  }
}

function renderApproval() {
  const modal = $("approval-modal");
  const approval = state.approvals[0];
  if (!approval) {
    modal.hidden = true;
    return;
  }
  $("approval-avatar").textContent = initial(approval.peer_name);
  $("approval-avatar").style.setProperty("--hue", hueFor(approval.peer_id));
  $("approval-dest").textContent = state.config.out_dir || "your download folder";
  $("approval-title").textContent = approval.peer_name;
  $("approval-sub").textContent = `id ${shortId(approval.peer_id)} · ${approval.files.length} files · ${fmtBytes(approval.total_bytes)}`;
  $("approval-files").innerHTML = approval.files
    .map(
      (file) =>
        `<li><span>${esc(file.name)}</span><span class="size">${fmtBytes(file.size)}</span></li>`,
    )
    .join("");
  modal.hidden = false;

  const respond = async (accept, remember) => {
    try {
      await api("POST", `/api/approvals/${encodeURIComponent(approval.id)}`, {
        accept,
        remember,
      });
      state.approvals = state.approvals.filter((a) => a.id !== approval.id);
      if (remember) refresh();
      renderApproval();
    } catch (error) {
      toast(`Could not answer: ${error}`);
      refresh();
    }
  };

  $("approval-decline").onclick = () => respond(false, false);
  $("approval-accept").onclick = () => respond(true, false);
  $("approval-always").onclick = () => respond(true, true);
}


async function sendToSelected() {
  const peer = peerByKey(state.selectedPeer);
  if (!peer) {
    toast("Select a device first.");
    return;
  }
  await sendFilesToPeer(peer);
}

async function sendFilesToPeer(peer) {
  try {
    const paths = await invoke("pick_files");
    if (!paths || paths.length === 0) return;
    await sendPaths(peer, paths);
  } catch (error) {
    toast(`Could not open the file picker: ${error}`);
  }
}

async function sendFoldersToPeer(peer) {
  try {
    const dirs = await invoke("pick_folders");
    if (!dirs || dirs.length === 0) return;
    await sendPaths(peer, dirs, `Sending ${dirs.length} folder(s) to ${peer.name}…`);
  } catch (error) {
    toast(`Could not open the folder picker: ${error}`);
  }
}

async function sendFolderToSelected() {
  const peer = peerByKey(state.selectedPeer);
  if (!peer) {
    toast("Select a device first.");
    return;
  }
  await sendFoldersToPeer(peer);
}

async function sendToTarget(to, name, paths, message) {
  try {
    await api("POST", "/api/send", {
      to,
      peer_name: name,
      paths,
      concurrency: 8,
    });
    toast(message || (name ? `Sending to ${name}…` : `Sending to ${to}…`));
  } catch (error) {
    if (state.manual.some((p) => p.addr === to)) {
      updateSavedRouteLiveState(to, "lost");
    }
    toast(`Send failed: ${error}`);
  }
}

async function sendPaths(peer, paths, message) {
  // Device id is unique and pins the certificate; a name can repeat.
  const target = peer.manual ? peer.addr : (peer.device_id || peer.addr);
  await sendToTarget(
    target,
    peer.name,
    paths,
    message ||
      (peer.manual
        ? `Sending to ${peer.name}…`
        : `Sending to ${peer.name} (${shortId(peer.device_id)})…`),
  );
}

async function forget(deviceId) {
  try {
    await api("DELETE", `/api/trust/${encodeURIComponent(deviceId.slice(0, 16))}`);
    state.trust = state.trust.filter((p) => p.device_id !== deviceId);
    renderAll();
    toast("Device forgotten");
  } catch (error) {
    toast(`Could not forget device: ${error}`);
  }
}

function showPane(name) {
  document
    .querySelectorAll("#settings-nav button")
    .forEach((b) => b.classList.toggle("active", b.dataset.pane === name));
  document
    .querySelectorAll(".settings-panes section")
    .forEach((s) => (s.hidden = s.dataset.pane !== name));
}

async function openSettings() {
  try {
    state.config = await api("GET", "/api/config");
  } catch (_) {
    /* Keep the last known config while the daemon is unreachable. */
  }
  const info = state.info || {};
  $("settings-name").value = info.name || "";
  $("settings-short-id").textContent = info.short_id || String(info.device_id || "").slice(0, 8) || "—";
  $("settings-theme").value = prefs.theme;
  $("settings-density").value = prefs.density;
  $("settings-motion").checked = prefs.motion;
  $("settings-open-after").checked = prefs.openAfter;
  $("settings-out").value = state.config.out_dir || "";
  $("settings-policy").value = state.config.policy || "ask";
  $("settings-concurrency").value = String(state.config.concurrency || 8);
  $("about-version").textContent = info.version || "—";
  $("about-platform").textContent = info.platform || "—";
  $("about-name").textContent = info.name || "—";
  $("about-short").textContent = info.short_id || "—";
  $("about-id").textContent = info.device_id || "—";
  $("about-config").textContent = info.config_dir || "—";
  $("about-quic").textContent = info.quic_port ? `udp/${info.quic_port}` : "—";
  $("about-disc").textContent = info.discovery_port ? `udp/${info.discovery_port}` : "—";
  renderLocalAddrs(info);
  $("settings-hint").textContent = "";
  showPane("general");
  $("settings-modal").hidden = false;
}

function isTailscale(ip) {
  const [a, b] = String(ip).split(".").map(Number);
  return a === 100 && b >= 64 && b <= 127;
}

function renderLocalAddrs(info) {
  const box = $("about-addrs");
  box.textContent = "";
  const port = info.quic_port;
  const addrs = info.local_addrs || [];
  if (!addrs.length) {
    box.textContent = "—";
    return;
  }
  addrs.forEach((ip) => {
    const target = port ? `${ip}:${port}` : ip;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "addr";
    button.title = `Copy ${target}`;
    const label = document.createElement("span");
    label.textContent = ip;
    button.append(label);
    if (isTailscale(ip)) {
      const tag = document.createElement("span");
      tag.className = "tag";
      tag.textContent = "tailscale";
      button.append(tag);
    }
    button.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(target);
        $("settings-hint").textContent = `Copied ${target}`;
      } catch (_) {
        $("settings-hint").textContent = "Could not copy.";
      }
    });
    box.append(button);
  });
}

async function saveSettings() {
  prefs.theme = $("settings-theme").value;
  prefs.density = $("settings-density").value;
  prefs.motion = $("settings-motion").checked;
  prefs.openAfter = $("settings-open-after").checked;
  savePrefs();

  const out_dir = $("settings-out").value.trim();
  const policy = $("settings-policy").value;
  const concurrency = Number($("settings-concurrency").value) || 8;
  const name = $("settings-name").value.trim();
  try {
    const patched = await api("PATCH", "/api/config", {
      out_dir: out_dir || undefined,
      policy,
      concurrency,
      name: name || undefined,
    });
    state.config = { ...state.config, out_dir, policy, concurrency };
    state.info = {
      ...state.info,
      name: patched.name || state.info.name,
      short_id: patched.short_id || state.info.short_id,
    };
    renderDevice();
    $("settings-modal").hidden = true;
    toast(patched.name ? `Saved as ${patched.name}` : "Settings saved");
    refresh();
  } catch (error) {
    $("settings-hint").textContent = String(error);
  }
}


$("btn-theme").addEventListener("click", toggleTheme);
$("settings-close").addEventListener("click", () => ($("settings-modal").hidden = true));
$("settings-nav").addEventListener("click", (event) => {
  const button = event.target.closest("[data-pane]");
  if (button) showPane(button.dataset.pane);
});
$("settings-clear").addEventListener("click", async () => {
  try {
    await api("POST", "/api/transfers/clear");
    state.transfers = [];
    speedSamples.clear();
    renderTransfers(true);
    $("settings-hint").textContent = "History cleared.";
  } catch (error) {
    $("settings-hint").textContent = String(error);
  }
});
$("settings-copy-short").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(state.info.short_id || "");
    $("settings-hint").textContent = "Id copied.";
  } catch (_) {
    $("settings-hint").textContent = "Could not copy.";
  }
});
$("about-copy-short").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(state.info.short_id || "");
    $("settings-hint").textContent = "Id copied.";
  } catch (_) {
    $("settings-hint").textContent = "Could not copy.";
  }
});
$("about-copy").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(state.info.device_id || "");
    $("settings-hint").textContent = "Device id copied.";
  } catch (_) {
    $("settings-hint").textContent = "Could not copy.";
  }
});
$("btn-send").addEventListener("click", sendToSelected);

const ADD_HINT =
  "A MagicDNS or DNS name works when discovery cannot cross the network. The port is ignored if the address already contains one.";

function setAddHint(message) {
  const hint = $("add-hint");
  hint.textContent = message || ADD_HINT;
  hint.dataset.error = message ? "1" : "";
}

function openAddDevice() {
  $("add-address").value = "";
  $("add-port").value = String(state.info.quic_port || 53320);
  $("add-label").value = "";
  setAddHint("");
  $("add-modal").hidden = false;
  $("add-address").focus();
}

function closeAddDevice() {
  $("add-modal").hidden = true;
}

function parsePort(value) {
  const text = String(value || "").trim();
  if (!text) return null;
  if (!/^\d+$/.test(text)) return 0;
  const port = Number(text);
  return port >= 1 && port <= 65535 ? port : 0;
}

$("btn-add-device").addEventListener("click", openAddDevice);
$("add-close").addEventListener("click", closeAddDevice);
$("add-cancel").addEventListener("click", closeAddDevice);
$("add-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const address = $("add-address").value.trim();
  if (!address) {
    setAddHint("Enter a hostname or IP address.");
    $("add-address").focus();
    return;
  }
  const port = parsePort($("add-port").value);
  if (port === 0) {
    setAddHint("Port must be between 1 and 65535.");
    $("add-port").focus();
    return;
  }
  const addr = addManualPeer(address, $("add-label").value, port);
  if (!addr) {
    setAddHint("That address is already in the list.");
    return;
  }
  closeAddDevice();
  state.selectedPeer = `saved:${addr}`;
  renderPeers();
  toast(`${addr} added. Choose files or a folder.`);
});

$("transfer-search").addEventListener("input", (event) => {
  state.fileQuery = event.target.value;
  invalidateTransfers();
});
$("btn-send-folder").addEventListener("click", sendFolderToSelected);
$("btn-settings").addEventListener("click", openSettings);
$("settings-cancel").addEventListener("click", () => ($("settings-modal").hidden = true));
$("settings-save").addEventListener("click", saveSettings);
$("settings-browse").addEventListener("click", async () => {
  try {
    const dir = await invoke("pick_folder");
    if (dir) $("settings-out").value = dir;
  } catch (error) {
    toast(`Could not open the folder picker: ${error}`);
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    $("settings-modal").hidden = true;
    $("add-modal").hidden = true;
  }
});


async function handleEvent(payload) {
  if (!payload || !payload.type) return;
  switch (payload.type) {
    case "hello":
      state.info = {
        ...state.info,
        name: payload.name,
        device_id: payload.device_id,
        short_id: String(payload.device_id || "").slice(0, 8),
      };
      mergePeers(payload.peers || []);
      state.approvals = payload.approvals || [];
      (payload.active || []).forEach(upsertTransfer);
      renderAll();
      break;
    case "peer":
      upsertPeer(payload.peer);
      schedulePeers();
      break;
    case "peer_lost":
      removeDiscoveredPeer(payload.device_id);
      break;
    case "session": {
      const before = state.transfers.find((t) => t.id === payload.transfer.id);
      upsertTransfer(payload.transfer);
      markTransferDirty(payload.transfer.id, true);
      const t = payload.transfer;
      if (t.direction === "out" && t.peer_id && state.manual.some((p) => p.addr === t.peer_id)) {
        if (t.status === "done") {
          updateSavedRouteLiveState(t.peer_id, "live");
        } else if (t.status === "failed") {
          updateSavedRouteLiveState(t.peer_id, "lost");
        }
      }
      if (prefs.openAfter && t.direction === "in" && t.status === "done" && before?.status !== "done") {
        invoke("open_path", { path: state.config.out_dir }).catch(() => {});
      }
      break;
    }
    case "file": {
      const transfer = state.transfers.find((t) => t.id === payload.id);
      if (transfer && transfer.files[payload.index]) {
        transfer.files[payload.index] = payload.file;
        markFileDirty(payload.id, payload.index);
      }
      break;
    }
    case "progress": {
      const transfer = state.transfers.find((t) => t.id === payload.id);
      if (transfer) {
        transfer.transferred = Math.max(transfer.transferred || 0, payload.transferred);
        if (payload.total > 0) transfer.total_bytes = payload.total;
        markTransferDirty(payload.id);
      }
      break;
    }
    case "approval":
      state.approvals.push(payload.request);
      renderApproval();
      break;
    case "approval_resolved":
      state.approvals = state.approvals.filter((a) => a.id !== payload.id);
      renderApproval();
      break;
    case "trust":
      state.trust = payload.peers || [];
      applyTrust();
      schedulePeers();
      scheduleTrust();
      break;
    case "config":
      state.config = {
        out_dir: payload.out_dir,
        policy: payload.policy,
        name: payload.name,
      };
      if (payload.name && payload.name !== state.info.name) {
        state.info = { ...state.info, name: payload.name };
        renderDevice();
      }
      break;
  }
}

async function setupDragDrop() {
  try {
    const webview = window.__TAURI__.webview.getCurrentWebview();
    await webview.onDragDropEvent((event) => {
      const payload = event.payload || {};
      const overlay = $("drop-overlay");
      if (payload.type === "enter" || payload.type === "over") {
        const peer = peerByKey(state.selectedPeer);
        $("drop-target").textContent = peer ? `to ${peer.name}` : "Select a device first";
        overlay.classList.add("show");
      } else if (payload.type === "leave") {
        overlay.classList.remove("show");
      } else if (payload.type === "drop") {
        overlay.classList.remove("show");
        const peer = peerByKey(state.selectedPeer);
        if (!peer) {
          toast("Select a device first, then drop files.");
          return;
        }
        if (payload.paths && payload.paths.length) sendPaths(peer, payload.paths);
      }
    });
  } catch (error) {
    console.warn("drag & drop unavailable", error);
  }
}

(async function boot() {
  loadPrefs();
  try {
    state.info = await invoke("daemon_info");
  } catch (error) {
    toast(`Daemon unavailable: ${error}`);
  }
  loadManualPeers();
  await listen("daemon-event", (event) => handleEvent(event.payload));
  // Progress and per-file events arrive in batches to keep one IPC message per tick.
  await listen("daemon-batch", (event) => {
    (event.payload || []).forEach((item) => handleEvent(item));
  });
  // The shell dropped events it could not keep up with; pull a whole snapshot.
  await listen("daemon-resync", () => refresh());
  await refresh();
  setupDragDrop();
  // Cheap safety net in case an event was missed while the window was hidden.
  setInterval(refresh, 15000);
})();
