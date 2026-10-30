/* QuicDrop desktop UI — talks to the daemon through the `api` Tauri command. */

const { invoke } = window.__TAURI__.core;
const { listen } = window.__TAURI__.event;

const state = {
  info: {},
  peers: [],
  transfers: [],
  approvals: [],
  trust: [],
  config: { out_dir: "", policy: "ask" },
  fileQuery: "",
  selectedPeer: null,
};

const $ = (id) => document.getElementById(id);

const icon = (name) => `<svg class="i"><use href="#i-${name}"/></svg>`;
const platformIcon = (platform) =>
  /android|ios/i.test(platform || "")
    ? "phone"
    : /windows|macos|linux/i.test(platform || "")
      ? "laptop"
      : "monitor";


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
};

function loadPrefs() {
  try {
    Object.assign(prefs, JSON.parse(localStorage.getItem(PREFS_KEY) || "{}"));
  } catch (_) {
    /* first run */
  }
  applyPrefs();
}

function savePrefs() {
  try {
    localStorage.setItem(PREFS_KEY, JSON.stringify(prefs));
  } catch (_) {
    /* ignore */
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
    state.peers = peers;
    state.transfers = mergeTransfers(state.transfers, transfers);
    state.trust = trust;
    state.config = config;
    state.approvals = approvals;
    renderAll();
  } catch (error) {
    toast(`Cannot reach daemon: ${error}`);
  }
}

function peerKey(peer) {
  return peer.device_id || peer.addr;
}

function upsertPeer(peer) {
  const index = state.peers.findIndex((p) => peerKey(p) === peerKey(peer));
  if (index >= 0) state.peers[index] = peer;
  else state.peers.push(peer);
  state.peers.sort((a, b) => a.name.localeCompare(b.name));
}

const TERMINAL_STATUS = new Set(["done", "failed", "cancelled", "declined"]);

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
  return {
    ...incoming,
    status: TERMINAL_STATUS.has(current.status) ? current.status : incoming.status,
    transferred: Math.max(current.transferred || 0, incoming.transferred || 0),
    total_bytes: total,
    files,
    error: incoming.error || current.error,
    finished_at: incoming.finished_at || current.finished_at,
  };
}

function mergeTransfers(current, incoming) {
  const local = new Map(current.map((transfer) => [transfer.id, transfer]));
  return incoming.map((transfer) => mergeTransferView(local.get(transfer.id), transfer));
}

function upsertTransfer(transfer) {
  const index = state.transfers.findIndex((t) => t.id === transfer.id);
  if (index >= 0) state.transfers[index] = mergeTransferView(state.transfers[index], transfer);
  else state.transfers.unshift(transfer);
  state.transfers.sort((a, b) => (b.started_at || 0) - (a.started_at || 0));
  if (state.transfers.length > 50) state.transfers.length = 50;
}

function applyTrust() {
  const trusted = new Set(state.trust.map((p) => p.device_id));
  state.peers.forEach((p) => (p.trusted = trusted.has(p.device_id)));
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
  $("device-id").textContent = shortIdValue ? `id ${shortIdValue}` : "";
  $("device-dot").classList.add("on");
  const avatar = $("self-avatar");
  if (avatar) {
    avatar.textContent = initial(name);
    avatar.style.setProperty("--hue", hueFor(device_id || shortIdValue));
  }
}

function renderPeers() {
  $("peer-count").textContent = state.peers.length;
  const list = $("peer-list");
  $("peers-hint").textContent =
    state.peers.length === 0
      ? "Looking for devices on your network…"
      : "Pick a device, then send files or drop them here.";

  const selected = Boolean(state.selectedPeer);
  $("btn-send").disabled = !selected;
  $("btn-send-folder").disabled = !selected;

  if (state.peers.length === 0) {
    list.innerHTML = `
      <li class="peer-empty">
        <div class="radar"></div>
        <div>No devices nearby.</div>
      </li>`;
    return;
  }

  list.innerHTML = state.peers
    .map((peer) => {
      const key = peerKey(peer);
      const isSelected = state.selectedPeer === key;
      return `
        <li class="peer ${isSelected ? "selected" : ""}" data-key="${esc(key)}"
            title="${esc(peer.name)}">
          <span class="peer-check">✓</span>
          <div class="peer-top">
            <div class="avatar" style="--hue:${hueFor(peer.device_id)}">${initial(peer.name)}</div>
            <div class="peer-main">
              <div class="peer-name">${esc(peer.name)}</div>
              <div class="peer-meta">
                <span>${shortId(peer.device_id)}</span>
                <span class="sep">·</span>
                <span>${esc(peer.source || "mdns")}</span>
              </div>
            </div>
          </div>
          <div class="peer-foot">
            <span class="chip" title="${esc(peer.platform || "unknown")}">${icon(platformIcon(peer.platform))}</span>
            ${peer.trusted ? `<span title="Trusted">${icon("shield")}</span>` : ""}
            <button class="icon-btn peer-send" data-send="${esc(key)}" title="Send">${icon("upload")}</button>
          </div>
        </li>`;
    })
    .join("");

  list.querySelectorAll(".peer").forEach((row) => {
    row.addEventListener("click", (event) => {
      if (event.target.closest("[data-send]")) return;
      selectPeer(row.dataset.key);
    });
  });
  list.querySelectorAll("[data-send]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      selectPeer(button.dataset.send);
      sendToSelected();
    });
  });
}

function selectPeer(key) {
  state.selectedPeer = state.selectedPeer === key ? null : key;
  renderPeers();
}

function peerByKey(key) {
  return state.peers.find((p) => peerKey(p) === key) || null;
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
  return `${(transfer.files || []).length}|${state.fileQuery}|${expandedFiles.has(transfer.id) ? "all" : "part"}`;
}

/* Peers and trust change on every beacon; rebuild their lists at most once a frame. */
let peersRenderQueued = false;
let trustRenderQueued = false;

function schedulePeers() {
  if (peersRenderQueued) return;
  peersRenderQueued = true;
  const run = () => {
    peersRenderQueued = false;
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
  const bits = [
    esc(transfer.peer_name),
    `${fmtBytes(done)} / ${fmtBytes(total)}`,
    rate > 0 ? fmtRate(rate) : "",
    eta,
    timeAgo(transfer.finished_at || transfer.started_at),
  ].filter(Boolean);
  if (transfer.error) bits.push(esc(transfer.error));

  view.el.className = `transfer ${transfer.status}`;
  view.dir.className = `dir ${transfer.direction}`;
  view.dir.innerHTML = icon(transfer.direction === "in" ? "download" : "upload");
  view.title.textContent = summary;
  view.title.title = summary;
  view.sub.innerHTML = bits.join(" · ");
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
  const matches = [];
  files.forEach((file, index) => {
    if (!query || file.name.toLowerCase().includes(query)) matches.push(index);
  });

  view.files.replaceChildren();
  view.fileNodes = new Map();
  view.files.hidden = files.length === 0;
  if (files.length === 0) {
    view.filesKey = filesKey(transfer);
    return;
  }

  matches.slice(0, limit).forEach((index) => {
    const row = createFileRow(index, files[index]);
    view.fileNodes.set(index, row);
    view.files.appendChild(row.el);
  });

  if (matches.length === 0) {
    const empty = document.createElement("li");
    empty.className = "file-more";
    empty.textContent = "No file matches.";
    view.files.appendChild(empty);
  } else if (matches.length > limit) {
    const more = document.createElement("li");
    more.className = "file-more";
    const remaining = matches.length - limit;
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
    view.files.appendChild(more);
  }
  view.filesKey = filesKey(transfer);
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
    if (cursor.dataset.transfer) transferRows.delete(cursor.dataset.transfer);
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
  $("approval-sub").textContent = `${approval.files.length} files · ${fmtBytes(approval.total_bytes)}`;
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
  try {
    const paths = await invoke("pick_files");
    if (!paths || paths.length === 0) return;
    await sendPaths(peer, paths);
  } catch (error) {
    toast(`Could not open the file picker: ${error}`);
  }
}

async function sendFolderToSelected() {
  const peer = peerByKey(state.selectedPeer);
  if (!peer) {
    toast("Select a device first.");
    return;
  }
  try {
    const dirs = await invoke("pick_folders");
    if (!dirs || dirs.length === 0) return;
    await sendPaths(peer, dirs, `Sending ${dirs.length} folder(s) to ${peer.name}…`);
  } catch (error) {
    toast(`Could not open the folder picker: ${error}`);
  }
}

async function sendPaths(peer, paths, message) {
  try {
    await api("POST", "/api/send", {
      to: peer.addr,
      paths,
      concurrency: 8,
    });
    toast(message || `Sending to ${peer.name}…`);
  } catch (error) {
    toast(`Send failed: ${error}`);
  }
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
    /* use cached */
  }
  const info = state.info || {};
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
  $("about-id").textContent = info.device_id || "—";
  $("about-config").textContent = info.config_dir || "—";
  $("about-quic").textContent = info.quic_port ? `udp/${info.quic_port}` : "—";
  $("about-disc").textContent = info.discovery_port ? `udp/${info.discovery_port}` : "—";
  $("about-addrs").textContent = (info.local_addrs || []).join(", ") || "—";
  $("settings-hint").textContent = "";
  showPane("general");
  $("settings-modal").hidden = false;
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
  try {
    await api("PATCH", "/api/config", { out_dir: out_dir || undefined, policy, concurrency });
    state.config = { ...state.config, out_dir, policy, concurrency };
    $("settings-modal").hidden = true;
    toast("Settings saved");
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
$("about-copy").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(state.info.device_id || "");
    $("settings-hint").textContent = "Device id copied.";
  } catch (_) {
    $("settings-hint").textContent = "Could not copy.";
  }
});
$("btn-send").addEventListener("click", sendToSelected);
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
      };
      state.peers = payload.peers || [];
      state.approvals = payload.approvals || [];
      (payload.active || []).forEach(upsertTransfer);
      renderAll();
      break;
    case "peer":
      upsertPeer(payload.peer);
      schedulePeers();
      break;
    case "session": {
      const before = state.transfers.find((t) => t.id === payload.transfer.id);
      upsertTransfer(payload.transfer);
      markTransferDirty(payload.transfer.id, true);
      const t = payload.transfer;
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
      state.config = { out_dir: payload.out_dir, policy: payload.policy };
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
