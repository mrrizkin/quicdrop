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

/* Smoothed throughput per transfer, fed by the SSE progress events. */
const speedSamples = new Map();

function rateFor(transfer) {
  if (transfer.status !== "active") return 0;
  const now = Date.now();
  const prev = speedSamples.get(transfer.id);
  const bytes = transfer.transferred || 0;
  if (!prev) {
    speedSamples.set(transfer.id, { bytes, at: now, rate: 0 });
    return 0;
  }
  const elapsed = (now - prev.at) / 1000;
  if (elapsed < 0.3) return prev.rate || 0;
  const instant = Math.max(0, bytes - prev.bytes) / elapsed;
  const rate = prev.rate ? prev.rate * 0.65 + instant * 0.35 : instant;
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
    state.transfers = transfers;
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

function upsertTransfer(transfer) {
  const index = state.transfers.findIndex((t) => t.id === transfer.id);
  if (index >= 0) state.transfers[index] = transfer;
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
  renderTransfers();
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

function fileRows(files) {
  const query = state.fileQuery.trim().toLowerCase();
  const shown = files
    .map((file, index) => ({ file, index }))
    .filter(({ file }) => !query || file.name.toLowerCase().includes(query));
  if (files.length === 0) return "";
  const rows = shown
    .slice(0, 500)
    .map(({ file, index }) => {
      const pct = file.size > 0 ? Math.min(100, Math.round(((file.done || 0) / file.size) * 100)) : 0;
      const mark = file.ok === true ? "✓" : file.ok === false ? "✕" : "";
      return `<li class="file-row" title="${esc(file.name)}">
        <span class="file-name">${esc(file.name)}</span>
        <span class="file-bytes">${fmtBytes(file.done || 0)} / ${fmtBytes(file.size)}</span>
        <span class="file-pct">${mark || pct + "%"}</span>
        <span class="file-bar"><span style="width:${pct}%"></span></span>
      </li>`;
    })
    .join("");
  const more =
    shown.length > 500 ? `<li class="file-more">+${shown.length - 500} more</li>` : "";
  const empty = shown.length === 0 ? `<li class="file-more">No file matches.</li>` : "";
  return `<ul class="file-rows">${empty}${rows}${more}</ul>`;
}

function renderTransfers() {
  const list = $("transfer-list");
  const active = state.transfers.filter((t) => t.status === "active").length;
  $("transfer-count").textContent =
    state.transfers.length === 0
      ? ""
      : active > 0
        ? `${active} active`
        : `${state.transfers.length} recent`;

  if (state.transfers.length === 0) {
    list.innerHTML = `<li class="empty">Nothing yet.</li>`;
    return;
  }
  list.innerHTML = state.transfers
    .map((transfer) => {
      const total = transfer.total_bytes || 0;
      const done = transfer.transferred || 0;
      const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;
      const files = transfer.files || [];
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

      const cancel =
        transfer.status === "active"
          ? `<button class="icon-btn" data-cancel="${esc(transfer.id)}" title="Cancel">${icon("stop")}</button>`
          : "";
      return `
        <li class="transfer ${esc(transfer.status)}" data-transfer="${esc(transfer.id)}">
          <div class="dir ${esc(transfer.direction)}">${icon(transfer.direction === "in" ? "download" : "upload")}</div>
          <div class="t-main">
            <div class="transfer-top">
              <div class="transfer-title" title="${esc(summary)}">${esc(summary)}</div>
              ${cancel}
            </div>
            <div class="transfer-sub">${bits.join(" · ")}</div>
            <div class="bar"><span style="width:${pct}%"></span></div>
            ${fileRows(files)}
          </div>
          <div class="t-side">
            <span class="status ${esc(transfer.status)}">${esc(transfer.status)}</span>
            <span class="t-pct">${transfer.status === "active" ? pct + "%" : ""}</span>
          </div>
        </li>`;
    })
    .join("");
  list.querySelectorAll("[data-cancel]").forEach((button) => {
    button.addEventListener("click", () => cancelTransfer(button.dataset.cancel));
  });
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
    renderTransfers();
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
  renderTransfers();
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
      renderPeers();
      break;
    case "session": {
      const before = state.transfers.find((t) => t.id === payload.transfer.id);
      upsertTransfer(payload.transfer);
      renderTransfers();
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
        renderTransfers();
      }
      break;
    }
    case "progress": {
      const transfer = state.transfers.find((t) => t.id === payload.id);
      if (transfer) {
        transfer.transferred = payload.transferred;
        transfer.total_bytes = payload.total;
        renderTransfers();
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
      renderAll();
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
  await refresh();
  setupDragDrop();
  // Cheap safety net in case an event was missed while the window was hidden.
  setInterval(refresh, 15000);
})();
