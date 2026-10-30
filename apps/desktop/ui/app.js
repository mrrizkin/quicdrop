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
  selectedPeer: null,
};

const $ = (id) => document.getElementById(id);

/* ---------- tiny helpers ---------- */

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

/* ---------- data ---------- */

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

/* ---------- rendering ---------- */

function renderAll() {
  renderDevice();
  applyTrust();
  renderPeers();
  renderTrust();
  renderTransfers();
  renderApproval();
}

function renderDevice() {
  const { name, short_id: shortIdValue } = state.info;
  $("device-name").textContent = name || "this device";
  $("device-id").textContent = shortIdValue ? `id ${shortIdValue}` : "";
  $("device-dot").classList.add("on");
}

function renderPeers() {
  $("peer-count").textContent = state.peers.length;
  const list = $("peer-list");
  $("peers-hint").textContent =
    state.peers.length === 0
      ? "Looking for devices on your network…"
      : "Pick a device, then send files or drop them here.";

  list.innerHTML = state.peers
    .map((peer) => {
      const selected = state.selectedPeer === peerKey(peer);
      return `
        <li class="peer ${selected ? "selected" : ""}" data-key="${esc(peerKey(peer))}">
          <div class="avatar ${peer.source === "beacon" ? "alt" : ""}">${initial(peer.name)}</div>
          <div class="peer-main">
            <div class="peer-name">${esc(peer.name)}</div>
            <div class="peer-meta">${esc(peer.platform)} · ${shortId(peer.device_id)}</div>
          </div>
          ${peer.trusted ? '<span class="trusted-tag">trusted</span>' : ""}
          <button class="secondary small" data-send="${esc(peerKey(peer))}">Send</button>
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
    list.innerHTML = `<li class="hint" style="padding:6px 2px">No devices trusted yet.</li>`;
    return;
  }
  list.innerHTML = state.trust
    .map(
      (peer) => `
      <li class="trust-item">
        <span class="name">${esc(peer.name)}</span>
        <span class="when">${timeAgo(peer.last_seen)}</span>
        <button class="icon-btn" data-forget="${esc(peer.device_id)}" title="Forget device">✕</button>
      </li>`,
    )
    .join("");
  list.querySelectorAll("[data-forget]").forEach((button) => {
    button.addEventListener("click", () => forget(button.dataset.forget));
  });
}

function renderTransfers() {
  const list = $("transfer-list");
  if (state.transfers.length === 0) {
    list.innerHTML = `<li class="empty">Nothing transferred yet.</li>`;
    return;
  }
  list.innerHTML = state.transfers
    .map((transfer) => {
      const total = transfer.total_bytes || 1;
      const pct = Math.min(100, Math.round(((transfer.transferred || 0) / total) * 100));
      const files = transfer.files || [];
      const summary =
        files.length === 0
          ? "no files"
          : files.length === 1
            ? files[0].name
            : `${files[0].name} +${files.length - 1} more`;
      return `
        <li class="transfer ${esc(transfer.status)}">
          <div class="transfer-top">
            <div class="dir ${transfer.direction}">${transfer.direction === "in" ? "↓" : "↑"}</div>
            <div class="transfer-title">${esc(summary)}</div>
            <span class="status ${esc(transfer.status)}">${esc(transfer.status)}</span>
          </div>
          <div class="transfer-sub">
            ${transfer.direction === "in" ? "from" : "to"} ${esc(transfer.peer_name)}
            · ${fmtBytes(transfer.transferred)} / ${fmtBytes(transfer.total_bytes)}
            · ${timeAgo(transfer.finished_at || transfer.started_at)}
            ${transfer.error ? ` · ${esc(transfer.error)}` : ""}
          </div>
          <div class="bar"><span style="width:${pct}%"></span></div>
        </li>`;
    })
    .join("");
}

function renderApproval() {
  const modal = $("approval-modal");
  const approval = state.approvals[0];
  if (!approval) {
    modal.hidden = true;
    return;
  }
  $("approval-avatar").textContent = initial(approval.peer_name);
  $("approval-title").textContent = `"${approval.peer_name}" wants to send`;
  $("approval-sub").textContent = `${approval.files.length} file(s) · ${fmtBytes(approval.total_bytes)} · ${approval.peer_platform || "unknown platform"}`;
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

/* ---------- actions ---------- */

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
    const dir = await invoke("pick_folder");
    if (!dir) return;
    await sendPaths(peer, [dir], `Sending folder to ${peer.name}…`);
  } catch (error) {
    toast(`Could not open the folder picker: ${error}`);
  }
}

async function sendPaths(peer, paths, message) {
  try {
    await api("POST", "/api/send", {
      to: peer.addr,
      paths,
      concurrency: 4,
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

async function openSettings() {
  try {
    state.config = await api("GET", "/api/config");
  } catch (_) {
    /* use cached */
  }
  $("settings-out").value = state.config.out_dir || "";
  $("settings-policy").value = state.config.policy || "ask";
  $("settings-modal").hidden = false;
}

async function saveSettings() {
  const out_dir = $("settings-out").value.trim();
  const policy = $("settings-policy").value;
  try {
    await api("PATCH", "/api/config", { out_dir: out_dir || undefined, policy });
    state.config = { out_dir, policy };
    $("settings-modal").hidden = true;
    toast("Settings saved");
  } catch (error) {
    toast(`Could not save settings: ${error}`);
  }
}

/* ---------- wiring ---------- */

$("btn-send").addEventListener("click", sendToSelected);
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

/* ---------- daemon events ---------- */

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
    case "session":
      upsertTransfer(payload.transfer);
      renderTransfers();
      break;
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
