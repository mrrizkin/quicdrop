/* Device and route model for the desktop UI. */

/**
 * Stable unique key for a route row.
 * Discovered LAN routes are keyed by device_id (or addr).
 * Saved address routes are keyed by the concrete address.
 */
function routeKey(device, route) {
  if (route.origin === "discovered") {
    return `lan:${(device && device.device_id) || route.addr}`;
  }
  return `saved:${route.addr}`;
}

/**
 * Builds a device -> routes[] model.
 * Device key is device_id when known; saved addresses without a known identity
 * are their own entry keyed by address.
 */
function buildDevices(serverPeers = [], savedAddresses = [], savedRouteStates = {}) {
  const deviceMap = new Map();

  for (const peer of serverPeers) {
    if (!peer || !peer.addr) continue;
    const deviceId = peer.device_id || "";
    const key = deviceId || peer.addr;
    let device = deviceMap.get(key);
    if (!device) {
      device = {
        id: key,
        device_id: deviceId,
        name: peer.name || peer.addr,
        platform: peer.platform || "",
        label: "",
        trusted: Boolean(peer.trusted),
        routes: [],
      };
      deviceMap.set(key, device);
    } else {
      if (peer.name) device.name = peer.name;
      if (peer.platform) device.platform = peer.platform;
      if (peer.trusted) device.trusted = true;
    }

    device.routes.push({
      addr: peer.addr,
      origin: "discovered",
      state: "live",
      last_seen: peer.last_seen || 0,
      label: "",
    });
  }

  for (const saved of savedAddresses) {
    if (!saved || !saved.addr) continue;
    const deviceId = saved.device_id || "";
    const key = deviceId || `saved:${saved.addr}`;
    let device = deviceMap.get(key);
    const stateVal =
      typeof savedRouteStates.get === "function"
        ? savedRouteStates.get(saved.addr)
        : savedRouteStates[saved.addr];
    const routeState = stateVal || "unknown";
    const label = saved.label || "";
    const platform = saved.platform || "";
    const declineLink = Boolean(saved.decline_link);

    if (!device) {
      device = {
        id: deviceId || saved.addr,
        device_id: deviceId,
        name: label || saved.addr,
        platform,
        label,
        trusted: false,
        routes: [],
        decline_link: declineLink,
      };
      deviceMap.set(key, device);
    } else {
      if (platform && !device.platform) device.platform = platform;
      if (label && !device.label) device.label = label;
      if (declineLink) device.decline_link = true;
    }

    device.routes.push({
      addr: saved.addr,
      origin: "saved",
      state: routeState,
      last_seen: 0,
      label,
      device_id: deviceId,
      platform,
      decline_link: declineLink,
    });
  }

  return Array.from(deviceMap.values());
}

/**
 * Assigns routes to panel sections:
 * - LAN: live discovered routes
 * - Mesh: reachable over a saved address route (live state in session)
 * - Saved addresses: saved address routes with no live state (unknown or lost)
 */
function assignRouteSections(devices = []) {
  const sections = {
    lan: [],
    mesh: [],
    saved: [],
  };

  for (const device of devices) {
    for (const route of device.routes) {
      const isManual = route.origin === "saved";
      const key = routeKey(device, route);
      const item = {
        key,
        device_id: device.device_id || route.device_id || "",
        name: device.name,
        label: route.label || device.label || "",
        platform: device.platform || route.platform || "",
        trusted: Boolean(device.trusted),
        addr: route.addr,
        origin: route.origin,
        state: route.state,
        manual: isManual,
        decline_link: Boolean(route.decline_link || device.decline_link),
        device,
        route,
      };

      if (route.origin === "discovered" && route.state === "live") {
        sections.lan.push(item);
      } else if (route.origin === "saved" && route.state === "live") {
        sections.mesh.push(item);
      } else if (route.origin === "saved") {
        sections.saved.push(item);
      }
    }
  }

  const sortFn = (a, b) => a.name.localeCompare(b.name) || a.addr.localeCompare(b.addr);
  sections.lan.sort(sortFn);
  sections.mesh.sort(sortFn);
  sections.saved.sort(sortFn);

  return sections;
}

/**
 * Flattens all section items into a single array of route rows.
 */
function flattenSections(sections) {
  if (!sections) return [];
  return [...(sections.lan || []), ...(sections.mesh || []), ...(sections.saved || [])];
}

/**
 * Finds a route row by exact key, device_id, or addr.
 */
function findRouteRow(sections, key) {
  if (!key) return null;
  const rows = flattenSections(sections);
  return (
    rows.find((r) => r.key === key) ||
    rows.find((r) => r.device_id && r.device_id === key) ||
    rows.find((r) => r.addr === key) ||
    null
  );
}

if (typeof module !== "undefined" && module.exports) {
  module.exports = {
    routeKey,
    buildDevices,
    assignRouteSections,
    flattenSections,
    findRouteRow,
  };
}
