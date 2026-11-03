const GIST = "https://gist.githubusercontent.com/loadlib/ab3496688642bc8f1c0a895cf23de184/raw";
const BROKERS = [
  "wss://broker.hivemq.com:8884/mqtt",
  "wss://broker.emqx.io:8084/mqtt",
];
const MAP_SIZE = 1024;
const INTERP_MS = 120;

const canvas = document.getElementById("map");
const ctx = canvas.getContext("2d");
const stateEl = document.getElementById("state");
const headEl = document.getElementById("head");
const mapNameEl = document.getElementById("map-name");
const playersEl = document.getElementById("players");
const clockEl = document.getElementById("clock");
const emptyEl = document.getElementById("empty");
const roomInput = document.getElementById("room");
const roomInput2 = document.getElementById("room2");

let maps = [];
let room = (new URLSearchParams(location.search).get("room") || localStorage.getItem("cs2-room") || "").toUpperCase();
let snapshot = null;
let snapshotTs = 0;
let lastSnapAt = 0;
let lastSnapEpoch = 0;
let liveAt = 0;
let mapDef = null;
let mapImage = null;
let mapLoading = false;
let clients = [];
let pollTimer = 0;
let tickTimer = 0;
let raf = 0;
let lastFrame = performance.now();

const players = new Map();
let self = null;
let bomb = null;

roomInput.value = room;

function setState(text, kind) {
  stateEl.textContent = text;
  stateEl.className = `state ${kind}`;
}

async function loadMaps() {
  try {
    const response = await fetch("data/maps.json", { cache: "no-store" });
    const data = await response.json();
    maps = data.maps || [];
  } catch (error) {
    console.warn("maps", error);
  }
}

function connect(code) {
  room = (code || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 6);
  if (room.length < 4) {
    setState("OFFLINE", "off");
    return;
  }

  roomInput.value = room;
  roomInput2.value = room;
  localStorage.setItem("cs2-room", room);
  const url = new URL(location.href);
  url.searchParams.set("room", room);
  history.replaceState(null, "", url);

  emptyEl.hidden = true;
  headEl.hidden = false;
  snapshot = null;
  snapshotTs = 0;
  lastSnapAt = 0;
  lastSnapEpoch = 0;
  liveAt = 0;
  players.clear();
  self = null;
  bomb = null;
  mapDef = null;
  mapImage = null;
  mapNameEl.textContent = "—";
  playersEl.textContent = "0";
  clockEl.textContent = "";
  setState("CONNECTING", "sync");

  stopLive();
  startLive();
  startPolling();
}

function startLive() {
  if (!window.mqtt) return;
  // Two independent public brokers: whichever delivers first wins. Public
  // brokers throttle long publish sessions, so the cheat also rotates sockets.
  for (const url of BROKERS) {
    const c = window.mqtt.connect(url, {
      clientId: `radar_${Math.random().toString(16).slice(2, 10)}`,
      clean: true,
      keepalive: 30,
      connectTimeout: 8000,
      reconnectPeriod: 1000,
      protocolVersion: 4,
    });
    clients.push(c);

    c.on("connect", () => {
      c.subscribe(`cs2radar/${room}`, { qos: 0 });
      if (!liveAt) setState("WAITING", "sync");
    });

    c.on("message", (topic, payload) => {
      if (topic !== `cs2radar/${room}`) return;
      try {
        accept(JSON.parse(payload.toString()), true);
      } catch (error) {
        console.warn("payload", error);
      }
    });

    c.on("close", () => {
      if (Date.now() - liveAt > 2500) {
        setState(snapshot ? "SYNC" : "WAITING", "sync");
      }
    });

    c.on("error", () => {});
  }
}

function stopLive() {
  for (const c of clients) {
    try { c.end(true); } catch (error) {}
  }
  clients = [];
}

function startPolling() {
  clearInterval(pollTimer);
  pollTimer = setInterval(fetchHistory, 5000);
  fetchHistory();
}

async function fetchHistory() {
  if (!room) return;
  try {
    const response = await fetch(`${GIST}/radar_${room}.json?ts=${Date.now()}`, { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    if (data.room && data.room.toUpperCase() !== room) return;
    if (Date.now() - liveAt < 2500) return;
    if (Number(data.ts || 0) > snapshotTs) accept(data, false);
  } catch (error) {
    if (!liveAt && !snapshot) setState("WAITING", "sync");
  }
}

function accept(data, live) {
  if (!data || (data.room && data.room.toUpperCase() !== room)) return;
  const ts = Number(data.ts || 0);
  if (snapshot && ts > 0 && ts <= snapshotTs) return;

  snapshot = data;
  snapshotTs = ts;
  lastSnapAt = performance.now();
  lastSnapEpoch = Date.now();
  if (live) {
    liveAt = Date.now();
    setState("LIVE", "live");
  } else {
    setState("SYNC", "sync");
  }

  ensureMap(data.map);
  updateEntities();
  updateHead();
}

function ensureMap(name) {
  if (!name || !maps.length || (mapDef && mapDef.name === name)) return;
  const next = maps.find((item) => item.name === name);
  if (!next) return;
  mapDef = next;
  mapImage = null;
  mapNameEl.textContent = next.display || name;
  loadMapImage(next.image, next.cdn_image);
}

function loadMapImage(local, cdn) {
  if (!local && !cdn) return;
  mapLoading = true;
  const image = new Image();
  image.onload = () => {
    mapImage = image;
    mapLoading = false;
  };
  image.onerror = () => {
    if (cdn && image.src !== cdn) {
      image.src = cdn;
      return;
    }
    mapLoading = false;
  };
  image.src = local || cdn;
}

function updateEntities() {
  if (!snapshot) return;
  const local = snapshot.local || {};
  const list = snapshot.players || [];
  const seen = new Set();
  const now = performance.now();

  for (let index = 0; index < list.length; index++) {
    const player = list[index];
    if (player.alive === false) continue;
    const id = player.i !== undefined ? `p${player.i}` : `x${index}`;
    seen.add(id);
    const x = Number(player.x);
    const y = Number(player.y);
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;

    const entry = players.get(id);
    if (!entry) {
      players.set(id, {
        x, y, yaw: Number(player.yaw) || 0,
        tx: x, ty: y, tyaw: Number(player.yaw) || 0,
        vx: 0, vy: 0, team: Number(player.team) || 0,
        visible: player.visible !== false, at: now,
      });
      continue;
    }

    const dt = Math.max(0.05, (now - entry.at) / 1000);
    entry.vx = (x - entry.tx) / dt;
    entry.vy = (y - entry.ty) / dt;
    entry.tx = x;
    entry.ty = y;
    entry.tyaw = Number(player.yaw) || 0;
    entry.team = Number(player.team) || 0;
    entry.visible = player.visible !== false;
    entry.at = now;
  }

  for (const id of players.keys()) {
    if (!seen.has(id)) players.delete(id);
  }

  const lx = Number(local.x);
  const ly = Number(local.y);
  if (Number.isFinite(lx) && Number.isFinite(ly)) {
    if (!self) {
      self = { x: lx, y: ly, yaw: Number(local.yaw) || 0 };
    } else {
      const dt = Math.max(0.05, (now - (self.at || now)) / 1000);
      self.vx = (lx - self.x) / dt;
      self.vy = (ly - self.y) / dt;
    }
    self.tx = lx;
    self.ty = ly;
    self.tyaw = Number(local.yaw) || 0;
    self.team = Number(local.team) || 0;
    self.at = now;
  }

  bomb = snapshot.bomb && snapshot.bomb.planted ? {
    x: Array.isArray(snapshot.bomb.pos) ? Number(snapshot.bomb.pos[0]) : NaN,
    y: Array.isArray(snapshot.bomb.pos) ? Number(snapshot.bomb.pos[1]) : NaN,
    timer: Number(snapshot.bomb.timer) || 0,
    at: Date.now(),
  } : null;
}

function updateHead() {
  if (!snapshot) return;
  playersEl.textContent = String(players.size);
  const round = snapshot.round || {};
  clockEl.textContent = clockText(round);
}

function clockText(round) {
  const remaining = Math.max(0, Number(round.time || 0) - (Date.now() - lastSnapEpoch) / 1000);
  const seconds = Math.floor(remaining);
  const clock = `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, "0")}`;
  return round.bomb ? `${clock} BOMB` : clock;
}

function resize() {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = Math.max(1, Math.floor(rect.width * dpr));
  canvas.height = Math.max(1, Math.floor(rect.height * dpr));
}

function frame(now) {
  raf = requestAnimationFrame(frame);
  const dt = Math.min(0.1, Math.max(0, (now - lastFrame) / 1000));
  lastFrame = now;

  const age = lastSnapAt ? Math.max(0, (now - lastSnapAt - INTERP_MS) / 1000) : 0;
  const extrapolate = Math.min(0.45, Math.max(0, age - 0.2));
  const blend = 1 - Math.exp(-dt / 0.05);
  const localTeam = self ? self.team : 0;

  for (const p of players.values()) {
    const tx = p.tx + (extrapolate ? p.vx * extrapolate : 0);
    const ty = p.ty + (extrapolate ? p.vy * extrapolate : 0);
    p.x += (tx - p.x) * blend;
    p.y += (ty - p.y) * blend;
    p.yaw += shortestAngle(p.tyaw - p.yaw) * blend;
    p.sameTeam = localTeam !== 0 && p.team === localTeam;
  }

  if (self) {
    self.x += (self.tx - self.x) * blend;
    self.y += (self.ty - self.y) * blend;
    self.yaw += shortestAngle(self.tyaw - self.yaw) * blend;
  }

  draw(now);
}

function shortestAngle(delta) {
  let d = delta % 360;
  if (d > 180) d -= 360;
  if (d < -180) d += 360;
  return d;
}

function draw(now) {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  const width = rect.width;
  const height = rect.height;
  if (!width || !height) return;

  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);

  const view = Math.min(width, height) * 0.94;
  const ox = (width - view) / 2;
  const oy = (height - view) / 2;

  ctx.fillStyle = "#14171e";
  ctx.fillRect(ox, oy, view, view);

  if (mapImage && mapImage.complete) ctx.drawImage(mapImage, ox, oy, view, view);

  if (!mapDef) {
    hint(width, height, mapLoading ? "loading map" : "");
    return;
  }

  const scale = mapDef.scale || 1;
  const point = (x, y) => [
    ox + ((x - mapDef.pos_x) / (MAP_SIZE * scale)) * view,
    oy + ((mapDef.pos_y - y) / (MAP_SIZE * scale)) * view,
  ];

  ctx.font = "600 11px Inter, sans-serif";
  ctx.fillStyle = "rgba(216, 184, 74, .7)";
  for (const [label, pair] of Object.entries(mapDef.sites || {})) {
    if (!Array.isArray(pair) || pair.length < 2) continue;
    ctx.fillText(label.toUpperCase(), ox + Number(pair[0]) * view + 3,
                 oy + Number(pair[1]) * view - 3);
  }

  if (!snapshot) {
    hint(width, height, "waiting for the session");
    return;
  }

  for (const p of players.values()) {
    const [x, y] = point(p.x, p.y);
    const color = p.sameTeam ? "#6fa8ff" : "#e06c75";
    ctx.globalAlpha = p.visible ? 1 : 0.5;
    cone(x, y, p.yaw, color, 14);
    ctx.fillStyle = color;
    dot(x, y, 4);
  }
  ctx.globalAlpha = 1;

  if (self) {
    const [x, y] = point(self.x, self.y);
    cone(x, y, self.yaw, "#f2f4ff", 16);
    ctx.fillStyle = "#f2f4ff";
    dot(x, y, 4.5);
  }

  if (bomb && Number.isFinite(bomb.x) && Number.isFinite(bomb.y)) {
    const [x, y] = point(bomb.x, bomb.y);
    const left = Math.max(0, bomb.timer - (Date.now() - bomb.at) / 1000);
    ctx.fillStyle = "#e0a35c";
    ctx.fillRect(x - 4, y - 4, 8, 8);
    ctx.font = "10px 'JetBrains Mono', Consolas, monospace";
    ctx.fillText(String(Math.ceil(left)), x + 8, y - 5);
  }
}

function cone(x, y, yaw, color, length) {
  const angle = (yaw * Math.PI) / 180;
  const dx = Math.cos(angle);
  const dy = -Math.sin(angle);
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.moveTo(x + dx * length, y + dy * length);
  ctx.lineTo(x - dy * 6 - dx * 2, y + dx * 6 - dy * 2);
  ctx.lineTo(x + dy * 6 - dx * 2, y - dx * 6 - dy * 2);
  ctx.closePath();
  ctx.fill();
}

function dot(x, y, radius) {
  ctx.beginPath();
  ctx.arc(x, y, radius, 0, Math.PI * 2);
  ctx.fill();
}

function hint(width, height, text) {
  if (!text) return;
  ctx.fillStyle = "rgba(215, 219, 229, .45)";
  ctx.font = "11px 'JetBrains Mono', Consolas, monospace";
  ctx.textAlign = "center";
  ctx.fillText(text, width / 2, height - 20);
  ctx.textAlign = "left";
}

function sanitize(input) {
  input.value = input.value.toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 6);
}

document.getElementById("connect").addEventListener("submit", (event) => {
  event.preventDefault();
  connect(roomInput.value);
});

document.getElementById("empty-form").addEventListener("submit", (event) => {
  event.preventDefault();
  connect(roomInput2.value);
});

roomInput.addEventListener("input", () => sanitize(roomInput));
roomInput2.addEventListener("input", () => sanitize(roomInput2));

new ResizeObserver(() => {
  resize();
}).observe(canvas);

tickTimer = setInterval(() => {
  if (!room) return;
  if (liveAt && Date.now() - liveAt > 2500) {
    setState(snapshot ? "SYNC" : "WAITING", "sync");
  }
  if (snapshot) clockEl.textContent = clockText(snapshot.round || {});
}, 1000);

loadMaps().then(() => {
  resize();
  raf = requestAnimationFrame(frame);
  if (room.length >= 4) connect(room);
});

// Debug hook used by the visual QA pass; no gameplay data beyond the snapshot.
window.__radar = () => ({
  players: [...players.values()],
  self,
  map: mapDef,
  snapshot,
});
