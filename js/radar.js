const GIST = "https://gist.githubusercontent.com/loadlib/ab3496688642bc8f1c0a895cf23de184/raw";
const BROKERS = [
  "wss://broker.hivemq.com:8884/mqtt",
  "wss://broker.emqx.io:8084/mqtt",
];
const PUSHER = { key: "1a7af9826748d4b5bce1", cluster: "eu" };
const MAP_SIZE = 1024;
const INTERP_MS = 120;
const TEAM_COLOR = "#73a6f2";
const ENEMY_COLOR = "#f24040";
const SELF_COLOR = "#ffffff";

const canvas = document.getElementById("map");
const ctx = canvas.getContext("2d");
const stateEl = document.getElementById("state");
const mapNameEl = document.getElementById("map-name");
const clockEl = document.getElementById("clock");
const phaseEl = document.getElementById("phase");
const aliveEl = document.getElementById("alive");
const bombEl = document.getElementById("bomb");
const rateEl = document.getElementById("rate");
const ctBody = document.getElementById("ct-body");
const tBody = document.getElementById("t-body");
const ctCount = document.getElementById("ct-count");
const tCount = document.getElementById("t-count");
const emptyEl = document.getElementById("empty");
const roomInput = document.getElementById("room");
const roomInput2 = document.getElementById("room2");
const fullscreenBtn = document.getElementById("fullscreen");

let maps = [];
let room = (new URLSearchParams(location.search).get("room") || "").toUpperCase();
const autoConnect = new URLSearchParams(location.search).get("go") === "1";
localStorage.removeItem("cs2-room");
let snapshot = null;
let snapshotTs = 0;
let lastSnapAt = 0;
let lastSnapEpoch = 0;
let liveAt = 0;
let mapDef = null;
let mapImage = null;
let mapLoading = false;
let clients = [];
let pusherSocket = null;
let pusherTimer = 0;
let pollTimer = 0;
let tickTimer = 0;
let raf = 0;
let lastFrame = performance.now();
const liveTimes = [];

const players = new Map();
let self = null;
let bomb = null;
let lastRosterKey = "";

roomInput.value = room;
roomInput2.value = room;

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
  const url = new URL(location.href);
  url.searchParams.set("room", room);
  history.replaceState(null, "", url);

  emptyEl.hidden = true;
  snapshot = null;
  snapshotTs = 0;
  lastSnapAt = 0;
  lastSnapEpoch = 0;
  liveAt = 0;
  liveTimes.length = 0;
  players.clear();
  self = null;
  bomb = null;
  mapDef = null;
  mapImage = null;
  mapNameEl.textContent = "—";
  clockEl.textContent = "—";
  phaseEl.textContent = "—";
  aliveEl.textContent = "—";
  bombEl.textContent = "—";
  rateEl.textContent = "—";
  ctBody.textContent = "";
  tBody.textContent = "";
  ctCount.textContent = "0";
  tCount.textContent = "0";
  lastRosterKey = "";
  setState("CONNECTING", "sync");

  stopLive();
  startLive();
  startPusher();
  startPolling();
}

function startLive() {
  if (!window.mqtt) return;
  for (const url of BROKERS) {
    const c = window.mqtt.connect(url, {
      clientId: `radar_${Math.random().toString(16).slice(2, 10)}`,
      clean: true,
      keepalive: 30,
      connectTimeout: 8000,
      reconnectPeriod: 2000,
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
  clearTimeout(pusherTimer);
  pusherTimer = 0;
  if (pusherSocket) {
    const socket = pusherSocket;
    pusherSocket = null;
    try { socket.close(); } catch (error) {}
  }
}

function startPusher() {
  if (!PUSHER.key || !window.WebSocket) return;
  const forRoom = room;
  const socket = new WebSocket(
    `wss://ws-${PUSHER.cluster}.pusher.com/app/${PUSHER.key}?protocol=7&client=cs2radar&version=1.0&flash=false`,
  );
  pusherSocket = socket;

  socket.onmessage = (event) => {
    if (room !== forRoom) return;
    let message = null;
    try { message = JSON.parse(event.data); } catch (error) { return; }
    if (!message || !message.event) return;

    if (message.event === "pusher:connection_established") {
      socket.send(JSON.stringify({
        event: "pusher:subscribe",
        data: { channel: `room-${room}` },
      }));
      return;
    }
    if (message.event === "pusher_internal:subscription_succeeded") {
      if (!liveAt) setState("WAITING", "sync");
      return;
    }
    if (message.event === "pusher:ping") {
      socket.send(JSON.stringify({ event: "pusher:pong", data: {} }));
      return;
    }
    if (message.event === "pusher:error") {
      console.warn("pusher", message.data);
      return;
    }
    if (message.event === "snapshot") {
      try {
        const data = typeof message.data === "string" ? JSON.parse(message.data) : message.data;
        accept(data, true);
      } catch (error) {
        console.warn("pusher payload", error);
      }
    }
  };

  socket.onclose = () => {
    if (pusherSocket !== socket) return;
    pusherSocket = null;
    clearTimeout(pusherTimer);
    pusherTimer = setTimeout(() => {
      if (room === forRoom) startPusher();
    }, 1500);
  };

  socket.onerror = () => {};
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
    liveTimes.push(lastSnapAt);
    while (liveTimes.length && lastSnapAt - liveTimes[0] > 2000) liveTimes.shift();
    setState("LIVE", "live");
  } else {
    setState("SYNC", "sync");
  }

  ensureMap(data.map);
  updateEntities();
  updateRoster();
  updateMatch();
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
    self.hp = Number(local.hp) || 0;
    self.at = now;
  }

  bomb = snapshot.bomb && snapshot.bomb.planted ? {
    x: Array.isArray(snapshot.bomb.pos) ? Number(snapshot.bomb.pos[0]) : NaN,
    y: Array.isArray(snapshot.bomb.pos) ? Number(snapshot.bomb.pos[1]) : NaN,
    timer: Number(snapshot.bomb.timer) || 0,
    site: snapshot.bomb.site || "",
    at: Date.now(),
  } : null;
}

function updateRoster() {
  if (!snapshot) return;
  const local = snapshot.local || {};
  const localTeam = Number(local.team) || 0;
  const localHp = Number(local.hp) || 0;
  const lx = Number(local.x);
  const ly = Number(local.y);
  const hasLocal = localHp > 0 && Number.isFinite(lx) && Number.isFinite(ly);
  const list = [];

  if (hasLocal) {
    list.push({
      id: "you", name: String(local.name || "you").trim() || "you",
      avatar: String(local.avatar || ""), team: localTeam,
      hp: localHp, armor: 0, weapon: "", distance: 0, isLocal: true,
    });
  }

  for (const player of snapshot.players || []) {
    if (player.alive === false) continue;
    const x = Number(player.x);
    const y = Number(player.y);
    let distance = 0;
    if (hasLocal && Number.isFinite(x) && Number.isFinite(y)) {
      distance = Math.hypot(x - lx, y - ly) * 0.0254;
    }
    list.push({
      id: player.i !== undefined ? `#${player.i}` : "?",
      name: String(player.name || "").trim() || "player",
      avatar: String(player.avatar || ""),
      team: Number(player.team) || 0,
      hp: Number(player.hp) || 0,
      armor: Number(player.armor) || 0,
      weapon: String(player.weapon || ""),
      distance,
      isLocal: false,
    });
  }

  const ct = list.filter((player) => player.team === 3);
  const t = list.filter((player) => player.team === 2);
  const other = list.filter((player) => player.team !== 2 && player.team !== 3);
  if (localTeam !== 2 && localTeam !== 3) {
    (localTeam === 3 ? ct : t).push(...other);
  }

  ctCount.textContent = String(ct.length);
  tCount.textContent = String(t.length);

  const key = JSON.stringify(list.map((player) => [
    player.id, player.team, player.hp, player.armor, player.weapon,
    player.name, player.avatar, Math.round(player.distance),
  ]));
  if (key === lastRosterKey) return;
  lastRosterKey = key;

  ctBody.innerHTML = ct.length ? ct.map(rowHtml).join("")
    : `<div class="empty-team">no players</div>`;
  tBody.innerHTML = t.length ? t.map(rowHtml).join("")
    : `<div class="empty-team">no players</div>`;
}

function rowHtml(player) {
  const hp = Math.max(0, Math.min(100, player.hp));
  const hpClass = hp <= 25 ? "low" : hp <= 50 ? "mid" : "";
  const dist = player.isLocal || !player.distance ? "" : `${Math.round(player.distance)} m`;
  const letter = (player.name[0] || "?").toUpperCase();
  const avatar = player.avatar
    ? `<img src="${escapeHtml(player.avatar)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()">`
    : "";
  return `<div class="row">`
    + `<div class="avatar"><span class="letter">${escapeHtml(letter)}</span>${avatar}</div>`
    + `<div class="info">`
    + `<div class="line1"><span class="nick" title="${escapeHtml(player.name)}">${escapeHtml(player.name)}</span>`
    + `<span class="dist">${dist}</span></div>`
    + `<div class="line2"><span class="weapon">${escapeHtml(player.weapon)}</span>`
    + `${player.armor ? `<span class="armor">A${player.armor}</span>` : ""}`
    + `<span class="hp">${Math.round(hp)}</span></div>`
    + `<div class="hp-bar"><i class="${hpClass}" style="width:${hp}%"></i></div>`
    + `</div></div>`;
}

function updateMatch() {
  if (!snapshot) return;
  const round = snapshot.round || {};
  clockEl.textContent = clockText(round);
  phaseEl.textContent = phaseLabel(round);

  let ct = 0;
  let t = 0;
  const local = snapshot.local || {};
  if ((Number(local.hp) || 0) > 0) {
    if (Number(local.team) === 3) ct++;
    else if (Number(local.team) === 2) t++;
  }
  for (const player of snapshot.players || []) {
    if (player.alive === false) continue;
    if (Number(player.team) === 3) ct++;
    else if (Number(player.team) === 2) t++;
  }
  aliveEl.textContent = `CT ${ct} · T ${t}`;

  if (bomb) {
    const left = Math.max(0, bomb.timer - (Date.now() - bomb.at) / 1000);
    const site = bomb.site ? ` ${bomb.site}` : "";
    bombEl.textContent = `${site} ${Math.ceil(left)}s`;
  } else {
    bombEl.textContent = "—";
  }

  if (liveTimes.length >= 2) {
    const now = performance.now();
    while (liveTimes.length && now - liveTimes[0] > 2000) liveTimes.shift();
    const span = (liveTimes[liveTimes.length - 1] - liveTimes[0]) / 1000;
    const rate = span > 0 ? (liveTimes.length - 1) / span : 0;
    rateEl.textContent = liveTimes.length >= 2 ? `${rate.toFixed(0)} Hz` : "—";
  } else {
    rateEl.textContent = "—";
  }
}

function phaseLabel(round) {
  if (round.freeze) return "FREEZE";
  if (Number(round.phase) === 2) return "LIVE";
  if (Number(round.phase) === 3) return "END";
  return "—";
}

function clockText(round) {
  const remaining = Math.max(0, Number(round.time || 0) - (Date.now() - lastSnapEpoch) / 1000);
  const seconds = Math.floor(remaining);
  return `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, "0")}`;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[ch]));
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

  const view = Math.min(width, height)
    * (document.body.classList.contains("radar-fullscreen") ? 0.99 : 0.94);
  const unit = Math.max(0.7, Math.min(3.2, view / 420));
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

  ctx.font = `600 ${Math.round(11 * unit)}px Inter, sans-serif`;
  ctx.fillStyle = "rgba(216, 184, 74, .75)";
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
    const color = p.sameTeam ? TEAM_COLOR : ENEMY_COLOR;
    cone(x, y, p.yaw, color, 9 * unit, 3.5 * unit, 2 * unit);
    dot(x, y, 3.2 * unit, color);
  }

  if (self) {
    const [x, y] = point(self.x, self.y);
    arrow(x, y, self.yaw, SELF_COLOR, 8 * unit, 4 * unit, 4.5 * unit);
  }

  if (bomb && Number.isFinite(bomb.x) && Number.isFinite(bomb.y)) {
    const [x, y] = point(bomb.x, bomb.y);
    const left = Math.max(0, bomb.timer - (Date.now() - bomb.at) / 1000);
    ctx.fillStyle = "#ff5950";
    ctx.fillRect(x - 4 * unit, y - 4 * unit, 8 * unit, 8 * unit);
    ctx.font = `${Math.round(10 * unit)}px 'JetBrains Mono', Consolas, monospace`;
    ctx.fillText(String(Math.ceil(left)), x + 8 * unit, y - 5 * unit);
  }
}

// Same shape as the cheat radar: tip forward, 3.5 base half-width, 2 back.
function cone(x, y, yaw, color, length, half, back) {
  const angle = (yaw * Math.PI) / 180;
  const dx = Math.cos(angle);
  const dy = -Math.sin(angle);
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.moveTo(x + dx * length, y + dy * length);
  ctx.lineTo(x - dy * half - dx * back, y + dx * half - dy * back);
  ctx.lineTo(x + dy * half - dx * back, y - dx * half - dy * back);
  ctx.closePath();
  ctx.fill();
}

function arrow(x, y, yaw, color, tip, back, half) {
  const angle = (yaw * Math.PI) / 180;
  const dx = Math.cos(angle);
  const dy = -Math.sin(angle);
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.moveTo(x + dx * tip, y + dy * tip);
  ctx.lineTo(x - dx * back + -dy * half, y - dy * back + dx * half);
  ctx.lineTo(x - dx * back - -dy * half, y - dy * back - dx * half);
  ctx.closePath();
  ctx.fill();
}

function dot(x, y, radius, color) {
  ctx.fillStyle = color;
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

function toggleFullscreen() {
  const active = document.body.classList.toggle("radar-fullscreen");
  fullscreenBtn.setAttribute("aria-pressed", active ? "true" : "false");
  resize();
}

document.addEventListener("keydown", (event) => {
  const tag = document.activeElement?.tagName;
  if (tag === "INPUT" || tag === "TEXTAREA") return;
  if (event.key === "Escape" && document.body.classList.contains("radar-fullscreen")) {
    document.body.classList.remove("radar-fullscreen");
    fullscreenBtn.setAttribute("aria-pressed", "false");
    resize();
  }
  if (event.key.toLowerCase() === "f") {
    event.preventDefault();
    toggleFullscreen();
  }
});

document.getElementById("connect").addEventListener("submit", (event) => {
  event.preventDefault();
  connect(roomInput.value);
});

document.getElementById("empty-form").addEventListener("submit", (event) => {
  event.preventDefault();
  connect(roomInput2.value);
});

fullscreenBtn.addEventListener("click", toggleFullscreen);

const shell = document.querySelector(".shell");
document.getElementById("toggle-ct").addEventListener("click", () => {
  shell.classList.toggle("show-ct");
  shell.classList.remove("show-t");
});
document.getElementById("toggle-t").addEventListener("click", () => {
  shell.classList.toggle("show-t");
  shell.classList.remove("show-ct");
});

roomInput.addEventListener("input", () => sanitize(roomInput));
roomInput2.addEventListener("input", () => sanitize(roomInput2));

new ResizeObserver(() => resize()).observe(canvas);

tickTimer = setInterval(() => {
  if (!room) return;
  if (liveAt && Date.now() - liveAt > 2500) {
    setState(snapshot ? "SYNC" : "WAITING", "sync");
  }
  if (snapshot) updateMatch();
}, 1000);

loadMaps().then(() => {
  resize();
  raf = requestAnimationFrame(frame);
  if (autoConnect && room.length >= 4) connect(room);
});

window.__radar = () => ({
  players: [...players.values()],
  self,
  map: mapDef,
  snapshot,
});

// Forces one frame for visual QA when rAF is paused (background tab).
window.__radarDraw = () => {
  lastFrame = performance.now();
  draw(lastFrame);
};
