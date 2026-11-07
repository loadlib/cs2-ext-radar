const GIST = "https://gist.githubusercontent.com/loadlib/ab3496688642bc8f1c0a895cf23de184/raw";
const PUSHER = { key: "8317659f6f67388add63", cluster: "eu" };
const RELAY = "wss://cs2-radar-relay.as2213004.workers.dev";
const MAP_SIZE = 1024;
const INTERP_MS = 120;
window.__radarBuild = "2026-11-07.6";

const css = getComputedStyle(document.documentElement);
const token = (name) => css.getPropertyValue(name).trim();
const COLOR = {
  team: token("--team"),
  enemy: token("--enemy"),
  self: token("--self"),
  bomb: token("--bomb"),
  mapVoid: token("--map-void"),
  mapSite: token("--map-site"),
  outline: token("--marker-outline"),
  text: token("--text"),
  muted: token("--text-muted"),
};
const FONT_UI = token("--font-ui");
const FONT_MONO = token("--font-mono");

const canvas = document.getElementById("map");
const ctx = canvas.getContext("2d");
const stage = document.getElementById("stage");
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
const followBtn = document.getElementById("follow");
const teamsBtn = document.getElementById("toggle-teams");
const exitBtn = document.getElementById("exit-fullscreen");
const tooltip = document.getElementById("tooltip");
const ttName = document.getElementById("tt-name");
const ttTeam = document.getElementById("tt-team");
const ttHp = document.getElementById("tt-hp");
const ttHpFill = document.getElementById("tt-hp-fill");
const ttArmor = document.getElementById("tt-armor");
const ttWeapon = document.getElementById("tt-weapon");
const ttDist = document.getElementById("tt-dist");

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
let pusherSocket = null;
let pusherTimer = 0;
let pusherDelay = 1500;
let relaySocket = null;
let relayTimer = 0;
let relayDelay = 1000;
let pollTimer = 0;
let pollFailures = 0;
let nextPollAt = 0;
let connectedAt = 0;
let idleAt = 0;
let tickTimer = 0;
let raf = 0;
let lastFrame = performance.now();
const liveTimes = [];

const players = new Map();
let self = null;
let bomb = null;
let lastRosterKey = "";
let followMode = false;
let selectedId = "self";
let hoveredKey = null;
let followLast = null;
let camYaw = 0;
let camInit = false;
const FOLLOW_ZOOM = 2.6;

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
  followMode = false;
  selectedId = "self";
  followLast = null;
  camInit = false;
  updateFollowButton();
  connectedAt = Date.now();
  pusherDelay = 1500;
  relayDelay = 1000;
  pollFailures = 0;
  nextPollAt = 0;
  idleAt = 0;
  setState("CONNECTING", "sync");

  stopLive();
  startPusher();
  startRelay();
  startPolling();
}

function stopLive() {
  clearTimeout(pusherTimer);
  pusherTimer = 0;
  if (pusherSocket) {
    const socket = pusherSocket;
    pusherSocket = null;
    try { socket.close(); } catch (error) {}
  }
  clearTimeout(relayTimer);
  relayTimer = 0;
  if (relaySocket) {
    const socket = relaySocket;
    relaySocket = null;
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
      pusherDelay = 1500;
      socket.send(JSON.stringify({
        event: "pusher:subscribe",
        data: { channel: `room-${room}` },
      }));
      return;
    }
    if (message.event === "pusher_internal:subscription_succeeded") {
      pusherDelay = 1500;
      if (!snapshot && !liveAt) setState("WAITING", "sync");
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
    const delay = pusherDelay;
    pusherDelay = Math.min(pusherDelay * 2, 30000);
    pusherTimer = setTimeout(() => {
      if (room === forRoom) startPusher();
    }, delay);
  };

  socket.onerror = () => {};
}

function startRelay() {
  if (!RELAY || !window.WebSocket) return;
  const forRoom = room;
  const socket = new WebSocket(`${RELAY}/ws?room=${encodeURIComponent(room)}`);
  relaySocket = socket;

  socket.onopen = () => {
    relayDelay = 1000;
  };

  socket.onmessage = (event) => {
    if (room !== forRoom) return;
    try {
      const data = JSON.parse(event.data);
      accept(data, true);
    } catch (error) {
      console.warn("relay payload", error);
    }
  };

  socket.onclose = () => {
    if (relaySocket !== socket) return;
    relaySocket = null;
    clearTimeout(relayTimer);
    const delay = relayDelay;
    relayDelay = Math.min(relayDelay * 2, 30000);
    relayTimer = setTimeout(() => {
      if (room === forRoom) startRelay();
    }, delay);
  };

  socket.onerror = () => {};
}

function startPolling() {
  clearInterval(pollTimer);
  pollFailures = 0;
  nextPollAt = 0;
  pollTimer = setInterval(fetchHistory, 5000);
  fetchHistory();
}

async function fetchHistory() {
  if (!room) return;
  if (Date.now() < nextPollAt) return;
  if (liveAt && Date.now() - liveAt < 2500) return;
  try {
    const response = await fetch(`${GIST}/radar_${room}.json?ts=${Date.now()}`, { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    if (data.room && data.room.toUpperCase() !== room) return;
    if (Number(data.ts || 0) > snapshotTs) accept(data, false);
    pollFailures = 0;
    nextPollAt = 0;
  } catch (error) {
    pollFailures += 1;
    // Back off on 403/404/offline instead of hammering the gist every 5 s.
    nextPollAt = Date.now() + Math.min(5000 * 2 ** Math.min(pollFailures, 3), 15000);
    if (!snapshot) setState("WAITING", "sync");
  }
}

function isIdle(data) {
  const roster = Array.isArray(data.players) ? data.players : [];
  const hp = Number(data.local?.hp ?? 0);
  return roster.length === 0 && hp <= 0;
}

function accept(data, live) {
  if (!data || (data.room && data.room.toUpperCase() !== room)) return;
  if (isIdle(data)) {
    idleAt = Date.now();
    if (!snapshot) setState("NO SESSION", "idle");
    return;
  }
  idleAt = 0;
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
  updateCanvasLabel();
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

function playerKey(player, index) {
  const steam = String(player.steam || "");
  if (steam && steam !== "0") return `s${steam}`;
  const name = String(player.name || "").trim();
  if (name && name !== "player") return `n${name}_${Number(player.team) || 0}`;
  return player.i !== undefined ? `p${player.i}` : `x${index}`;
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
    const id = playerKey(player, index);
    seen.add(id);
    const x = Number(player.x);
    const y = Number(player.y);
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;

    const entry = players.get(id);
    if (!entry) {
      players.set(id, {
        id, x, y, yaw: Number(player.yaw) || 0,
        tx: x, ty: y, tyaw: Number(player.yaw) || 0,
        vx: 0, vy: 0, team: Number(player.team) || 0,
        visible: player.visible !== false, at: now,
        name: String(player.name || "player"), hp: Number(player.hp) || 0,
        armor: Number(player.armor) || 0, weapon: String(player.weapon || ""),
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
    entry.name = String(player.name || "player");
    entry.hp = Number(player.hp) || 0;
    entry.armor = Number(player.armor) || 0;
    entry.weapon = String(player.weapon || "");
    entry.at = now;
  }

  for (const id of players.keys()) {
    if (!seen.has(id)) players.delete(id);
  }
  if (hoveredKey && hoveredKey !== "self" && !players.has(hoveredKey)) {
    hoveredKey = null;
    hideTooltip();
  }

  const lx = Number(local.x);
  const ly = Number(local.y);
  if (Number.isFinite(lx) && Number.isFinite(ly)) {
    if (!self) {
      self = { id: "self", x: lx, y: ly, yaw: Number(local.yaw) || 0 };
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
    self.name = String(local.name || "you");
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
      id: "you", key: "self", name: String(local.name || "you").trim() || "you",
      avatar: String(local.avatar || ""), team: localTeam,
      hp: localHp, armor: 0, weapon: "", distance: 0, isLocal: true,
    });
  }

  const roster = snapshot.players || [];
  for (let index = 0; index < roster.length; index++) {
    const player = roster[index];
    if (player.alive === false) continue;
    const x = Number(player.x);
    const y = Number(player.y);
    let distance = 0;
    if (hasLocal && Number.isFinite(x) && Number.isFinite(y)) {
      distance = Math.hypot(x - lx, y - ly) * 0.0254;
    }
    list.push({
      id: player.i !== undefined ? `#${player.i}` : "?",
      key: playerKey(player, index),
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

  const key = selectedId + "|" + JSON.stringify(list.map((player) => [
    player.id, player.team, player.hp, player.armor, player.weapon,
    player.name, player.avatar, Math.round(player.distance),
  ]));
  if (key === lastRosterKey) return;
  lastRosterKey = key;

  ctBody.innerHTML = ct.length ? ct.map(rowHtml).join("")
    : `<div class="empty-team">no players</div>`;
  tBody.innerHTML = t.length ? t.map(rowHtml).join("")
    : `<div class="empty-team">no players</div>`;
  updateCanvasLabel();
}

function rowHtml(player) {
  const hp = Math.max(0, Math.min(100, player.hp));
  const hpClass = hp <= 25 ? "low" : hp <= 50 ? "mid" : "";
  const dist = player.isLocal || !player.distance ? "" : `${Math.round(player.distance)} m`;
  const letter = (player.name[0] || "?").toUpperCase();
  const selected = player.key === selectedId;
  const avatar = player.avatar
    ? `<img src="${escapeHtml(player.avatar)}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer" onerror="this.remove()">`
    : "";
  return `<div class="row${selected ? " selected" : ""}" data-key="${escapeHtml(player.key)}" role="button" tabindex="0" aria-pressed="${selected}" aria-label="Follow ${escapeHtml(player.name)}">`
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

function updateCanvasLabel() {
  const map = mapDef && mapDef.display ? mapDef.display : "unknown";
  const tracked = players.size + (self ? 1 : 0);
  canvas.setAttribute("aria-label",
    `Live radar, ${map} map, ${tracked} players tracked${self ? ", local player visible" : ""}`);
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

  if (followMode) {
    const target = followTarget();
    if (target) {
      const yaw = Number(target.yaw) || 0;
      if (!camInit) {
        camYaw = yaw;
        camInit = true;
      }
      camYaw += shortestAngle(yaw - camYaw) * (1 - Math.exp(-dt / 0.07));
    }
  }

  draw(now);
}

function shortestAngle(delta) {
  let d = delta % 360;
  if (d > 180) d -= 360;
  if (d < -180) d += 360;
  return d;
}

function followTarget() {
  if (!followMode) return null;
  const target = selectedId === "self" ? self : players.get(selectedId);
  if (target) {
    followLast = { x: target.x, y: target.y, yaw: Number(target.yaw) || 0 };
    return target;
  }
  return followLast || self || null;
}

function updateFollowButton() {
  followBtn.classList.toggle("active", followMode);
  followBtn.setAttribute("aria-pressed", followMode ? "true" : "false");
}

function selectPlayer(key) {
  if (!key) return;
  clearHover();
  selectedId = key;
  followMode = true;
  followLast = null;
  camInit = false;
  updateFollowButton();
  lastRosterKey = "";
  updateRoster();
}

function hideTooltip() {
  tooltip.hidden = true;
}

function clearHover() {
  hoveredKey = null;
  hideTooltip();
}

function showTooltip(player, x, y) {
  const isSelf = player.id === "self";
  const hp = Math.max(0, Math.min(100, Number(player.hp) || 0));
  const team = Number(player.team) || 0;
  if (team === 3) ttTeam.dataset.team = "ct";
  else if (team === 2) ttTeam.dataset.team = "t";
  else ttTeam.removeAttribute("data-team");
  ttName.textContent = isSelf ? `${player.name || "you"} (you)` : (player.name || "player");
  ttHp.textContent = `${Math.round(hp)} HP`;
  ttHpFill.className = hp <= 25 ? "low" : hp <= 50 ? "mid" : "";
  ttHpFill.style.width = `${hp}%`;
  ttArmor.textContent = player.armor ? `${player.armor} ARM` : "";
  ttWeapon.textContent = player.weapon || "";
  const distance = (!isSelf && self)
    ? Math.round(Math.hypot(player.x - self.x, player.y - self.y) * 0.0254)
    : 0;
  ttDist.textContent = distance ? `${distance} m` : "";

  tooltip.hidden = false;
  const width = tooltip.offsetWidth;
  const height = tooltip.offsetHeight;
  const maxX = Math.max(8, stage.clientWidth - width - 8);
  const maxY = Math.max(8, stage.clientHeight - height - 8);
  let left = x + 14;
  let top = y + 14;
  if (left > maxX) left = x - width - 14;
  if (top > maxY) top = y - height - 14;
  tooltip.style.left = `${Math.max(8, Math.min(left, maxX))}px`;
  tooltip.style.top = `${Math.max(8, Math.min(top, maxY))}px`;
}

function hitTest(x, y) {
  let best = null;
  let bestDistance = 14;
  for (const player of players.values()) {
    if (player.sx === undefined) continue;
    const distance = Math.hypot(player.sx - x, player.sy - y);
    if (distance < bestDistance) {
      bestDistance = distance;
      best = player;
    }
  }
  if (self && self.sx !== undefined) {
    const distance = Math.hypot(self.sx - x, self.sy - y);
    if (distance < bestDistance) best = self;
  }
  return best;
}

function toggleFollow() {
  clearHover();
  followMode = !followMode;
  if (followMode) {
    followLast = null;
    camInit = false;
    if (!selectedId) selectedId = "self";
  }
  updateFollowButton();
  lastRosterKey = "";
  updateRoster();
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
  const cx = width / 2;
  const cy = height / 2;

  if (!mapDef) {
    ctx.fillStyle = COLOR.mapVoid;
    ctx.fillRect(ox, oy, view, view);
    hint(width, height, mapLoading ? "loading map" : "");
    return;
  }

  const scale = mapDef.scale || 1;
  const base = (x, y) => [
    ox + ((x - mapDef.pos_x) / (MAP_SIZE * scale)) * view,
    oy + ((mapDef.pos_y - y) / (MAP_SIZE * scale)) * view,
  ];

  const follow = followMode ? followTarget() : null;
  const following = Boolean(follow);
  let followX = 0;
  let followY = 0;
  let phi = 0;
  if (following) {
    [followX, followY] = base(follow.x, follow.y);
    phi = ((camYaw - 90) * Math.PI) / 180;
  }

  const fromBase = (bx, by) => {
    if (!following) return [bx, by];
    const dx = bx - followX;
    const dy = by - followY;
    const cos = Math.cos(phi);
    const sin = Math.sin(phi);
    return [
      cx + (dx * cos - dy * sin) * FOLLOW_ZOOM,
      cy + (dx * sin + dy * cos) * FOLLOW_ZOOM,
    ];
  };
  const point = (x, y) => fromBase(...base(x, y));

  ctx.fillStyle = COLOR.mapVoid;
  ctx.fillRect(ox, oy, view, view);

  if (mapImage && mapImage.complete) {
    if (following) {
      ctx.save();
      ctx.translate(cx, cy);
      ctx.rotate(phi);
      ctx.scale(FOLLOW_ZOOM, FOLLOW_ZOOM);
      ctx.translate(-followX, -followY);
      ctx.drawImage(mapImage, ox, oy, view, view);
      ctx.restore();
    } else {
      ctx.drawImage(mapImage, ox, oy, view, view);
    }
  }

  ctx.font = `600 ${Math.round(11 * unit)}px ${FONT_UI}`;
  ctx.textAlign = "left";
  ctx.textBaseline = "alphabetic";
  ctx.lineWidth = 2;
  ctx.strokeStyle = COLOR.outline;
  ctx.fillStyle = COLOR.mapSite;
  ctx.globalAlpha = 0.85;
  for (const [label, pair] of Object.entries(mapDef.sites || {})) {
    if (!Array.isArray(pair) || pair.length < 2) continue;
    const [x, y] = fromBase(ox + Number(pair[0]) * view, oy + Number(pair[1]) * view);
    ctx.strokeText(label.toUpperCase(), x + 3, y - 3);
    ctx.fillText(label.toUpperCase(), x + 3, y - 3);
  }
  ctx.globalAlpha = 1;

  if (!snapshot) {
    hint(width, height, "waiting for the session");
    return;
  }

  for (const p of players.values()) {
    const [x, y] = point(p.x, p.y);
    p.sx = x;
    p.sy = y;
    const color = p.sameTeam ? COLOR.team : COLOR.enemy;
    const markerYaw = following ? p.yaw - camYaw + 90 : p.yaw;
    if (p === follow) glow(x, y, 8 * unit, color);
    ctx.save();
    ctx.globalAlpha = 0.28;
    cone(x, y, markerYaw, color, 9 * unit, 3.5 * unit, 2 * unit);
    ctx.restore();
    if (p.sameTeam) {
      dot(x, y, 3.4 * unit, color);
    } else {
      diamond(x, y, 3.8 * unit, color);
    }
  }

  if (self) {
    const [x, y] = point(self.x, self.y);
    self.sx = x;
    self.sy = y;
    const markerYaw = following ? self.yaw - camYaw + 90 : self.yaw;
    if (self === follow) glow(x, y, 9 * unit, COLOR.text);
    arrow(x, y, markerYaw, COLOR.self, 8 * unit, 4 * unit, 4.5 * unit);
  }

  if (bomb && Number.isFinite(bomb.x) && Number.isFinite(bomb.y)) {
    const [x, y] = point(bomb.x, bomb.y);
    const left = Math.max(0, bomb.timer - (Date.now() - bomb.at) / 1000);
    ctx.fillStyle = COLOR.outline;
    ctx.fillRect(x - 4.5 * unit, y - 4.5 * unit, 9 * unit, 9 * unit);
    ctx.fillStyle = COLOR.bomb;
    ctx.fillRect(x - 3.5 * unit, y - 3.5 * unit, 7 * unit, 7 * unit);
    ctx.font = `${Math.round(10 * unit)}px ${FONT_MONO}`;
    ctx.lineWidth = 2;
    ctx.strokeStyle = COLOR.outline;
    ctx.strokeText(String(Math.ceil(left)), x + 8 * unit, y - 5 * unit);
    ctx.fillStyle = COLOR.bomb;
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
  ctx.strokeStyle = COLOR.outline;
  ctx.lineWidth = 1;
  ctx.lineJoin = "round";
  ctx.stroke();
}

function diamond(x, y, radius, color) {
  const path = (r) => {
    ctx.beginPath();
    ctx.moveTo(x, y - r);
    ctx.lineTo(x + r, y);
    ctx.lineTo(x, y + r);
    ctx.lineTo(x - r, y);
    ctx.closePath();
  };
  ctx.fillStyle = COLOR.outline;
  path(radius + 1);
  ctx.fill();
  ctx.fillStyle = color;
  path(radius);
  ctx.fill();
}

function dot(x, y, radius, color) {
  ctx.fillStyle = COLOR.outline;
  ctx.beginPath();
  ctx.arc(x, y, radius + 1, 0, Math.PI * 2);
  ctx.fill();
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.arc(x, y, radius, 0, Math.PI * 2);
  ctx.fill();
}

function glow(x, y, radius, color) {
  ctx.save();
  ctx.fillStyle = color;
  const layers = [
    [radius + 6, 0.035],
    [radius + 3, 0.06],
    [radius + 1, 0.09],
  ];
  for (const [r, alpha] of layers) {
    ctx.globalAlpha = alpha;
    ctx.beginPath();
    ctx.arc(x, y, r, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.restore();
}

function hint(width, height, text) {
  if (!text) return;
  ctx.globalAlpha = 0.6;
  ctx.fillStyle = COLOR.muted;
  ctx.font = `11px ${FONT_MONO}`;
  ctx.textAlign = "center";
  ctx.textBaseline = "alphabetic";
  ctx.fillText(text, width / 2, height - 20);
  ctx.textAlign = "left";
  ctx.globalAlpha = 1;
}

function sanitize(input) {
  input.value = input.value.toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 6);
}

function setMapOnly(active) {
  document.body.classList.toggle("radar-fullscreen", active);
  fullscreenBtn.setAttribute("aria-pressed", active ? "true" : "false");
  exitBtn.hidden = !active;
  if (active) clearHover();
  resize();
}

function toggleFullscreen() {
  setMapOnly(!document.body.classList.contains("radar-fullscreen"));
}

canvas.addEventListener("click", () => {
  if (document.body.classList.contains("radar-fullscreen")) {
    setMapOnly(false);
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && document.body.classList.contains("radar-fullscreen")) {
    setMapOnly(false);
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
exitBtn.addEventListener("click", () => setMapOnly(false));

const shell = document.querySelector(".shell");
teamsBtn.addEventListener("click", () => {
  const open = shell.classList.toggle("show-teams");
  teamsBtn.setAttribute("aria-pressed", open ? "true" : "false");
});
followBtn.addEventListener("click", toggleFollow);

canvas.addEventListener("pointermove", (event) => {
  if (event.pointerType === "touch") return;
  if (document.body.classList.contains("radar-fullscreen")) return;
  const rect = canvas.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const player = hitTest(x, y);
  if (!player) {
    clearHover();
    return;
  }
  hoveredKey = player.id || "self";
  showTooltip(player, x, y);
});

canvas.addEventListener("pointerleave", clearHover);

canvas.addEventListener("pointerdown", (event) => {
  if (document.body.classList.contains("radar-fullscreen")) return;
  const rect = canvas.getBoundingClientRect();
  const player = hitTest(event.clientX - rect.left, event.clientY - rect.top);
  if (!player) return;
  event.preventDefault();
  clearHover();
  selectPlayer(player.id || "self");
});

for (const body of [ctBody, tBody]) {
  body.addEventListener("pointerdown", (event) => {
    const row = event.target.closest(".row");
    if (!row) return;
    event.preventDefault();
    selectPlayer(row.dataset.key);
  });
  body.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const row = event.target.closest(".row");
    if (!row) return;
    event.preventDefault();
    selectPlayer(row.dataset.key);
  });
}

roomInput.addEventListener("input", () => sanitize(roomInput));
roomInput2.addEventListener("input", () => sanitize(roomInput2));

new ResizeObserver(() => resize()).observe(canvas);

// A background tab or a dropped connection must not stay stale: retry as soon
// as the page is visible or the network comes back.
window.addEventListener("online", () => {
  nextPollAt = 0;
  fetchHistory();
});
document.addEventListener("visibilitychange", () => {
  if (document.hidden) return;
  nextPollAt = 0;
  fetchHistory();
});

tickTimer = setInterval(() => {
  if (!room) return;
  const now = Date.now();
  if (snapshot) {
    if (liveAt && now - liveAt <= 2500) {
      setState("LIVE", "live");
    } else if (lastSnapEpoch && now - lastSnapEpoch > 30000) {
      setState("STALE", "warn");
    } else {
      setState("SYNC", "sync");
    }
    updateMatch();
  } else if (idleAt && now - idleAt < 20000) {
    setState("NO SESSION", "idle");
  } else if (now - connectedAt > 4000) {
    setState(pollFailures >= 3 ? "NO DATA" : "WAITING",
             pollFailures >= 3 ? "off" : "sync");
  }
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
  follow: followMode,
  selected: selectedId,
  target: followMode ? followTarget() : null,
  camYaw,
});

// Forces one frame for visual QA when rAF is paused (background tab).
window.__radarDraw = () => {
  lastFrame = performance.now();
  frame(lastFrame);
};
