const GIST_OWNER = "loadlib";
const GIST_ID = "ab3496688642bc8f1c0a895cf23de184";
const RAW = `https://gist.githubusercontent.com/${GIST_OWNER}/${GIST_ID}/raw`;

const canvas = document.getElementById("map");
const ctx = canvas.getContext("2d");
const statusEl = document.getElementById("status");
const sessionEl = document.getElementById("session");
const mapNameEl = document.getElementById("map-name");
const playersEl = document.getElementById("players");
const roundEl = document.getElementById("round");
const updatedEl = document.getElementById("updated");
const emptyEl = document.getElementById("empty");
const roomInput = document.getElementById("room");

let maps = [];
let room = (new URLSearchParams(location.search).get("room") || localStorage.getItem("cs2-room") || "").toUpperCase();
let snapshot = null;
let mapDef = null;
let mapImage = null;
let mapLoading = false;
let failures = 0;
let lastUpdate = 0;
let timer = 0;

roomInput.value = room;

async function loadMaps() {
  try {
    const response = await fetch("data/maps.json", { cache: "no-store" });
    const data = await response.json();
    maps = data.maps || [];
  } catch (error) {
    console.warn("maps", error);
  }
}

function setStatus(text, kind = "") {
  statusEl.textContent = text;
  statusEl.className = `status ${kind}`;
}

function currentRoom() {
  return room.trim().toUpperCase();
}

function connect(code) {
  room = code.trim().toUpperCase();
  if (room.length < 4) {
    setStatus("offline", "offline");
    emptyEl.style.display = "grid";
    return;
  }
  roomInput.value = room;
  localStorage.setItem("cs2-room", room);
  const url = new URL(location.href);
  url.searchParams.set("room", room);
  history.replaceState(null, "", url);
  sessionEl.textContent = room;
  emptyEl.style.display = "none";
  snapshot = null;
  failures = 0;
  lastUpdate = 0;
  mapDef = null;
  mapImage = null;
  mapNameEl.textContent = "—";
  playersEl.textContent = "0";
  roundEl.textContent = "—";
  updatedEl.textContent = "—";
  clearInterval(timer);
  fetchSnapshot();
  timer = setInterval(fetchSnapshot, 2000);
  draw();
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
    draw();
  };
  image.onerror = () => {
    if (cdn && image.src !== cdn) {
      image.src = cdn;
      return;
    }
    mapLoading = false;
    draw();
  };
  image.src = local || cdn;
}

async function fetchSnapshot() {
  if (!currentRoom()) return;
  try {
    const url = `${RAW}/radar_${currentRoom()}.json?ts=${Date.now()}`;
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    if (data.room && data.room.toUpperCase() !== currentRoom()) {
      throw new Error("room mismatch");
    }
    snapshot = data;
    failures = 0;
    lastUpdate = Date.now();
    setStatus("online", "online");
    ensureMap(data.map);
    updateInfo();
    draw();
  } catch (error) {
    failures++;
    if (failures >= 2) {
      setStatus(failures >= 4 ? "offline" : "waiting", failures >= 4 ? "offline" : "");
    }
    updateInfo();
    draw();
  }
}

function updateInfo() {
  if (!snapshot) {
    updatedEl.textContent = "—";
    return;
  }
  const players = snapshot.players || [];
  playersEl.textContent = String(players.length);
  const round = snapshot.round || {};
  const seconds = Number(round.time || 0);
  roundEl.textContent = `${round.phase ?? 0} · ${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, "0")}`;
  updatedEl.textContent = lastUpdate ? `${Math.max(0, Math.round((Date.now() - lastUpdate) / 1000))}s ago` : "—";
  if (snapshot.map && !mapDef) ensureMap(snapshot.map);
}

function resize() {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = Math.max(1, Math.floor(rect.width * dpr));
  canvas.height = Math.max(1, Math.floor(rect.height * dpr));
  draw();
}

function draw() {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  const width = rect.width;
  const height = rect.height;
  if (!width || !height) return;

  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);

  const size = 1024;
  const view = Math.min(width, height) * 0.92;
  const ox = (width - view) / 2;
  const oy = (height - view) / 2;

  ctx.fillStyle = "#14171e";
  ctx.fillRect(ox, oy, view, view);

  if (mapImage && mapImage.complete) {
    ctx.drawImage(mapImage, ox, oy, view, view);
  } else {
    ctx.strokeStyle = "#262c3a";
    ctx.lineWidth = 1;
    for (let i = 0; i <= 8; i++) {
      const x = ox + (view / 8) * i;
      const y = oy + (view / 8) * i;
      ctx.beginPath();
      ctx.moveTo(x, oy);
      ctx.lineTo(x, oy + view);
      ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(ox, y);
      ctx.lineTo(ox + view, y);
      ctx.stroke();
    }
  }

  if (!mapDef) {
    drawHint(width, height, mapLoading ? "loading map…" : "waiting for a map snapshot");
    return;
  }

  const scale = mapDef.scale || 1;
  const point = (x, y) => {
    const u = (x - mapDef.pos_x) / (size * scale);
    const v = (mapDef.pos_y - y) / (size * scale);
    return [ox + u * view, oy + v * view];
  };

  const sites = mapDef.sites || {};
  for (const [label, pair] of Object.entries(sites)) {
    if (!Array.isArray(pair) || pair.length < 2) continue;
    const x = ox + Number(pair[0]) * view;
    const y = oy + Number(pair[1]) * view;
    ctx.fillStyle = "#d8b84a";
    ctx.font = "600 11px Inter, sans-serif";
    ctx.fillText(label.toUpperCase(), x + 4, y - 4);
  }

  if (!snapshot) {
    drawHint(width, height, "waiting for the session…");
    return;
  }

  const local = snapshot.local || {};
  const players = snapshot.players || [];
  for (const player of players) {
    const [x, y] = point(Number(player.x), Number(player.y));
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
    const team = Number(player.team) === Number(local.team);
    ctx.fillStyle = team ? "#6fa8ff" : "#e06c75";
    ctx.globalAlpha = player.visible === false ? 0.55 : 1;
    drawDirection(x, y, Number(player.yaw) || 0, ctx.fillStyle);
    ctx.beginPath();
    ctx.arc(x, y, 3.2, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.globalAlpha = 1;

  if (local.x !== undefined && local.y !== undefined) {
    const [x, y] = point(Number(local.x), Number(local.y));
    if (Number.isFinite(x) && Number.isFinite(y)) {
      ctx.fillStyle = "#f2f4ff";
      drawDirection(x, y, Number(local.yaw) || 0, ctx.fillStyle, 10);
      ctx.beginPath();
      ctx.arc(x, y, 3.6, 0, Math.PI * 2);
      ctx.fill();
    }
  }

  const bomb = snapshot.bomb;
  if (bomb && bomb.planted && Array.isArray(bomb.pos)) {
    const [x, y] = point(Number(bomb.pos[0]), Number(bomb.pos[1]));
    if (Number.isFinite(x) && Number.isFinite(y)) {
      ctx.strokeStyle = "#e06c75";
      ctx.lineWidth = 2;
      ctx.strokeRect(x - 5, y - 5, 10, 10);
      ctx.fillStyle = "#e06c75";
      ctx.font = "10px 'JetBrains Mono', monospace";
      ctx.fillText(`${Math.max(0, Math.round(bomb.timer || 0))}`, x + 8, y - 6);
    }
  }

  if (!players.length && !mapImage) {
    drawHint(width, height, "no players in snapshot");
  }
}

function drawDirection(x, y, yaw, color, length = 8) {
  const angle = (yaw * Math.PI) / 180;
  const dx = Math.cos(angle);
  const dy = -Math.sin(angle);
  const sideX = -dy;
  const sideY = dx;
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.moveTo(x + dx * length, y + dy * length);
  ctx.lineTo(x + sideX * 4 - dx * 2, y + sideY * 4 - dy * 2);
  ctx.lineTo(x - sideX * 4 - dx * 2, y - sideY * 4 - dy * 2);
  ctx.closePath();
  ctx.fill();
}

function drawHint(width, height, text) {
  ctx.fillStyle = "rgba(217, 222, 233, .55)";
  ctx.font = "12px 'JetBrains Mono', monospace";
  ctx.textAlign = "center";
  ctx.fillText(text, width / 2, height - 22);
  ctx.textAlign = "left";
}

document.getElementById("connect").addEventListener("submit", (event) => {
  event.preventDefault();
  connect(roomInput.value);
});
roomInput.addEventListener("input", () => {
  roomInput.value = roomInput.value.toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 6);
});

new ResizeObserver(resize).observe(canvas);
loadMaps().then(() => {
  resize();
  if (currentRoom()) connect(currentRoom());
  else draw();
});
