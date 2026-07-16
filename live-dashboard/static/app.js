const PANELS = ["sucursal01", "sucursal02", "sucursal03", "central"];
const BRANCH_PANELS = ["sucursal01", "sucursal02", "sucursal03"];
const rowsEl = {};
for (const p of PANELS) rowsEl[p] = document.getElementById(`rows-${p}`);
const countEl = {};
for (const p of PANELS) countEl[p] = document.getElementById(`count-${p}`);
const ticker = document.getElementById("ticker");
const toast = document.getElementById("toast");
const connDot = document.getElementById("conn-dot");
const connLabel = document.getElementById("conn-label");
const statSucursales = document.getElementById("stat-sucursales");
const statCentral = document.getElementById("stat-central");
const statLatencia = document.getElementById("stat-latencia");

const OP_LABEL = { c: "Alta", u: "Actualización", d: "Baja", r: "Catálogo" };

function rowKey(panel, sucursal, id) {
  return panel === "central" ? `${sucursal}:${id}` : String(id);
}

function fmtMoney(v) {
  return v == null ? "—" : `$${Number(v).toFixed(2)}`;
}

function panelLabel(panel) {
  return panel === "central" ? "Base Central" : panel.replace("sucursal", "Sucursal ");
}

function sucursalShortLabel(sucursal) {
  return sucursal ? sucursal.replace("sucursal", "Suc ") : "";
}

function branchDotClass(sucursal) {
  return `dot-${parseInt(sucursal.replace("sucursal", ""), 10)}`;
}

function updatePanelCount(panel) {
  const count = rowsEl[panel].querySelectorAll("li").length;
  if (countEl[panel]) countEl[panel].textContent = count;
  if (panel === "central" && statCentral) statCentral.textContent = count;
}

function renderRow(panel, key, row) {
  const container = rowsEl[panel];
  let el = container.querySelector(`[data-key="${key}"]`);

  if (!row) {
    if (el) {
      el.classList.add("flash-delete");
      setTimeout(() => { el.remove(); updatePanelCount(panel); }, 700);
    }
    return null;
  }

  const isCentral = panel === "central";

  if (!el) {
    el = document.createElement("li");
    el.dataset.key = key;
    const actions = isCentral ? "" : `
      <div class="row-actions">
        <button class="btn-edit" type="button" title="Editar" aria-label="Editar">✎</button>
        <button class="btn-delete" type="button" title="Dar de baja" aria-label="Dar de baja">✕</button>
      </div>`;
    el.innerHTML = `
      <div class="row-id">
        <span class="row-name"></span>
        <span class="row-sub"></span>
      </div>
      <span class="row-qty"></span>
      <span class="row-price"></span>${actions}`;
    container.prepend(el);
  }

  el.querySelector(".row-name").textContent = row.nombre ?? "—";

  const subEl = el.querySelector(".row-sub");
  if (isCentral && row.sucursal) {
    el.dataset.sucursal = row.sucursal;
    subEl.innerHTML = `<span class="origin-dot ${branchDotClass(row.sucursal)}"></span>${sucursalShortLabel(row.sucursal)} · ${row.sku ?? ""}`;
  } else {
    subEl.textContent = row.sku ?? "";
  }

  el.querySelector(".row-qty").textContent = row.inventario != null ? `${row.inventario} u.` : "—";
  el.querySelector(".row-price").textContent = fmtMoney(row.precio_venta);

  if (isCentral) applyCentralFilterTo(el);

  el._row = row;
  updatePanelCount(panel);
  return el;
}

function retrigger(el, cls) {
  el.classList.remove(cls);
  void el.offsetWidth; // fuerza reflow para poder re-disparar la misma animación
  el.classList.add(cls);
}

function flash(el, op, changedFields) {
  if (!el) return;
  const cls = op === "u" ? "flash-update" : "flash-insert";
  retrigger(el, cls);

  for (const field of changedFields || []) {
    const target = field === "precio_venta" ? el.querySelector(".row-price")
      : field === "inventario" ? el.querySelector(".row-qty")
      : null;
    if (target) retrigger(target, "flash-field");
  }
}

function opEventClass(op) {
  return op === "d" ? "event-delete" : op === "u" ? "event-update" : "event-insert";
}

function addTicker(msg, text) {
  const li = document.createElement("li");
  li.className = `event ${opEventClass(msg.op)}`;
  const time = new Date().toLocaleTimeString("es-MX", { hour12: false });
  const metaLine = msg.panel === "central" && msg.latency_ms != null
    ? `<p class="event-meta">Central confirmó en ${msg.latency_ms} ms</p>`
    : "";
  li.innerHTML = `
    <span class="event-dot"></span>
    <div class="event-body">
      <p></p>
      ${metaLine}
    </div>
    <time>${time}</time>`;
  li.querySelector(".event-body p").textContent = text;
  ticker.prepend(li);
  while (ticker.children.length > 30) ticker.removeChild(ticker.lastChild);
}

function describeChange(msg) {
  const nombre = (msg.after || msg.before || {}).nombre || "producto";
  const label = OP_LABEL[msg.op] || msg.op;
  const panelName = panelLabel(msg.panel);

  if (msg.op === "d") {
    return `${panelName} · ${label} de baja: ${nombre}`;
  }
  if (msg.before && msg.changed_fields?.includes("precio_venta")) {
    return `${panelName} · ${nombre} — precio ${fmtMoney(msg.before.precio_venta)} → ${fmtMoney(msg.after.precio_venta)}`;
  }
  if (msg.before && msg.changed_fields?.includes("inventario")) {
    return `${panelName} · ${nombre} — inventario ${msg.before.inventario} → ${msg.after.inventario}`;
  }
  return `${panelName} · ${label}: ${nombre}`;
}

const latencySamples = [];
function recordLatency(ms) {
  latencySamples.push(ms);
  if (latencySamples.length > 8) latencySamples.shift();
  const avg = Math.round(latencySamples.reduce((a, b) => a + b, 0) / latencySamples.length);
  if (statLatencia) statLatencia.innerHTML = `${avg} <span class="stat-unit">ms</span>`;
}

function applyChange(msg) {
  const key = rowKey(msg.panel, msg.sucursal, msg.id);
  const el = renderRow(msg.panel, key, msg.after);
  if (msg.op !== "d") flash(el, msg.op, msg.changed_fields);
  addTicker(msg, describeChange(msg));
  if (msg.panel === "central" && msg.latency_ms != null) recordLatency(msg.latency_ms);
}

function applySnapshot(panels) {
  for (const panel of PANELS) {
    rowsEl[panel].innerHTML = "";
    const rows = panels[panel] || {};
    for (const [key, row] of Object.entries(rows)) {
      renderRow(panel, key, row);
    }
    updatePanelCount(panel);
  }
}

function setConnStatus(state) {
  connDot.className = `status-dot conn-${state}`;
  connLabel.textContent = state === "connected" ? "En vivo" : state === "connecting" ? "Conectando…" : "Desconectado";
  if (statSucursales) {
    statSucursales.innerHTML = state === "connected"
      ? `3 <span class="stat-unit">/ 3</span>`
      : `0 <span class="stat-unit">/ 3</span>`;
  }
}

function connect() {
  setConnStatus("connecting");
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => setConnStatus("connected");
  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "snapshot") applySnapshot(msg.panels);
    else if (msg.type === "change") applyChange(msg);
  };
  ws.onclose = () => {
    setConnStatus("disconnected");
    setTimeout(connect, 1500);
  };
  ws.onerror = () => ws.close();
}

connect();

// --- Filtro del panel Central por sucursal de origen ---

let centralFilter = "all";

function applyCentralFilterTo(el) {
  const matches = centralFilter === "all" || el.dataset.sucursal === centralFilter;
  el.style.display = matches ? "" : "none";
}

function applyCentralFilterAll() {
  rowsEl.central.querySelectorAll("li").forEach(applyCentralFilterTo);
}

document.querySelectorAll("#central-filters .chip").forEach((chip) => {
  chip.addEventListener("click", () => {
    centralFilter = chip.dataset.filter;
    document.querySelectorAll("#central-filters .chip").forEach((c) => {
      c.classList.toggle("active", c === chip);
    });
    applyCentralFilterAll();
  });
});

// --- Formulario interactivo: alta / edición / baja de productos por sucursal ---
// Central nunca aparece acá: no tiene botón "+ Nuevo" ni acciones por fila
// en el HTML/render, y el backend además rechaza con 404 cualquier
// /api/central/... — la Central sigue siendo de solo lectura.

let toastTimer = null;
function showToast(text, isError = false) {
  toast.textContent = text;
  toast.classList.toggle("error", isError);
  toast.classList.add("visible");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.remove("visible"), 4000);
}

let lookupsCache = null;
async function ensureLookups() {
  if (!lookupsCache) {
    const res = await fetch("/api/lookups");
    lookupsCache = await res.json();
  }
  return lookupsCache;
}

function populateSelect(select, items) {
  select.innerHTML = items.map((i) => `<option value="${i.id}">${i.nombre}</option>`).join("");
}

const modal = document.getElementById("producto-modal");
const form = document.getElementById("producto-form");
const modalTitle = document.getElementById("producto-modal-title");
const modalError = document.getElementById("producto-modal-error");
const submitBtn = document.getElementById("producto-submit");

async function openModal({ mode, branch, id, current }) {
  modal.dataset.mode = mode;
  modal.dataset.branch = branch;
  modal.dataset.id = id ?? "";
  modalError.hidden = true;
  form.reset();

  const isCreate = mode === "create";
  modalTitle.textContent = isCreate
    ? `Nuevo producto — ${panelLabel(branch)}`
    : `Editar "${current?.nombre ?? ""}" — ${panelLabel(branch)}`;
  submitBtn.textContent = isCreate ? "Crear" : "Guardar";
  form.querySelectorAll(".solo-crear").forEach((el) => {
    el.style.display = isCreate ? "" : "none";
    el.querySelectorAll("input, select").forEach((field) => { field.disabled = !isCreate; });
  });

  if (isCreate) {
    const lookups = await ensureLookups();
    populateSelect(form.categoria_id, lookups.categorias);
    populateSelect(form.laboratorio_id, lookups.laboratorios);
  } else if (current) {
    form.precio_venta.value = current.precio_venta ?? "";
    form.inventario.value = current.inventario ?? "";
  }

  modal.showModal();
}

document.querySelectorAll(".btn-new").forEach((btn) => {
  btn.addEventListener("click", () => openModal({ mode: "create", branch: btn.dataset.branch }));
});

document.getElementById("producto-cancel").addEventListener("click", () => modal.close());

for (const panel of BRANCH_PANELS) {
  rowsEl[panel].addEventListener("click", async (e) => {
    const rowEl = e.target.closest("li");
    if (!rowEl || !rowEl._row) return;

    if (e.target.closest(".btn-edit")) {
      openModal({ mode: "edit", branch: panel, id: rowEl._row.id, current: rowEl._row });
      return;
    }

    if (e.target.closest(".btn-delete")) {
      if (!confirm(`¿Dar de baja "${rowEl._row.nombre}"?`)) return;
      try {
        const res = await fetch(`/api/${panel}/medicamentos/${rowEl._row.id}`, { method: "DELETE" });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || `Error ${res.status}`);
        showToast(`Baja enviada a ${panelLabel(panel)} — esperando confirmación…`);
      } catch (err) {
        showToast(err.message, true);
      }
    }
  });
}

function readCreateForm() {
  return {
    nombre: form.nombre.value.trim(),
    categoria_id: Number(form.categoria_id.value),
    laboratorio_id: Number(form.laboratorio_id.value),
    requiere_receta: form.requiere_receta.checked,
    precio_costo: Number(form.precio_costo.value),
    precio_venta: Number(form.precio_venta.value),
    inventario: Number(form.inventario.value),
  };
}

function readEditForm() {
  return {
    precio_venta: Number(form.precio_venta.value),
    inventario: Number(form.inventario.value),
  };
}

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const mode = modal.dataset.mode;
  const branch = modal.dataset.branch;
  modalError.hidden = true;
  submitBtn.disabled = true;

  try {
    const res = mode === "create"
      ? await fetch(`/api/${branch}/medicamentos`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(readCreateForm()),
        })
      : await fetch(`/api/${branch}/medicamentos/${modal.dataset.id}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(readEditForm()),
        });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.detail || `Error ${res.status}`);

    modal.close();
    showToast(`Enviado a ${panelLabel(branch)} — esperando confirmación…`);
  } catch (err) {
    modalError.textContent = err.message;
    modalError.hidden = false;
  } finally {
    submitBtn.disabled = false;
  }
});

// --- Botones "simular caída": apagan/prenden de verdad mysql-central o
// kafka (vía Docker) para demostrar en vivo que el pipeline se recupera solo.

const CHAOS_TARGETS = ["mysql-central", "kafka"];
const CHAOS_LABEL = { "mysql-central": "Base Central", kafka: "Kafka" };
const chaosButtons = {};
for (const target of CHAOS_TARGETS) {
  chaosButtons[target] = document.querySelector(`.chaos-btn[data-target="${target}"]`);
}

function applyChaosStatus(target, status) {
  const btn = chaosButtons[target];
  if (!btn) return;
  const isDown = status !== "running";
  btn.classList.toggle("chaos-is-down", isDown);
  btn.querySelector(".chaos-dot").classList.toggle("chaos-down", isDown);
  btn.querySelector(".chaos-action").textContent = isDown ? "Reiniciar" : "Detener";
}

async function refreshChaosStatus() {
  try {
    const res = await fetch("/api/infra/status");
    if (!res.ok) return;
    const status = await res.json();
    for (const target of CHAOS_TARGETS) applyChaosStatus(target, status[target]);
  } catch {
    // silencioso — un tick fallido no debe interrumpir la demo
  }
}

for (const target of CHAOS_TARGETS) {
  const btn = chaosButtons[target];
  if (!btn) continue;
  btn.addEventListener("click", async () => {
    btn.disabled = true;
    try {
      const res = await fetch(`/api/infra/${target}/toggle`, { method: "POST" });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.detail || `Error ${res.status}`);
      applyChaosStatus(target, data.status);
      showToast(
        data.status === "running"
          ? `${CHAOS_LABEL[target]} arrancando de nuevo — el pipeline se pondrá al día solo`
          : `${CHAOS_LABEL[target]} detenida — mirá cómo el resto sigue funcionando`
      );
    } catch (err) {
      showToast(err.message, true);
    } finally {
      btn.disabled = false;
    }
  });
}

refreshChaosStatus();
setInterval(refreshChaosStatus, 5000);
