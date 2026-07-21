"""
live-dashboard: observador puro de Kafka que expone, por WebSocket, los
cambios en vivo de `medicamentos` en las 3 sucursales y en la Base Central
— pensado para una demo visual a clientes no técnicos, sin exponer Kafka
ni JSON crudo.

No escribe nada en ninguna base ni en Kafka (salvo la excepción de
central-sync, que publica `central.applied` tras cada commit exitoso — ver
central-sync/app.py). Este servicio solo consume y retransmite.

Arranca en modo "catch-up" silencioso (reconstruye el estado actual sin
animar cada evento histórico) y pasa a modo "en vivo" apenas se pone al día
con el final de los tópicos — mismo espíritu que
`scripts/4-verificar-cambio.sh` (por defecto solo eventos nuevos).
"""

import asyncio
import json
import logging
import os
import re
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import docker
import mysql.connector
from confluent_kafka import Consumer, KafkaException
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("live-dashboard")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CENTRAL_APPLIED_TOPIC = os.environ.get("CENTRAL_APPLIED_TOPIC", "central.applied")

# Solo para la lectura única de arranque que puebla el panel de Central con
# el catálogo que ya existía antes de que este dashboard existiera — el
# tópico central.applied no tiene (ni puede tener) historial anterior a su
# propia creación. A partir de esa lectura, el panel se alimenta 100% de
# Kafka, igual que los paneles de sucursal.
CENTRAL_DB_HOST = os.environ.get("CENTRAL_DB_HOST", "mysql-central")
CENTRAL_DB_PORT = int(os.environ.get("CENTRAL_DB_PORT", "3306"))
CENTRAL_DB_USER = os.environ.get("CENTRAL_DB_USER", "root")
CENTRAL_DB_PASSWORD = os.environ.get("CENTRAL_DB_PASSWORD", "rootpassword")
CENTRAL_DB_NAME = os.environ.get("CENTRAL_DB_NAME", "central_farmacias")

# Igual que central-sync: la suscripción por regex nativa de librdkafka no
# funciona contra este broker, así que los tópicos se descubren con
# list_topics() + filtro en Python (ver central-sync/app.py).
BRANCH_TOPIC_PATTERN = re.compile(r"^pos\.(sucursal\d+)\..*\.medicamentos$")
TOPIC_DISCOVERY_INTERVAL_S = 5
CATCH_UP_MAX_S = 20  # red de seguridad; el mecanismo principal es snapshot_watermarks()

DISPLAY_FIELDS = ["sku", "nombre", "precio_venta", "inventario"]

# Credenciales de escritura hacia cada sucursal: el usuario "appuser" ya lo
# provee la propia imagen de MySQL (MYSQL_USER/MYSQL_PASSWORD en
# docker-compose.yml, con todos los privilegios sobre su propia base) — es
# el usuario correcto para esto, distinto del `debezium` de solo lectura de
# binlog y de `root`. "central" nunca aparece en BRANCHES a propósito: así
# cualquier /api/central/... 404 solo, sin necesidad de un chequeo aparte —
# la Central sigue siendo de solo lectura, poblada únicamente por
# central-sync.
BRANCH_DB_USER = os.environ.get("BRANCH_DB_USER", "appuser")
BRANCH_DB_PASSWORD = os.environ.get("BRANCH_DB_PASSWORD", "apppassword")
BRANCH_DB_PORT = int(os.environ.get("BRANCH_DB_PORT", "3306"))
BRANCHES = {
    "sucursal01": {"host": os.environ.get("SUCURSAL01_DB_HOST", "mysql-sucursal-01"), "database": "pos_sucursal_01"},
    "sucursal02": {"host": os.environ.get("SUCURSAL02_DB_HOST", "mysql-sucursal-02"), "database": "pos_sucursal_02"},
    "sucursal03": {"host": os.environ.get("SUCURSAL03_DB_HOST", "mysql-sucursal-03"), "database": "pos_sucursal_03"},
}

# Mismo espíritu que TABLE_COLUMNS en central-sync/app.py: hardcodeado en vez
# de consultado, porque las 3 sucursales montan el mismo
# mysql-init/pos/01-schema.sql sin cambios — estas filas no pueden divergir
# entre sucursales sin tocar ese archivo compartido.
LOOKUPS = {
    "categorias": [
        {"id": 1, "nombre": "Analgésicos"}, {"id": 2, "nombre": "Antibióticos"},
        {"id": 3, "nombre": "Antihistamínicos"}, {"id": 4, "nombre": "Gastrointestinal"},
        {"id": 5, "nombre": "Cardiovascular"}, {"id": 6, "nombre": "Metabólico"},
    ],
    "laboratorios": [
        {"id": 1, "nombre": "Bayer"}, {"id": 2, "nombre": "Pfizer"},
        {"id": 3, "nombre": "Genfar"}, {"id": 4, "nombre": "Novartis"}, {"id": 5, "nombre": "Roche"},
    ],
}

# Botones "simular caída" del panel: apagan/prenden de verdad el contenedor
# de mysql-central o de kafka (vía el socket de Docker montado en
# docker-compose.yml), para demostrar en vivo que central-sync y
# connector-watchdog se recuperan solos — no es una animación, es la misma
# caída que ya se documentó en docs/informe-resolucion-fallas.html.
DOCKER_SOCKET_URL = os.environ.get("DOCKER_SOCKET_URL", "unix:///var/run/docker.sock")
CHAOS_TARGETS = {
    "mysql-central": os.environ.get("MYSQL_CENTRAL_CONTAINER", "dbz-lab-mysql-central"),
    "kafka": os.environ.get("KAFKA_CONTAINER", "dbz-lab-kafka"),
}

_docker_client = None


def get_docker_client():
    global _docker_client
    if _docker_client is None:
        _docker_client = docker.DockerClient(base_url=DOCKER_SOCKET_URL)
    return _docker_client


def get_chaos_container(target: str):
    container_name = CHAOS_TARGETS.get(target)
    if container_name is None:
        raise HTTPException(status_code=404, detail="Objetivo desconocido")
    try:
        return get_docker_client().containers.get(container_name)
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail=f"Contenedor {container_name} no encontrado")
    except docker.errors.DockerException as exc:
        raise HTTPException(status_code=503, detail=f"Docker no disponible: {exc}")


STATIC_DIR = Path(__file__).parent / "static"

state_lock = threading.Lock()
state = {"sucursal01": {}, "sucursal02": {}, "sucursal03": {}, "central": {}}

connections = set()
main_loop = None
broadcast_queue = None


def discover_topics(consumer):
    metadata = consumer.list_topics(timeout=10)
    names = set(metadata.topics)
    topics = sorted(t for t in names if BRANCH_TOPIC_PATTERN.match(t))
    if CENTRAL_APPLIED_TOPIC in names:
        topics.append(CENTRAL_APPLIED_TOPIC)
    return topics


def parse_branch_topic(topic):
    # pos.sucursal01.pos_sucursal_01.medicamentos -> "sucursal01"
    return topic.split(".")[1]


def extract_fields(row):
    if row is None:
        return None
    out = {"id": row.get("id")}
    for field in DISPLAY_FIELDS:
        value = row.get(field)
        if field == "precio_venta" and value is not None:
            value = float(value)
        out[field] = value
    return out


def diff_fields(before, after):
    if before is None or after is None:
        return list(DISPLAY_FIELDS)
    return [f for f in DISPLAY_FIELDS if before.get(f) != after.get(f)]


def queue_message(msg):
    if main_loop is not None and broadcast_queue is not None:
        main_loop.call_soon_threadsafe(broadcast_queue.put_nowait, msg)


def handle_branch_message(topic, raw_value, live):
    sucursal = parse_branch_topic(topic)
    event = json.loads(raw_value)
    payload = event.get("payload")
    if payload is None:
        return

    op = payload["op"]
    before = extract_fields(payload.get("before"))
    after = extract_fields(payload.get("after"))
    row_id = (after or before)["id"]
    key = str(row_id)

    with state_lock:
        panel = state[sucursal]
        if op == "d":
            panel.pop(key, None)
        else:
            panel[key] = after

    if not live:
        return

    queue_message({
        "type": "change",
        "panel": sucursal,
        "sucursal": sucursal,
        "op": op,
        "id": row_id,
        "before": before,
        "after": after,
        "changed_fields": [] if op == "d" else diff_fields(before, after),
        "ts_ms": int(time.time() * 1000),
    })


def handle_central_message(raw_value, live):
    payload = json.loads(raw_value)
    if payload.get("tabla") != "medicamentos":
        return

    sucursal = payload["sucursal"]
    op = payload["op"]
    row_id = payload["id"]
    key = f"{sucursal}:{row_id}"
    after = extract_fields(payload.get("after")) if payload.get("after") else None
    if after is not None:
        after["sucursal"] = sucursal

    with state_lock:
        before = state["central"].get(key)
        if op == "d":
            state["central"].pop(key, None)
        else:
            state["central"][key] = after

    if not live:
        return

    source_ts_ms = payload.get("source_ts_ms")
    latency_ms = (payload["applied_ts_ms"] - source_ts_ms) if source_ts_ms is not None else None

    msg = {
        "type": "change",
        "panel": "central",
        "sucursal": sucursal,
        "op": op,
        "id": row_id,
        "before": before,
        "after": after,
        "changed_fields": [] if op == "d" else diff_fields(before, after),
        "ts_ms": payload["applied_ts_ms"],
    }
    if latency_ms is not None:
        msg["latency_ms"] = latency_ms
    queue_message(msg)


def bootstrap_central_state():
    """Lectura única (no polling) a mysql-central al arrancar, para que el
    panel de Central no aparezca vacío mientras no haya cambios nuevos.
    De acá en más, el panel se actualiza solo por eventos de Kafka
    (central.applied) — esta consulta corre una vez, no en loop.
    """
    try:
        conn = mysql.connector.connect(
            host=CENTRAL_DB_HOST,
            port=CENTRAL_DB_PORT,
            user=CENTRAL_DB_USER,
            password=CENTRAL_DB_PASSWORD,
            database=CENTRAL_DB_NAME,
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT sucursal, id, sku, nombre, precio_venta, inventario FROM medicamentos")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
    except mysql.connector.Error as exc:
        log.warning("No se pudo leer el estado inicial de la Base Central (%s) — el panel arranca vacío y se completa con los próximos cambios", exc)
        return

    with state_lock:
        for row in rows:
            key = f"{row['sucursal']}:{row['id']}"
            state["central"][key] = {
                "id": row["id"],
                "sucursal": row["sucursal"],
                "sku": row["sku"],
                "nombre": row["nombre"],
                "precio_venta": float(row["precio_venta"]) if row["precio_venta"] is not None else None,
                "inventario": row["inventario"],
            }
    log.info("Estado inicial de Central cargado: %d productos", len(rows))


def snapshot_watermarks(consumer, caught_up):
    """Congela, una sola vez por (re)suscripción, el offset de fin de cada
    partición asignada — así "estar al día" se mide contra un objetivo fijo
    (el fin del log en el momento de arrancar), no contra un objetivo que se
    sigue moviendo si llegan mensajes nuevos mientras se hace catch-up.
    Devuelve None si la asignación todavía no está lista (recién después de
    subscribe() hace falta un poll para que el rebalanceo se complete).
    """
    assignment = consumer.assignment()
    if not assignment:
        return None
    targets = {}
    for tp in assignment:
        low, high = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
        key = (tp.topic, tp.partition)
        targets[key] = high
        if low >= high:
            caught_up.add(key)  # partición vacía: no hay nada que esperar
    return targets


def kafka_consumer_loop():
    # group.id único por arranque de proceso a propósito: este consumer nunca
    # confirma offsets (siempre relee desde "earliest"), así que no gana nada
    # reusando un group.id fijo — y si lo reusara, cada restart tendría que
    # esperar a que el broker expire la membresía del proceso anterior (que
    # murió sin avisar, sin un consumer.close() limpio) antes de poder
    # asignarle particiones al nuevo, dejando el catch-up colgado varios
    # segundos de más sin necesidad.
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"live-dashboard-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    subscribed = []
    last_discovery = 0.0
    catchup = {"live": False, "targets": None, "caught_up": set(), "deadline": 0.0}
    # Próximo offset a leer por partición, llevado por nosotros mismos (este
    # consumer no confirma nada al broker). Es la clave para no repetir el
    # catch-up silencioso en cada reasignación: una partición que ya
    # aparece acá es una que ya veníamos leyendo, y hay que RETOMARLA ahí,
    # no dejar que caiga a "earliest" de nuevo.
    resume_offsets = {}

    def reset_catchup():
        catchup["live"] = False
        catchup["targets"] = None
        catchup["caught_up"] = set()
        catchup["deadline"] = time.monotonic() + CATCH_UP_MAX_S

    def on_assign(consumer_, partitions):
        # Se dispara en CUALQUIER (re)asignación de particiones, no solo la
        # que sigue a nuestro propio consumer.subscribe() de más abajo —
        # también en un rejoin interno de librdkafka (p.ej. tras un
        # SESSTMOUT porque Kafka estuvo caída más que session.timeout.ms).
        #
        # Particiones YA conocidas (veníamos leyéndolas) se retoman en el
        # offset donde se quedaron — no hay que perder ni repetir nada.
        # Solo las particiones NUEVAS (recién vistas, típicamente al
        # arrancar el proceso) pasan por el catch-up silencioso, que evita
        # que la primera lectura desde "earliest" muestre décadas de
        # historial como si fueran cambios en vivo.
        #
        # Sin esto, CUALQUIER reasignación (incluida una tras un rejoin
        # interno, sin pérdida real de posición) volvía a silenciar todo
        # hasta alcanzar un nuevo watermark — y si Kafka tardaba en
        # estabilizarse y se reasignaba varias veces seguidas, los cambios
        # reales que iban llegando en el medio se aplicaban en silencio al
        # estado y NUNCA llegaban al ticker: solo se veía el último cambio
        # una vez que las reasignaciones por fin paraban.
        fresh = []
        for tp in partitions:
            key = (tp.topic, tp.partition)
            offset = resume_offsets.get(key)
            if offset is not None:
                tp.offset = offset
            else:
                fresh.append(key)
        consumer_.assign(partitions)

        if fresh:
            reset_catchup()
            log.info(
                "Asignación con %d partición(es) nunca vista(s) — catch-up silencioso antes de transmitir en vivo",
                len(fresh),
            )
        else:
            log.info(
                "Reasignación de %d partición(es) ya conocidas — se retoma sin perder eventos, sin silenciar",
                len(partitions),
            )

    try:
        while True:
            now = time.monotonic()
            if now - last_discovery >= TOPIC_DISCOVERY_INTERVAL_S:
                last_discovery = now
                try:
                    current = discover_topics(consumer)
                except KafkaException as exc:
                    log.warning("No se pudo listar tópicos todavía: %s", exc)
                    current = subscribed
                if current and current != subscribed:
                    consumer.subscribe(current, on_assign=on_assign)
                    log.info("Suscrito a %d tópicos: %s", len(current), ", ".join(current))
                    subscribed = current

            if not subscribed:
                time.sleep(1)
                continue

            if not catchup["live"] and catchup["targets"] is None:
                catchup["targets"] = snapshot_watermarks(consumer, catchup["caught_up"])

            msg = consumer.poll(timeout=1.0)

            if msg is not None and not msg.error():
                resume_offsets[(msg.topic(), msg.partition())] = msg.offset() + 1

            if not catchup["live"] and msg is not None and not msg.error() and catchup["targets"] is not None:
                key = (msg.topic(), msg.partition())
                if msg.offset() + 1 >= catchup["targets"].get(key, 0):
                    catchup["caught_up"].add(key)

            if not catchup["live"] and catchup["targets"] is not None and set(catchup["targets"]) <= catchup["caught_up"]:
                catchup["live"] = True
                log.info("Catch-up completo (offsets al día) — transmitiendo cambios en vivo")
            elif not catchup["live"] and catchup["deadline"] and time.monotonic() >= catchup["deadline"]:
                catchup["live"] = True
                log.warning("Catch-up forzado por timeout de seguridad — puede haber quedado historial sin drenar")

            if msg is None:
                continue
            if msg.error():
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                log.warning("Error no fatal del consumidor: %s", msg.error())
                continue

            raw_value = msg.value()
            if raw_value is None:
                continue  # tombstone: nada que reflejar

            try:
                if msg.topic() == CENTRAL_APPLIED_TOPIC:
                    handle_central_message(raw_value, catchup["live"])
                else:
                    handle_branch_message(msg.topic(), raw_value, catchup["live"])
            except Exception:
                log.exception("Error procesando mensaje de topic=%s, se omite", msg.topic())
    finally:
        consumer.close()


async def broadcaster():
    while True:
        msg = await broadcast_queue.get()
        data = json.dumps(msg)
        for ws in list(connections):
            try:
                await ws.send_text(data)
            except Exception:
                connections.discard(ws)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_loop, broadcast_queue
    main_loop = asyncio.get_event_loop()
    broadcast_queue = asyncio.Queue()
    bootstrap_central_state()
    threading.Thread(target=kafka_consumer_loop, daemon=True).start()
    task = asyncio.create_task(broadcaster())
    yield
    task.cancel()


class ProductoCreate(BaseModel):
    nombre: str
    categoria_id: int
    laboratorio_id: int
    requiere_receta: bool = False
    precio_costo: float
    precio_venta: float
    inventario: int = 0


class ProductoUpdate(BaseModel):
    precio_venta: float | None = None
    inventario: int | None = None


def get_branch_config(branch):
    cfg = BRANCHES.get(branch)
    if cfg is None:
        raise HTTPException(status_code=404, detail=f"Sucursal desconocida: {branch}")
    return cfg


def get_branch_connection(branch):
    cfg = get_branch_config(branch)
    return mysql.connector.connect(
        host=cfg["host"],
        port=BRANCH_DB_PORT,
        user=BRANCH_DB_USER,
        password=BRANCH_DB_PASSWORD,
        database=cfg["database"],
    )


def translate_db_error(exc):
    if exc.errno == 1062:
        return "Ya existe un producto con ese SKU (raro — probá de nuevo)"
    if exc.errno in (1451, 1452):
        return "Categoría/laboratorio inválido, o el producto todavía tiene ventas asociadas"
    return str(exc)


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/lookups")
async def lookups():
    return LOOKUPS


@app.get("/api/infra/status")
async def infra_status():
    status = {}
    for target in CHAOS_TARGETS:
        try:
            status[target] = get_chaos_container(target).status
        except HTTPException:
            status[target] = "unknown"
    return status


@app.post("/api/infra/{target}/toggle")
async def infra_toggle(target: str):
    container = get_chaos_container(target)
    container.reload()
    if container.status == "running":
        container.stop(timeout=5)
        new_status = "exited"
    else:
        container.start()
        new_status = "running"
    return {"target": target, "status": new_status}


@app.post("/api/{branch}/medicamentos", status_code=201)
async def crear_medicamento(branch: str, producto: ProductoCreate):
    get_branch_config(branch)
    sku = f"MED-{int(time.time() * 1000)}"
    conn = get_branch_connection(branch)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO medicamentos
               (sku, nombre, categoria_id, laboratorio_id, requiere_receta,
                precio_costo, precio_venta, inventario)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (sku, producto.nombre, producto.categoria_id, producto.laboratorio_id,
             producto.requiere_receta, producto.precio_costo, producto.precio_venta,
             producto.inventario),
        )
        conn.commit()
        return {"status": "ok", "sku": sku, "id": cursor.lastrowid}
    except mysql.connector.Error as exc:
        raise HTTPException(status_code=400, detail=translate_db_error(exc))
    finally:
        conn.close()


@app.patch("/api/{branch}/medicamentos/{medicamento_id}")
async def actualizar_medicamento(branch: str, medicamento_id: int, cambios: ProductoUpdate):
    get_branch_config(branch)
    fields, values = [], []
    if cambios.precio_venta is not None:
        fields.append("precio_venta = %s")
        values.append(cambios.precio_venta)
    if cambios.inventario is not None:
        fields.append("inventario = %s")
        values.append(cambios.inventario)
    if not fields:
        raise HTTPException(status_code=400, detail="Nada que actualizar")
    values.append(medicamento_id)

    conn = get_branch_connection(branch)
    try:
        cursor = conn.cursor()
        cursor.execute(f"UPDATE medicamentos SET {', '.join(fields)} WHERE id = %s", values)
        conn.commit()
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        return {"status": "ok"}
    except mysql.connector.Error as exc:
        raise HTTPException(status_code=400, detail=translate_db_error(exc))
    finally:
        conn.close()


@app.delete("/api/{branch}/medicamentos/{medicamento_id}")
async def borrar_medicamento(branch: str, medicamento_id: int):
    get_branch_config(branch)
    conn = get_branch_connection(branch)
    try:
        cursor = conn.cursor()
        # detalle_venta no tiene ON DELETE CASCADE a propósito (ver
        # mysql-init/pos/01-schema.sql) — hay que borrarlo explícito antes,
        # igual que scripts/3-simular-cambios.sh, para que Debezium sí
        # capture esta baja en el binlog.
        cursor.execute("DELETE FROM detalle_venta WHERE medicamento_id = %s", (medicamento_id,))
        cursor.execute("DELETE FROM medicamentos WHERE id = %s", (medicamento_id,))
        conn.commit()
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        return {"status": "ok"}
    except mysql.connector.Error as exc:
        raise HTTPException(status_code=400, detail=translate_db_error(exc))
    finally:
        conn.close()


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    connections.add(websocket)
    with state_lock:
        panels_copy = {panel: dict(rows) for panel, rows in state.items()}
    await websocket.send_text(json.dumps({"type": "snapshot", "panels": panels_copy}))
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        connections.discard(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8090)
