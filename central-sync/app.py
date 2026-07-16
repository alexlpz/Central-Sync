"""
central-sync: consume los eventos de cambio de las 3 sucursales desde Kafka
y los aplica sobre la Base Central (mysql-central), tabla por tabla.

Es el "consumidor que escribe a la Base Central" que se menciona en el
README del lab como el paso que falta para ir de esta demo a producción.
Aquí lo resolvemos con una app simple y transparente (no un plugin JDBC
Sink) para que sea fácil de leer y depurar.

Flujo por cada mensaje:
  - se ignoran los tombstones (value=None, los usa Kafka para compactar)
  - op en (c, r, u) -> INSERT ... ON DUPLICATE KEY UPDATE (upsert)
  - op == d         -> DELETE usando (sucursal, id)
"""

import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timedelta

import mysql.connector
from confluent_kafka import Consumer, KafkaException, Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("central-sync")

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
CENTRAL_DB_HOST = os.environ["CENTRAL_DB_HOST"]
CENTRAL_DB_PORT = int(os.environ.get("CENTRAL_DB_PORT", "3306"))
CENTRAL_DB_USER = os.environ["CENTRAL_DB_USER"]
CENTRAL_DB_PASSWORD = os.environ["CENTRAL_DB_PASSWORD"]
CENTRAL_DB_NAME = os.environ["CENTRAL_DB_NAME"]

# Tópicos que produce cada conector: pos.sucursalNN.<db>.<tabla>
#
# Nota: se descubren los nombres de tópico explícitamente con
# list_topics() + este patrón, en vez de usar la suscripción por regex
# nativa de librdkafka (subscribe(["^..."])) — con el broker de Kafka
# 4.x que trae esta imagen, esa suscripción por regex nunca matcheaba
# ningún tópico (UNKNOWN_TOPIC_OR_PART indefinidamente) aunque los
# tópicos sí existían. Descubrirlos nosotros mismos es más verboso pero
# funciona igual en cualquier versión de broker.
TOPIC_PATTERN = re.compile(
    r"^pos\.sucursal\d+\..*\.(categorias|laboratorios|medicamentos|promociones|ventas|detalle_venta)$"
)
TOPIC_DISCOVERY_INTERVAL_S = 5

# Tópico donde se anuncia, best-effort, que un cambio ya quedó aplicado en la
# Base Central — lo consume live-dashboard para mostrar el panel de Central
# dirigido 100% por eventos (sin polling a mysql-central).
CENTRAL_APPLIED_TOPIC = os.environ.get("CENTRAL_APPLIED_TOPIC", "central.applied")

# Backoff para reintentar una escritura fallida en la Base Central (no
# confundir con el backoff de connect_central_db(), que es para reconectar).
RETRY_BACKOFF_INITIAL_S = 1
RETRY_BACKOFF_MAX_S = 30

# Heartbeat para el healthcheck de Docker: touch_heartbeat() actualiza este
# archivo, y el healthcheck (definido en docker-compose.yml) lo considera
# sano mientras su antigüedad no supere HEARTBEAT_MAX_AGE_S (esa variable
# la usa el healthcheck directamente, no este script).
HEARTBEAT_FILE = os.environ.get("HEARTBEAT_FILE", "/tmp/central-sync-heartbeat")

# Detección de crash-loop: ver check_crash_loop() más abajo.
RESTART_STATE_FILE = os.environ.get("RESTART_STATE_FILE", "/tmp/central-sync-restarts.json")
CRASH_LOOP_WINDOW_S = int(os.environ.get("CRASH_LOOP_WINDOW_S", "600"))
CRASH_LOOP_THRESHOLD = int(os.environ.get("CRASH_LOOP_THRESHOLD", "5"))

# Columnas por tabla, en el mismo orden que el esquema de la Base Central
# (sin incluir `sucursal`, que se antepone siempre). Se hardcodea en vez de
# inferirse del "schema" de Kafka Connect para mantener el sink simple.
TABLE_COLUMNS = {
    "categorias": ["id", "nombre"],
    "laboratorios": ["id", "nombre", "pais"],
    "medicamentos": [
        "id", "sku", "nombre", "categoria_id", "laboratorio_id",
        "requiere_receta", "precio_costo", "precio_venta", "inventario",
        "actualizado_en",
    ],
    "promociones": [
        "id", "medicamento_id", "descripcion", "descuento_pct",
        "fecha_inicio", "fecha_fin", "activa",
    ],
    "ventas": ["id", "fecha", "total", "metodo_pago"],
    "detalle_venta": ["id", "venta_id", "medicamento_id", "cantidad", "precio_unitario"],
}

EPOCH = date(1970, 1, 1)


def convert_value(column, value):
    """Debezium serializa fechas distinto según el tipo de columna en MySQL:
    DATE -> días desde epoch (int), DATETIME -> milisegundos desde epoch
    (int, sin timezone), TIMESTAMP -> string ISO-8601 (con timezone). Acá
    se normalizan de vuelta a algo que MySQL acepte directamente.
    """
    if value is None:
        return None
    if column in ("fecha_inicio", "fecha_fin"):
        return (EPOCH + timedelta(days=int(value))).isoformat()
    if column == "fecha":
        return (datetime(1970, 1, 1) + timedelta(milliseconds=int(value))).strftime("%Y-%m-%d %H:%M:%S")
    if column == "actualizado_en":
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
    return value


def connect_central_db():
    attempt = 0
    while True:
        attempt += 1
        try:
            conn = mysql.connector.connect(
                host=CENTRAL_DB_HOST,
                port=CENTRAL_DB_PORT,
                user=CENTRAL_DB_USER,
                password=CENTRAL_DB_PASSWORD,
                database=CENTRAL_DB_NAME,
            )
            log.info("Conectado a la Base Central (%s:%s/%s)", CENTRAL_DB_HOST, CENTRAL_DB_PORT, CENTRAL_DB_NAME)
            return conn
        except mysql.connector.Error as exc:
            log.warning("Base Central no disponible todavía (intento %d): %s", attempt, exc)
            # También se toca acá (no solo en el loop de reintento de
            # escritura que la llama): una reconexión puede por sí sola
            # tardar minutos si la Base Central está caída, y ese tiempo
            # no debe hacer que el healthcheck marque el contenedor como
            # unhealthy.
            touch_heartbeat()
            time.sleep(min(2 * attempt, 15))


def parse_topic(topic):
    # pos.sucursal01.pos_sucursal_01.medicamentos -> ("sucursal01", "medicamentos")
    parts = topic.split(".")
    sucursal = parts[1]
    tabla = parts[-1]
    return sucursal, tabla


def upsert(cursor, tabla, sucursal, after):
    columns = TABLE_COLUMNS[tabla]
    all_columns = ["sucursal"] + columns
    values = [sucursal] + [convert_value(c, after.get(c)) for c in columns]
    placeholders = ", ".join(["%s"] * len(all_columns))
    update_clause = ", ".join(f"{c}=VALUES({c})" for c in columns)
    sql = (
        f"INSERT INTO {tabla} ({', '.join(all_columns)}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )
    cursor.execute(sql, values)


def delete(cursor, tabla, sucursal, before):
    cursor.execute(f"DELETE FROM {tabla} WHERE sucursal=%s AND id=%s", (sucursal, before["id"]))


def publish_applied(producer, tabla, sucursal, op, row_id, after, source_ts_ms):
    # Best-effort a propósito: este anuncio es solo para que live-dashboard
    # pinte el panel de Central en vivo — si falla, no debe interrumpir el
    # flujo principal de sync, que sigue siendo la fuente de verdad.
    #
    # source_ts_ms es el ts_ms que el propio Debezium puso en el evento
    # original (el momento en que capturó el cambio del binlog) — viaja tal
    # cual hasta acá para que live-dashboard pueda calcular la latencia real
    # de este evento puntual (applied_ts_ms - source_ts_ms) sin tener que
    # adivinar a qué evento de sucursal corresponde cada anuncio.
    try:
        payload = {
            "tabla": tabla,
            "sucursal": sucursal,
            "op": op,
            "id": row_id,
            "after": after,
            "source_ts_ms": source_ts_ms,
            "applied_ts_ms": int(time.time() * 1000),
        }
        producer.produce(
            CENTRAL_APPLIED_TOPIC,
            key=f"{sucursal}:{row_id}".encode(),
            value=json.dumps(payload).encode(),
        )
        producer.poll(0)
    except Exception:
        log.exception("No se pudo publicar en %s (no afecta el sync)", CENTRAL_APPLIED_TOPIC)


def handle_message(conn, producer, topic, raw_value):
    sucursal, tabla = parse_topic(topic)
    if tabla not in TABLE_COLUMNS:
        return

    event = json.loads(raw_value)
    payload = event.get("payload")
    if payload is None:
        return

    op = payload["op"]
    cursor = conn.cursor()
    try:
        if op in ("c", "r", "u"):
            upsert(cursor, tabla, sucursal, payload["after"])
            row_id = payload["after"]["id"]
        elif op == "d":
            delete(cursor, tabla, sucursal, payload["before"])
            row_id = payload["before"]["id"]
        else:
            return
        conn.commit()
        log.info("[%s] %-14s op=%s id=%s", sucursal, tabla, op, row_id)
        publish_applied(producer, tabla, sucursal, op, row_id, payload.get("after"), payload.get("ts_ms"))
    except mysql.connector.Error:
        conn.rollback()
        raise
    finally:
        cursor.close()


def discover_topics(consumer):
    metadata = consumer.list_topics(timeout=10)
    return sorted(t for t in metadata.topics if TOPIC_PATTERN.match(t))


def touch_heartbeat():
    # Tolerante a fallos de escritura a propósito: un healthcheck que no se
    # pudo actualizar no debe tumbar el proceso que está monitoreando.
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(str(time.time()))
    except OSError:
        pass


def check_crash_loop():
    """Detecta si el proceso se está reiniciando en bucle.

    El archivo en RESTART_STATE_FILE sobrevive a los reinicios porque
    Docker, bajo `restart: unless-stopped`, reinicia el mismo contenedor
    (no crea uno nuevo) — su filesystem en /tmp persiste entre esos
    reinicios del proceso. Esto es solo una señal en el log (el
    "gancho"): conectarlo a un canal de notificación real (email, Slack,
    etc.) es una decisión futura que depende de la herramienta de
    monitoreo que se use en producción.
    """
    now = time.time()

    try:
        with open(RESTART_STATE_FILE) as f:
            timestamps = json.load(f)
        if not isinstance(timestamps, list):
            timestamps = []
    except (OSError, ValueError):
        timestamps = []

    timestamps = [t for t in timestamps if isinstance(t, (int, float)) and now - t < CRASH_LOOP_WINDOW_S]
    timestamps.append(now)

    try:
        with open(RESTART_STATE_FILE, "w") as f:
            json.dump(timestamps, f)
    except OSError:
        pass

    if len(timestamps) >= CRASH_LOOP_THRESHOLD:
        log.critical(
            "ALERTA: posible crash-loop — %d arranques en los últimos %ds",
            len(timestamps), CRASH_LOOP_WINDOW_S,
        )


def main():
    conn = connect_central_db()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "central-sync",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    subscribed = []
    last_discovery = 0.0

    try:
        while True:
            try:
                now = time.monotonic()
                if now - last_discovery >= TOPIC_DISCOVERY_INTERVAL_S:
                    last_discovery = now
                    try:
                        current = discover_topics(consumer)
                    except KafkaException as exc:
                        log.warning("No se pudo listar tópicos todavía: %s", exc)
                        current = subscribed
                    if current and current != subscribed:
                        consumer.subscribe(current)
                        log.info("Suscrito a %d tópicos: %s", len(current), ", ".join(current))
                        subscribed = current

                if not subscribed:
                    time.sleep(1)
                    continue

                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().fatal():
                        raise KafkaException(msg.error())
                    log.warning("Error no fatal del consumidor: %s", msg.error())
                    continue

                raw_value = msg.value()
                if raw_value is None:
                    # tombstone: solo sirve para compactar el log, no hay nada
                    # que aplicar. Antes el offset lo confirmaba el auto-commit;
                    # ahora que está apagado hay que confirmarlo a mano.
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                attempt = 0
                while True:
                    try:
                        handle_message(conn, producer, msg.topic(), raw_value)
                        break
                    except mysql.connector.Error as exc:
                        attempt += 1
                        wait_s = min(
                            RETRY_BACKOFF_INITIAL_S * (2 ** (attempt - 1)),
                            RETRY_BACKOFF_MAX_S,
                        )
                        log.warning(
                            "Error escribiendo en la Base Central (topic=%s), reintento %d en %ds: %s",
                            msg.topic(), attempt, wait_s, exc,
                        )
                        # No se llama a consumer.poll() de nuevo mientras se
                        # reintenta: si la Base Central está caída, de todas
                        # formas no se podría escribir nada de ninguna sucursal,
                        # así que bloquear el loop acá no pierde paralelismo
                        # real, y evita que un commit posterior "salte por
                        # encima" de este mensaje fallido.
                        #
                        # Se toca el heartbeat antes de dormir: un reintento
                        # legítimo y prolongado contra la Base Central caída
                        # (puede durar minutos) no debe hacer que el
                        # healthcheck marque el contenedor como unhealthy.
                        touch_heartbeat()
                        time.sleep(wait_s)
                        if not conn.is_connected():
                            conn = connect_central_db()
                    except Exception:
                        # Asimetría a propósito: a diferencia de
                        # mysql.connector.Error (falla de conectividad, se
                        # resuelve sola con reintentar), esto es un problema de
                        # datos/código (bug de parseo, payload con forma
                        # inesperada, etc.) que reintentar para siempre no
                        # arregla, y bloquearía el pipeline completo por un
                        # mensaje que nunca va a poder procesarse. Por eso acá
                        # se loguea y se avanza (confirmando el offset más
                        # abajo) en vez de reintentar. Manejo de "mensajes
                        # envenenados" / dead-letter queda fuera de alcance.
                        log.exception("Error procesando mensaje de topic=%s, se omite", msg.topic())
                        break

                consumer.commit(message=msg, asynchronous=False)
            except Exception:
                # Red de seguridad para lo que se escape de los manejos de
                # arriba (que cubren específicamente mysql.connector.Error y
                # errores de contenido del mensaje): registra el traceback
                # completo y sigue el loop en vez de tumbar el proceso. Un
                # `except Exception` no captura KeyboardInterrupt ni
                # SystemExit (no heredan de Exception), así que esos siguen
                # terminando el proceso como corresponde.
                log.exception("Error no anticipado en el loop principal, se continúa")
                time.sleep(2)
                continue
            finally:
                # Al final de cada vuelta, se haya manejado un mensaje o
                # no (o incluso si esta vuelta terminó en el except de
                # arriba): así el healthcheck ve que el proceso sigue vivo.
                touch_heartbeat()
    finally:
        consumer.close()
        producer.flush(5)


if __name__ == "__main__":
    check_crash_loop()
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
