"""
connector-watchdog: vigila el estado de los conectores de Kafka Connect y
los recupera automáticamente cuando quedan atorados, sin intervención
manual. Atiende dos problemas independientes:

  - FAILED: el conector o alguna de sus tareas falló (por ejemplo, tras
    una caída prolongada de Kafka que agota el timeout del productor).
    Se reintenta indefinidamente con backoff exponencial por conector.
  - UNASSIGNED: estado de coordinación interna de Kafka Connect (falta
    de asignación a un worker). Es normal que dure una fracción de
    segundo durante un rebalanceo, pero si se queda atorado (se observó
    esto al apagar Kafka por completo, no solo el MySQL de una
    sucursal) no hay una API de arreglo limpia — se escala primero a un
    reinicio suave y, si eso no alcanza, a recrear el conector desde
    cero con su configuración original.

En ambos casos, Kafka Connect no se recupera solo — se queda atorado
indefinidamente hasta que alguien llame a su API REST a mano, incluso
después de que la causa original ya se resolvió. Esta app automatiza
esa recuperación.
"""

import glob
import json
import logging
import os
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("connector-watchdog")

CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083")
CHECK_INTERVAL_S = int(os.environ.get("CHECK_INTERVAL_S", "15"))
REQUEST_TIMEOUT_S = 10

# Backoff por conector para FAILED: arranca en 30s, se duplica en cada
# intento fallido consecutivo, tope de 15 minutos.
RETRY_BACKOFF_INITIAL_S = 30
RETRY_BACKOFF_MAX_S = 15 * 60

# A partir de este número de intentos consecutivos sin éxito, se agrega
# una advertencia extra en el log (pero se sigue reintentando igual).
WARN_AFTER_ATTEMPTS = 5

# Manejo de UNASSIGNED atorado: reinicio suave tras UNASSIGNED_GRACE_S
# segundos continuos, recreación forzada tras
# UNASSIGNED_HARD_RESET_AFTER_S segundos continuos si el reinicio suave
# no alcanzó, con un cooldown entre recreaciones forzadas consecutivas
# del mismo conector.
UNASSIGNED_GRACE_S = int(os.environ.get("UNASSIGNED_GRACE_S", "60"))
UNASSIGNED_HARD_RESET_AFTER_S = int(os.environ.get("UNASSIGNED_HARD_RESET_AFTER_S", "300"))
HARD_RESET_COOLDOWN_S = int(os.environ.get("HARD_RESET_COOLDOWN_S", "300"))

# Directorio (montado como volumen de solo lectura) con los JSON de
# configuración originales de cada conector, usados para recrearlos en
# la recuperación forzada.
CONNECTOR_CONFIGS_DIR = os.environ.get("CONNECTOR_CONFIGS_DIR", "/connector-configs")


def get_connectors():
    resp = requests.get(f"{CONNECT_URL}/connectors", timeout=REQUEST_TIMEOUT_S)
    resp.raise_for_status()
    return resp.json()


def get_status(name):
    resp = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=REQUEST_TIMEOUT_S)
    resp.raise_for_status()
    return resp.json()


def failed_task_ids(status):
    return [t["id"] for t in status.get("tasks", []) if t.get("state") == "FAILED"]


def is_failed(status):
    return status.get("connector", {}).get("state") == "FAILED" or bool(failed_task_ids(status))


def is_unassigned(status):
    return status.get("connector", {}).get("state") == "UNASSIGNED" or any(
        t.get("state") == "UNASSIGNED" for t in status.get("tasks", [])
    )


def is_fully_running(status):
    if status.get("connector", {}).get("state") != "RUNNING":
        return False
    return all(t.get("state") == "RUNNING" for t in status.get("tasks", []))


def load_connector_configs():
    # Carga y cachea en memoria el JSON de registro original de cada
    # conector (indexado por su "name"), para poder recrearlo entero en
    # una recuperación forzada sin depender de que Kafka Connect todavía
    # recuerde su configuración.
    configs = {}
    pattern = os.path.join(CONNECTOR_CONFIGS_DIR, "register-sucursal*-connector.json")
    for path in sorted(glob.glob(pattern)):
        try:
            with open(path) as f:
                data = json.load(f)
            configs[data["name"]] = data
        except (OSError, ValueError, KeyError) as exc:
            log.warning("No se pudo cargar la configuración de conector desde %s: %s", path, exc)
    return configs


def restart_connector(name, failed_tasks):
    # Camino preferido: un solo llamado que reinicia el conector y (si
    # hace falta) sus tareas en FAILED de una sola vez.
    resp = requests.post(
        f"{CONNECT_URL}/connectors/{name}/restart",
        params={"includeTasks": "true", "onlyFailed": "true"},
        timeout=REQUEST_TIMEOUT_S,
    )
    if resp.status_code == 404:
        # Fallback para versiones de Kafka Connect sin esos query params:
        # reiniciar el conector y cada tarea en FAILED por separado.
        requests.post(f"{CONNECT_URL}/connectors/{name}/restart", timeout=REQUEST_TIMEOUT_S)
        for task_id in failed_tasks:
            requests.post(
                f"{CONNECT_URL}/connectors/{name}/tasks/{task_id}/restart",
                timeout=REQUEST_TIMEOUT_S,
            )
    else:
        resp.raise_for_status()


def soft_restart_connector(name, status):
    # A diferencia de restart_connector() (que solo toca tareas FAILED),
    # acá se reinicia el conector y TODAS sus tareas sin importar su
    # estado — es el remedio para UNASSIGNED, que no es un estado FAILED.
    resp = requests.post(
        f"{CONNECT_URL}/connectors/{name}/restart",
        params={"includeTasks": "true", "onlyFailed": "false"},
        timeout=REQUEST_TIMEOUT_S,
    )
    if resp.status_code == 404:
        requests.post(f"{CONNECT_URL}/connectors/{name}/restart", timeout=REQUEST_TIMEOUT_S)
        for t in status.get("tasks", []):
            requests.post(
                f"{CONNECT_URL}/connectors/{name}/tasks/{t['id']}/restart",
                timeout=REQUEST_TIMEOUT_S,
            )
    else:
        resp.raise_for_status()


def hard_reset_connector(name, config):
    # Último recurso: no hay una API de "arreglo" limpia para un
    # conector atorado en UNASSIGNED, así que se borra y se recrea desde
    # cero con su configuración original. El DELETE es best-effort (si
    # ya no existe, no es un error); lo que importa es que el POST que
    # lo recrea tenga éxito.
    requests.delete(f"{CONNECT_URL}/connectors/{name}", timeout=REQUEST_TIMEOUT_S)
    resp = requests.post(f"{CONNECT_URL}/connectors", json=config, timeout=REQUEST_TIMEOUT_S)
    resp.raise_for_status()


def handle_failed(name, retry_state, status, now):
    state = retry_state.setdefault(name, {"consecutive_failures": 0, "next_retry_at": 0.0})
    if now < state["next_retry_at"]:
        log.info(
            "%s sigue en FAILED, esperando el próximo intento permitido (en %ds)",
            name, int(state["next_retry_at"] - now),
        )
        return

    state["consecutive_failures"] += 1
    attempt = state["consecutive_failures"]
    wait_s = min(RETRY_BACKOFF_INITIAL_S * (2 ** (attempt - 1)), RETRY_BACKOFF_MAX_S)
    state["next_retry_at"] = now + wait_s

    try:
        restart_connector(name, failed_task_ids(status))
        log.warning(
            "%s estaba FAILED, disparado reinicio (intento %d; próximo intento en %ds si vuelve a fallar)",
            name, attempt, wait_s,
        )
    except requests.RequestException as exc:
        log.warning("Fallo al intentar reiniciar %s (intento %d): %s", name, attempt, exc)

    if attempt >= WARN_AFTER_ATTEMPTS:
        log.warning(
            "El conector %s lleva %d reinicios fallidos seguidos, ya no parece un "
            "problema transitorio — revisar manualmente",
            name, attempt,
        )


def handle_unassigned(name, unassigned_state, connector_configs, status, now):
    state = unassigned_state.setdefault(
        name, {"since": None, "soft_restart_done": False, "last_hard_reset": None},
    )

    if state["since"] is None:
        state["since"] = now
        state["soft_restart_done"] = False
        log.info("%s entró en estado UNASSIGNED", name)
        return

    elapsed = now - state["since"]

    if elapsed >= UNASSIGNED_HARD_RESET_AFTER_S:
        if state["last_hard_reset"] is not None and (now - state["last_hard_reset"]) < HARD_RESET_COOLDOWN_S:
            log.info(
                "%s sigue UNASSIGNED hace %ds, recreación forzada en cooldown (%ds restantes)",
                name, int(elapsed), int(HARD_RESET_COOLDOWN_S - (now - state["last_hard_reset"])),
            )
            return

        config = connector_configs.get(name)
        if config is None:
            log.error(
                "%s lleva %ds en UNASSIGNED y necesita RECUPERACIÓN FORZADA, pero no hay "
                "configuración cacheada para recrearlo (revisa %s)",
                name, int(elapsed), CONNECTOR_CONFIGS_DIR,
            )
            return

        try:
            hard_reset_connector(name, config)
            log.error(
                "RECUPERACIÓN FORZADA: %s llevaba %ds en UNASSIGNED, se borró y se recreó desde su configuración original",
                name, int(elapsed),
            )
        except requests.RequestException as exc:
            log.error("RECUPERACIÓN FORZADA de %s falló al recrear el conector: %s", name, exc)
        state["last_hard_reset"] = now
        return

    if elapsed >= UNASSIGNED_GRACE_S and not state["soft_restart_done"]:
        try:
            soft_restart_connector(name, status)
            log.warning(
                "%s lleva %ds en UNASSIGNED, disparado reinicio suave (conector + todas sus tareas)",
                name, int(elapsed),
            )
        except requests.RequestException as exc:
            log.warning("Fallo al intentar el reinicio suave de %s: %s", name, exc)
        state["soft_restart_done"] = True


def check_connector(name, retry_state, unassigned_state, connector_configs, now):
    try:
        status = get_status(name)
    except requests.RequestException as exc:
        log.warning("No se pudo consultar el estado de %s: %s", name, exc)
        return

    if is_fully_running(status):
        if name in retry_state:
            log.info("%s volvió a RUNNING, se reinicia su backoff", name)
            del retry_state[name]
        u = unassigned_state.get(name)
        if u and u["since"] is not None:
            log.info("%s volvió a RUNNING, se reinicia su seguimiento de UNASSIGNED", name)
            u["since"] = None
            u["soft_restart_done"] = False
        return

    if is_failed(status):
        handle_failed(name, retry_state, status, now)
        return

    if is_unassigned(status):
        handle_unassigned(name, unassigned_state, connector_configs, status, now)
        return

    # Ni FAILED, ni UNASSIGNED, ni completamente RUNNING (ej. PAUSED, o
    # arrancando todavía): no es nuestro problema, no tocarlo.


def main():
    log.info("connector-watchdog arrancando: vigilando %s cada %ds", CONNECT_URL, CHECK_INTERVAL_S)
    connector_configs = load_connector_configs()
    log.info(
        "Configuraciones cacheadas para recuperación forzada: %s",
        ", ".join(sorted(connector_configs)) or "(ninguna encontrada)",
    )

    # Estado en memoria por conector; si el watchdog se reinicia, arranca
    # de cero (no hace falta persistirlo, es solo un ritmo de reintento).
    retry_state = {}
    unassigned_state = {}

    while True:
        now = time.monotonic()
        try:
            names = get_connectors()
        except requests.RequestException as exc:
            log.warning("No se pudo listar conectores en %s: %s", CONNECT_URL, exc)
            names = None

        if names is not None:
            for name in names:
                check_connector(name, retry_state, unassigned_state, connector_configs, now)

        time.sleep(CHECK_INTERVAL_S)


if __name__ == "__main__":
    main()
