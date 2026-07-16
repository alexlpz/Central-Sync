# Prompt: auto-recuperación de conectores atorados en UNASSIGNED

Pega este prompt completo en tu asistente de código de VS Code, dentro del
proyecto `debezium-lab`, con `connector-watchdog/app.py` visible en el
contexto (ya existe — este cambio lo extiende, no lo reescribe).

## Contexto

`connector-watchdog` es el servicio que ya monitorea el estado de los 3
conectores Debezium vía la API REST de Kafka Connect y reinicia
automáticamente los que caen en `FAILED`, con backoff exponencial por
conector. Ese mecanismo ya funciona bien y **no debe modificarse**.

Al probar el watchdog apagando Kafka por completo (no el disparador que se
usa para la demo, que es la caída del MySQL de una sucursal), se encontró
que los 3 conectores terminaron en estado `UNASSIGNED` al volver Kafka — un
estado que el watchdog actual no atiende — y hubo que borrarlos y
volver a registrarlos a mano. `UNASSIGNED` es un estado de coordinación
interna de Kafka Connect (falta de asignación a un worker), distinto de
`FAILED`; es normal que dure una fracción de segundo durante un rebalanceo,
pero si se queda atorado no hay una API de arreglo limpia — el remedio
conocido es recrear el conector, que es justo lo que hay que automatizar.

## Cambio requerido

Extiende `connector-watchdog/app.py` con una segunda vía de atención,
independiente de la lógica de `FAILED` ya existente, con dos niveles de
escalamiento **por conector**:

### Nivel 1 — reinicio suave

Si un conector lleva `UNASSIGNED_GRACE_S` segundos (env var, default `60`)
**continuos** con `connector.state == "UNASSIGNED"` o alguna tarea con
`state == "UNASSIGNED"` (y ninguna tarea en `FAILED` — ese caso ya lo cubre
la lógica existente), dispara:

```
POST /connectors/{name}/restart?includeTasks=true&onlyFailed=false
```

Esto reinicia el conector y TODAS sus tareas sin importar su estado (a
diferencia del reinicio para `FAILED`, que usa `onlyFailed=true`). Hazlo
una sola vez por episodio (no en cada ciclo mientras siga en `UNASSIGNED`).

### Nivel 2 — recreación forzada

Si sigue `UNASSIGNED` (continuo, sin haber pasado por `RUNNING`) durante
`UNASSIGNED_HARD_RESET_AFTER_S` segundos (env var, default `300`) desde que
inició el episodio — es decir, el reinicio suave del Nivel 1 no lo
resolvió — el watchdog debe:

1. `DELETE /connectors/{name}`
2. `POST /connectors` con el JSON de configuración original de ese
   conector (mismo formato `{"name": ..., "config": {...}}` que ya usan los
   archivos en `connector/register-sucursalNN-connector.json`).

Para esto, al arrancar (`main()`), carga y cachea en memoria el contenido
de todos los archivos `connector/register-sucursal*-connector.json`
disponibles en el directorio montado (ver más abajo), indexados por el
campo `name` de cada uno, para poder recuperarlos por nombre de conector
cuando haga falta.

Aplica un cooldown de `HARD_RESET_COOLDOWN_S` segundos (env var, default
`300`) entre dos recreaciones forzadas consecutivas del mismo conector,
para no entrar en un ciclo de recrear-fallar-recrear si el problema de
fondo persiste.

**Logging de esta acción**: usa `log.error(...)` (no `log.warning`, para
que resalte) con un mensaje explícito que incluya la palabra
`RECUPERACIÓN FORZADA`, el nombre del conector, y cuánto tiempo llevaba en
`UNASSIGNED`.

### Reinicio del contador

El contador de "tiempo continuo en UNASSIGNED" para un conector se reinicia
(vuelve a `None`) en cuanto ese conector se observa con
`connector.state == "RUNNING"` y todas sus tareas también `RUNNING`. El
intento de "Nivel 1 ya hecho" (bandera para no repetir el soft-restart en
cada ciclo) también se reinicia junto con el contador.

Esta lógica de UNASSIGNED es **independiente** del backoff que ya existe
para `FAILED` — mantenlos como dos caminos separados con su propio estado
en memoria por conector (puedes usar el mismo dict de estado por conector
que ya exista, agregando las claves nuevas que necesites, ej.
`unassigned_since`, `soft_restart_done`, `last_hard_reset`).

## Cambios en `docker-compose.yml`

Monta la carpeta `connector/` (donde están los JSON de configuración) como
volumen de solo lectura dentro de `connector-watchdog`:

```yaml
connector-watchdog:
  build: ./connector-watchdog
  ...
  volumes:
    - ./connector:/connector-configs:ro
  environment:
    CONNECT_URL: http://connect:8083
    CHECK_INTERVAL_S: "15"
    UNASSIGNED_GRACE_S: "60"
    UNASSIGNED_HARD_RESET_AFTER_S: "300"
    HARD_RESET_COOLDOWN_S: "300"
```

Ajusta el `app.py` para leer los JSON desde `/connector-configs/*.json`
(usa una env var `CONNECTOR_CONFIGS_DIR` con ese default, no lo
hardcodees).

## No toques

- La lógica existente de detección y reintento de `FAILED` (backoff
  exponencial, el log de advertencia tras 5 intentos).
- `central-sync/`.
- El estilo del archivo: comentarios explicativos en español, mismo
  formato de logging que ya usa `connector-watchdog/app.py`.

## Criterio de aceptación / cómo probarlo

1. Con el laboratorio corriendo y los 3 conectores en `RUNNING`:
   `docker compose stop kafka`.
2. Espera un par de minutos y vuelve a levantarlo: `docker compose start kafka`.
3. En `docker compose logs -f connector-watchdog` deberías ver, en orden:
   detección de `UNASSIGNED`, el intento de reinicio suave a los ~60s, y
   —si no fue suficiente— la recreación forzada (con el log
   `RECUPERACIÓN FORZADA`) a los ~5 minutos.
4. Confirma con `curl http://localhost:8083/connectors/{name}/status` (o
   revisando los 3) que terminan en `RUNNING` sin que hayas tenido que
   borrarlos tú a mano.
5. **No debe haber regresión** en los dos flujos ya probados antes: repite
   la prueba de la Base Central caída (`docker compose stop mysql-central`)
   y la de la contraseña incorrecta de un conector — ambas deben seguir
   comportándose exactamente igual que antes de este cambio.

## Al terminar

Actualiza `README.md`: agrega una breve mención, en la sección de
`connector-watchdog`, de que también detecta y recupera conectores
atorados en `UNASSIGNED` (no solo `FAILED`), con reinicio suave primero y
recreación forzada como último recurso, y por qué existe ese segundo
mecanismo (aclara que es para el caso de contingencia de Kafka caído, no
para el disparador usado en la demo).
