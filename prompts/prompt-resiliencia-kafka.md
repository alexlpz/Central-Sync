# Prompt: resiliencia ante caída de Kafka (persistencia + auto-reinicio de conectores)

Pega este prompt completo en tu asistente de código de VS Code, dentro del
proyecto `debezium-lab`, con `docker-compose.yml` y la carpeta `central-sync/`
visibles en el contexto (esta última como referencia de estilo, no se
modifica).

Son dos cambios independientes pero relacionados — impleméntalos juntos.

---

## Contexto

`debezium-lab` es un laboratorio de Debezium/CDC con 3 sucursales MySQL,
Kafka (`quay.io/debezium/kafka:3.5`, un solo broker en modo KRaft) y Kafka
Connect (`quay.io/debezium/connect:3.5`) con un conector Debezium por
sucursal. Ya existe `central-sync/`, una app Python que consume los eventos
de Kafka y los aplica sobre una Base Central — úsala como referencia de
estilo (comentarios explicativos en español, logging consistente,
Dockerfile simple basado en `python:3.12-slim`).

## Parte A — Persistencia de datos de Kafka

### Problema

El servicio `kafka` en `docker-compose.yml` no tiene ningún volumen
declarado. La imagen `quay.io/debezium/kafka` guarda los datos de los
tópicos en `/kafka/data` y sus logs de aplicación en `/kafka/logs` dentro
del contenedor. Sin un volumen nombrado, un `docker compose down` (incluso
sin `-v`) o cualquier recreación del contenedor borra todo el historial de
Kafka — a diferencia de los 4 servicios MySQL, que sí persisten vía
volúmenes nombrados (`mysql-sucursal01-data`, etc.).

### Cambio requerido

En `docker-compose.yml`:

1. Agrega dos volúmenes nombrados al servicio `kafka`:
   ```yaml
   volumes:
     - kafka-data:/kafka/data
     - kafka-logs:/kafka/logs
   ```
2. Declara `kafka-data:` y `kafka-logs:` en la sección `volumes:` de nivel
   raíz del archivo, junto a los volúmenes de MySQL que ya existen.
3. No cambies nada más de la configuración del servicio `kafka` (imagen,
   variables de entorno de KRaft, puertos).

### Prueba manual

1. Con el laboratorio corriendo, genera cambios:
   `./scripts/3-simular-cambios.sh sucursal01`, y confírmalos con
   `./scripts/5-ver-central.sh`.
2. `docker compose down` (sin `-v`).
3. `docker compose up -d`.
4. Confirma en Kafdrop (http://localhost:9000) que los tópicos y su
   historial siguen ahí — no se perdieron.

## Parte B — Auto-reinicio de conectores (`connector-watchdog`)

### Problema

Kafka Connect no reinicia automáticamente un conector o tarea que quedó en
estado `FAILED` (por ejemplo, tras una caída prolongada de Kafka que agotó
el timeout del productor, o cualquier otro error). Se queda así de forma
indefinida hasta que alguien lo reinicie manualmente vía la API REST
(`POST /connectors/{name}/restart`), incluso después de que la causa
original ya se resolvió.

### Cambio requerido

Crea un nuevo servicio, `connector-watchdog`, como una app Python
standalone (mismo patrón que `central-sync/`: `Dockerfile` +
`requirements.txt` + `app.py`), con esta carpeta:

```
connector-watchdog/
├── Dockerfile
├── requirements.txt      # solo necesita "requests"
└── app.py
```

**Lógica de `app.py`:**

1. Cada `CHECK_INTERVAL_S` segundos (env var, default `15`):
   - `GET {CONNECT_URL}/connectors` para listar los conectores registrados
     (`CONNECT_URL` viene de env var, ej. `http://connect:8083`).
   - Para cada conector, `GET /connectors/{name}/status` y revisa:
     - `connector.state` (el estado del propio conector, no de sus tareas).
     - el `state` de cada elemento en `tasks[]`.
2. Si `connector.state == "FAILED"` o cualquier tarea tiene
   `state == "FAILED"`:
   - Revisa el backoff en memoria para ese conector (ver abajo). Si todavía
     está en cooldown, no hagas nada este ciclo (solo loguea que sigue en
     FAILED, esperando el próximo intento permitido).
   - Si ya se puede reintentar: llama
     `POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true`.
     Si esa llamada responde 404 (versión de Kafka Connect sin ese
     parámetro), haz fallback: `POST /connectors/{name}/restart` para el
     conector, y `POST /connectors/{name}/tasks/{taskId}/restart` para
     cada tarea individual en FAILED.
   - Loguea el intento: nombre del conector, intento número N, y cuándo
     será el próximo intento si este también falla.
3. Backoff exponencial **por conector** (mantenlo en un dict en memoria,
   no hace falta persistirlo): arranca en 30 segundos, se duplica en cada
   intento fallido consecutivo, tope de 15 minutos. Se reinicia a 0 en
   cuanto ese conector vuelve a verse `RUNNING` (tanto a nivel conector
   como en todas sus tareas).
4. Después de 5 intentos de reinicio consecutivos sin éxito para un mismo
   conector, agrega una línea de log explícita en nivel `WARNING` (algo
   como: *"el conector {name} lleva 5 reinicios fallidos seguidos, ya no
   parece un problema transitorio — revisar manualmente"*), pero sigue
   reintentando igual (al ritmo del tope de 15 min), no te des por vencido
   del todo.
5. Maneja errores de red hacia `CONNECT_URL` (Kafka Connect también puede
   estar temporalmente inalcanzable) sin caerse: loguea y reintenta en el
   siguiente ciclo, igual que hace `central-sync` con sus propios errores
   transitorios.
6. Logging: mismo formato que `central-sync/app.py`
   (`logging.basicConfig` con timestamp, nivel, mensaje), comentarios
   explicativos en español.

**En `docker-compose.yml`**, agrega el servicio:

```yaml
connector-watchdog:
  build: ./connector-watchdog
  container_name: dbz-lab-connector-watchdog
  restart: unless-stopped
  depends_on:
    - connect
  environment:
    CONNECT_URL: http://connect:8083
    CHECK_INTERVAL_S: "15"
```

### Prueba manual

1. Con todo corriendo y los 3 conectores en `RUNNING`, fuerza una falla
   real cambiando temporalmente la contraseña de un conector a una
   incorrecta:
   ```bash
   curl -X PUT -H "Content-Type: application/json" \
     --data '{"connector.class":"io.debezium.connector.mysql.MySqlConnector", ... "database.password":"password-incorrecta", ...}' \
     http://localhost:8083/connectors/pos-sucursal-01-connector/config
   ```
   (usa el mismo contenido que `connector/register-sucursal01-connector.json`,
   solo con la contraseña incorrecta — recuerda que este endpoint espera el
   objeto `config` directamente, no el `{name, config}` completo).
2. En `docker compose logs -f connector-watchdog` deberías ver que detecta
   el estado `FAILED` y reintenta con backoff creciente (30s, 1m, 2m...).
3. Restaura la contraseña correcta con el mismo `PUT` y confirma que, en el
   siguiente ciclo del watchdog, el conector vuelve solo a `RUNNING` — sin
   que tengas que reiniciarlo tú a mano.

## Al terminar

Actualiza `README.md`:
- Agrega `connector-watchdog` al diagrama de arquitectura y a la lista de
  servicios/carpetas del proyecto.
- En la sección "De aquí a producción: qué falta", quita o ajusta la
  mención de que Kafka Connect no se auto-recupera de fallas (ya no aplica
  tal cual), y anota en su lugar que el laboratorio ahora corre con un solo
  broker de Kafka sin replicación — que en producción se resuelve con un
  clúster real (3+ brokers, factor de replicación ≥ 3), no con más código.
- Documenta brevemente el nuevo servicio `connector-watchdog` (qué hace,
  cómo ver sus logs) en la sección correspondiente.
