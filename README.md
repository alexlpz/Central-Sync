# Laboratorio: Debezium centralizando 3 sucursales de farmacia

Este laboratorio monta, de forma mínima pero real, el mecanismo que se
propuso para centralizar los MySQL de los puntos de venta: **3 sucursales**
con su propio MySQL (binlog configurado como Debezium lo requiere), Kafka
Connect con el conector MySQL de Debezium instalado una vez por sucursal, y
una **app propia** que consume esos eventos y mantiene sincronizada una
**Base Central**, sin que nadie haya tenido que hacer polling a ninguna
base de sucursal.

No sustituye una prueba de carga real, pero es suficiente para:
- Ver instalado y configurado un Kafka Connect + conector Debezium de punta a punta, por triplicado (una sucursal por conector).
- Entender qué configuración exige Debezium en cada MySQL origen (binlog, usuario, privilegios).
- Ver la forma real de un evento de cambio (`before` / `after` / metadata) sobre un esquema de POS con varias tablas relacionadas.
- Ver un consumidor real aplicando esos eventos en una base destino (upsert/delete), que es la pieza que en el laboratorio anterior faltaba.
- Editar las bases de las sucursales desde DBeaver y ver el efecto reflejado en la Base Central sin correr ningún script.

## Arquitectura

```
mysql-sucursal-01 ─┐                            ▲
mysql-sucursal-02 ─┼─> connect (3 conectores) ───┤ vigilado por connector-watchdog
mysql-sucursal-03 ─┘        │                    ▼
                             └─> kafka ─┬─> kafdrop (UI, http://localhost:9000)
                                        └─> central-sync (app Python) ─> mysql-central
```

- **3 MySQL independientes** (`mysql-sucursal-01/02/03`), mismo esquema,
  distinta data — simulan 3 sucursales físicas reales, cada una con su
  propio binlog, su propio `database.server.id` y su propio conector.
- **`mysql-central`**: espejo consolidado de las 3, poblado **únicamente**
  por `central-sync`. No se edita a mano.
- **`central-sync`**: app Python que consume los tópicos de las 3
  sucursales y aplica los cambios en la Base Central (INSERT/UPDATE →
  upsert, DELETE → delete). Código en `central-sync/app.py`.
- **`connector-watchdog`**: app Python que vigila el estado de los 3
  conectores en Kafka Connect y recupera automáticamente los que quedan
  en `FAILED` (backoff exponencial) o atorados en `UNASSIGNED` (reinicio
  suave y, como último recurso, recreación forzada), ya que Kafka
  Connect no hace esto solo. Código en `connector-watchdog/app.py`.

## Requisitos

- Docker y Docker Compose (`docker compose version` ≥ v2).
- Conexión a internet para descargar las imágenes la primera vez.
- Puertos libres en tu máquina: `3306`, `3307`, `3308`, `3309`, `8083`, `9000`, `9092`.
- (Opcional) [DBeaver](https://dbeaver.io/) u otro cliente MySQL, para ver/editar las bases de forma visual.

## Qué incluye

```
debezium-lab/
├── docker-compose.yml
├── mysql-init/
│   ├── pos/
│   │   ├── 01-schema.sql           # DDL común a las 3 sucursales + usuario debezium
│   │   ├── seed-sucursal01.sql     # data semilla propia de cada sucursal
│   │   ├── seed-sucursal02.sql
│   │   └── seed-sucursal03.sql
│   └── central/
│       └── 01-schema.sql           # mismas tablas + columna `sucursal`, sin FKs
├── connector/
│   ├── register-sucursal01-connector.json
│   ├── register-sucursal02-connector.json
│   └── register-sucursal03-connector.json
├── central-sync/                   # app que sincroniza Kafka -> Base Central
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
├── connector-watchdog/              # app que recupera conectores en FAILED o UNASSIGNED
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
└── scripts/
    ├── 0-verificar-mysql.sh        # confirma binlog/privilegios (las 3, o una)
    ├── 1-registrar-conector.sh     # da de alta los 3 conectores en Kafka Connect
    ├── 2-ver-eventos.sh            # eventos en vivo de una sucursal/tabla (crudo)
    ├── 3-simular-cambios.sh        # simula el ciclo de vida de un producto
    ├── 4-verificar-cambio.sh       # eventos resumidos y legibles
    ├── 5-ver-central.sh            # consulta la Base Central desde terminal
    └── lib/formato_eventos.py      # filtro usado por 4-verificar-cambio.sh
```

## Esquema de cada sucursal (POS)

`categorias`, `laboratorios`, `medicamentos` (con `precio_costo` y
`precio_venta`, para poder ver margen), `promociones` (descuentos con
vigencia), `ventas` y `detalle_venta` (transacciones reales de caja). Las
3 sucursales comparten el mismo DDL (`mysql-init/pos/01-schema.sql`) pero
cada una arranca con un subconjunto distinto de medicamentos, precios,
promociones y ventas de ejemplo (`seed-sucursalNN.sql`) — para que se note
que son sucursales de verdad y no copias idénticas.

## 1. Levantar el laboratorio

```bash
cd debezium-lab
docker compose up -d --build
docker compose ps
```

Espera ~30-60 segundos a que las 4 bases MySQL terminen de inicializar.

## 2. Verificar que las sucursales quedaron listas para CDC

```bash
./scripts/0-verificar-mysql.sh              # las 3 sucursales
./scripts/0-verificar-mysql.sh sucursal02   # solo una
```

Deberías ver `log_bin = ON`, `binlog_format = ROW`, la posición del binlog,
y los privilegios del usuario `debezium` en cada sucursal.

## 3. Registrar los 3 conectores

```bash
./scripts/1-registrar-conector.sh
```

Registra los 3 conectores (uno por sucursal) contra la API REST de Kafka
Connect (`localhost:8083`), usando `connector/register-sucursal0N-connector.json`.
Cada uno vigila las 6 tablas de su sucursal (`table.include.list`) con
`snapshot.mode: initial` (foto completa al arrancar, luego solo incrementales).

Revisa el estado también en Kafdrop: http://localhost:9000

## 4. Ver los eventos de cambio

**Crudo** (con el schema completo de Kafka Connect):
```bash
./scripts/2-ver-eventos.sh sucursal01 medicamentos
```

**Resumido y legible** (recomendado — solo op, id, antes/después):
```bash
./scripts/4-verificar-cambio.sh sucursal01 medicamentos
```
Por defecto solo muestra eventos **nuevos** desde que lo abres; agrega
`--historial` para ver el tópico completo desde el snapshot inicial.

## 5. Simular cambios reales del POS

```bash
./scripts/3-simular-cambios.sh sucursal02
```

Simula el ciclo de vida de un producto: **llega al inventario (INSERT) →
se vende dos veces (cada venta registra una fila en `ventas`/`detalle_venta`
y baja el inventario) → cambia de precio por promoción → se agota y se da
de baja (DELETE)**. Cada corrida usa un SKU nuevo con timestamp, así que
puedes repetirla las veces que quieras sin chocar con corridas anteriores.

Corre esto con `4-verificar-cambio.sh sucursal02 medicamentos` (o `ventas`,
o `detalle_venta`) abierto en otra terminal para ver los eventos en vivo.

## 6. Editar las sucursales desde DBeaver

Conéctate a cualquiera de las 4 bases con estos datos:

| Sucursal / Base | Host | Puerto | Base de datos | Usuario | Password |
|---|---|---|---|---|---|
| Sucursal 01 | localhost | 3306 | `pos_sucursal_01` | `root` | `rootpassword` |
| Sucursal 02 | localhost | 3307 | `pos_sucursal_02` | `root` | `rootpassword` |
| Sucursal 03 | localhost | 3308 | `pos_sucursal_03` | `root` | `rootpassword` |
| Base Central | localhost | 3309 | `central_farmacias` | `root` | `rootpassword` |

Puedes ver, insertar, actualizar o borrar filas directamente en cualquier
sucursal desde DBeaver (o `mysql`/`INSERT`/`UPDATE`/`DELETE` a mano) — no
hace falta usar los scripts. Cualquier cambio hecho así también dispara
Debezium igual que si lo hubiera hecho el POS real.

**La Base Central es de solo lectura desde tu lado**: se puebla sola vía
`central-sync`. Para confirmarlo sin abrir DBeaver:

```bash
./scripts/5-ver-central.sh              # medicamentos por sucursal
./scripts/5-ver-central.sh ventas       # cualquiera de las 6 tablas
```

O revisa los logs de la app en vivo:
```bash
docker compose logs -f central-sync
```

## Forma de un evento de cambio

**INSERT** (alta de un producto nuevo):
```json
{
  "before": null,
  "after": {
    "id": 12, "sku": "MED-1783454666", "nombre": "Metformina 850mg (30 tabs)",
    "categoria_id": 6, "laboratorio_id": 3, "requiere_receta": true,
    "precio_costo": "26.40", "precio_venta": "55.00", "inventario": 100
  },
  "op": "c"
}
```

**UPDATE** (venta que baja el inventario, o cambio de precio) trae tanto el
estado anterior como el nuevo — justo lo que un agente por polling
normalmente NO puede darte sin trabajo extra:
```json
{
  "before": { "id": 12, "sku": "MED-1783454666", "precio_venta": "55.00", "inventario": 59 },
  "after":  { "id": 12, "sku": "MED-1783454666", "precio_venta": "55.00", "inventario": 49 },
  "op": "u"
}
```

**DELETE** (baja de un producto) trae el último estado conocido en
`before`, `after: null`, y es seguido de un "tombstone" (evento con la
misma llave y valor `null`, usado para que Kafka pueda compactar el log):
```json
{ "before": { "id": 12, "sku": "MED-1783454666" }, "after": null, "op": "d" }
```

## Cómo aplica `central-sync` estos eventos

`central-sync/app.py` se suscribe (por patrón de tópico) a las 18 tablas
que exponen las 3 sucursales, y por cada evento:

- `op` en `c`/`r`/`u` → `INSERT ... ON DUPLICATE KEY UPDATE` en la tabla
  central correspondiente (con `sucursal` + todas las columnas de `after`).
- `op == d` → `DELETE FROM <tabla> WHERE sucursal=%s AND id=%s`.
- tombstone (`null`) → se ignora.

Como los 3 MySQL de sucursal tienen IDs autoincrementales que se repiten
entre sí (las 3 tienen un `medicamentos.id = 1`), la Base Central usa
`PRIMARY KEY (sucursal, id)` en cada tabla para no chocar — y no tiene
foreign keys, porque es un espejo de replicación, no una base
transaccional (el orden de llegada entre tablas relacionadas no está
garantizado).

**Commit manual + reintento ante caídas de la Base Central**: el
`Consumer` de Kafka tiene `enable.auto.commit` apagado — el offset de
cada mensaje se confirma a mano (`consumer.commit(...)`) recién después
de que la escritura en la Base Central tuvo éxito (o de que el mensaje se
descartó a propósito: tombstone, tabla no reconocida, o un error de
datos/parseo que no es de conectividad). Si la escritura falla por un
`mysql.connector.Error` (Base Central caída, conexión inestable,
deadlock, etc.), `central-sync` reintenta la misma escritura
indefinidamente con backoff exponencial (1s, 2s, 4s... hasta 30s),
reconectando con `connect_central_db()` si hace falta, sin avanzar al
siguiente mensaje ni perder el que falló. En cuanto la Base Central
vuelve, el mensaje pendiente se aplica solo, sin intervención manual.

**Healthcheck, manejo defensivo y detección de crash-loop**: además de no
perder mensajes, `central-sync` tiene tres capas de pulido operativo:

- **Heartbeat + healthcheck**: cada vuelta del loop principal (y cada
  intento del reintento contra la Base Central, incluida la reconexión)
  actualiza un archivo (`HEARTBEAT_FILE`, default
  `/tmp/central-sync-heartbeat`) con la hora actual. El `healthcheck` de
  Docker (definido en `docker-compose.yml`) lo marca `unhealthy` solo si
  ese archivo no se actualiza en `HEARTBEAT_MAX_AGE_S` segundos (60 por
  defecto) — así un reintento legítimo y prolongado contra la Base
  Central caída (puede durar minutos) no dispara una falsa alarma de
  salud, pero un proceso realmente colgado sí se detecta.
- **Manejo defensivo de errores no anticipados**: todo el cuerpo de cada
  vuelta del loop principal está envuelto en un `try/except Exception`
  adicional (más externo que los manejos ya existentes de
  `mysql.connector.Error` y de errores de contenido del mensaje). Si algo
  no previsto se escapa igual, se loguea con traceback completo
  (`log.exception`), se espera 2 segundos y el loop continúa — el proceso
  ya no depende de que Docker lo reinicie ante cualquier error que nadie
  haya anticipado todavía.
- **Detección de crash-loop**: al arrancar, `check_crash_loop()` registra
  la hora de este arranque en `RESTART_STATE_FILE` (default
  `/tmp/central-sync-restarts.json`, sobrevive a los reinicios porque
  Docker reutiliza el mismo contenedor bajo `restart: unless-stopped`) y,
  si hubo `CRASH_LOOP_THRESHOLD` arranques o más (5 por defecto) en los
  últimos `CRASH_LOOP_WINDOW_S` segundos (600 por defecto), loguea
  `ALERTA: posible crash-loop` en nivel `CRITICAL`. **Esto es solo una
  señal en el log** (el "gancho"), no un sistema de alertas real —
  conectarlo a un canal de notificación (email, Slack, etc.) queda como
  decisión futura, según la herramienta de monitoreo que se use en
  producción.

## Cómo `connector-watchdog` vigila los conectores

Kafka Connect no reinicia solo un conector o tarea que queda en estado
`FAILED` (por ejemplo, tras una caída prolongada de Kafka) — se queda así
indefinidamente hasta que alguien llame a su API REST de restart a mano,
incluso después de que la causa original ya se resolvió.
`connector-watchdog/app.py` automatiza eso: cada `CHECK_INTERVAL_S`
segundos (15 por defecto) consulta el estado de los 3 conectores, y si
alguno (o alguna de sus tareas) está en `FAILED`, lo reinicia vía
`POST /connectors/{name}/restart`. Los reintentos usan backoff
exponencial por conector (30s, 1m, 2m... hasta 15 minutos), y si un
conector lleva 5 reinicios seguidos sin éxito se agrega una advertencia
extra en el log — pero sigue reintentando, no se da por vencido.

**También recupera conectores atorados en `UNASSIGNED`** (no solo
`FAILED`). Este es un mecanismo aparte, pensado para un escenario de
contingencia distinto al disparador que se usa en la demo (la caída del
MySQL de una sucursal, que produce `FAILED`): al apagar **Kafka por
completo**, se observó que los 3 conectores podían quedar atorados en
`UNASSIGNED` (un estado de coordinación interna de Kafka Connect, normal
por una fracción de segundo durante un rebalanceo, pero sin una API de
arreglo limpia si se queda atorado) al volver Kafka, y había que
borrarlos y volver a registrarlos a mano. Ahora, si un conector (o
alguna de sus tareas) lleva `UNASSIGNED_GRACE_S` segundos seguidos así
(60 por defecto), `connector-watchdog` dispara un reinicio suave
(conector + todas sus tareas); si eso no alcanza y sigue atorado tras
`UNASSIGNED_HARD_RESET_AFTER_S` segundos (300 por defecto), lo borra y
lo recrea desde cero con su configuración original (cacheada al
arrancar desde `connector/register-sucursalNN-connector.json`, montada
como volumen de solo lectura), con un cooldown entre recreaciones
forzadas consecutivas para no entrar en un ciclo de recrear-fallar-recrear.

Para verlo en acción:
```bash
docker compose logs -f connector-watchdog
```

## 7. Detener y limpiar

```bash
docker compose down -v
```

El `-v` también borra los 4 volúmenes de datos, para arrancar limpio la
próxima vez.

## De aquí a producción: qué falta

Este laboratorio ya cubre "3 sucursales + Base Central sincronizada vía
Kafka" de punta a punta, pero para el escenario real (50+ sucursales con
conectividad mixta) faltaría:

- **`snapshot.mode: when_needed` + snapshots incrementales**: para las
  sucursales con conectividad inestable, así el conector puede
  recuperarse solo si se queda desconectado más tiempo del que el binlog
  retiene, sin bloquear la tabla en el resnapshot.
- **Acceso de red seguro**: en producción, cada sucursal necesita una
  forma segura de llegar al clúster de Kafka Connect (VPN, túnel SSH,
  etc.), no exponerlo directamente a internet.
- **Garantías más fuertes en `central-sync`**: hoy es un único proceso,
  sin exactly-once — suficiente para el lab, no para producción (ahí
  conviene evaluar también un JDBC Sink connector real, o
  particionar/escalar el consumidor). La pérdida de mensajes ante una
  caída de la Base Central específicamente ya está resuelta (commit
  manual del offset solo tras éxito + reintento indefinido con backoff,
  ver ["Cómo aplica `central-sync` estos
  eventos"](#cómo-aplica-central-sync-estos-eventos)), pero eso no
  equivale a exactly-once: sigue siendo posible, por ejemplo, que un
  mensaje se aplique dos veces si el proceso se cae justo después de
  escribir en la Base Central pero antes de confirmar el offset.
- **Motor destino real**: la Base Central aquí es MySQL por simplicidad;
  en el escenario original se planteaba PostgreSQL/Azure SQL — el mismo
  patrón de `central-sync` aplica, solo cambia el driver.
- **Clúster de Kafka real**: el laboratorio corre con un solo broker de
  Kafka, sin replicación (`connector-watchdog` cubre el reinicio
  automático de conectores/tareas que quedan en `FAILED`, pero no la
  pérdida de datos si el único broker se cae o su disco falla). En
  producción esto se resuelve con un clúster real (3+ brokers, factor de
  replicación ≥ 3), no con más código.
