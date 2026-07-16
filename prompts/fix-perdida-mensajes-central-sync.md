# Prompt: corregir pérdida de mensajes en central-sync ante fallas de la Base Central

Pega este prompt completo en tu asistente de código de VS Code (Claude Code u
otro), dentro del proyecto `debezium-lab`, con `central-sync/app.py` visible
en el contexto.

---

## Contexto

Este es el proyecto `debezium-lab`: un laboratorio de Debezium/CDC con 3
sucursales MySQL, Kafka y una app propia (`central-sync/app.py`) que consume
los eventos de cambio desde Kafka y los aplica sobre una Base Central MySQL
(upsert para INSERT/UPDATE, delete para DELETE).

Lee **todo el archivo `central-sync/app.py`** antes de modificar nada, para
respetar su estilo (comentarios explicativos en español, sin frameworks,
logging con el logger `log` ya configurado).

## Objetivo

Eliminar el riesgo actual de pérdida de mensajes cuando la Base Central no
responde (caída, conexión inestable, timeout, deadlock, etc.) en el momento
de aplicar un cambio.

## Problema actual

- El `Consumer` se crea con `"enable.auto.commit": True`. Esto hace que
  Kafka confirme offsets automáticamente cada pocos segundos según lo que ya
  fue *entregado* por `poll()`, sin importar si `handle_message()` tuvo
  éxito aplicándolo en la Base Central.
- En el loop principal (`main()`), si `handle_message()` lanza
  `mysql.connector.Error`, el error se loguea y el loop simplemente continúa
  con el siguiente mensaje — no hay ningún reintento.
- Combinados, estos dos puntos hacen que un cambio que falla por conectividad
  con la Base Central se pierda para siempre: el offset se termina
  confirmando de todas formas por el auto-commit.

## Cambios requeridos

1. **Apaga el auto-commit**: cambia `"enable.auto.commit": True` a `False`
   en la configuración del `Consumer`.

2. **Confirma el offset manualmente, solo tras éxito**: refactoriza el flujo
   para que `consumer.commit(message=msg, asynchronous=False)` se llame
   ÚNICAMENTE después de que la escritura en la Base Central haya tenido
   éxito para ese mensaje.

3. **Ante `mysql.connector.Error`, reintenta — no avances**: si falla la
   escritura por un error de conexión/base de datos, reintenta la MISMA
   escritura indefinidamente, con backoff exponencial acotado:
   - Arranca en 1 segundo, se duplica en cada intento, tope de 30 segundos.
   - Mientras se reintenta, **no llames a `consumer.poll()` de nuevo** — el
     loop principal debe quedarse bloqueado en ese mensaje hasta que la
     escritura tenga éxito. (Esto es intencional: si la Base Central está
     caída, de todas formas no se podría escribir nada de ninguna sucursal,
     así que pausar todo el consumo no pierde paralelismo real, y evita que
     un commit posterior "salte por encima" del mensaje fallido.)
   - Loguea cada intento: número de intento y segundos de espera antes del
     siguiente (`log.warning`, mismo formato que el resto del archivo).
   - Si `conn.is_connected()` es `False`, reconéctate reutilizando la
     función `connect_central_db()` que ya existe en el archivo (ya trae su
     propio backoff) antes de reintentar la escritura.

4. **Mensajes que no aplican** (tombstones con `value=None`, tópicos de
   tablas no reconocidas, `payload` en `None`): mantén la lógica actual de
   ignorarlos, pero ahora confirma su offset explícitamente con
   `consumer.commit(message=msg, asynchronous=False)` — antes lo hacía el
   auto-commit de forma implícita, y ahora que está apagado hay que hacerlo
   a propósito.

5. **Errores que NO sean `mysql.connector.Error` quedan fuera de alcance a
   propósito** (bugs de parseo, payload con forma inesperada, etc.): mantén
   el comportamiento actual de loguear con `log.exception(...)` y continuar,
   pero confirma explícitamente el offset de ese mensaje después de
   loguear el error, para no bloquear indefinidamente el pipeline completo
   por un mensaje que nunca va a poder procesarse (es un problema de
   datos/código, no de conectividad — reintentarlo para siempre no lo
   arregla). Agrega un comentario en el código explicando esta asimetría a
   propósito (por qué SÍ se reintenta indefinidamente ante fallas de
   conexión pero NO ante estos otros errores), para que quede documentado y
   no sea sorpresa para quien lo lea después. **No** implementes manejo de
   "mensajes envenenados" / dead-letter en este cambio — eso queda como
   mejora futura, fuera de alcance.

6. **No toques la lógica de negocio**: `upsert()`, `delete()`,
   `convert_value()`, `parse_topic()`, `discover_topics()` deben quedar
   igual. El cambio es específicamente sobre el manejo de commits y
   reintentos en el loop principal y en cómo se invoca la escritura a la
   Base Central.

7. **Estilo**: comentarios explicativos en español, mismo formato de logging
   que el resto del archivo, sin introducir dependencias nuevas.

## Criterio de aceptación / cómo probarlo manualmente

1. `docker compose up -d` con todo corriendo y los 3 conectores en estado
   `RUNNING`.
2. `docker compose stop mysql-central` (simula la Base Central caída).
3. Corre `./scripts/3-simular-cambios.sh sucursal01` para generar cambios.
4. En `docker compose logs -f central-sync` deberías ver reintentos con
   backoff creciente (1s, 2s, 4s, 8s... hasta 30s), sin que el proceso se
   caiga ni se pierda el mensaje.
5. `docker compose start mysql-central` — en cuanto vuelva, central-sync
   debería aplicar automáticamente el/los cambio(s) pendientes sin
   intervención manual.
6. `./scripts/5-ver-central.sh` debe reflejar el estado final correcto,
   como si la Base Central nunca se hubiera caído.
7. Verifica también el caso feliz (Base Central arriba todo el tiempo): los
   cambios se siguen aplicando en tiempo real como hasta ahora, sin
   regresión de latencia perceptible.

## Al terminar

Actualiza `README.md` (sección "De aquí a producción: qué falta" y/o "Cómo
aplica `central-sync` estos eventos") para reflejar que este punto
específico (pérdida de mensajes ante caída de la Base Central) ya está
resuelto, describiendo brevemente el mecanismo de commit manual + reintento.
No borres la mención existente sobre la falta de garantías exactly-once —
sigue siendo cierta y relevante; ajústala para aclarar que la pérdida de
mensajes por caída de la Base Central específicamente ya no aplica.
