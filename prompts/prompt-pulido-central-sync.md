# Prompt: healthcheck, manejo defensivo y detección de crash-loop en central-sync

Pega este prompt completo en tu asistente de código de VS Code, dentro del
proyecto `debezium-lab`, con `central-sync/app.py` visible en el contexto
(ya existe con el commit manual + reintento contra la Base Central — este
cambio lo extiende, no lo reescribe).

Son tres mejoras relacionadas, impleméntalas juntas.

## Contexto

`central-sync` ya no pierde mensajes (commit manual solo tras éxito) y ya
se reinicia solo ante una caída (`restart: unless-stopped` en
`docker-compose.yml`, vía Docker). Lo que falta es pulido de observabilidad
y robustez: saber si el proceso está realmente vivo (no solo "corriendo"),
no depender de que Docker reinicie el proceso ante cualquier error no
anticipado, y tener una señal explícita si los reinicios automáticos
empiezan a repetirse en bucle.

## Parte A — Heartbeat + healthcheck

### Cambio en `central-sync/app.py`

1. Agrega una función `touch_heartbeat()` que escriba la hora actual
   (`time.time()`) en el archivo indicado por la env var `HEARTBEAT_FILE`
   (default `/tmp/central-sync-heartbeat`). Debe ser tolerante a fallos de
   escritura (un `try/except OSError: pass` — nunca debe tumbar el proceso
   por esto).
2. Llama a `touch_heartbeat()`:
   - Al final de cada vuelta del loop principal (`while True`), después de
     manejar (o no) un mensaje.
   - Dentro del loop de reintento contra la Base Central (`apply_with_retry`
     o como se llame la función de reintento que ya existe), en cada
     intento — específicamente antes de cada `time.sleep(delay)`. Esto es
     importante: un reintento legítimo y prolongado contra la Base Central
     caída (puede durar minutos) NO debe hacer que el healthcheck marque el
     contenedor como `unhealthy` — el heartbeat debe seguir actualizándose
     mientras el proceso sigue activamente reintentando.

### Cambio en `docker-compose.yml`

Agrega un bloque `healthcheck` al servicio `central-sync`, mismo patrón que
ya usan los servicios MySQL:

```yaml
healthcheck:
  test: ["CMD", "python3", "-c", "import os,sys,time; p=os.environ.get('HEARTBEAT_FILE','/tmp/central-sync-heartbeat'); age=int(os.environ.get('HEARTBEAT_MAX_AGE_S','60')); sys.exit(0 if os.path.exists(p) and time.time()-os.path.getmtime(p) < age else 1)"]
  interval: 15s
  timeout: 5s
  retries: 3
  start_period: 30s
```

Agrega también `HEARTBEAT_FILE` y `HEARTBEAT_MAX_AGE_S` a la sección
`environment` del servicio (mismos defaults que en el healthcheck: default
`/tmp/central-sync-heartbeat` y `60`).

## Parte B — Manejo defensivo de errores no anticipados

En el loop principal de `main()`, envuelve el cuerpo completo de cada
vuelta del `while True` (descubrimiento de tópicos + `poll()` + manejo del
mensaje) en un `try/except Exception` adicional, a nivel más externo que
los que ya existen. Si algo no previsto se escapa de los manejos ya
existentes (fallas de conexión con la Base Central, errores de contenido
del mensaje):

1. Regístralo con `log.exception(...)` (traceback completo).
2. Duerme un par de segundos (`time.sleep(2)`) para no entrar en un ciclo
   cerrado si el error se repite de inmediato en la siguiente vuelta.
3. Continúa el loop (`continue`) — no dejes que la excepción termine el
   proceso.

**No captures `KeyboardInterrupt` ni `SystemExit`** en este nuevo bloque
(un `except Exception` normal ya no los captura, así que no necesitas
lógica extra para eso — solo confírmalo al escribir el código).

No cambies el comportamiento de los manejos de error ya existentes
(reintento indefinido ante `mysql.connector.Error`, log + commit-y-omitir
ante errores de contenido del mensaje) — esta captura nueva es solo para lo
que hoy se escaparía de esos dos caminos.

## Parte C — Detección de crash-loop

1. Agrega una función `check_crash_loop()` que:
   - Lea el archivo indicado por `RESTART_STATE_FILE` (env var, default
     `/tmp/central-sync-restarts.json`) — una lista JSON de timestamps
     (`float`). Si no existe o está corrupto, trátalo como lista vacía.
   - Filtre esa lista a solo los timestamps dentro de los últimos
     `CRASH_LOOP_WINDOW_S` segundos (env var, default `600`).
   - Agregue el timestamp actual (`time.time()`) a la lista.
   - Guarde la lista filtrada + el nuevo timestamp de vuelta en el archivo
     (JSON). Sé tolerante a errores de lectura/escritura (no debe tumbar el
     proceso).
   - Si la cantidad de timestamps en la ventana es `>= CRASH_LOOP_THRESHOLD`
     (env var, default `5`), registra una línea con `log.critical(...)`
     que incluya explícitamente el texto `ALERTA: posible crash-loop`, el
     número de arranques, y la ventana de tiempo considerada.
2. Llama a `check_crash_loop()` como lo primero que se ejecuta, antes de
   `main()`, dentro del bloque `if __name__ == "__main__":`.

**Nota para el código**: agrega un comentario explicando que este contador
sobrevive a los reinicios porque Docker reinicia el mismo contenedor (no
uno nuevo) bajo la política `restart: unless-stopped`, así que el archivo
en `/tmp` persiste entre reinicios del proceso dentro de ese contenedor; y
que esto es solo una señal en el log (el "gancho"), no un sistema de
alertas real — conectarlo a un canal de notificación (email, Slack, etc.)
es una decisión futura que depende de la herramienta de monitoreo que se
use en producción.

## Variables de entorno nuevas (agrégalas también a `docker-compose.yml`)

```yaml
environment:
  # ... las que ya existen ...
  HEARTBEAT_FILE: /tmp/central-sync-heartbeat
  HEARTBEAT_MAX_AGE_S: "60"
  RESTART_STATE_FILE: /tmp/central-sync-restarts.json
  CRASH_LOOP_WINDOW_S: "600"
  CRASH_LOOP_THRESHOLD: "5"
```

## No toques

- La lógica ya existente de commit manual, reintento contra la Base
  Central, y manejo de errores de contenido — esto es aditivo.
- `connector-watchdog/`.
- El estilo del archivo: comentarios explicativos en español, mismo
  formato de logging que ya usa `central-sync/app.py`.

## Criterio de aceptación / cómo probarlo

1. Con el laboratorio corriendo normalmente, `docker compose ps` debe
   mostrar `central-sync` como `healthy` después del `start_period`
   inicial (~30s).
2. `docker compose stop mysql-central`, genera un cambio
   (`./scripts/3-simular-cambios.sh sucursal01`), y confirma que
   `central-sync` **sigue `healthy`** mientras reintenta (no se marca
   `unhealthy` solo por tardarse). Restaura con
   `docker compose start mysql-central` y confirma que el cambio pendiente
   se aplica.
3. Provoca varios reinicios seguidos:
   `for i in 1 2 3 4 5; do docker compose restart central-sync; sleep 5; done`
   — confirma en `docker compose logs central-sync` que aparece la línea
   `ALERTA: posible crash-loop` después del quinto arranque dentro de la
   ventana.
4. Repite las pruebas de las notas anteriores (Base Central caída,
   mensajes malformados) y confirma que no hay regresión: siguen sin
   perder datos y sin caerse por errores ya conocidos.

## Al terminar

Actualiza `README.md`: agrega una breve sección sobre estas tres mejoras
en `central-sync` (healthcheck, manejo defensivo, detección de
crash-loop), aclarando que la detección de crash-loop es solo una señal en
el log, no una alerta real enviada a algún canal.
