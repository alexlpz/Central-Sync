#!/usr/bin/env bash
# Demo narrada de resiliencia: qué pasa cuando el conector Debezium de una
# sucursal queda en FAILED (ej. porque el MySQL de esa sucursal se cae) y
# nadie llama a la API de restart a mano. Pensada para presentar en vivo
# frente a un cliente.
#
# Nota de diseño: se prueba con una caída de MySQL (no de Kafka). Kafka
# caído produce un cuadro distinto — el conector queda en estado
# intermedio "UNASSIGNED", que connector-watchdog SÍ sabe recuperar
# (reinicio suave a los 60s, recreación forzada a los 300s si hace
# falta — verificado: se recupera solo, sin intervención manual), pero
# la recuperación completa tarda varios minutos en estabilizar. Para una
# demo EN VIVO conviene esta variante (MySQL de una sucursal): el
# conector falla al arrancar la tarea de forma inmediata y limpia
# (excepción de conexión), y el watchdog lo recupera en ~30-60s — mucho
# más ágil frente a un cliente. Si quieres mostrar también el caso
# Kafka-caído, es mejor explicarlo en vivo y correrlo de antemano
# (grabado o ya resuelto), no en tiempo real.
#
# Uso:
#   ./scripts/7-demo-watchdog.sh              # sucursal01
#   ./scripts/7-demo-watchdog.sh sucursal02

set -euo pipefail

SUCURSAL="${1:-sucursal01}"
SERVICIO="mysql-${SUCURSAL/sucursal/sucursal-}"   # sucursal02 -> mysql-sucursal-02
CONECTOR="pos-${SUCURSAL/sucursal/sucursal-}-connector"
CONTAINER="dbz-lab-mysql-${SUCURSAL}"
CONNECT_URL="http://localhost:8083"

pausa() {
  echo
  echo ">>> $1"
  read -rp "    (Enter para continuar) " _
}

esperar_healthy() {
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null)" = "healthy" ]; do
    sleep 1
  done
}

estado_conector() {
  curl -s "${CONNECT_URL}/connectors/${CONECTOR}/status" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['connector']['state']+':'+','.join(t['state'] for t in d.get('tasks',[])))" 2>/dev/null || echo "sin respuesta"
}

echo "=== Demo: connector-watchdog recuperando un conector en FAILED (${CONECTOR}) ==="

pausa "1) Verificando que todo esté sano antes de empezar"
echo "Conector: $(estado_conector)"
docker compose ps --format "table {{.Name}}\t{{.Status}}"

pausa "2) Apagando ${CONTAINER} (simula que el servidor de esa sucursal se cae)"
docker compose stop "${SERVICIO}"

pausa "3) Forzando un restart del conector mientras la sucursal está caída, para que falle al arrancar la tarea (así se ve el FAILED de inmediato, en vez de esperar)"
curl -s -X POST "${CONNECT_URL}/connectors/${CONECTOR}/restart?includeTasks=true" -o /dev/null
sleep 2
echo "Estado: $(estado_conector)"
echo
echo "Motivo del fallo:"
curl -s "${CONNECT_URL}/connectors/${CONECTOR}/status" | python3 -c "
import json,sys
d = json.load(sys.stdin)
for t in d.get('tasks', []):
    if 'trace' in t:
        print(t['trace'].splitlines()[0])
"

pausa "4) Veamos a connector-watchdog notar el FAILED y empezar a reintentar solo (backoff: 30s, 60s, 120s...)"
docker compose logs --since 30s connector-watchdog

pausa "5) Levantando ${CONTAINER} de nuevo"
docker compose start "${SERVICIO}"
echo -n "Esperando a que quede 'healthy'..."
esperar_healthy "${CONTAINER}"
echo " listo."

pausa "6) Esperando a que connector-watchdog haga su próximo intento y se recupere solo"
START=$(date +%s)
while true; do
  ESTADO=$(estado_conector)
  ELAPSED=$(( $(date +%s) - START ))
  echo "t+${ELAPSED}s: ${ESTADO}"
  if [ "${ESTADO}" = "RUNNING:RUNNING" ]; then
    echo ">>> Recuperado en ${ELAPSED}s, sin intervención manual <<<"
    break
  fi
  sleep 5
done

echo
echo "Listo. El conector volvió solo a RUNNING — nadie llamó a la API de"
echo "restart a mano. Puedes confirmar en Kafdrop (http://localhost:9000)"
echo "que el snapshot/los eventos de ${SUCURSAL} siguen llegando con normalidad."
