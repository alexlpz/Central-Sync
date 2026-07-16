#!/usr/bin/env bash
# Muestra los eventos de una tabla/sucursal de forma resumida (sin el
# "schema" de Kafka Connect que satura la salida): solo op, id, y los
# campos antes/después.
#
# Por defecto solo muestra eventos NUEVOS (los que ocurran desde que
# corres este script en adelante) — así ves justo lo que acaba de pasar
# al correr 3-simular-cambios.sh, sin repetir todo el historial viejo.
# Agrega --historial (en cualquier posición) para ver el topic completo.
#
# Uso:
#   ./scripts/4-verificar-cambio.sh                        # sucursal01 / medicamentos
#   ./scripts/4-verificar-cambio.sh sucursal02 ventas
#   ./scripts/4-verificar-cambio.sh sucursal02 ventas --historial

set -euo pipefail

SUCURSAL=""
TABLA=""
FROM_BEGINNING=""

for arg in "$@"; do
  if [[ "${arg}" == "--historial" ]]; then
    FROM_BEGINNING="--from-beginning"
  elif [[ -z "${SUCURSAL}" ]]; then
    SUCURSAL="${arg}"
  elif [[ -z "${TABLA}" ]]; then
    TABLA="${arg}"
  fi
done

SUCURSAL="${SUCURSAL:-sucursal01}"
TABLA="${TABLA:-medicamentos}"
NUM="${SUCURSAL#sucursal}"
DB="pos_sucursal_${NUM}"
TOPIC="pos.${SUCURSAL}.${DB}.${TABLA}"

if [[ -n "${FROM_BEGINNING}" ]]; then
  echo "Mostrando historial completo del topic: ${TOPIC}"
else
  echo "Viendo solo eventos NUEVOS del topic: ${TOPIC} (agrega --historial para ver todo)"
fi
echo "(Ctrl+C para salir)"
echo

docker exec -i dbz-lab-kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic "${TOPIC}" \
  ${FROM_BEGINNING} \
  | python3 -u "$(dirname "$0")/lib/formato_eventos.py"
