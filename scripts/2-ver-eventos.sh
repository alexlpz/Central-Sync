#!/usr/bin/env bash
# Muestra en vivo los eventos de cambio capturados por Debezium para una
# tabla de una sucursal (crudo, con el "schema" completo de Kafka Connect).
# Para una vista resumida y legible usa 4-verificar-cambio.sh.
#
# Uso:
#   ./scripts/2-ver-eventos.sh                      # sucursal01 / medicamentos
#   ./scripts/2-ver-eventos.sh sucursal02 ventas     # otra sucursal / tabla

set -euo pipefail

SUCURSAL="${1:-sucursal01}"
TABLA="${2:-medicamentos}"
NUM="${SUCURSAL#sucursal}"
DB="pos_sucursal_${NUM}"
TOPIC="pos.${SUCURSAL}.${DB}.${TABLA}"

echo "Viendo eventos del topic: ${TOPIC}"
echo "(Ctrl+C para salir)"
echo

docker exec -i dbz-lab-kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic "${TOPIC}" \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "
