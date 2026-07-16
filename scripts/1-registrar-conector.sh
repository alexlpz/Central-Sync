#!/usr/bin/env bash
# Registra los 3 conectores MySQL de Debezium (uno por sucursal) en Kafka
# Connect. Ejecutar después de que `docker compose up -d` esté corriendo y
# Kafka Connect responda en http://localhost:8083

set -euo pipefail

CONNECT_URL="http://localhost:8083"
CONNECTOR_DIR="$(dirname "$0")/../connector"

echo "Esperando a que Kafka Connect esté listo en ${CONNECT_URL} ..."
until curl -s -o /dev/null -w '%{http_code}' "${CONNECT_URL}/connectors" | grep -q "200"; do
  printf '.'
  sleep 3
done
echo " listo."

for config_file in "${CONNECTOR_DIR}"/register-sucursal0*-connector.json; do
  connector_name=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['name'])" "${config_file}")

  echo
  echo "Registrando ${connector_name} (${config_file})..."
  curl -s -X POST -H "Content-Type: application/json" \
    --data @"${config_file}" \
    "${CONNECT_URL}/connectors" | python3 -m json.tool

  echo "Estado de ${connector_name}:"
  sleep 2
  curl -s "${CONNECT_URL}/connectors/${connector_name}/status" | python3 -m json.tool
done
