#!/usr/bin/env bash
# Consulta la Base Central (mysql-central) para confirmar, sin abrir
# DBeaver, que ya tiene la foto consolidada de las 3 sucursales — poblada
# únicamente por la app central-sync a partir de lo que llega por Kafka.
#
# Uso:
#   ./scripts/5-ver-central.sh                # resumen de medicamentos por sucursal
#   ./scripts/5-ver-central.sh ventas         # cualquier otra tabla de las 6

set -euo pipefail

TABLA="${1:-medicamentos}"
MYSQL="docker exec -i dbz-lab-mysql-central mysql -uroot -prootpassword central_farmacias"

case "${TABLA}" in
  medicamentos)
    echo "### medicamentos por sucursal (Base Central) ###"
    $MYSQL -e "
      SELECT sucursal, sku, nombre, precio_venta, inventario, actualizado_en
      FROM medicamentos
      ORDER BY sucursal, sku;
    "
    ;;
  *)
    echo "### ${TABLA} (Base Central) ###"
    $MYSQL -e "SELECT * FROM ${TABLA} ORDER BY sucursal, id;"
    ;;
esac
