#!/usr/bin/env bash
# Verifica que el/los MySQL de las sucursales quedaron configurados tal
# como Debezium lo necesita: binlog en formato ROW y el usuario "debezium"
# con privilegios de replicación. Útil para entender qué es exactamente
# lo que hay que pedirle al equipo de infraestructura en un servidor real.
#
# Uso:
#   ./scripts/0-verificar-mysql.sh              # verifica las 3 sucursales
#   ./scripts/0-verificar-mysql.sh sucursal02    # verifica solo una

set -euo pipefail

verificar() {
  local sucursal="$1"
  local container="dbz-lab-mysql-${sucursal}"
  local MYSQL="docker exec -i ${container} mysql -uroot -prootpassword"

  echo "=== ${sucursal} (${container}) ==="

  echo "### ¿Binlog habilitado? ###"
  $MYSQL -e "SHOW VARIABLES LIKE 'log_bin';"

  echo
  echo "### ¿Formato del binlog (debe ser ROW)? ###"
  $MYSQL -e "SHOW VARIABLES LIKE 'binlog_format';"

  echo
  echo "### Posición actual del binlog ###"
  $MYSQL -e "SHOW MASTER STATUS;"

  echo
  echo "### Privilegios del usuario 'debezium' ###"
  $MYSQL -e "SHOW GRANTS FOR 'debezium'@'%';"
  echo
}

if [[ $# -ge 1 ]]; then
  verificar "$1"
else
  for sucursal in sucursal01 sucursal02 sucursal03; do
    verificar "${sucursal}"
  done
fi
