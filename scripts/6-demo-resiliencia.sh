#!/usr/bin/env bash
# Demo narrada de resiliencia: qué pasa si la Base Central se cae mientras
# las sucursales siguen operando. Pensada para presentar en vivo frente a
# un cliente, alternando entre esta terminal y DBeaver.
#
# Guion:
#   1. Se apaga mysql-central (simula una caída del servidor central).
#   2. TÚ haces un cambio real en cualquier sucursal desde DBeaver — la
#      sucursal sigue operando normal, no depende de que la central esté viva.
#   3. Se muestra cómo central-sync detecta la caída y se queda
#      reintentando (con backoff), sin descartar el evento — y que su
#      propio healthcheck de Docker lo sigue viendo "healthy" mientras
#      reintenta (no es solo "no se cayó", está activo de verdad).
#   4. Se levanta mysql-central.
#   5. Se ve en vivo la reconexión y el catch-up automático.
#   6. Confirmas en DBeaver (refrescando la conexión a la Base Central)
#      que el cambio llegó solo, sin haber corrido nada manualmente.
#
# Requisito: ten DBeaver abierto con conexiones a una sucursal y a la
# Base Central antes de empezar.

set -euo pipefail

pausa() {
  echo
  echo ">>> $1"
  read -rp "    (Enter para continuar) " _
}

seguir_logs() {
  local segundos="$1"
  docker compose logs -f --tail 5 central-sync </dev/null &
  local pid=$!
  sleep "${segundos}"
  kill "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
}

echo "=== Demo: resiliencia de central-sync ante una caída de la Base Central ==="
echo "Ten abierto DBeaver con conexiones a una sucursal y a la Base Central."

pausa "1) Verificando que todo esté sano antes de empezar"
docker compose ps --format "table {{.Name}}\t{{.Status}}"

pausa "2) Apagando mysql-central (simula que el servidor central se cae)"
docker compose stop mysql-central
echo "mysql-central está abajo. Las 3 sucursales NO se enteran — siguen vendiendo normal."

pausa "3) Ve a DBeaver: en cualquier sucursal, inserta/actualiza/borra una fila (como una venta real). Cuando termines, vuelve aquí y presiona Enter."

pausa "4) Veamos qué está haciendo central-sync mientras la central sigue caída"
sleep 5
echo "(últimos logs — deberías ver reintentos con backoff, sin perder el evento)"
docker compose logs --tail 15 central-sync
echo
echo -n "Estado de salud de central-sync (Docker healthcheck): "
docker inspect -f '{{.State.Health.Status}}' dbz-lab-central-sync
echo "^ sigue 'healthy' aunque lleve rato reintentando — el proceso está"
echo "  activo de verdad, no solo 'no se cayó'. Esto también se ve en:"
echo "  docker compose ps"

pausa "5) Levantando mysql-central de nuevo"
docker compose start mysql-central
echo -n "Esperando a que quede 'healthy'..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' dbz-lab-mysql-central 2>/dev/null)" = "healthy" ]; do
  echo -n "."
  sleep 1
done
echo " listo."

pausa "6) Viendo la reconexión y el catch-up automático en vivo (20s)"
seguir_logs 20

echo
echo "Ahora ve a DBeaver, refresca la conexión de la Base Central (F5) y"
echo "confirma que el cambio del paso 3 ya está ahí — sin que nadie haya"
echo "corrido nada manual para 'reparar' la sincronización."
