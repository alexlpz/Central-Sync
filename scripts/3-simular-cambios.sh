#!/usr/bin/env bash
# Simula el ciclo de vida real de un producto en una sucursal del POS:
# llega al inventario, se vende un par de veces (con su propia fila en
# ventas/detalle_venta, como haría una caja real), cambia de precio
# (promoción) y finalmente se agota y se da de baja. Cada corrida usa un
# SKU nuevo (con timestamp), así que se puede ejecutar las veces que
# quieras sin chocar con datos de corridas anteriores.
#
# Uso:
#   ./scripts/3-simular-cambios.sh             # sucursal01
#   ./scripts/3-simular-cambios.sh sucursal02
#
# Corre esto MIENTRAS tienes 2-ver-eventos.sh o 4-verificar-cambio.sh
# abierto en otra terminal (misma sucursal) para ver aparecer cada evento
# en tiempo real.

set -euo pipefail

SUCURSAL="${1:-sucursal01}"
NUM="${SUCURSAL#sucursal}"
DB="pos_sucursal_${NUM}"
MYSQL="docker exec -i dbz-lab-mysql-${SUCURSAL} mysql -uroot -prootpassword ${DB}"

NOMBRES=(
  "Cetirizina 10mg (10 tabs)"
  "Naproxeno 250mg (10 tabs)"
  "Metformina 850mg (30 tabs)"
  "Losartan 50mg (30 tabs)"
  "Azitromicina 500mg (3 caps)"
)
NOMBRE="${NOMBRES[$((RANDOM % ${#NOMBRES[@]}))]}"
SKU="MED-$(date +%s)"
PRECIO_INICIAL=$(( (RANDOM % 8000 + 2000) ))       # 20.00 - 100.00
PRECIO_INICIAL_FMT=$(printf '%d.%02d' $((PRECIO_INICIAL/100)) $((PRECIO_INICIAL%100)))
INVENTARIO_INICIAL=$(( (RANDOM % 80 + 40) ))        # 40 - 120

pausa() {
  echo
  echo ">>> $1"
  read -rp "    (Enter para continuar) " _
}

echo "Simulando actividad en ${SUCURSAL} (${DB})"

pausa "1) Llega inventario nuevo a la sucursal: ${NOMBRE} (${SKU})"
$MYSQL -e "
INSERT INTO medicamentos (sku, nombre, categoria_id, laboratorio_id, requiere_receta, precio_costo, precio_venta, inventario)
VALUES ('${SKU}', '${NOMBRE}', 1, 1, FALSE, ${PRECIO_INICIAL_FMT} * 0.6, ${PRECIO_INICIAL_FMT}, ${INVENTARIO_INICIAL});
"
echo "Insertado con inventario=${INVENTARIO_INICIAL}. Deberías ver un evento con \"op\":\"c\" en medicamentos."

pausa "2) Se vende en caja (venta #1: registra la venta y baja el inventario)"
VENTA1=$(( (RANDOM % 5 + 1) ))
$MYSQL -e "
SET @med_id := (SELECT id FROM medicamentos WHERE sku = '${SKU}');
SET @precio := (SELECT precio_venta FROM medicamentos WHERE sku = '${SKU}');
INSERT INTO ventas (total, metodo_pago) VALUES (@precio * ${VENTA1}, 'efectivo');
INSERT INTO detalle_venta (venta_id, medicamento_id, cantidad, precio_unitario)
VALUES (LAST_INSERT_ID(), @med_id, ${VENTA1}, @precio);
UPDATE medicamentos SET inventario = inventario - ${VENTA1} WHERE sku = '${SKU}';
"
echo "Vendidas ${VENTA1} unidades. Deberías ver eventos \"op\":\"c\" en ventas y detalle_venta, y \"op\":\"u\" en medicamentos."

pausa "3) Se vende en caja (venta #2: otra venta, baja más el inventario)"
VENTA2=$(( (RANDOM % 5 + 1) ))
$MYSQL -e "
SET @med_id := (SELECT id FROM medicamentos WHERE sku = '${SKU}');
SET @precio := (SELECT precio_venta FROM medicamentos WHERE sku = '${SKU}');
INSERT INTO ventas (total, metodo_pago) VALUES (@precio * ${VENTA2}, 'tarjeta');
INSERT INTO detalle_venta (venta_id, medicamento_id, cantidad, precio_unitario)
VALUES (LAST_INSERT_ID(), @med_id, ${VENTA2}, @precio);
UPDATE medicamentos SET inventario = inventario - ${VENTA2} WHERE sku = '${SKU}';
"
echo "Vendidas ${VENTA2} unidades más. Otra venta registrada."

pausa "4) Cambia el precio (promoción de fin de mes)"
$MYSQL -e "
UPDATE medicamentos SET precio_venta = precio_venta * 0.85 WHERE sku = '${SKU}';
"
echo "Precio ajustado -15%. Otro evento \"op\":\"u\" en medicamentos, esta vez solo cambia 'precio_venta'."

pausa "5) Se agota y se da de baja del catálogo (DELETE)"
$MYSQL -e "
SET @med_id := (SELECT id FROM medicamentos WHERE sku = '${SKU}');
DELETE FROM detalle_venta WHERE medicamento_id = @med_id;
DELETE FROM medicamentos WHERE sku = '${SKU}';
"
echo "Dado de baja (junto con su detalle_venta). Eventos \"op\":\"d\" en detalle_venta y medicamentos, cada uno con su tombstone."

echo
echo "Listo. Estos eventos (alta, dos ventas con su detalle, promoción, baja)"
echo "son exactamente lo que llegaría a la Base Central para mantenerla"
echo "sincronizada con ${SUCURSAL}, sin que nadie haya consultado directamente su MySQL."
