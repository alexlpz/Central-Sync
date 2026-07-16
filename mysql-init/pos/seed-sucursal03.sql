-- Sucursal 03 (Sur): sucursal más pequeña y de reciente apertura, catálogo
-- reducido pero incluye metabólicos (Metformina) que las otras dos no
-- manejan. Inventario más bajo en general por ser sucursal nueva.

INSERT INTO medicamentos
    (id, sku, nombre, categoria_id, laboratorio_id, requiere_receta, precio_costo, precio_venta, inventario) VALUES
    (1, 'MED-001', 'Paracetamol 500mg (20 tabs)',   1, 3, FALSE, 17.80, 31.90,  90),
    (2, 'MED-003', 'Naproxeno 250mg (10 tabs)',     1, 1, FALSE, 20.50, 36.00,  45),
    (3, 'MED-005', 'Azitromicina 500mg (3 caps)',   2, 2, TRUE,  56.00, 94.00,  25),
    (4, 'MED-006', 'Loratadina 10mg (10 tabs)',     3, 4, FALSE, 21.50, 37.50,  60),
    (5, 'MED-009', 'Ranitidina 150mg (20 tabs)',    4, 5, FALSE, 24.00, 40.00,  30),
    (6, 'MED-010', 'Losartan 50mg (30 tabs)',       5, 4, TRUE,  35.50, 59.00,  70),
    (7, 'MED-011', 'Enalapril 10mg (30 tabs)',      5, 5, TRUE,  30.50, 51.00,  55),
    (8, 'MED-012', 'Metformina 850mg (30 tabs)',    6, 3, TRUE,  26.00, 44.00,  80);

INSERT INTO promociones
    (medicamento_id, descripcion, descuento_pct, fecha_inicio, fecha_fin, activa) VALUES
    (8, 'Apertura de sucursal: Metformina de lanzamiento', 30.00, '2026-07-01', '2026-07-31', TRUE),
    (3, 'Azitromicina: stock limitado con descuento',      10.00, '2026-07-01', '2026-07-20', TRUE);

INSERT INTO ventas (id, fecha, total, metodo_pago) VALUES
    (1, '2026-07-06 10:15:00', 44.00, 'efectivo');

INSERT INTO detalle_venta (venta_id, medicamento_id, cantidad, precio_unitario) VALUES
    (1, 8, 1, 44.00);
