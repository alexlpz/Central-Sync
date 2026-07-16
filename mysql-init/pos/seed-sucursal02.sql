-- Sucursal 02 (Norte): zona residencial con más adultos mayores, por eso
-- sí maneja línea cardiovascular. No maneja naproxeno ni azitromicina.
-- Precios ligeramente distintos a Sucursal 01 (variación regional real).

INSERT INTO medicamentos
    (id, sku, nombre, categoria_id, laboratorio_id, requiere_receta, precio_costo, precio_venta, inventario) VALUES
    (1, 'MED-001', 'Paracetamol 500mg (20 tabs)',   1, 3, FALSE, 18.50, 33.90, 180),
    (2, 'MED-002', 'Ibuprofeno 400mg (10 tabs)',    1, 1, FALSE, 15.50, 29.50, 100),
    (3, 'MED-004', 'Amoxicilina 500mg (12 caps)',   2, 2, TRUE,  41.00, 70.00,  55),
    (4, 'MED-006', 'Loratadina 10mg (10 tabs)',     3, 4, FALSE, 22.50, 39.00,  75),
    (5, 'MED-007', 'Cetirizina 10mg (10 tabs)',     3, 1, FALSE, 19.50, 34.00,  90),
    (6, 'MED-008', 'Omeprazol 20mg (14 caps)',      4, 3, FALSE, 28.50, 48.50, 140),
    (7, 'MED-009', 'Ranitidina 150mg (20 tabs)',    4, 5, FALSE, 24.50, 41.00,  50),
    (8, 'MED-010', 'Losartan 50mg (30 tabs)',       5, 4, TRUE,  35.00, 58.00, 160),
    (9, 'MED-011', 'Enalapril 10mg (30 tabs)',      5, 5, TRUE,  30.00, 50.00, 130);

INSERT INTO promociones
    (medicamento_id, descripcion, descuento_pct, fecha_inicio, fecha_fin, activa) VALUES
    (8, 'Descuento adulto mayor en Losartan',  20.00, '2026-07-01', '2026-12-31', TRUE),
    (9, 'Descuento adulto mayor en Enalapril', 20.00, '2026-07-01', '2026-12-31', TRUE),
    (6, 'Promo cierre de temporada Omeprazol', 12.00, '2026-06-20', '2026-07-10', FALSE);

INSERT INTO ventas (id, fecha, total, metodo_pago) VALUES
    (1, '2026-07-05 11:05:00', 108.00, 'tarjeta'),
    (2, '2026-07-06 18:22:00',  82.50, 'transferencia');

INSERT INTO detalle_venta (venta_id, medicamento_id, cantidad, precio_unitario) VALUES
    (1, 8, 1, 58.00),
    (1, 9, 1, 50.00),
    (2, 6, 1, 48.50),
    (2, 5, 1, 34.00);
