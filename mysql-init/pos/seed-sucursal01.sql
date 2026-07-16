-- Sucursal 01 (Centro): farmacia de alto tráfico, catálogo enfocado en
-- lo más recetado del día a día (analgésicos, antibióticos, antihistamínicos,
-- gastro). No maneja línea cardiovascular/metabólica.

INSERT INTO medicamentos
    (id, sku, nombre, categoria_id, laboratorio_id, requiere_receta, precio_costo, precio_venta, inventario) VALUES
    (1, 'MED-001', 'Paracetamol 500mg (20 tabs)',   1, 3, FALSE, 18.00, 32.50, 220),
    (2, 'MED-002', 'Ibuprofeno 400mg (10 tabs)',    1, 1, FALSE, 15.00, 28.00, 150),
    (3, 'MED-003', 'Naproxeno 250mg (10 tabs)',     1, 1, FALSE, 20.00, 35.00,  70),
    (4, 'MED-004', 'Amoxicilina 500mg (12 caps)',   2, 2, TRUE,  40.00, 68.00,  95),
    (5, 'MED-005', 'Azitromicina 500mg (3 caps)',   2, 2, TRUE,  55.00, 92.00,  40),
    (6, 'MED-006', 'Loratadina 10mg (10 tabs)',     3, 4, FALSE, 22.00, 38.00, 110),
    (7, 'MED-007', 'Cetirizina 10mg (10 tabs)',     3, 1, FALSE, 19.00, 33.00, 130),
    (8, 'MED-008', 'Omeprazol 20mg (14 caps)',      4, 3, FALSE, 28.00, 47.00,  85),
    (9, 'MED-009', 'Ranitidina 150mg (20 tabs)',    4, 5, FALSE, 24.00, 40.00,  60);

INSERT INTO promociones
    (medicamento_id, descripcion, descuento_pct, fecha_inicio, fecha_fin, activa) VALUES
    (1, 'Paracetamol al 2x1 en compras mayores a $100', 15.00, '2026-07-01', '2026-07-31', TRUE),
    (6, 'Temporada de alergias: Loratadina con descuento', 10.00, '2026-07-01', '2026-08-15', TRUE),
    (9, 'Liquidación Ranitidina (próxima a vencer)',      25.00, '2026-06-15', '2026-07-15', TRUE);

INSERT INTO ventas (id, fecha, total, metodo_pago) VALUES
    (1, '2026-07-05 09:12:00', 60.50, 'efectivo'),
    (2, '2026-07-06 16:40:00', 92.00, 'tarjeta');

INSERT INTO detalle_venta (venta_id, medicamento_id, cantidad, precio_unitario) VALUES
    (1, 1, 1, 32.50),
    (1, 7, 1, 28.00),
    (2, 5, 1, 92.00);
