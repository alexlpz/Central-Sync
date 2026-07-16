-- Base Central: espejo consolidado de las 3 sucursales, poblado
-- ÚNICAMENTE por la app central-sync (que consume los topics de Kafka).
-- Nadie debería escribir aquí a mano.
--
-- Mismas tablas que en el POS, pero con la columna `sucursal` como parte
-- de la llave primaria (los ids autoincrementales se repiten entre
-- sucursales) y SIN foreign keys: esto es un espejo de replicación, no
-- una base transaccional, y el orden de llegada entre tablas relacionadas
-- no está garantizado (ej. una promoción podría llegar antes que el
-- medicamento al que referencia si van en tópicos/particiones distintas).

CREATE TABLE categorias (
    sucursal VARCHAR(20) NOT NULL,
    id       INT NOT NULL,
    nombre   VARCHAR(80) NOT NULL,
    PRIMARY KEY (sucursal, id)
) ENGINE=InnoDB;

CREATE TABLE laboratorios (
    sucursal VARCHAR(20) NOT NULL,
    id       INT NOT NULL,
    nombre   VARCHAR(100) NOT NULL,
    pais     VARCHAR(60),
    PRIMARY KEY (sucursal, id)
) ENGINE=InnoDB;

CREATE TABLE medicamentos (
    sucursal        VARCHAR(20) NOT NULL,
    id              INT NOT NULL,
    sku             VARCHAR(20) NOT NULL,
    nombre          VARCHAR(150) NOT NULL,
    categoria_id    INT NOT NULL,
    laboratorio_id  INT NOT NULL,
    requiere_receta BOOLEAN NOT NULL DEFAULT FALSE,
    precio_costo    DECIMAL(10,2) NOT NULL,
    precio_venta    DECIMAL(10,2) NOT NULL,
    inventario      INT NOT NULL DEFAULT 0,
    actualizado_en  TIMESTAMP NULL,
    PRIMARY KEY (sucursal, id),
    INDEX idx_medicamentos_sku (sku),
    INDEX idx_medicamentos_categoria (sucursal, categoria_id),
    INDEX idx_medicamentos_laboratorio (sucursal, laboratorio_id)
) ENGINE=InnoDB;

CREATE TABLE promociones (
    sucursal       VARCHAR(20) NOT NULL,
    id             INT NOT NULL,
    medicamento_id INT NOT NULL,
    descripcion    VARCHAR(150) NOT NULL,
    descuento_pct  DECIMAL(5,2) NOT NULL,
    fecha_inicio   DATE NOT NULL,
    fecha_fin      DATE NOT NULL,
    activa         BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (sucursal, id),
    INDEX idx_promociones_medicamento (sucursal, medicamento_id)
) ENGINE=InnoDB;

CREATE TABLE ventas (
    sucursal    VARCHAR(20) NOT NULL,
    id          INT NOT NULL,
    fecha       DATETIME NOT NULL,
    total       DECIMAL(10,2) NOT NULL DEFAULT 0,
    metodo_pago VARCHAR(20) NOT NULL,
    PRIMARY KEY (sucursal, id)
) ENGINE=InnoDB;

CREATE TABLE detalle_venta (
    sucursal        VARCHAR(20) NOT NULL,
    id              INT NOT NULL,
    venta_id        INT NOT NULL,
    medicamento_id  INT NOT NULL,
    cantidad        INT NOT NULL,
    precio_unitario DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (sucursal, id),
    INDEX idx_detalle_venta_venta (sucursal, venta_id),
    INDEX idx_detalle_venta_medicamento (sucursal, medicamento_id)
) ENGINE=InnoDB;
