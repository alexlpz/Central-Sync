-- Esquema común a las 3 sucursales (POS). El nombre de la base de datos
-- ya viene seleccionado por MYSQL_DATABASE (ver docker-compose.yml), así
-- que aquí no hace falta un USE explícito.
--
-- Modela lo mínimo de un POS de farmacia real: catálogo de medicamentos
-- con costo/precio (para poder calcular margen), categorías, laboratorios,
-- promociones vigentes, y las ventas que se van registrando en caja.

CREATE TABLE categorias (
    id     INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(80) NOT NULL UNIQUE
) ENGINE=InnoDB;

CREATE TABLE laboratorios (
    id     INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE,
    pais   VARCHAR(60)
) ENGINE=InnoDB;

CREATE TABLE medicamentos (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    sku             VARCHAR(20) NOT NULL UNIQUE,
    nombre          VARCHAR(150) NOT NULL,
    categoria_id    INT NOT NULL,
    laboratorio_id  INT NOT NULL,
    requiere_receta BOOLEAN NOT NULL DEFAULT FALSE,
    precio_costo    DECIMAL(10,2) NOT NULL,
    precio_venta    DECIMAL(10,2) NOT NULL,
    inventario      INT NOT NULL DEFAULT 0,
    actualizado_en  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (categoria_id) REFERENCES categorias(id),
    FOREIGN KEY (laboratorio_id) REFERENCES laboratorios(id)
) ENGINE=InnoDB;

CREATE TABLE promociones (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    medicamento_id INT NOT NULL,
    descripcion    VARCHAR(150) NOT NULL,
    descuento_pct  DECIMAL(5,2) NOT NULL,
    fecha_inicio   DATE NOT NULL,
    fecha_fin      DATE NOT NULL,
    activa         BOOLEAN NOT NULL DEFAULT TRUE,
    FOREIGN KEY (medicamento_id) REFERENCES medicamentos(id)
) ENGINE=InnoDB;

CREATE TABLE ventas (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    fecha       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total       DECIMAL(10,2) NOT NULL DEFAULT 0,
    metodo_pago ENUM('efectivo', 'tarjeta', 'transferencia') NOT NULL DEFAULT 'efectivo'
) ENGINE=InnoDB;

CREATE TABLE detalle_venta (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    venta_id        INT NOT NULL,
    medicamento_id  INT NOT NULL,
    cantidad        INT NOT NULL,
    precio_unitario DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (venta_id) REFERENCES ventas(id),
    -- Sin ON DELETE CASCADE a propósito: MySQL no escribe al binlog las
    -- filas borradas por cascada de InnoDB, así que Debezium nunca las
    -- vería y la Base Central quedaría con basura huérfana. Cualquier
    -- baja de un medicamento con ventas asociadas debe borrar primero
    -- su detalle_venta explícitamente (ver scripts/3-simular-cambios.sh)
    -- — igual que tendría que hacerlo cualquier app real sobre un pipeline CDC.
    FOREIGN KEY (medicamento_id) REFERENCES medicamentos(id)
) ENGINE=InnoDB;

-- Catálogo de referencia común a las 3 sucursales (categorías y
-- laboratorios). Lo que sí varía por sucursal es el subconjunto de
-- medicamentos que cada una tiene en inventario, sus precios y sus
-- promociones/ventas — eso vive en seed-sucursalNN.sql.
INSERT INTO categorias (id, nombre) VALUES
    (1, 'Analgésicos'),
    (2, 'Antibióticos'),
    (3, 'Antihistamínicos'),
    (4, 'Gastrointestinal'),
    (5, 'Cardiovascular'),
    (6, 'Metabólico');

INSERT INTO laboratorios (id, nombre, pais) VALUES
    (1, 'Bayer', 'Alemania'),
    (2, 'Pfizer', 'Estados Unidos'),
    (3, 'Genfar', 'Colombia'),
    (4, 'Novartis', 'Suiza'),
    (5, 'Roche', 'Suiza');

-- Usuario dedicado para Debezium, con únicamente los privilegios que
-- necesita el conector para leer el binlog (sin acceso de escritura a
-- los datos de negocio).
CREATE USER 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'dbz_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
    ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
