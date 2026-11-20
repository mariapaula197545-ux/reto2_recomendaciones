
-- Crear base de datos lógica
CREATE KEYSPACE IF NOT EXISTS reto2_retail
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE reto2_retail;

-- Tabla 1: navegación por cliente
CREATE TABLE IF NOT EXISTS navegacion_por_cliente (
    cliente_id text,
    fecha_evento timestamp,
    evento_id uuid,
    sesion_id text,
    tipo_evento text,
    producto_id text,
    PRIMARY KEY (
        (cliente_id),
        fecha_evento,
        evento_id
    )
) WITH CLUSTERING ORDER BY (
    fecha_evento DESC,
    evento_id ASC
);

-- Tabla 2: eventos de cada sesión
CREATE TABLE IF NOT EXISTS eventos_por_sesion (
    sesion_id text,
    fecha_evento timestamp,
    evento_id uuid,
    cliente_id text,
    tipo_evento text,
    producto_id text,
    PRIMARY KEY (
        (sesion_id),
        fecha_evento,
        evento_id
    )
) WITH CLUSTERING ORDER BY (
    fecha_evento ASC,
    evento_id ASC
);

-- Tabla 3: información de las sesiones
CREATE TABLE IF NOT EXISTS sesiones_por_id (
    sesion_id text PRIMARY KEY,
    cliente_id text,
    inicio timestamp,
    ultima_actividad timestamp,
    estado text
);
