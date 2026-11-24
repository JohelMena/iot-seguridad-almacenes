
BEGIN;

-- =============================================
-- DATA WAREHOUSE - SEGURIDAD INDUSTRIAL
-- =============================================

CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS etl;


-- =============================================
-- DIMENSION FECHA
-- =============================================

CREATE TABLE IF NOT EXISTS dw.dim_fecha (
    fecha_id DATE PRIMARY KEY,
    anio INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    dia_semana INTEGER NOT NULL,
    nombre_dia VARCHAR(20) NOT NULL,
    nombre_mes VARCHAR(20) NOT NULL
);


-- =============================================
-- DIMENSION DISPOSITIVO
-- =============================================

CREATE TABLE IF NOT EXISTS dw.dim_dispositivo (
    dispositivo_id SERIAL PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    source VARCHAR(20) NOT NULL,

    UNIQUE (device_id, source),

    CHECK (source IN ('physical', 'virtual'))
);


-- =============================================
-- DIMENSION ZONA
-- =============================================

CREATE TABLE IF NOT EXISTS dw.dim_zona (
    zona_id SERIAL PRIMARY KEY,
    warehouse_id VARCHAR(50) NOT NULL,
    zone_code VARCHAR(50) NOT NULL,
    descripcion VARCHAR(150),

    UNIQUE (warehouse_id, zone_code)
);


-- =============================================
-- DIMENSION TIPO DE EVENTO
-- =============================================

CREATE TABLE IF NOT EXISTS dw.dim_tipo_evento (
    tipo_evento_id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) UNIQUE NOT NULL,
    descripcion VARCHAR(200)
);


-- =============================================
-- TABLA DE HECHOS
-- =============================================

CREATE TABLE IF NOT EXISTS dw.fact_eventos_seguridad (
    event_id UUID PRIMARY KEY,

    fecha_id DATE NOT NULL
        REFERENCES dw.dim_fecha(fecha_id),

    dispositivo_id INTEGER NOT NULL
        REFERENCES dw.dim_dispositivo(dispositivo_id),

    zona_id INTEGER NOT NULL
        REFERENCES dw.dim_zona(zona_id),

    tipo_evento_id INTEGER NOT NULL
        REFERENCES dw.dim_tipo_evento(tipo_evento_id),

    timestamp_evento TIMESTAMPTZ NOT NULL,
    hora SMALLINT NOT NULL,

    system_armed BOOLEAN NOT NULL,
    motion_detected BOOLEAN NOT NULL,
    door_open BOOLEAN NOT NULL,

    distance_cm NUMERIC(10,2) NOT NULL,

    laser_detected BOOLEAN,
    relay_active BOOLEAN NOT NULL,

    response_time_ms INTEGER,
    ground_truth BOOLEAN,
    simulation_case VARCHAR(50),

    mongo_document_id VARCHAR(200) NOT NULL,

    fecha_carga TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    CHECK (hora BETWEEN 0 AND 23),
    CHECK (distance_cm BETWEEN 0 AND 400),
    CHECK (response_time_ms >= 0)
);


-- =============================================
-- CONTROL DE EJECUCIONES ETL
-- =============================================

CREATE TABLE IF NOT EXISTS etl.etl_control (
    ejecucion_id BIGSERIAL PRIMARY KEY,

    fecha_inicio TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    fecha_fin TIMESTAMPTZ,

    registros_leidos INTEGER DEFAULT 0,
    registros_insertados INTEGER DEFAULT 0,
    registros_rechazados INTEGER DEFAULT 0,
    registros_duplicados INTEGER DEFAULT 0,

    estado VARCHAR(30) DEFAULT 'EN_PROCESO',
    observaciones TEXT
);


-- =============================================
-- REGISTROS RECHAZADOS
-- =============================================

CREATE TABLE IF NOT EXISTS etl.etl_rechazos (
    rechazo_id BIGSERIAL PRIMARY KEY,

    ejecucion_id BIGINT
        REFERENCES etl.etl_control(ejecucion_id),

    mongo_document_id VARCHAR(200) UNIQUE NOT NULL,
    datos_originales JSONB,
    motivo TEXT NOT NULL,

    fecha_rechazo TIMESTAMPTZ
        DEFAULT CURRENT_TIMESTAMP
);


-- =============================================
-- CATALOGO INICIAL DE EVENTOS
-- =============================================

INSERT INTO dw.dim_tipo_evento (
    event_type,
    descripcion
)
VALUES
    ('normal', 'Actividad normal del almacen'),
    ('proximity', 'Proximidad a una zona restringida'),
    ('suspicious', 'Actividad sospechosa'),
    ('intrusion', 'Alerta de posible intrusion')

ON CONFLICT (event_type) DO NOTHING;


-- =============================================
-- CATALOGO INICIAL DE ZONAS
-- =============================================

INSERT INTO dw.dim_zona (
    warehouse_id,
    zone_code,
    descripcion
)
VALUES
    ('ALM-01', 'ZONA-01', 'Entrada principal'),
    ('ALM-01', 'ZONA-02', 'Area de almacenamiento'),
    ('ALM-01', 'ZONA-03', 'Area restringida')

ON CONFLICT (warehouse_id, zone_code)
DO NOTHING;


-- =============================================
-- INDICES PARA POWER BI
-- =============================================

CREATE INDEX IF NOT EXISTS idx_eventos_fecha
ON dw.fact_eventos_seguridad(fecha_id);

CREATE INDEX IF NOT EXISTS idx_eventos_zona
ON dw.fact_eventos_seguridad(zona_id);

CREATE INDEX IF NOT EXISTS idx_eventos_tipo
ON dw.fact_eventos_seguridad(tipo_evento_id);

CREATE INDEX IF NOT EXISTS idx_eventos_timestamp
ON dw.fact_eventos_seguridad(timestamp_evento);

CREATE INDEX IF NOT EXISTS idx_eventos_dispositivo
ON dw.fact_eventos_seguridad(dispositivo_id);

COMMIT;
