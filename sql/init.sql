-- =============================================================================
-- Script de Inicialización de Base de Datos: Observatorio Sísmico
-- PostgreSQL 16
-- Modelo Dimensional y Tablas de Hechos, Versiones y Cuarentena
-- =============================================================================

-- 1. Dimensión: Catálogo Geográfico de Países (Natural Earth 1:110m)
CREATE TABLE IF NOT EXISTS pais (
    country_id VARCHAR(10) PRIMARY KEY, -- Código ADM0_A3 único
    name VARCHAR(100) NOT NULL,         -- Nombre común en español/inglés
    continent VARCHAR(50),              -- Continente
    subregion VARCHAR(50)               -- Subregión geográfica
);

-- 2. Dimensión: Fecha (Día UTC)
CREATE TABLE IF NOT EXISTS fecha (
    fecha_id DATE PRIMARY KEY,
    anio INT NOT NULL,
    mes INT NOT NULL,
    dia INT NOT NULL,
    dia_semana INT NOT NULL,
    nombre_mes VARCHAR(20),
    nombre_dia VARCHAR(20)
);

-- 3. Tabla de Hechos: Evento Actual (estado vigente y deduplicado de cada sismo)
CREATE TABLE IF NOT EXISTS evento_actual (
    event_id VARCHAR(50) PRIMARY KEY,
    time_utc TIMESTAMPTZ NOT NULL,
    updated_utc TIMESTAMPTZ NOT NULL,
    mag NUMERIC(4,2),
    mag_type VARCHAR(10),
    depth_km NUMERIC(7,2),
    latitude NUMERIC(9,6) NOT NULL,
    longitude NUMERIC(9,6) NOT NULL,
    place TEXT,
    country_id VARCHAR(10) REFERENCES pais(country_id) ON DELETE SET NULL,
    fecha_id DATE REFERENCES fecha(fecha_id),
    mode VARCHAR(20) NOT NULL DEFAULT 'live', -- 'live' o 'replay'
    raw_properties JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Tabla de Histórico: Versiones recibidas (inmutable y trazable)
CREATE TABLE IF NOT EXISTS evento_version (
    event_id VARCHAR(50) NOT NULL,
    updated_utc TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mag NUMERIC(4,2),
    mag_type VARCHAR(10),
    depth_km NUMERIC(7,2),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    mode VARCHAR(20) NOT NULL DEFAULT 'live',
    raw_payload JSONB NOT NULL,
    PRIMARY KEY (event_id, updated_utc)
);

-- 5. Tabla de Cuarentena: Registros defectuosos, no conformes o no parseables
CREATE TABLE IF NOT EXISTS cuarentena (
    id BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_id VARCHAR(50),
    raw_payload JSONB,
    reason TEXT NOT NULL
);

-- 6. Tabla de Estado Operativo e Ingesta
CREATE TABLE IF NOT EXISTS estado_ingesta (
    clave VARCHAR(50) PRIMARY KEY,
    valor TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- Índices justificados para el rendimiento de la API y agregaciones
-- =============================================================================

-- Índice temporal: acelera filtros por intervalo de tiempo (start, end, últimas 24h)
CREATE INDEX IF NOT EXISTS idx_evento_actual_time ON evento_actual(time_utc DESC);

-- Índice por magnitud: optimiza filtros por umbral mínimo (mag >= X) y distribuciones
CREATE INDEX IF NOT EXISTS idx_evento_actual_mag ON evento_actual(mag);

-- Índice por país: optimiza filtros geográficos y agregaciones GROUP BY country_id
CREATE INDEX IF NOT EXISTS idx_evento_actual_country ON evento_actual(country_id);

-- Índice por modo: aísla las consultas de datos en vivo de las de reproducción (replay)
CREATE INDEX IF NOT EXISTS idx_evento_actual_mode ON evento_actual(mode);

-- Índice en versiones: optimiza consultas de auditoría cronológica
CREATE INDEX IF NOT EXISTS idx_evento_version_received ON evento_version(received_at DESC);

-- =============================================================================
-- Carga inicial del catálogo de países y estado operativo
-- =============================================================================

INSERT INTO estado_ingesta (clave, valor, updated_at)
VALUES ('last_ingestion_success', '0', NOW())
ON CONFLICT (clave) DO NOTHING;

INSERT INTO pais (country_id, name, continent, subregion) VALUES
('FJI', 'Fiyi', 'Oceania', 'Melanesia'),
('TZA', 'Tanzania', 'Africa', 'Eastern Africa'),
('SAH', 'Sahara Occidental', 'Africa', 'Northern Africa'),
('CAN', 'Canadá', 'North America', 'Northern America'),
('USA', 'Estados Unidos', 'North America', 'Northern America'),
('KAZ', 'Kazajistán', 'Asia', 'Central Asia'),
('UZB', 'Uzbekistán', 'Asia', 'Central Asia'),
('PNG', 'Papúa Nueva Guinea', 'Oceania', 'Melanesia'),
('IDN', 'Indonesia', 'Asia', 'South-Eastern Asia'),
('ARG', 'Argentina', 'South America', 'South America'),
('CHL', 'Chile', 'South America', 'South America'),
('COD', 'República Democrática del Congo', 'Africa', 'Middle Africa'),
('SOM', 'Somalia', 'Africa', 'Eastern Africa'),
('KEN', 'Kenia', 'Africa', 'Eastern Africa'),
('SDN', 'Sudán', 'Africa', 'Northern Africa'),
('TCD', 'Chad', 'Africa', 'Middle Africa'),
('HTI', 'Haití', 'North America', 'Caribbean'),
('DOM', 'República Dominicana', 'North America', 'Caribbean'),
('RUS', 'Rusia', 'Europe', 'Eastern Europe'),
('BHS', 'Bahamas', 'North America', 'Caribbean'),
('FLK', 'Islas Malvinas', 'South America', 'South America'),
('NOR', 'Noruega', 'Europe', 'Northern Europe'),
('GRL', 'Groenlandia', 'North America', 'Northern America'),
('ATF', 'Tierras Australes y Antárticas Francesas', 'Seven seas (open ocean)', 'Seven seas (open ocean)'),
('TLS', 'Timor Oriental', 'Asia', 'South-Eastern Asia'),
('ZAF', 'Sudáfrica', 'Africa', 'Southern Africa'),
('LSO', 'Lesoto', 'Africa', 'Southern Africa'),
('MEX', 'México', 'North America', 'Central America'),
('URY', 'Uruguay', 'South America', 'South America'),
('BRA', 'Brasil', 'South America', 'South America'),
('BOL', 'Bolivia', 'South America', 'South America'),
('PER', 'Perú', 'South America', 'South America'),
('COL', 'Colombia', 'South America', 'South America'),
('PAN', 'Panamá', 'North America', 'Central America'),
('CRI', 'Costa Rica', 'North America', 'Central America'),
('NIC', 'Nicaragua', 'North America', 'Central America'),
('HND', 'Honduras', 'North America', 'Central America'),
('SLV', 'El Salvador', 'North America', 'Central America'),
('GTM', 'Guatemala', 'North America', 'Central America'),
('BLZ', 'Belice', 'North America', 'Central America'),
('VEN', 'Venezuela', 'South America', 'South America'),
('GUY', 'Guyana', 'South America', 'South America'),
('SUR', 'Surinam', 'South America', 'South America'),
('FRA', 'Francia', 'Europe', 'Western Europe'),
('ECU', 'Ecuador', 'South America', 'South America'),
('PRI', 'Puerto Rico', 'North America', 'Caribbean'),
('JAM', 'Jamaica', 'North America', 'Caribbean'),
('CUB', 'Cuba', 'North America', 'Caribbean'),
('ZWE', 'Zimbabue', 'Africa', 'Eastern Africa'),
('BWA', 'Botsuana', 'Africa', 'Southern Africa'),
('NAM', 'Namibia', 'Africa', 'Southern Africa'),
('SEN', 'Senegal', 'Africa', 'Western Africa'),
('MLI', 'Malí', 'Africa', 'Western Africa'),
('MRT', 'Mauritania', 'Africa', 'Western Africa'),
('BEN', 'Benín', 'Africa', 'Western Africa'),
('NER', 'Níger', 'Africa', 'Western Africa'),
('NGA', 'Nigeria', 'Africa', 'Western Africa'),
('CMR', 'Camerún', 'Africa', 'Middle Africa'),
('TGO', 'Togo', 'Africa', 'Western Africa'),
('GHA', 'Ghana', 'Africa', 'Western Africa'),
('CIV', 'Costa de Marfil', 'Africa', 'Western Africa'),
('GIN', 'Guinea', 'Africa', 'Western Africa'),
('GNB', 'Guinea-Bisáu', 'Africa', 'Western Africa'),
('LBR', 'Liberia', 'Africa', 'Western Africa'),
('SLE', 'Sierra Leona', 'Africa', 'Western Africa'),
('BFA', 'Burkina Faso', 'Africa', 'Western Africa'),
('CAF', 'República Centroafricana', 'Africa', 'Middle Africa'),
('COG', 'República del Congo', 'Africa', 'Middle Africa'),
('GAB', 'Gabón', 'Africa', 'Middle Africa'),
('GNQ', 'Guinea Ecuatorial', 'Africa', 'Middle Africa'),
('ZMB', 'Zambia', 'Africa', 'Eastern Africa'),
('MWI', 'Malaui', 'Africa', 'Eastern Africa'),
('MOZ', 'Mozambique', 'Africa', 'Eastern Africa'),
('SWZ', 'Suazilandia', 'Africa', 'Southern Africa'),
('AGO', 'Angola', 'Africa', 'Middle Africa'),
('BDI', 'Burundi', 'Africa', 'Eastern Africa'),
('ISR', 'Israel', 'Asia', 'Western Asia'),
('LBN', 'Líbano', 'Asia', 'Western Asia'),
('MDG', 'Madagascar', 'Africa', 'Eastern Africa'),
('PSX', 'Palestina', 'Asia', 'Western Asia'),
('GMB', 'Gambia', 'Africa', 'Western Africa'),
('TUN', 'Túnez', 'Africa', 'Northern Africa'),
('DZA', 'Argelia', 'Africa', 'Northern Africa'),
('JOR', 'Jordania', 'Asia', 'Western Asia'),
('ARE', 'Emiratos Árabes Unidos', 'Asia', 'Western Asia'),
('QAT', 'Catar', 'Asia', 'Western Asia'),
('KWT', 'Kuwait', 'Asia', 'Western Asia'),
('IRQ', 'Irak', 'Asia', 'Western Asia'),
('OMN', 'Omán', 'Asia', 'Western Asia'),
('VUT', 'Vanuatu', 'Oceania', 'Melanesia'),
('KHM', 'Camboya', 'Asia', 'South-Eastern Asia'),
('THA', 'Tailandia', 'Asia', 'South-Eastern Asia'),
('LAO', 'Laos', 'Asia', 'South-Eastern Asia'),
('MMR', 'Birmania', 'Asia', 'South-Eastern Asia'),
('VNM', 'Vietnam', 'Asia', 'South-Eastern Asia'),
('PRK', 'Corea del Norte', 'Asia', 'Eastern Asia'),
('KOR', 'Corea del Sur', 'Asia', 'Eastern Asia'),
('MNG', 'Mongolia', 'Asia', 'Eastern Asia'),
('IND', 'India', 'Asia', 'Southern Asia'),
('BGD', 'Bangladés', 'Asia', 'Southern Asia'),
('BTN', 'Bután', 'Asia', 'Southern Asia'),
('NPL', 'Nepal', 'Asia', 'Southern Asia'),
('PAK', 'Pakistán', 'Asia', 'Southern Asia'),
('AFG', 'Afganistán', 'Asia', 'Southern Asia'),
('TJK', 'Tayikistán', 'Asia', 'Central Asia'),
('KGZ', 'Kirguistán', 'Asia', 'Central Asia'),
('TKM', 'Turkmenistán', 'Asia', 'Central Asia'),
('IRN', 'Irán', 'Asia', 'Southern Asia'),
('SYR', 'Siria', 'Asia', 'Western Asia'),
('ARM', 'Armenia', 'Asia', 'Western Asia'),
('SWE', 'Suecia', 'Europe', 'Northern Europe'),
('BLR', 'Bielorrusia', 'Europe', 'Eastern Europe'),
('UKR', 'Ucrania', 'Europe', 'Eastern Europe'),
('POL', 'Polonia', 'Europe', 'Eastern Europe'),
('AUT', 'Austria', 'Europe', 'Western Europe'),
('HUN', 'Hungría', 'Europe', 'Eastern Europe'),
('MDA', 'Moldavia', 'Europe', 'Eastern Europe'),
('ROU', 'Rumania', 'Europe', 'Eastern Europe'),
('LTU', 'Lituania', 'Europe', 'Northern Europe'),
('LVA', 'Letonia', 'Europe', 'Northern Europe'),
('EST', 'Estonia', 'Europe', 'Northern Europe'),
('DEU', 'Alemania', 'Europe', 'Western Europe'),
('BGR', 'Bulgaria', 'Europe', 'Eastern Europe'),
('GRC', 'Grecia', 'Europe', 'Southern Europe'),
('TUR', 'Turquía', 'Asia', 'Western Asia'),
('ALB', 'Albania', 'Europe', 'Southern Europe'),
('HRV', 'Croacia', 'Europe', 'Southern Europe'),
('CHE', 'Suiza', 'Europe', 'Western Europe'),
('LUX', 'Luxemburgo', 'Europe', 'Western Europe'),
('BEL', 'Bélgica', 'Europe', 'Western Europe'),
('NLD', 'Países Bajos', 'Europe', 'Western Europe'),
('PRT', 'Portugal', 'Europe', 'Southern Europe'),
('ESP', 'España', 'Europe', 'Southern Europe'),
('IRL', 'Irlanda', 'Europe', 'Northern Europe'),
('NCL', 'Nueva Caledonia', 'Oceania', 'Melanesia'),
('SLB', 'Islas Salomón', 'Oceania', 'Melanesia'),
('NZL', 'Nueva Zelanda', 'Oceania', 'Australia and New Zealand'),
('AUS', 'Australia', 'Oceania', 'Australia and New Zealand'),
('LKA', 'Sri Lanka', 'Asia', 'Southern Asia'),
('CHN', 'China', 'Asia', 'Eastern Asia'),
('TWN', 'República de China', 'Asia', 'Eastern Asia'),
('ITA', 'Italia', 'Europe', 'Southern Europe'),
('DNK', 'Dinamarca', 'Europe', 'Northern Europe'),
('GBR', 'Reino Unido', 'Europe', 'Northern Europe'),
('ISL', 'Islandia', 'Europe', 'Northern Europe'),
('AZE', 'Azerbaiyán', 'Asia', 'Western Asia'),
('GEO', 'Georgia', 'Asia', 'Western Asia'),
('PHL', 'Filipinas', 'Asia', 'South-Eastern Asia'),
('MYS', 'Malasia', 'Asia', 'South-Eastern Asia'),
('BRN', 'Brunéi', 'Asia', 'South-Eastern Asia'),
('SVN', 'Eslovenia', 'Europe', 'Southern Europe'),
('FIN', 'Finlandia', 'Europe', 'Northern Europe'),
('SVK', 'Eslovaquia', 'Europe', 'Eastern Europe'),
('CZE', 'República Checa', 'Europe', 'Eastern Europe'),
('ERI', 'Eritrea', 'Africa', 'Eastern Africa'),
('JPN', 'Japón', 'Asia', 'Eastern Asia'),
('PRY', 'Paraguay', 'South America', 'South America'),
('YEM', 'Yemen', 'Asia', 'Western Asia'),
('SAU', 'Arabia Saudita', 'Asia', 'Western Asia'),
('ATA', 'Antártida', 'Antarctica', 'Antarctica'),
('CYN', 'República Turca del Norte de Chipre', 'Asia', 'Western Asia'),
('CYP', 'Chipre', 'Asia', 'Western Asia'),
('MAR', 'Marruecos', 'Africa', 'Northern Africa'),
('EGY', 'Egipto', 'Africa', 'Northern Africa'),
('LBY', 'Libia', 'Africa', 'Northern Africa'),
('ETH', 'Etiopía', 'Africa', 'Eastern Africa'),
('DJI', 'Yibuti', 'Africa', 'Eastern Africa'),
('SOL', 'Somalilandia', 'Africa', 'Eastern Africa'),
('UGA', 'Uganda', 'Africa', 'Eastern Africa'),
('RWA', 'Ruanda', 'Africa', 'Eastern Africa'),
('BIH', 'Bosnia y Herzegovina', 'Europe', 'Southern Europe'),
('MKD', 'Macedonia del Norte', 'Europe', 'Southern Europe'),
('SRB', 'Serbia', 'Europe', 'Southern Europe'),
('MNE', 'Montenegro', 'Europe', 'Southern Europe'),
('KOS', 'Kosovo', 'Europe', 'Southern Europe'),
('TTO', 'Trinidad y Tobago', 'North America', 'Caribbean'),
('SDS', 'Sudán del Sur', 'Africa', 'Eastern Africa')
ON CONFLICT (country_id) DO NOTHING;

