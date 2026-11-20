-- =============================================================
-- PME_base - PLATA PME (tipada y sin duplicados)
-- =============================================================
-- 1 fila por punto de medicion (CIRCUITO) e intervalo de 15 min (FECHA_LECTURA).
-- * Duplicados (clave CIRCUITO + FECHA_LECTURA), dos casos en origen:
--   a) filas identicas repetidas (2026-03 viene en 2 archivos);
--   b) el mismo CIRCUITO reportado con 2 nombres de SUBESTACION (viejo y nuevo) y
--      valores identicos (9 puntos SERSolar/RECONECTADOR, 2026-02-01..24).
--   Se conserva 1 fila, prefiriendo SUBESTACION = CIRCUITO (nombre nuevo).
--   COPIAS_ORIGEN = filas de bronce, SUBESTACIONES_ORIGEN = nombres distintos,
--   FLAG_VALORES_DISTINTOS = 1 si las copias no traen los mismos valores.
-- * Tipos y nombres alineados con AMI_base (plata_carga / plata_950_carga):
--   FECHA_LECTURA timestamp, FECHA date, HORA int, INTERVALO 15, ENERGIA/DEMANDA double.
-- * DEMANDA conserva el signo de la energia activa: PME entrega POTENCIA_KW
--   = 4 x |ENERGIA_ACTIVA| (pierde el signo en flujo inverso). DEMANDA_PME_ORIGINAL
--   guarda el valor tal como viene.
-- * TIPO_PUNTO segun el prefijo del CIRCUITO (SUB_*, PLANTA, CLIENTES, SERSolar,
--   RECONECTADOR, CNFL).
-- =============================================================
CREATE OR REFRESH MATERIALIZED VIEW pme_base_plata_pme
COMMENT 'PME_base plata: lecturas PME tipadas, 1 fila por CIRCUITO e intervalo de 15 min (sin duplicados), DEMANDA con signo.'
CLUSTER BY (periodo, mes, CIRCUITO)
AS
WITH tipado AS (
  SELECT
    periodo,
    mes,
    to_timestamp(concat(DIA, ' ', HORA), 'dd/MM/yyyy HH:mm')                AS FECHA_LECTURA,
    SUBESTACION,
    CIRCUITO,
    REGION,
    CAST(ENERGIA_ACTIVA AS DOUBLE)                                          AS ENERGIA_ACTIVA,
    CAST(ENERGIA_REACTIVA AS DOUBLE)                                        AS ENERGIA_REACTIVA,
    CAST(CASE WHEN ENERGIA_ACTIVA < 0 THEN -abs(POTENCIA_KW) ELSE POTENCIA_KW END AS DOUBLE) AS DEMANDA,
    CAST(POTENCIA_KW AS DOUBLE)                                             AS DEMANDA_PME_ORIGINAL,
    ARCHIVO,
    -- prioridad para elegir la fila: nombre nuevo (SUBESTACION = CIRCUITO) y luego archivo
    named_struct('nuevo', SUBESTACION = CIRCUITO, 'archivo', ARCHIVO)       AS prioridad
  FROM pme_base_bronce_pme
)
SELECT
  periodo,
  mes,
  FECHA_LECTURA,
  to_date(FECHA_LECTURA)                                              AS FECHA,
  hour(FECHA_LECTURA)                                                 AS HORA,
  15                                                                  AS INTERVALO,
  CASE
    WHEN CIRCUITO LIKE 'SUB\_%'         THEN 'Subestacion'
    WHEN CIRCUITO LIKE 'PLANTA.%'       THEN 'Planta'
    WHEN CIRCUITO LIKE 'CLIENTES.%'     THEN 'Cliente'
    WHEN CIRCUITO LIKE 'SERSolar.%'     THEN 'Solar'
    WHEN CIRCUITO LIKE 'RECONECTADOR.%' THEN 'Reconectador'
    WHEN CIRCUITO LIKE 'CNFL.%'         THEN 'CNFL'
    ELSE 'Otro'
  END                                                                 AS TIPO_PUNTO,
  max_by(SUBESTACION, prioridad)                                      AS SUBESTACION,
  CIRCUITO,
  max_by(REGION, prioridad)                                           AS REGION,
  max_by(ENERGIA_ACTIVA, prioridad)                                   AS ENERGIA_ACTIVA,
  max_by(ENERGIA_REACTIVA, prioridad)                                 AS ENERGIA_REACTIVA,
  max_by(DEMANDA, prioridad)                                          AS DEMANDA,
  max_by(DEMANDA_PME_ORIGINAL, prioridad)                             AS DEMANDA_PME_ORIGINAL,
  COUNT(*)                                                            AS COPIAS_ORIGEN,
  COUNT(DISTINCT SUBESTACION)                                         AS SUBESTACIONES_ORIGEN,
  CASE WHEN COUNT(DISTINCT ENERGIA_ACTIVA, ENERGIA_REACTIVA, DEMANDA_PME_ORIGINAL) > 1
       THEN 1 ELSE 0 END                                              AS FLAG_VALORES_DISTINTOS,
  max_by(ARCHIVO, prioridad)                                          AS ARCHIVO
FROM tipado
GROUP BY periodo, mes, CIRCUITO, FECHA_LECTURA;
