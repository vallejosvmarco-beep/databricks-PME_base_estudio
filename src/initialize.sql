-- PME_base: verificacion del destino y del origen de datos.
-- No crea esquemas ni volumenes: se usa el esquema existente datosami.datos
-- (sin permiso CREATE SCHEMA en datosami) y el volumen de aterrizaje existente.
-- Origen: /Volumes/datosami/datos/vol_alunizaje/ami/PME/
--         source=PME/format=PARQUET/periodo=AAAA/mes=M/*.parquet
DESCRIBE SCHEMA datosami.datos;
LIST '/Volumes/datosami/datos/vol_alunizaje/ami/PME/source=PME/format=PARQUET/';
