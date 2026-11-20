# Databricks notebook source
# DBTITLE 1,Verificar origen PME_base
# PME_base: verifica el destino (datosami.datos) y el origen de datos PME.
# No crea esquemas ni volumenes (se usan los existentes).
CATALOG = 'datosami'
SCHEMA = 'datos'
PME_BASE_PATH = '/Volumes/datosami/datos/vol_alunizaje/ami/PME/'

spark.sql(f'DESCRIBE SCHEMA `{CATALOG}`.`{SCHEMA}`').show(truncate=False)

origen = PME_BASE_PATH + 'source=PME/format=PARQUET/'
periodos = sorted(f.name.strip('/') for f in dbutils.fs.ls(origen))
for p in periodos:
    meses = sorted(f.name.strip('/') for f in dbutils.fs.ls(origen + p + '/'))
    print(p, meses)
print(f'Origen PME_base: {PME_BASE_PATH}')