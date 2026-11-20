# PME_base

Bundle (Declarative Automation Bundle) del proyecto PME_base.

## Origen de datos

Los datos PME se leen del volumen de aterrizaje:

`/Volumes/datosami/datos/vol_alunizaje/ami/PME/`

Estructura: `source=PME/format=PARQUET/periodo=AAAA/mes=M/*.parquet` (un parquet por mes; 2024-01 a 2026-10 al 2026-11-20).

## Destino

Catalogo `datosami`, esquema existente `datos` (variables `catalog` y `schema`). No se crean esquemas ni volumenes: el usuario no tiene CREATE SCHEMA en `datosami`.

## Variables

* `catalog`: `datosami`
* `schema`: `datos`
* `pme_base_path`: `/Volumes/datosami/datos/vol_alunizaje/ami/PME/`
* `warehouse_id`: en `dev`, `fac1da9aa4b8e6dd` (Serverless Starter Warehouse)

## Uso

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run pme_base_initialize --target dev
```

El job `PME_base_initialize` solo verifica el esquema destino y lista el origen PME.

## Pipeline PME_base

Pipeline serverless `PME_base` (`resources/pme_base.pipeline.yml`) con la streaming table `datosami.datos.pme_base_bronce_pme`, alimentada con Auto Loader (`cloudFiles`, parquet), con la misma logica que tenia AMI_base: un append flow por mes de la ventana (`src/pme_base/utilities/ventana.py`), particion (`periodo`, `mes`) tomada de la ruta (`mes` entero reemplaza la columna `MES` texto del parquet), mas `ARCHIVO` y `FECHA_INGESTA`. Cada corrida solo ingiere archivos nuevos.

Ventana: `pme_periodo`/`pme_mes` = `ultimo` (o mes fijo), `pme_meses_historial` = `todos` (o `1`/`N`).

PME ya no se ingiere en AMI_base. `datosami.datos.pme` es una vista de compatibilidad sobre `pme_base_bronce_pme` (mismas columnas que la antigua tabla de AMI_base, todo el historial).

Nota: 2026-03 trae dos archivos con las mismas filas (duplicado en origen); bronce lo conserva tal cual.

## Plata PME

Materialized view `datosami.datos.pme_base_plata_pme`: 1 fila por `CIRCUITO` y `FECHA_LECTURA` (intervalo de 15 min), tipada y alineada con AMI_base (`FECHA_LECTURA`, `FECHA`, `HORA`, `INTERVALO`, `ENERGIA_ACTIVA`, `ENERGIA_REACTIVA`, `DEMANDA` en double).

* Quita dos duplicados de origen: filas identicas de 2026-03 (707,066) y 9 puntos SERSolar/RECONECTADOR reportados con dos nombres de `SUBESTACION` del 2026-02-01 al 2026-02-24 (17,767); se conserva el nombre nuevo (`SUBESTACION = CIRCUITO`).
* `DEMANDA` lleva el signo de la energia activa (PME entrega `POTENCIA_KW` = 4 x |energia|); `DEMANDA_PME_ORIGINAL` guarda el valor de origen.
* `TIPO_PUNTO`: Subestacion, Planta, Cliente, Solar, Reconectador, CNFL.
* Trazabilidad: `COPIAS_ORIGEN`, `SUBESTACIONES_ORIGEN`, `FLAG_VALORES_DISTINTOS` (1 si las copias difieren), `ARCHIVO`.

## Estructura

* `databricks.yml`: configuracion del bundle
* `resources/init.yml`: job `PME_base_initialize`
* `resources/pme_base.pipeline.yml`: pipeline `PME_base`
* `src/pme_base/transformations/bronce/pme_base_bronce_pme.py`: bronce PME con Auto Loader
* `src/pme_base/utilities/ventana.py`: ventana de meses (copiada de AMI_base)
* `src/pme_base/transformations/plata/pme_base_plata_pme.sql`: plata PME tipada y sin duplicados
* `src/initialize`: notebook de verificacion del origen PME_base
* `src/initialize.sql`: SQL de verificacion usado por el job
