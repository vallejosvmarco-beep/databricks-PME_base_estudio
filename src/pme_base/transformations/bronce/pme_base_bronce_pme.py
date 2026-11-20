from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.ventana import meses_ventana_pipeline, ruta_mes

# =============================================================
# PME_base - BRONCE PME (logica copiada de AMI_base: src/ami_base/bronce/pme.py)
# =============================================================
# Ingesta incremental con Auto Loader desde el volumen de aterrizaje:
#   <pme_base_path>/source=PME/format=PARQUET/periodo=AAAA/mes=M/[r1|r2/]*.parquet
# Streaming table con un append flow por mes de la ventana (utilities/ventana.py;
# en dev = todos los meses del landing). Cada corrida solo lee archivos nuevos;
# al aparecer un mes nuevo se crea su flujo y solo se ingiere ese mes.
# periodo y mes salen de la ruta (mes reemplaza la columna MES texto del parquet),
# igual que en AMI_base. ARCHIVO y FECHA_INGESTA para trazabilidad.
# =============================================================

TABLA = "pme_base_bronce_pme"


dp.create_streaming_table(
    name=TABLA,
    comment="PME_base bronce: lecturas PME (parquet de vol_alunizaje/ami/PME), ingesta incremental por mes con Auto Loader.",
    partition_cols=["periodo", "mes"],
    table_properties={
        "delta.enableRowTracking": "true",
        "delta.enableDeletionVectors": "true",
    },
)


def _crear_flujo_mes(p: int, m: int):
    @dp.append_flow(target=TABLA, name=f"{TABLA}_{p}_{m:02d}")
    def _flujo():
        path = ruta_mes(spark.conf.get("pme_base.path"), p, m)
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load(path)
            .withColumn("periodo", F.lit(p))
            .withColumn("mes", F.lit(m))
            .withColumn("ARCHIVO", F.col("_metadata.file_path"))
            .withColumn("FECHA_INGESTA", F.current_timestamp())
        )


for _p, _m in meses_ventana_pipeline(spark):
    _crear_flujo_mes(_p, _m)
