"""Ventana de meses PME_base (copiada de AMI_base: src/ami_base/utilities/ventana.py).

Los meses se descubren listando el landing PME:
    <pme_base_path>/source=PME/format=PARQUET/periodo=<yyyy>/mes=<m>/[r1|r2/]*.parquet
Solo cuentan los meses que tienen al menos un .parquet.

Parametros (configuracion del pipeline, pme_base.*):
    periodo, mes        "ultimo" = ultimo mes disponible en el landing,
                        o un mes fijo (ej. "2026", "10") como fin de la ventana.
    meses_historial     "1" = solo el mes final; "N" = N meses hacia atras;
                        "todos" = todos los meses disponibles hasta el mes final.
"""
import os
import re

SOURCE = "PME"

_RE_PERIODO = re.compile(r"periodo=(\d{4})")
_RE_MES = re.compile(r"mes=(\d{1,2})")


def _raiz(base: str) -> str:
    return f"{base.rstrip('/')}/source={SOURCE}/format=PARQUET"


def ruta_mes(base: str, periodo: int, mes: int) -> str:
    return f"{_raiz(base)}/periodo={periodo}/mes={mes}"


def _tiene_parquet(path: str) -> bool:
    for _, _, archivos in os.walk(path):
        if any(a.endswith(".parquet") for a in archivos):
            return True
    return False


def meses_disponibles(base: str) -> list:
    raiz = _raiz(base)
    if not os.path.isdir(raiz):
        return []
    meses = set()
    for carpeta_p in os.listdir(raiz):
        mp = _RE_PERIODO.fullmatch(carpeta_p)
        if not mp:
            continue
        for carpeta_m in os.listdir(f"{raiz}/{carpeta_p}"):
            mm = _RE_MES.fullmatch(carpeta_m)
            if not mm:
                continue
            p, m = int(mp.group(1)), int(mm.group(1))
            if 1 <= m <= 12 and _tiene_parquet(f"{raiz}/{carpeta_p}/{carpeta_m}"):
                meses.add((p, m))
    return sorted(meses)


def meses_ventana(base: str, periodo="ultimo", mes="ultimo", historial="todos") -> list:
    """Lista ordenada de (periodo, mes) a ingerir."""
    disponibles = meses_disponibles(base)
    if not disponibles:
        raise ValueError(f"No hay meses con datos PME bajo {base}")

    periodo, mes, historial = (str(v).strip().lower() for v in (periodo, mes, historial))
    if periodo == "ultimo" or mes == "ultimo":
        fin = disponibles[-1]
    else:
        fin = (int(periodo), int(mes))
        if not 1 <= fin[1] <= 12:
            raise ValueError(f"Mes invalido: {mes}")
    fin_i = fin[0] * 12 + fin[1] - 1

    if historial == "todos":
        ini_i = None
    else:
        n = int(historial)
        if n < 1:
            raise ValueError(f"meses_historial invalido: {historial}")
        ini_i = fin_i - n + 1

    ventana = [
        (p, m) for p, m in disponibles
        if p * 12 + m - 1 <= fin_i and (ini_i is None or p * 12 + m - 1 >= ini_i)
    ]
    if not ventana:
        raise ValueError(f"La ventana PME esta vacia (fin={fin}, historial={historial})")
    return ventana


def meses_ventana_pipeline(spark) -> list:
    """Ventana leida de la configuracion del pipeline (pme_base.*)."""
    return meses_ventana(
        spark.conf.get("pme_base.path"),
        spark.conf.get("pme_base.periodo", "ultimo"),
        spark.conf.get("pme_base.mes", "ultimo"),
        spark.conf.get("pme_base.meses_historial", "todos"),
    )
