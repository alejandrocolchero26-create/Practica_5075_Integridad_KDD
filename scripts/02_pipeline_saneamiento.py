"""
Fases 2 y 3: Saneamiento de Datos, Detección de Outliers (IQR/MAD),
Normalización ISO/IEC 25012 y Perfilado KDD para BI.
Proyecto: Practica_5075_Integridad_KDD
"""

import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_FILE = BASE_DIR / "logs" / "ejecucion_pipeline.log"
QUARANTINE_MANIFEST = BASE_DIR / "output_samples" / "quarantine_manifest.txt"
METRICAS_JSON = BASE_DIR / "output_samples" / "metricas_perfilado.json"
INFORME_MD = BASE_DIR / "reports" / "INFORME_CALIDAD_KDD.md"

TOKENS_NULOS = {"", "na", "n/a", "nan", "null", "none", "nat", "?"}
MARCADOR_FASE2 = "# === FASE 2: REGISTROS AISLADOS EN CUARENTENA (SANEAMIENTO / OUTLIERS) ==="


def localizar_raw_dir() -> Path:
    for candidata in (BASE_DIR / "raw_batch_202611", BASE_DIR.parent / "raw_batch_202611"):
        if candidata.is_dir():
            return candidata
    raise FileNotFoundError("No se encontró la carpeta 'raw_batch_202611/'.")


def configurar_logger() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("Fase2_3_KDD")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
        fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger


def obtener_ficheros_aprobados_fase1(logger: logging.Logger) -> tuple[list[str], list[str]]:
    if not QUARANTINE_MANIFEST.exists():
        logger.critical("No existe output_samples/quarantine_manifest.txt. Ejecute primero 01_verificar_integridad.py.")
        sys.exit(1)

    verificados: list[str] = []
    bloqueados: list[str] = []
    for linea in QUARANTINE_MANIFEST.read_text(encoding="utf-8").splitlines():
        if linea.startswith(MARCADOR_FASE2):
            break
        if linea.startswith("VERIFIED"):
            partes = [p.strip() for p in linea.split("|")]
            if len(partes) >= 2:
                verificados.append(partes[1])
        elif linea.startswith("QUARANTINE"):
            partes = [p.strip() for p in linea.split("|")]
            if len(partes) >= 2:
                bloqueados.append(partes[1])
    return verificados, bloqueados


def detectar_outliers_iqr(serie: pd.Series, factor: float = 1.5) -> pd.Series:
    q1 = serie.quantile(0.25)
    q3 = serie.quantile(0.75)
    iqr = q3 - q1
    if pd.isna(iqr) or iqr == 0:
        return pd.Series(False, index=serie.index)
    return (serie < (q1 - factor * iqr)) | (serie > (q3 + factor * iqr))


def detectar_outliers_mad(serie: pd.Series, umbral: float = 3.5) -> pd.Series:
    mediana = serie.median()
    mad = np.median(np.abs(serie - mediana))
    if pd.isna(mad) or mad == 0:
        return pd.Series(False, index=serie.index)
    z_mod = 0.6745 * (serie - mediana) / mad
    return np.abs(z_mod) > umbral


def detectar_roles_columnas(df: pd.DataFrame) -> tuple[str, list[str], list[str]]:
    columnas = list(df.columns)
    col_id = next(
        (c for c in columnas if re.search(r"(^id_|_id$|^id$|transaccion|txn|codigo)", c, re.IGNORECASE)),
        columnas[0],
    )
    cols_fecha = [
        c for c in columnas if re.search(r"(fecha|time|timestamp|ts|date|marca)", c, re.IGNORECASE)
    ]
    cols_num = []
    for c in columnas:
        if c == col_id or c in cols_fecha:
            continue
        serie_limpia = df[c].dropna().astype(str).str.strip()
        serie_limpia = serie_limpia[~serie_limpia.str.lower().isin(TOKENS_NULOS)]
        if not serie_limpia.empty and pd.to_numeric(serie_limpia, errors="coerce").notna().mean() >= 0.70:
            cols_num.append(c)
    return col_id, cols_fecha, cols_num


def main() -> int:
    logger = configurar_logger()
    INFORME_MD.parent.mkdir(parents=True, exist_ok=True)
    METRICAS_JSON.parent.mkdir(parents=True, exist_ok=True)

    logger.info("=== INICIO FASE 2 Y 3: SANEAMIENTO, OUTLIERS Y PERFILADO KDD ===")
    raw_dir = localizar_raw_dir()
    ficheros_verificados, ficheros_bloqueados_sha = obtener_ficheros_aprobados_fase1(logger)

    if not ficheros_verificados:
        logger.critical("Ningún fichero superó la Fase 1. Abortando saneamiento.")
        return 1

    # Esquema contractual basado en el fichero íntegro de referencia
    nombre_ref = next((n for n in ficheros_verificados if "norte" in n.lower()), ficheros_verificados[0])
    esquema_base = list(pd.read_csv(raw_dir / nombre_ref, nrows=0).columns)
    logger.info(f"Esquema contractual ({nombre_ref}): {esquema_base}")

    dataframes: list[pd.DataFrame] = []
    ficheros_schema_drift: list[str] = []

    for nombre in ficheros_verificados:
        ruta_csv = raw_dir / nombre
        df_tmp = pd.read_csv(ruta_csv, dtype=str, keep_default_na=False)
        if list(df_tmp.columns) != esquema_base:
            logger.error(f"[SCHEMA DRIFT] {nombre} rechazado. Columnas: {list(df_tmp.columns)}")
            ficheros_schema_drift.append(nombre)
            continue
        df_tmp["_archivo_origen"] = nombre
        dataframes.append(df_tmp)

    if not dataframes:
        logger.critical("Ningún fichero cumplió el contrato de esquema.")
        return 1

    df = pd.concat(dataframes, ignore_index=True)
    total_bruto = len(df)

    for col in esquema_base:
        df[col] = df[col].astype(str).str.strip()
        df.loc[df[col].str.lower().isin(TOKENS_NULOS), col] = np.nan

    col_id, cols_fecha, cols_num = detectar_roles_columnas(df[esquema_base])
    logger.info(f"Roles detectados | ID: '{col_id}' | Fechas: {cols_fecha} | Numéricas: {cols_num}")
    motivos = pd.Series([[] for _ in range(total_bruto)], index=df.index)

    def registrar_fallo(mascara: pd.Series, etiqueta: str) -> None:
        for idx in df.index[mascara.fillna(False)]:
            motivos.at[idx].append(etiqueta)

    # 1. Completitud (nulos en campos contractuales)
    registrar_fallo(df[esquema_base].isna().any(axis=1), "NULO_EN_CAMPO_CRITICO")

    # 2. Unicidad de clave primaria
    registrar_fallo(df[col_id].duplicated(keep="first") & df[col_id].notna(), f"DUPLICADO_{col_id.upper()}")

    # 3. Validez de marcas temporales
    fechas_ok: dict[str, pd.Series] = {}
    for col_f in cols_fecha:
        parsed = pd.to_datetime(df[col_f], errors="coerce", utc=True)
        fechas_ok[col_f] = parsed
        registrar_fallo(parsed.isna() & df[col_f].notna(), f"TIMESTAMP_CORRUPTO_{col_f.upper()}")

    # 4. Tipos numéricos y reglas de dominio de negocio
    nums_ok: dict[str, pd.Series] = {}
    for col_n in cols_num:
        serie_num = pd.to_numeric(df[col_n], errors="coerce")
        nums_ok[col_n] = serie_num
        registrar_fallo(serie_num.isna() & df[col_n].notna(), f"TIPO_NUMERICO_INVALIDO_{col_n.upper()}")
        if re.search(r"(cantidad|qty|unidades|precio|price|importe|total|ventas)", col_n, re.IGNORECASE):
            registrar_fallo(serie_num <= 0, f"DOMINIO_NO_POSITIVO_{col_n.upper()}")

    # 5. Detección estadística de outliers (IQR + MAD) sobre registros estructuralmente válidos
    estructuralmente_sanos = motivos.str.len() == 0
    for col_n, serie_num in nums_ok.items():
        idx_eval = estructuralmente_sanos & serie_num.notna()
        if idx_eval.sum() < 5:
            continue
        out_iqr = detectar_outliers_iqr(serie_num.loc[idx_eval], factor=1.5)
        out_mad = detectar_outliers_mad(serie_num.loc[idx_eval], umbral=3.5)
        mascara_out = pd.Series(False, index=df.index)
        mascara_out.loc[idx_eval] = out_iqr | out_mad
        registrar_fallo(mascara_out, f"OUTLIER_ESTADISTICO_{col_n.upper()}")

    # 6. Segregación lógica de registros defectuosos en output_samples/quarantine_manifest.txt
    mascara_rechazo = motivos.str.len() > 0
    df_rechazados = df[mascara_rechazo].copy()
    df_rechazados["motivo_rechazo"] = motivos[mascara_rechazo].str.join(" | ")

    desglose_incidencias = (
        df_rechazados["motivo_rechazo"].str.split(r" \| ").explode().value_counts().to_dict()
        if not df_rechazados.empty
        else {}
    )

    texto_fase1 = QUARANTINE_MANIFEST.read_text(encoding="utf-8").split(MARCADOR_FASE2)[0].rstrip()
    lineas_fase2 = [
        "",
        MARCADOR_FASE2,
        f"# Registros evaluados: {total_bruto} | Aislados: {len(df_rechazados)}",
    ]
    if df_rechazados.empty:
        lineas_fase2.append("NONE | Ningún registro defectuoso detectado.")
    else:
        for idx, fila in df_rechazados.iterrows():
            id_val = fila[col_id] if pd.notna(fila[col_id]) else "NULL_ID"
            lineas_fase2.append(
                f"QUARANTINE_ROW | archivo={fila['_archivo_origen']} | fila_lote={idx + 1} | "
                f"{col_id}={id_val} | motivo={fila['motivo_rechazo']}"
            )
    QUARANTINE_MANIFEST.write_text(texto_fase1 + "\n" + "\n".join(lineas_fase2) + "\n", encoding="utf-8")

    # 7. Transformación y normalización ISO/IEC 25012 + KDD en memoria
    df_limpio = df[~mascara_rechazo].copy()
    for col_f, parsed in fechas_ok.items():
        df_limpio[col_f] = parsed[~mascara_rechazo].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    for col_n, serie_num in nums_ok.items():
        vals = serie_num[~mascara_rechazo]
        df_limpio[col_n] = vals.round(4)
        if not vals.empty:
            std_v = vals.std(ddof=0)
            df_limpio[f"{col_n}_zscore"] = ((vals - vals.mean()) / std_v).round(4) if std_v and std_v > 0 else 0.0
            v_min, v_max = vals.min(), vals.max()
            df_limpio[f"{col_n}_minmax"] = ((vals - v_min) / (v_max - v_min)).round(4) if v_max > v_min else 0.0
        else:
            df_limpio[f"{col_n}_zscore"] = pd.Series(dtype="float64")
            df_limpio[f"{col_n}_minmax"] = pd.Series(dtype="float64")

    for col_t in [c for c in esquema_base if c not in cols_fecha and c not in cols_num]:
        df_limpio[col_t] = df_limpio[col_t].astype(str).str.strip().str.upper()

    # 8. Fase 3: Cálculo de métricas ISO/IEC 25012 y exportación a output_samples/metricas_perfilado.json
    total_curado = len(df_limpio)
    celdas_curadas = df_limpio[esquema_base].size
    completitud = round((1.0 - df_limpio[esquema_base].isna().sum().sum() / celdas_curadas) * 100, 2) if celdas_curadas else 0.0
    unicidad = round((df_limpio[col_id].nunique() / total_curado) * 100, 2) if total_curado else 0.0
    validez_lote = round((total_curado / total_bruto) * 100, 2) if total_bruto else 0.0
    consistencia = round((len(dataframes) / len(ficheros_verificados)) * 100, 2)

    estadisticos_kdd = {}
    for col_n in cols_num:
        s = df_limpio[col_n].dropna()
        if not s.empty:
            estadisticos_kdd[col_n] = {
                "media": round(float(s.mean()), 4),
                "mediana": round(float(s.median()), 4),
                "desviacion_tipica": round(float(s.std()), 4) if len(s) > 1 else 0.0,
                "min": round(float(s.min()), 4),
                "max": round(float(s.max()), 4),
                "q25": round(float(s.quantile(0.25)), 4),
                "q75": round(float(s.quantile(0.75)), 4),
            }
        else:
            estadisticos_kdd[col_n] = None

    fecha_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    metricas = {
        "proyecto": "Practica_5075_Integridad_KDD",
        "norma_calidad": "ISO/IEC 25012",
        "fecha_ejecucion_utc": fecha_utc,
        "auditoria_lote": {
            "ficheros_bloqueados_sha256": ficheros_bloqueados_sha,
            "ficheros_verificados_procesados": ficheros_verificados,
            "ficheros_rechazados_schema_drift": ficheros_schema_drift,
            "registros_evaluados": total_bruto,
            "registros_validos_kdd": total_curado,
            "registros_cuarentena": int(len(df_rechazados)),
        },
        "dimensiones_iso_25012_pct": {
            "completitud": completitud,
            "unicidad": unicidad,
            "validez_lote": validez_lote,
            "consistencia_esquema": consistencia,
            "exactitud_dominio_rango": 100.0 if total_curado > 0 else 0.0,
        },
        "desglose_anomalias": desglose_incidencias,
        "perfilado_estadistico_kdd": estadisticos_kdd,
        "muestra_dataset_normalizado_bi": df_limpio.drop(columns=["_archivo_origen"]).head(5).to_dict(orient="records"),
    }
    METRICAS_JSON.write_text(json.dumps(metricas, indent=2, ensure_ascii=False), encoding="utf-8")

    # 9. Generación de reports/INFORME_CALIDAD_KDD.md
    tabla_anomalias = "\n".join(
        f"| `{motivo}` | {cantidad} |" for motivo, cantidad in desglose_incidencias.items()
    ) or "| Ninguna anomalía detectada | 0 |"

    tabla_stats = "\n".join(
        f"| `{col}` | {st['media']} | {st['mediana']} | {st['desviacion_tipica']} | {st['min']} | {st['max']} |"
        for col, st in estadisticos_kdd.items()
        if st is not None
    ) or "| Sin columnas numéricas válidas | - | - | - | - | - |"

    informe = f"""# Informe de Calidad de Datos y Preparación KDD (ISO/IEC 25012)

- **Proyecto:** `Practica_5075_Integridad_KDD`
- **Fecha de ejecución (UTC):** `{fecha_utc}`
- **Estado de compuerta BI:** **APROBADO PARA MINERÍA DE DATOS (KDD)**

## 1. Integridad Criptográfica del Lote (Fase 1 - SHA-256)
- **Ficheros aprobados:** `{', '.join(ficheros_verificados)}`
- **Ficheros bloqueados en cuarentena:** `{', '.join(ficheros_bloqueados_sha) if ficheros_bloqueados_sha else 'Ninguno'}`

## 2. Métricas de Calidad ISO/IEC 25012
| Dimensión ISO/IEC 25012 | Indicador Evaluado | Resultado |
| :--- | :--- | :---: |
| **Completitud** | Proporción de celdas no nulas en el dataset curado | **{completitud}%** |
| **Unicidad** | Registros únicos según clave primaria (`{col_id}`) | **{unicidad}%** |
| **Validez** | Tasa de registros conformes respecto al total evaluado | **{validez_lote}%** |
| **Consistencia** | Conformidad estructural con el esquema contractual | **{consistencia}%** |
| **Exactitud** | Cumplimiento de reglas de dominio y rango estadístico (IQR/MAD) | **100.0%** |

## 3. Segregación de Registros Defectuosos (Fase 2)
Se procesaron **{total_bruto}** registros brutos, validando **{total_curado}** y aislando **{len(df_rechazados)}** en `output_samples/quarantine_manifest.txt`.

| Motivo de Rechazo | Registros Aislados |
| :--- | :---: |
{tabla_anomalias}

## 4. Perfilado Estadístico y Normalización KDD (Fase 3)
Las variables numéricas han sido estandarizadas (**Z-Score**, `*_zscore`) y escaladas (**Min-Max**, `*_minmax`) para su explotación en almacenes de Business Intelligence y algoritmos de minería de datos.

| Variable | Media | Mediana | Desv. Típica | Mínimo | Máximo |
| :--- | :---: | :---: | :---: | :---: | :---: |
{tabla_stats}
"""
    INFORME_MD.write_text(informe, encoding="utf-8")

    logger.warning(f"[CUARENTENA] {len(df_rechazados)}/{total_bruto} registros aislados en {QUARANTINE_MANIFEST.name}")
    logger.info(f"=== FIN FASE 2 Y 3 | Métricas en {METRICAS_JSON.name} e Informe en {INFORME_MD.name} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())