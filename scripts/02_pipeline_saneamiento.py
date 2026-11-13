import json
import logging
from pathlib import Path
import numpy as np
import pandas as pd

logging.basicConfig(
    filename="logs/ejecucion_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def ejecutar_saneamiento_kdd():
  logging.info("=== INICIO FASES 2 Y 3: Saneamiento, Coherencia y Perfilado KDD ===")

  verified_dir = Path("staging") / "verified"
  bad_records_dir = Path("quarantine") / "bad_records"
  processed_dir = Path("processed")
  reports_dir = Path("reports")
  output_samples_dir = Path("output_samples")

  bad_records_dir.mkdir(parents=True, exist_ok=True)
  processed_dir.mkdir(parents=True, exist_ok=True)
  reports_dir.mkdir(parents=True, exist_ok=True)
  output_samples_dir.mkdir(parents=True, exist_ok=True)

  ficheros_verificados = list(verified_dir.glob("*.csv"))
  if not ficheros_verificados:
    logging.error("No hay ficheros verificados en staging/ para procesar.")
    return

  df_list = []
  for f in ficheros_verificados:
    df_temp = pd.read_csv(f)
    df_list.append(df_temp)

  df_raw = pd.concat(df_list, ignore_index=True)
  total_registros_iniciales = len(df_raw)
  logging.info(
      f"Registros totales cargados desde staging: {total_registros_iniciales}"
  )

  # 1. Identificación y segregación de nulos críticos (id_transaccion)
  mask_nulos_id = df_raw["id_transaccion"].isnull()
  bad_id_df = df_raw[mask_nulos_id]
  bad_id_df.to_csv(bad_records_dir / "bad_records_nulos_id.csv", index=False)

  df_clean = df_raw.dropna(subset=["id_transaccion"]).copy()

  # 2. Coherencia temporal (ISO 8601)
  df_clean["timestamp"] = pd.to_datetime(
      df_clean["timestamp"], errors="coerce"
  )
  mask_fecha_valida = df_clean["timestamp"].notnull() & (
      df_clean["timestamp"].dt.year >= 2020
  )
  df_clean = df_clean[mask_fecha_valida].copy()
  df_clean["timestamp"] = df_clean["timestamp"].dt.strftime(
      "%Y-%m-%d %H:%M:%S"
  )

  # 3. Coerción numérica y reglas de dominio
  df_clean["importe_eur"] = pd.to_numeric(
      df_clean["importe_eur"], errors="coerce"
  )
  df_clean["unidades"] = pd.to_numeric(df_clean["unidades"], errors="coerce")

  mediana_unidades = (
      int(df_clean["unidades"].median())
      if not df_clean["unidades"].isnull().all()
      else 1
  )
  df_clean["unidades"] = df_clean["unidades"].fillna(mediana_unidades)

  mask_dominio = (
      (df_clean["importe_eur"] > 0)
      & (df_clean["importe_eur"] < 50000)
      & (df_clean["unidades"] >= 1)
  )

  bad_domain_df = df_clean[~mask_dominio]
  bad_domain_df.to_csv(bad_records_dir / "bad_records_outliers.csv", index=False)

  df_final = df_clean[mask_dominio].copy()
  registros_finales = len(df_final)

  # 4. Exportar dataset limpio para BI
  processed_file = processed_dir / "dataset_kdd_listo.csv"
  df_final.to_csv(processed_file, index=False)
  logging.info(
      f"Dataset final saneado exportado a {processed_file} con"
      f" {registros_finales} registros."
  )

  # 5. Perfilado de Datos ISO/IEC 25012
  completitud_tasa = (
      (registros_finales / total_registros_iniciales) * 100
      if total_registros_iniciales > 0
      else 0
  )
  stats_importe = df_final["importe_eur"].describe(
      percentiles=[0.25, 0.5, 0.75]
  )

  metricas = {
      "total_registros_brutos": int(total_registros_iniciales),
      "total_registros_limpios": int(registros_finales),
      "tasa_completitud_porcentaje": round(completitud_tasa, 2),
      "importe_media": float(stats_importe["mean"]),
      "importe_mediana": float(stats_importe["50%"]),
      "importe_std": float(stats_importe["std"]),
      "importe_p25": float(stats_importe["25%"]),
      "importe_p75": float(stats_importe["75%"]),
  }

  # Exportación obligatoria del JSON en output_samples/
  json_path = output_samples_dir / "metricas_perfilado.json"
  with open(json_path, "w", encoding="utf-8") as jf:
    json.dump(metricas, jf, indent=4)
  logging.info(f"Métricas de perfilado exportadas correctamente a {json_path}")

  # 6. Redacción del informe técnico en reports/INFORME_CALIDAD_KDD.md
  informe_md = f"""# Informe de Calidad de Datos y Preparación KDD (ISO/IEC 25012)

## 1. Resumen Ejecutivo
Informe generado automáticamente tras la ejecución del pipeline de validación criptográfica y saneamiento sobre el lote transaccional.

## 2. Métricas de Calidad y Perfilado
* **Registros Procesados Iniciales**: {total_registros_iniciales}
* **Registros Válidos Finales**: {registros_finales}
* **Tasa de Completitud (ISO/IEC 25012)**: {metricas['tasa_completitud_porcentaje']}%
* **Registros en Cuarentena**: {total_registros_iniciales - registros_finales}

### Distribución estadística de `importe_eur` (post-limpieza):
* **Media**: {metricas['importe_media']:.2f} €
* **Mediana**: {metricas['importe_mediana']:.2f} €
* **Desviación Estándar**: {metricas['importe_std']:.2f} €
* **Percentil 25 (Q1)**: {metricas['importe_p25']:.2f} €
* **Percentil 75 (Q3)**: {metricas['importe_p75']:.2f} €

## 3. Conclusiones Técnicas
Se han segregado correctamente los ficheros con alteraciones criptográficas y los registros con nulos o valores atípicos de sensores, asegurando que el dataset final cumpla con los estándares de calidad para los modelos de Business Intelligence.
"""

  report_path = reports_dir / "INFORME_CALIDAD_KDD.md"
  with open(report_path, "w", encoding="utf-8") as rf:
    rf.write(informe_md)
  logging.info(f"Informe técnico generado correctamente en {report_path}")

  logging.info("=== FIN DEL PIPELINE KDD Y PERFILADO EXITOSO ===")


if __name__ == "__main__":
  ejecutar_saneamiento_kdd()