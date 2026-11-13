import hashlib
import logging
from pathlib import Path
import shutil

# Configuración del sistema de registro (Logs)
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "ejecucion_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def calcular_sha256(ruta_fichero):
  """Calcula el hash criptográfico SHA-256 de un archivo en bloques de 64KB."""
  hasher = hashlib.sha256()
  with open(ruta_fichero, "rb") as f:
    while chunk := f.read(65536):
      hasher.update(chunk)
  return hasher.hexdigest()


def verificar_pipeline():
  # Definición de directorios oficiales
  raw_dir = Path("raw_batch_202611")
  verified_dir = Path("staging/verified")
  tampered_dir = Path("quarantine/tampered")
  manifest_path = raw_dir / "manifest_checksums.sha256"

  # Aseguramos directorios de destino
  verified_dir.mkdir(parents=True, exist_ok=True)
  tampered_dir.mkdir(parents=True, exist_ok=True)

  logging.info(
      "=== INICIO FASE 1: Pasarela de Integridad Criptográfica (SHA-256) ==="
  )

  archivos_validados = 0
  archivos_rechazados = 0
  quarantine_records = []

  if not manifest_path.exists():
    logging.error(f"Fichero de manifiesto no encontrado en {manifest_path}")
    return

  # Lectura línea a línea del manifiesto
  with open(manifest_path, "r") as f:
    for linea in f:
      partes = linea.strip().split()
      if len(partes) < 2:
        continue
      expected_hash, filename = partes[0], partes[1]
      fichero_path = raw_dir / filename

      if not fichero_path.exists():
        logging.warning(
            f"El fichero listado en manifiesto no existe físicamente:"
            f" {filename}"
        )
        continue

      calculated_hash = calcular_sha256(fichero_path)

      if calculated_hash == expected_hash:
        shutil.copy(fichero_path, verified_dir / filename)
        logging.info(
            f"[APROBADO] {filename} -> Integridad SHA-256 verificada con éxito."
        )
        archivos_validados += 1
      else:
        shutil.copy(fichero_path, tampered_dir / filename)
        logging.error(
            f"[RECHAZADO - TAMPERED] {filename} -> Hash discrepante! Movido a"
            " quarantine/tampered/"
        )
        archivos_rechazados += 1
        quarantine_records.append(
            f"Fichero: {filename} | Esperado: {expected_hash} | Obtenido:"
            f" {calculated_hash}\n"
        )

  # Guardar muestra de cuarentena requerida en output_samples/
  output_samples_dir = Path("output_samples")
  output_samples_dir.mkdir(exist_ok=True)
  with open(output_samples_dir / "quarantine_manifest.txt", "w") as qm:
    qm.writelines(quarantine_records)

  logging.info(
      f"Resumen Fase 1 -> Validados: {archivos_validados} | Rechazados:"
      f" {archivos_rechazados}"
  )


if __name__ == "__main__":
  verificar_pipeline()