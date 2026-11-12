import hashlib
import hmac
import logging
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_FILE = BASE_DIR / 'logs' / 'verificar_integridad.log'
QUARANTINE_MANIFEST = BASE_DIR / 'output_samples' / 'quarantine_manifest.txt'

'''
Buscamos la carpeta 'raw_batch_202611', primero buscamos en la carpeta del proyecto, si no está,
buscamos un nivel más arriba.
'''
# Localización dinámica de los datos en bruto
def localizar_raw_dir() -> Path:
   for candidata in (BASE_DIR / 'raw_batch_202611', BASE_DIR.parent / 'raw_batch_202611'):
      if candidata.is_dir():
         return candidata
   raise FileNotFoundError("No se encontró el directorio 'raw_batch_202611' en las ubicaciones esperadas.")

'''
Todo lo que registre este logger, se nos mostrará en nuestra consola, y se escribirá y guardará en el archiivo 'ejecucion_pipeline.log'
'''
# Configuración del sistemas de alertas
def configurar_logger() -> logging.Logger:
   LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
   logger = logging.getLogger("Fase1_SHA256")
   logger.setLevel(logging.INFO)

'''
Abrimos el archivo en modo lectura binaria y lo leemos en trozos pequeños de 64KB (65536 bytes)
'''
# Motor que hace el calculo del SHA-256
def calcular_sha256(ruta_fichero):
   hasher = hashlib.sha256()
   with open(ruta_fichero, 'rb') as f:
      while chunk := f.read(65536):
         hasher.update(chunk)
   return hasher.hexdigest()

'''
Abrimos el archivo original, leemos cada linea, lo separamos por espacios, y creamos un diccionario, donde la clave es el nombre del archivo y el valor es su hash SHA256.
'''
# Lectura del manifiesto
def leer_manifiesto(ruta_manifiesto: Path, logger: logging.Logger) -> dict[str, str]:
    if not ruta_manifiesto.exists():
        logger.critical(f"Manifiesto oficial no encontrado: {ruta_manifiesto}")
        sys.exit(1)

    firmas: dict[str, str] = {}
    with open(ruta_manifiesto, "r", encoding="utf-8") as f:
        for num_linea, linea in enumerate(f, start=1):
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue
            partes = linea.split()
            if len(partes) != 2:
                logger.warning(f"Línea {num_linea} malformada en manifiesto: {linea}")
                continue
            firmas[Path(partes[1]).name] = partes[0].lower()
    return firmas

def main() -> int:
    logger = configurar_logger()
    QUARANTINE_MANIFEST.parent.mkdir(parents=True, exist_ok=True)

    try:
        raw_dir = localizar_raw_dir()
    except FileNotFoundError as err:
        logger.critical(str(err))
        return 1

    manifest_path = raw_dir / "manifest_checksums.sha256"
    logger.info("=== INICIO FASE 1: VALIDACIÓN CRIPTOGRÁFICA SHA-256 POR LOTES ===")
    logger.info(f"Directorio de origen localizado: {raw_dir}")

    firmas_oficiales = leer_manifiesto(manifest_path, logger)
    verificados = 0
    cuarentena = 0
    registros_manifiesto: list[str] = []

    for nombre_archivo, hash_esperado in firmas_oficiales.items():
        ruta_fichero = raw_dir / nombre_archivo

        if not ruta_fichero.exists():
            logger.error(f"[AUSENTE] {nombre_archivo} declarado en manifiesto pero no existe en disco.")
            registros_manifiesto.append(f"QUARANTINE | {nombre_archivo} | ARCHIVO_AUSENTE | esperado={hash_esperado}")
            cuarentena += 1
            continue

        hash_calculado = calcular_sha256(ruta_fichero)

        if hmac.compare_digest(hash_calculado, hash_esperado):
            logger.info(f"[VERIFICADO] {nombre_archivo} | SHA-256 OK ({hash_calculado})")
            registros_manifiesto.append(f"VERIFIED   | {nombre_archivo} | sha256={hash_calculado}")
            verificados += 1
        else:
            logger.critical(
                f"[ALERTA SEGURIDAD - CUARENTENA] {nombre_archivo} | Firma inválida | "
                f"Esperado: {hash_esperado} != Calculado: {hash_calculado}"
            )
            registros_manifiesto.append(
                f"QUARANTINE | {nombre_archivo} | HASH_MISMATCH | "
                f"esperado={hash_esperado} | calculado={hash_calculado}"
            )
            cuarentena += 1

    for fichero_csv in sorted(raw_dir.glob("*.csv")):
        if fichero_csv.name not in firmas_oficiales:
            logger.warning(f"[NO DECLARADO - CUARENTENA] {fichero_csv.name} no figura en manifest_checksums.sha256")
            registros_manifiesto.append(f"QUARANTINE | {fichero_csv.name} | NO_DECLARADO_EN_MANIFIESTO")
            cuarentena += 1

    ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    contenido_manifest = [
        "# =========================================================================",
        "# MANIFIESTO DE CONTROL Y CUARENTENA - PRACTICA_5075_INTEGRIDAD_KDD",
        f"# Auditoría Fase 1 (UTC): {ts_utc}",
        "# =========================================================================",
        "# === FASE 1: CONTROL DE INTEGRIDAD DE FICHEROS (SHA-256) ===",
        *registros_manifiesto,
        "",
    ]
    QUARANTINE_MANIFEST.write_text("\n".join(contenido_manifest), encoding="utf-8")

    logger.info(f"=== FIN FASE 1 | Verificados: {verificados} | En cuarentena: {cuarentena} ===")
    return 0 if verificados > 0 else 2


if __name__ == "__main__":
    sys.exit(main())