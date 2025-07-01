import os
from app.utils.github_downloader import download_excel_from_github
from app.rule_parser.excel_loader import load_rules_from_excel
from app.utils.hash_utils import (
    calculate_file_hash,
    get_hash_from_s3,
    save_hash_to_s3
)
from app.utils.uploader import upload_rules_to_s3
from app.utils.logger import get_logger
from app import config

logger = get_logger(__name__)


def get_excel_from_github() -> str:
    """
    Descarga el archivo Excel desde GitHub.

    Returns:
        str: Ruta local del archivo descargado.
    """
    return download_excel_from_github(
        repo_url=config.GITHUB_REPO_URL,
        file_path=config.GITHUB_FILE_PATH,
        branch=config.GITHUB_BRANCH,
        token=config.GITHUB_TOKEN
    )


def has_file_changed_s3(file_path: str, bucket: str, hash_key: str) -> bool:
    """
    Compara el hash actual del archivo con el hash almacenado en S3.

    Args:
        file_path (str): Ruta del archivo a verificar.
        bucket (str): Nombre del bucket S3.
        hash_key (str): Clave del objeto hash en S3.

    Returns:
        bool: True si el archivo ha cambiado, False si es idéntico.
    """
    current_hash = calculate_file_hash(file_path)
    previous_hash = get_hash_from_s3(bucket, hash_key)
    if current_hash == previous_hash:
        return False
    save_hash_to_s3(current_hash, bucket, hash_key)
    return True


def process_rules(file_path: str) -> list:
    """
    Carga y filtra reglas desde el archivo Excel.

    Args:
        file_path (str): Ruta del archivo Excel.

    Returns:
        list: Lista de reglas cargadas.
    """
    rules = load_rules_from_excel(
        file_path, type_filter=config.DEFAULT_RULE_TYPE)
    logger.info(
        f"[LOCAL] Total de reglas '{config.DEFAULT_RULE_TYPE}' cargadas: {len(rules)}")
    return rules


def clean_temp_file(file_path: str):
    """
    Elimina un archivo temporal del sistema.

    Args:
        file_path (str): Ruta del archivo a eliminar.
    """
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.info(f"[LOCAL] Archivo temporal eliminado: {file_path}")
        except Exception as e:
            logger.warning(
                f"[LOCAL] No se pudo eliminar archivo temporal: {e}")


def sync_rules_from_github() -> bool:
    """
    Orquesta la sincronización de reglas: descarga, validación, subida y limpieza.

    Returns:
        bool: True si el proceso fue exitoso o no hubo cambios. False si hubo error.
    """
    excel_path = None

    try:
        logger.info("[LOCAL] Iniciando sincronización de reglas desde GitHub")

        # Paso 1: Descargar Excel
        excel_path = get_excel_from_github()

        # Paso 2: Verificar cambios usando S3
        if not has_file_changed_s3(excel_path, config.S3_BUCKET_NAME, config.S3_HASH_OBJECT_KEY):
            logger.info(
                "[LOCAL] No hay cambios en el archivo. Sincronización omitida.")
            return True

        # Paso 3: Procesar reglas
        rules = process_rules(excel_path)
        if not rules:
            logger.warning(
                "[LOCAL] No se encontraron reglas para sincronizar.")
            return False

        # Paso 4: Subir a S3
        success = upload_rules_to_s3(
            rules, config.S3_BUCKET_NAME, config.S3_RULES_OBJECT_KEY
        )

        if not success:
            logger.error("[LOCAL] Falló la subida a S3.")
            return False

        logger.info(
            "[LOCAL] ✅ Reglas sincronizadas y subidas exitosamente a S3")
        return True

    except Exception as e:
        logger.exception("[LOCAL] ❌ Error durante sincronización de reglas")
        return False

    finally:
        clean_temp_file(excel_path)


if __name__ == "__main__":
    sync_rules_from_github()
