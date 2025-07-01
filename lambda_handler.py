import os
from app.utils.github_downloader import download_excel_from_github
from app.rule_parser.excel_loader import load_rules_from_excel
from app.utils.uploader import upload_rules_to_s3
from app.utils.hash_utils import (
    calculate_file_hash,
    get_hash_from_s3,
    save_hash_to_s3
)
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
    Compara el hash actual del archivo con el valor almacenado en S3.

    Args:
        file_path (str): Ruta del archivo local.
        bucket (str): Nombre del bucket S3.
        hash_key (str): Clave del objeto hash en S3.

    Returns:
        bool: True si el archivo ha cambiado, False si no hay cambios.
    """
    current_hash = calculate_file_hash(file_path)
    previous_hash = get_hash_from_s3(bucket, hash_key)
    if current_hash == previous_hash:
        return False
    save_hash_to_s3(current_hash, bucket, hash_key)
    return True


def process_rules(file_path: str) -> list:
    """
    Carga y filtra las reglas desde el archivo Excel.

    Args:
        file_path (str): Ruta del archivo Excel.

    Returns:
        list: Lista de reglas válidas.
    """
    rules = load_rules_from_excel(
        file_path, type_filter=config.DEFAULT_RULE_TYPE)
    logger.info(
        f"[LAMBDA] Total de reglas '{config.DEFAULT_RULE_TYPE}' cargadas: {len(rules)}")
    return rules


def clean_temp_file(file_path: str):
    """
    Elimina el archivo temporal creado durante la ejecución.

    Args:
        file_path (str): Ruta del archivo temporal.
    """
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.info(f"[LAMBDA] Archivo temporal eliminado: {file_path}")
        except Exception as e:
            logger.warning(
                f"[LAMBDA] No se pudo eliminar archivo temporal: {e}")


def lambda_handler(event, context):
    """
    Lambda que sincroniza reglas desde un Excel en GitHub,
    las filtra por tipo y las sube a S3 en formato JSON si hay cambios.

    Args:
        event (dict): Evento que activa la ejecución.
        context (LambdaContext): Contexto de ejecución de AWS Lambda.

    Returns:
        dict: Respuesta con código HTTP y mensaje informativo.
    """
    excel_path = None
    hash_key = "rules/rules.hash"  # Puedes mover a config.S3_HASH_OBJECT_KEY

    try:
        logger.info("[LAMBDA] Iniciando sincronización de reglas desde GitHub")

        # Paso 1: Descargar Excel
        excel_path = get_excel_from_github()

        # Paso 2: Verificar si hubo cambios
        if not has_file_changed_s3(excel_path, config.S3_BUCKET_NAME, hash_key):
            logger.info(
                "[LAMBDA] No hay cambios en el archivo. Sincronización omitida.")
            return {
                "statusCode": 200,
                "body": "No hay cambios en el archivo. No se realizó sincronización."
            }

        # Paso 3: Procesar reglas
        rules = process_rules(excel_path)
        if not rules:
            logger.warning(
                "[LAMBDA] No se encontraron reglas para sincronizar.")
            return {
                "statusCode": 204,
                "body": "El archivo no contiene reglas válidas."
            }

        # Paso 4: Subir a S3
        success = upload_rules_to_s3(rules)
        if not success:
            raise Exception("Falló la subida a S3")

        logger.info(
            "[LAMBDA] ✅ Reglas sincronizadas y subidas exitosamente a S3")
        return {
            "statusCode": 200,
            "body": f"{len(rules)} reglas cargadas y subidas correctamente"
        }

    except Exception as e:
        logger.exception("[LAMBDA] ❌ Error durante sincronización de reglas")
        return {
            "statusCode": 500,
            "body": f"Error al sincronizar reglas: {str(e)}"
        }

    finally:
        clean_temp_file(excel_path)
