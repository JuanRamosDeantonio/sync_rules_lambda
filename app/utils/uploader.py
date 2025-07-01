import json
import boto3
from typing import List, Union
from botocore.exceptions import ClientError
from app.models.rule_data import RuleData
from app.utils.logger import get_logger
from app import config

logger = get_logger("s3-uploader")


def upload_rules_to_s3(
    rules: Union[List[RuleData], List[dict]],
    bucket_name: str = config.S3_BUCKET_NAME,
    key: str = config.S3_KEY
) -> bool:
    """
    Sube una lista de reglas (RuleData o diccionarios) al bucket de S3 en formato JSON.

    Esta función convierte los objetos a JSON serializable, se conecta a AWS S3
    (usando credenciales implícitas si se ejecuta en Lambda), y escribe el contenido
    en la ruta especificada.

    Args:
        rules (Union[List[RuleData], List[dict]]): Reglas a subir. Pueden ser modelos Pydantic o diccionarios.
        bucket_name (str): Nombre del bucket S3 destino.
        key (str): Ruta clave donde se almacenará el JSON (ej. 'rules/rules_metadata.json').

    Returns:
        bool: True si la operación fue exitosa, False en caso de error.
    """
    if not rules or not isinstance(rules, list):
        logger.warning(
            "La lista de reglas está vacía o no es una lista válida.")
        return False

    try:
        if config.IS_LAMBDA:
            s3 = boto3.client("s3")
        else:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
                aws_session_token=config.AWS_SESSION_TOKEN,
                region_name=config.AWS_REGION,
            )

        # Serializar objetos RuleData si es necesario
        if isinstance(rules[0], RuleData):
            serialized = [r.dict() for r in rules]
        else:
            serialized = rules

        json_data = json.dumps(serialized, indent=2, ensure_ascii=False)

        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json_data.encode("utf-8")
        )

        logger.info(f"Archivo JSON subido a s3://{bucket_name}/{key}")
        return True

    except ClientError as e:
        logger.error(
            f"Error AWS al subir archivo a S3: {e.response.get('Error', {}).get('Message')}")
        return False

    except Exception as e:
        logger.exception("Excepción inesperada al intentar subir a S3")
        return False
