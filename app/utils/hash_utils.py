import hashlib
import os
import boto3
from botocore.exceptions import ClientError


def calculate_file_hash(file_path: str) -> str:
    """
    Calcula el hash SHA256 de un archivo binario.

    Args:
        file_path (str): Ruta absoluta del archivo.

    Returns:
        str: Valor hash en formato hexadecimal.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def read_previous_hash(hash_path: str) -> str:
    """
    Lee el valor hash previamente almacenado en disco local.

    Args:
        hash_path (str): Ruta absoluta del archivo que contiene el hash.

    Returns:
        str: Valor del hash anterior, o cadena vacía si no existe o no es legible.
    """
    if not os.path.exists(hash_path):
        return ""
    try:
        with open(hash_path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def store_hash(hash_value: str, hash_path: str) -> None:
    """
    Guarda el valor hash actual en un archivo local.

    Args:
        hash_value (str): Valor del hash SHA256 a guardar.
        hash_path (str): Ruta absoluta del archivo donde se almacenará el hash.
    """
    os.makedirs(os.path.dirname(hash_path), exist_ok=True)
    with open(hash_path, "w", encoding="utf-8") as f:
        f.write(hash_value)


def get_hash_from_s3(bucket: str, key: str) -> str:
    """
    Obtiene el hash previamente almacenado en S3.

    Args:
        bucket (str): Nombre del bucket S3.
        key (str): Clave del objeto hash (ej. "rules/rules.hash").

    Returns:
        str: Hash anterior, o cadena vacía si no existe.
    """
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8").strip()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return ""
        raise


def save_hash_to_s3(hash_value: str, bucket: str, key: str) -> None:
    """
    Guarda el nuevo hash en S3.

    Args:
        hash_value (str): Hash actual a guardar.
        bucket (str): Bucket destino.
        key (str): Clave del objeto destino.
    """
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=hash_value.encode("utf-8")
    )
