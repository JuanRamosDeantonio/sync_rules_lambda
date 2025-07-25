import os
import requests
from app.utils.logger import get_logger

logger = get_logger(__name__)


def download_json_from_github(repo_url: str, file_path: str, branch: str = "main", token: str = None) -> str:
    """
    Descarga un archivo JSON (.json) desde un repositorio de GitHub usando su URL cruda.

    Esta función construye la URL de descarga directa del archivo alojado en GitHub,
    realiza la solicitud HTTP (autenticada si se proporciona token), guarda el archivo localmente,
    y retorna su ubicación.

    Args:
        repo_url (str): URL del repositorio GitHub (sin ".git").
                        Ejemplo: "https://github.com/org/repo"
        file_path (str): Ruta del archivo JSON dentro del repo. Ej: "carpeta/rules.json"
        branch (str, optional): Rama del repositorio a usar. Por defecto "main".
        token (str, optional): Token de acceso personal (PAT) para repositorios privados.

    Returns:
        str: Ruta local al archivo JSON descargado, en /tmp/ si está en AWS Lambda, o local si no.

    Raises:
        ValueError: Si la URL del repositorio no es válida.
        Exception: Si falla la descarga HTTP o el archivo no es recuperable.
    """
    if not repo_url.startswith("https://github.com/"):
        raise ValueError(
            "La URL del repositorio no es válida. Debe comenzar con https://github.com/")

    try:
        org_repo = repo_url.rstrip("/").split("/")[-2:]
        raw_base_url = f"https://raw.githubusercontent.com/{org_repo[0]}/{org_repo[1]}/{branch}/{file_path}"
    except Exception as e:
        logger.error(f"Error procesando la URL del repositorio: {e}")
        raise ValueError(
            "No se pudo construir la URL de descarga desde la ruta proporcionada.")

    logger.info(f"Descargando archivo desde GitHub: {raw_base_url}")

    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"

    response = requests.get(raw_base_url, headers=headers)
    if response.status_code != 200:
        logger.error(
            f"Error HTTP {response.status_code} al descargar archivo: {response.text}")
        raise Exception(
            f"No se pudo descargar el archivo: {response.status_code} - {response.reason}")

    content_type = response.headers.get("Content-Type", "")
    if "application/json" not in content_type and "text/plain" not in content_type:
        logger.warning(
            f"Advertencia: El archivo descargado no parece ser un JSON válido. Content-Type: {content_type}")

    # Extraer el nombre del archivo desde file_path para usar como nombre local
    file_name = os.path.basename(file_path)
    if not file_name.endswith('.json'):
        file_name += '.json'
    
    local_path = f"/tmp/{file_name}" if os.getenv("AWS_EXECUTION_ENV") else file_name
    
    with open(local_path, "wb") as f:
        f.write(response.content)

    logger.info(f"Archivo JSON guardado exitosamente en: {local_path}")
    return local_path