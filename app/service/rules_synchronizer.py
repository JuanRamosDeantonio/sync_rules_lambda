# app/services/rules_synchronizer.py

"""
Módulo de Sincronización de Reglas
===================================

Este módulo proporciona funcionalidades para sincronizar reglas de negocio desde
repositorios de GitHub hacia Amazon S3, incluyendo validación de cambios mediante
hashes y logging estructurado para observabilidad.

Características principales:
- Descarga automática de archivos Excel desde GitHub
- Validación de cambios usando comparación de hashes en S3
- Procesamiento y filtrado de reglas de negocio
- Subida automática a S3 con retry automático
- Logging estructurado con métricas de rendimiento
- Manejo robusto de errores y limpieza de recursos

Ejemplo de uso:
    >>> from app.services.rules_synchronizer import sync_rules_from_github
    >>> result = sync_rules_from_github("mi-ejecucion-001")
    >>> print(f"Éxito: {result.success}, Reglas: {result.rules_count}")

Autor: Equipo de Desarrollo
Versión: 2.0.0
Fecha: 2025
"""

import os
import json
import time
import uuid
from typing import List, Dict, Optional
from dataclasses import dataclass
from contextlib import contextmanager

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


@dataclass
class SyncResult:
    """
    Resultado de una operación de sincronización de reglas.
    
    Esta clase encapsula toda la información relevante sobre el resultado
    de una sincronización, incluyendo métricas de rendimiento y detalles
    de la ejecución para facilitar el debugging y monitoreo.
    
    Attributes:
        success (bool): Indica si la sincronización fue exitosa.
        rules_count (int): Número total de reglas procesadas y sincronizadas.
        message (str): Mensaje descriptivo del resultado de la operación.
        status_code (int): Código de estado HTTP equivalente (200, 404, 500, etc.).
        execution_id (str): Identificador único de la ejecución para trazabilidad.
        execution_time (float): Tiempo total de ejecución en segundos.
    
    Example:
        >>> result = SyncResult(
        ...     success=True,
        ...     rules_count=150,
        ...     message="Sincronización completada",
        ...     status_code=200,
        ...     execution_id="abc123",
        ...     execution_time=4.567
        ... )
        >>> print(f"Procesadas {result.rules_count} reglas en {result.execution_time}s")
    """
    success: bool
    rules_count: int
    message: str
    status_code: int
    execution_id: str
    execution_time: float


class StructuredLogger:
    """
    Wrapper para logging estructurado con contexto automático.
    
    Esta clase proporciona una interfaz de logging que automáticamente
    enriquece cada mensaje con contexto relevante como ID de ejecución,
    tiempo transcurrido y metadatos adicionales en formato JSON.
    
    El logging estructurado facilita la búsqueda, filtrado y análisis
    de logs en sistemas de observabilidad como CloudWatch, ELK Stack, etc.
    
    Args:
        logger: Instancia del logger base de Python.
        execution_id (str): Identificador único para esta ejecución.
    
    Attributes:
        logger: Logger base utilizado para escribir los mensajes.
        execution_id (str): ID único que se incluye en todos los logs.
        start_time (float): Timestamp de inicio para calcular tiempo transcurrido.
    
    Example:
        >>> from app.utils.logger import get_logger
        >>> base_logger = get_logger(__name__)
        >>> structured = StructuredLogger(base_logger, "exec-001")
        >>> structured.info("Proceso iniciado", archivo="data.xlsx", tamaño=1024)
        # Output: [Proceso iniciado] {"execution_id": "exec-001", "tiempo_transcurrido": 0.001, "archivo": "data.xlsx", "tamaño": 1024}
    """
    
    def __init__(self, logger, execution_id: str):
        """
        Inicializa el logger estructurado.
        
        Args:
            logger: Instancia del logger base de Python.
            execution_id (str): Identificador único para esta ejecución.
        """
        self.logger = logger
        self.execution_id = execution_id
        self.start_time = time.time()
    
    def _log_with_context(self, level: str, message: str, **kwargs) -> None:
        """
        Registra un mensaje con contexto estructurado automático.
        
        Enriquece cada mensaje de log con metadatos estándar y
        cualquier información adicional proporcionada via kwargs.
        
        Args:
            level (str): Nivel de logging ('info', 'error', 'warning', etc.).
            message (str): Mensaje principal del log.
            **kwargs: Metadatos adicionales a incluir en el contexto.
        """
        context = {
            'execution_id': self.execution_id,
            'tiempo_transcurrido': round(time.time() - self.start_time, 3),
            **kwargs
        }
        
        formatted_msg = f"[{message}] {json.dumps(context, ensure_ascii=False)}"
        getattr(self.logger, level)(formatted_msg)
    
    def info(self, message: str, **kwargs) -> None:
        """Registra un mensaje informativo con contexto."""
        self._log_with_context('info', message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Registra un mensaje de advertencia con contexto."""
        self._log_with_context('warning', message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Registra un mensaje de error con contexto."""
        self._log_with_context('error', message, **kwargs)
    
    def exception(self, message: str, **kwargs) -> None:
        """Registra una excepción con stack trace y contexto."""
        self._log_with_context('exception', message, **kwargs)
    
    def debug(self, message: str, **kwargs) -> None:
        """Registra un mensaje de debug con contexto."""
        self._log_with_context('debug', message, **kwargs)


class RulesSynchronizer:
    """
    Servicio principal para la sincronización de reglas de negocio.
    
    Esta clase encapsula toda la lógica necesaria para sincronizar reglas
    desde repositorios de GitHub hacia Amazon S3, incluyendo validación
    de cambios, procesamiento de datos y manejo de errores.
    
    El proceso de sincronización incluye:
    1. Descarga del archivo Excel desde GitHub
    2. Validación de cambios mediante comparación de hashes
    3. Procesamiento y filtrado de reglas según criterios de negocio
    4. Subida de datos procesados a S3
    5. Limpieza de recursos temporales
    
    Args:
        execution_id (str, optional): Identificador único para esta ejecución.
            Si no se proporciona, se genera automáticamente.
    
    Attributes:
        execution_id (str): ID único de la ejecución actual.
        logger (StructuredLogger): Logger estructurado para esta instancia.
        start_time (float): Timestamp de inicio de la ejecución.
        hash_key (str): Clave S3 donde se almacena el hash del archivo.
        bucket (str): Nombre del bucket S3 de destino.
        rule_type (str): Tipo de reglas a filtrar durante el procesamiento.
    
    Example:
        >>> synchronizer = RulesSynchronizer("mi-sync-001")
        >>> result = synchronizer.sync_rules()
        >>> if result.success:
        ...     print(f"Sincronizadas {result.rules_count} reglas")
        ... else:
        ...     print(f"Error: {result.message}")
    
    Note:
        Esta clase requiere que las siguientes configuraciones estén presentes:
        - GITHUB_REPO_URL: URL del repositorio GitHub
        - GITHUB_FILE_PATH: Ruta del archivo Excel en el repositorio
        - GITHUB_BRANCH: Rama del repositorio a utilizar
        - GITHUB_TOKEN: Token de acceso a GitHub
        - S3_BUCKET_NAME: Nombre del bucket S3 de destino
        - DEFAULT_RULE_TYPE: Tipo de reglas a procesar
    """
    
    def __init__(self, execution_id: str = None):
        """
        Inicializa una nueva instancia del sincronizador.
        
        Args:
            execution_id (str, optional): ID único para esta ejecución.
                Si es None, se genera automáticamente un UUID corto.
        """
        self.execution_id = execution_id or str(uuid.uuid4())[:8]
        self.logger = StructuredLogger(logger, self.execution_id)
        self.start_time = time.time()
        
        # Configuración del servicio
        self.hash_key = getattr(config, 'S3_HASH_OBJECT_KEY', "rules/rules.hash")
        self.bucket = config.S3_BUCKET_NAME
        self.rule_type = config.DEFAULT_RULE_TYPE
        
        self.logger.info("RulesSynchronizer inicializado", 
                        clave_hash=self.hash_key, 
                        bucket=self.bucket, 
                        tipo_regla=self.rule_type)
    
    def sync_rules(self) -> SyncResult:
        """
        Ejecuta el proceso completo de sincronización de reglas.
        
        Este método orquesta todo el flujo de sincronización desde la descarga
        hasta la subida final, manejando errores y recursos de manera robusta.
        
        Returns:
            SyncResult: Objeto con el resultado completo de la sincronización,
                incluyendo métricas de rendimiento y detalles de la ejecución.
        
        Raises:
            Exception: Las excepciones son capturadas internamente y se retorna
                un SyncResult con success=False y detalles del error.
        
        Example:
            >>> synchronizer = RulesSynchronizer()
            >>> result = synchronizer.sync_rules()
            >>> print(f"Éxito: {result.success}")
            >>> print(f"Reglas procesadas: {result.rules_count}")
            >>> print(f"Tiempo de ejecución: {result.execution_time}s")
        """
        with self._managed_temp_file() as excel_path:
            try:
                self.logger.info("Iniciando sincronización de reglas desde GitHub")
                
                # Paso 1: Descargar Excel desde GitHub
                excel_path = self._download_excel_file()
                
                # Paso 2: Verificar si el archivo ha cambiado
                if not self._has_file_changed(excel_path):
                    return self._create_result(
                        success=True,
                        rules_count=0,
                        message="No hay cambios en el archivo. No se realizó sincronización.",
                        status_code=200
                    )
                
                # Paso 3: Procesar y filtrar reglas
                rules = self._process_rules(excel_path)
                if not rules:
                    return self._create_result(
                        success=False,
                        rules_count=0,
                        message="El archivo no contiene reglas válidas.",
                        status_code=204
                    )
                
                # Paso 4: Subir reglas procesadas a S3
                if not self._upload_rules_to_s3(rules):
                    raise Exception("Falló la subida a S3")
                
                return self._create_result(
                    success=True,
                    rules_count=len(rules),
                    message=f"{len(rules)} reglas cargadas y subidas correctamente",
                    status_code=200
                )
                
            except Exception as e:
                self.logger.exception("Falló la sincronización de reglas", 
                                    tipo_error=type(e).__name__,
                                    mensaje_error=str(e))
                return self._create_result(
                    success=False,
                    rules_count=0,
                    message=f"Error al sincronizar reglas: {str(e)}",
                    status_code=500
                )
    
    def _create_result(self, success: bool, rules_count: int, 
                      message: str, status_code: int) -> SyncResult:
        """
        Crea un objeto SyncResult con información completa de la ejecución.
        
        Args:
            success (bool): Indica si la operación fue exitosa.
            rules_count (int): Número de reglas procesadas.
            message (str): Mensaje descriptivo del resultado.
            status_code (int): Código de estado HTTP equivalente.
        
        Returns:
            SyncResult: Objeto con toda la información de la ejecución.
        """
        execution_time = time.time() - self.start_time
        
        result = SyncResult(
            success=success,
            rules_count=rules_count,
            message=message,
            status_code=status_code,
            execution_id=self.execution_id,
            execution_time=round(execution_time, 3)
        )
        
        self.logger.info("Operación de sincronización completada", 
                        exitoso=success,
                        cantidad_reglas=rules_count,
                        tiempo_ejecucion=execution_time)
        
        return result
    
    def _download_excel_file(self) -> str:
        """
        Descarga el archivo Excel desde el repositorio GitHub configurado.
        
        Utiliza las credenciales y configuración del módulo config para
        autenticarse y descargar el archivo especificado desde GitHub.
        
        Returns:
            str: Ruta local del archivo descargado exitosamente.
        
        Raises:
            FileNotFoundError: Si el archivo descargado no existe en el sistema local.
            Exception: Para cualquier error durante la descarga (red, autenticación, etc.).
        
        Note:
            El archivo se descarga a un directorio temporal que será
            limpiado automáticamente al finalizar la ejecución.
        """
        try:
            self.logger.info("Iniciando descarga de Excel desde GitHub",
                           url_repositorio=config.GITHUB_REPO_URL,
                           ruta_archivo=config.GITHUB_FILE_PATH,
                           rama=config.GITHUB_BRANCH)
            
            file_path = download_excel_from_github(
                repo_url=config.GITHUB_REPO_URL,
                file_path=config.GITHUB_FILE_PATH,
                branch=config.GITHUB_BRANCH,
                token=config.GITHUB_TOKEN
            )
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Archivo descargado no encontrado: {file_path}")
                
            file_size = os.path.getsize(file_path)
            self.logger.info("Archivo Excel descargado exitosamente", 
                           ruta_archivo=file_path, 
                           tamaño_bytes=file_size)
            
            return file_path
            
        except Exception as e:
            self.logger.error("Falló la descarga del archivo Excel", 
                            tipo_error=type(e).__name__,
                            mensaje_error=str(e))
            raise
    
    def _has_file_changed(self, file_path: str) -> bool:
        """
        Determina si el archivo ha cambiado comparando hashes con S3.
        
        Calcula el hash SHA-256 del archivo local y lo compara con el hash
        previamente almacenado en S3. Si son diferentes, guarda el nuevo hash.
        
        Args:
            file_path (str): Ruta del archivo local a verificar.
        
        Returns:
            bool: True si el archivo ha cambiado o es la primera vez,
                 False si el archivo es idéntico al anterior.
        
        Note:
            En caso de error al acceder a S3 o calcular hashes, se asume
            que el archivo ha cambiado para evitar perder actualizaciones.
        """
        try:
            current_hash = calculate_file_hash(file_path)
            previous_hash = get_hash_from_s3(self.bucket, self.hash_key)
            
            if current_hash == previous_hash:
                self.logger.info("El archivo no ha cambiado", 
                               hash_actual=current_hash)
                return False
            
            save_hash_to_s3(current_hash, self.bucket, self.hash_key)
            self.logger.info("Cambios detectados en el archivo", 
                           hash_actual=current_hash, 
                           hash_anterior=previous_hash)
            return True
            
        except Exception as e:
            self.logger.warning("Error al comparar hashes - asumiendo archivo cambiado", 
                              tipo_error=type(e).__name__)
            return True
    
    def _process_rules(self, file_path: str) -> List[Dict]:
        """
        Procesa y filtra las reglas desde el archivo Excel.
        
        Carga el archivo Excel, aplica filtros según el tipo de regla configurado
        y retorna la lista de reglas válidas para ser sincronizadas.
        
        Args:
            file_path (str): Ruta del archivo Excel a procesar.
        
        Returns:
            List[Dict]: Lista de reglas válidas después del filtrado.
                       Cada regla es un diccionario con sus propiedades.
        
        Raises:
            Exception: Si ocurre un error durante la carga o procesamiento
                      del archivo Excel.
        
        Note:
            Las reglas se filtran según el valor de config.DEFAULT_RULE_TYPE.
            Solo las reglas que coincidan con este tipo serán incluidas.
        """
        try:
            rules = load_rules_from_excel(file_path)
            
            self.logger.info("Reglas procesadas exitosamente", 
                           total_reglas=len(rules), 
                           tipo_regla=self.rule_type)
            
            return rules
            
        except Exception as e:
            self.logger.error("Falló el procesamiento de reglas", 
                            tipo_error=type(e).__name__)
            raise
    
    def _upload_rules_to_s3(self, rules: List[Dict]) -> bool:
        """
        Sube las reglas procesadas al bucket S3 configurado.
        
        Convierte las reglas a formato JSON y las sube a S3, registrando
        métricas sobre el tamaño de los datos y el éxito de la operación.
        
        Args:
            rules (List[Dict]): Lista de reglas a subir a S3.
        
        Returns:
            bool: True si la subida fue exitosa, False en caso contrario.
        
        Note:
            El método incluye logging detallado del tamaño de datos
            y manejo de errores para facilitar el debugging.
        """
        try:
            data_size = len(json.dumps(rules, ensure_ascii=False))
            
            success = upload_rules_to_s3(rules)
            
            if success:
                self.logger.info("Reglas subidas exitosamente a S3", 
                               cantidad_reglas=len(rules), 
                               tamaño_datos_bytes=data_size)
            else:
                self.logger.error("Falló la subida de reglas a S3")
            
            return success
            
        except Exception as e:
            self.logger.error("Excepción durante subida a S3", 
                            tipo_error=type(e).__name__)
            return False 
    
    @contextmanager
    def _managed_temp_file(self):
        """
        Context manager para el manejo seguro de archivos temporales.
        
        Garantiza que los archivos temporales creados durante la ejecución
        sean limpiados automáticamente, incluso en caso de errores.
        
        Yields:
            str: Ruta del archivo temporal (inicialmente None).
        
        Example:
            >>> with self._managed_temp_file() as temp_path:
            ...     temp_path = download_file()
            ...     process_file(temp_path)
            # El archivo se limpia automáticamente aquí
        """
        temp_file_path = None
        try:
            yield temp_file_path
        finally:
            self._cleanup_temp_file(temp_file_path)
    
    def _cleanup_temp_file(self, file_path: Optional[str]) -> None:
        """
        Elimina de forma segura un archivo temporal del sistema.
        
        Verifica la existencia del archivo antes de intentar eliminarlo
        y registra el resultado de la operación para auditoría.
        
        Args:
            file_path (Optional[str]): Ruta del archivo a eliminar.
                                     Si es None o vacío, no se realiza acción.
        
        Note:
            Los errores durante la limpieza se registran como warnings
            pero no interrumpen la ejecución del programa.
        """
        if not file_path or not os.path.exists(file_path):
            return
            
        try:
            file_size = os.path.getsize(file_path)
            os.remove(file_path)
            self.logger.info("Archivo temporal limpiado", 
                           ruta_archivo=file_path, 
                           tamaño_archivo_bytes=file_size)
        except Exception as e:
            self.logger.warning("Falló la limpieza del archivo temporal", 
                              tipo_error=type(e).__name__)


def validate_configuration() -> None:
    """
    Valida la presencia de todas las configuraciones requeridas.
    
    Verifica que todas las variables de configuración necesarias para
    el funcionamiento del sincronizador estén presentes y no vacías.
    
    Raises:
        ValueError: Si una o más configuraciones requeridas están ausentes.
    
    Note:
        Las configuraciones requeridas son:
        - GITHUB_REPO_URL: URL del repositorio GitHub
        - GITHUB_FILE_PATH: Ruta del archivo en el repositorio
        - GITHUB_BRANCH: Rama del repositorio
        - GITHUB_TOKEN: Token de autenticación
        - S3_BUCKET_NAME: Nombre del bucket S3
        - DEFAULT_RULE_TYPE: Tipo de reglas a procesar
    
    Example:
        >>> try:
        ...     validate_configuration()
        ...     print("Configuración válida")
        ... except ValueError as e:
        ...     print(f"Error de configuración: {e}")
    """
    required_configs = [
        'GITHUB_REPO_URL', 'GITHUB_FILE_PATH', 'GITHUB_BRANCH',
        'GITHUB_TOKEN', 'S3_BUCKET_NAME', 'DEFAULT_RULE_TYPE'
    ]
    
    missing_configs = [
        config_name for config_name in required_configs
        if not hasattr(config, config_name) or not getattr(config, config_name)
    ]
    
    if missing_configs:
        logger.error("Configuraciones requeridas faltantes", extra={
                    'configuraciones_faltantes': missing_configs
        })
        raise ValueError(f"Configuraciones faltantes: {', '.join(missing_configs)}")
    
    logger.info("Validación de configuración exitosa")


def sync_rules_from_github(execution_id: str = None) -> SyncResult:
    """
    Función principal para sincronizar reglas desde GitHub hacia S3.
    
    Esta función de alto nivel orquesta todo el proceso de sincronización,
    incluyendo validación de configuración, creación del sincronizador
    y ejecución del proceso completo.
    
    Args:
        execution_id (str, optional): Identificador único para esta ejecución.
                                    Si es None, se genera automáticamente.
    
    Returns:
        SyncResult: Objeto con el resultado completo de la sincronización,
                   incluyendo estado, métricas y detalles de la ejecución.
    
    Raises:
        ValueError: Si la configuración requerida está incompleta.
        Exception: Para otros errores durante la sincronización.
    
    Example:
        >>> # Sincronización básica
        >>> result = sync_rules_from_github()
        >>> if result.success:
        ...     print(f"✅ Sincronizadas {result.rules_count} reglas")
        ... else:
        ...     print(f"❌ Error: {result.message}")
        
        >>> # Con ID personalizado para trazabilidad
        >>> result = sync_rules_from_github("batch-sync-001")
        >>> print(f"Ejecución {result.execution_id}: {result.message}")
    
    Note:
        Esta función valida automáticamente la configuración antes de
        proceder con la sincronización. Todas las excepciones son
        manejadas internamente y retornadas como parte del SyncResult.
    """
    validate_configuration()
    synchronizer = RulesSynchronizer(execution_id)
    return synchronizer.sync_rules()


def main():
    """
    Función principal para testing local del módulo.
    
    Ejecuta una sincronización de prueba con ID "test-local" y
    muestra el resultado en la consola. Útil para desarrollo
    y verificación del funcionamiento del módulo.
    
    Example:
        >>> python -m app.services.rules_synchronizer
        🚀 Probando RulesSynchronizer localmente...
        ✅ Resultado: SyncResult(success=True, rules_count=150, ...)
    
    Note:
        Esta función solo se ejecuta cuando el módulo se ejecuta
        directamente, no cuando es importado por otros módulos.
    """
    print("🚀 Probando RulesSynchronizer localmente...")
    
    try:
        result = sync_rules_from_github("test-local")
        print(f"✅ Resultado: {result}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()