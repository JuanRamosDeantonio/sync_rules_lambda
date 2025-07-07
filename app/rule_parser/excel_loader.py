from typing import List, Optional, Dict, Any, Set
import time
from pathlib import Path
from functools import wraps
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from app.models.rule_data import RuleData
from app.utils.logger import get_logger

# Importar InvalidFileException de forma segura
try:
    from openpyxl.utils.exceptions import InvalidFileException
except ImportError:
    # Para versiones más antiguas de openpyxl
    InvalidFileException = Exception

logger = get_logger("excel-loader")


class ExcelProcessingError(Exception):
    """Excepción base para errores de procesamiento de Excel."""
    def __init__(self, message: str, file_path: Optional[str] = None, sheet_name: Optional[str] = None):
        self.file_path = file_path
        self.sheet_name = sheet_name
        super().__init__(message)


class ExcelFileNotFoundError(ExcelProcessingError):
    """Excepción cuando no se encuentra el archivo Excel."""
    pass


class InvalidExcelFileError(ExcelProcessingError):
    """Excepción cuando el archivo no es un Excel válido."""
    pass


class HeaderNotFoundError(ExcelProcessingError):
    """Excepción cuando no se encuentra el encabezado requerido."""
    pass


class MissingRequiredFieldsError(ExcelProcessingError):
    """Excepción cuando faltan campos requeridos en el encabezado."""
    def __init__(self, message: str, missing_fields: List[str], **kwargs):
        self.missing_fields = missing_fields
        super().__init__(message, **kwargs)


class RuleValidationError(ExcelProcessingError):
    """Excepción cuando una regla no pasa la validación."""
    def __init__(self, message: str, row_data: Dict[str, Any], row_number: int, **kwargs):
        self.row_data = row_data
        self.row_number = row_number
        super().__init__(message, **kwargs)


EXCEL_FIELD_MAP = {
    "Id": "id",
    "Descripcion": "description",
    "Tipo": "type",
    "Artefacto": "references",
    "Criticidad": "criticality",
}

REQUIRED_EXCEL_FIELDS = list(EXCEL_FIELD_MAP.keys())


def load_rules_from_excel(excel_path: str, type_filter: str = "SEMANTICA") -> List[RuleData]:
    """
    Carga y filtra reglas de validación desde un archivo Excel (.xlsx) usando openpyxl.

    Recorre todas las hojas, busca el encabezado, valida campos requeridos
    y construye objetos RuleData por cada fila válida.

    Args:
        excel_path (str): Ruta al archivo Excel.
        type_filter (str): Tipo de regla a filtrar (estructura, contenido, semántica).

    Returns:
        List[RuleData]: Lista de reglas válidas.
        
    Raises:
        ExcelFileNotFoundError: Si el archivo no existe.
        InvalidExcelFileError: Si el archivo no es un Excel válido.
    """
    start_time = time.time()
    file_path = Path(excel_path)
    
    # Log inicial con información del archivo
    logger.info(
        f"🚀 Iniciando carga de reglas",
        extra={
            "file_path": str(file_path),
            "file_name": file_path.name,
            "file_size": f"{file_path.stat().st_size / 1024:.2f} KB" if file_path.exists() else "N/A",
            "type_filter": type_filter
        }
    )
    
    # Validación de existencia del archivo
    if not file_path.exists():
        error_msg = f"Archivo no encontrado: {excel_path}"
        logger.error(f"❌ {error_msg}")
        raise ExcelFileNotFoundError(error_msg, file_path=excel_path)
    
    # Validación de extensión
    if file_path.suffix.lower() not in ['.xlsx', '.xlsm']:
        error_msg = f"Formato de archivo no soportado: {file_path.suffix}. Solo se admiten .xlsx y .xlsm"
        logger.error(f"❌ {error_msg}")
        raise InvalidExcelFileError(error_msg, file_path=excel_path)
    
    rules: List[RuleData] = []
    wb = None
    
    try:
        wb = load_workbook(excel_path, data_only=True)
        logger.info(f"📂 Archivo Excel cargado exitosamente. Hojas encontradas: {len(wb.sheetnames)}")
        logger.debug(f"📋 Nombres de hojas: {wb.sheetnames}")
        
    except InvalidFileException as e:
        error_msg = f"Archivo Excel corrupto o inválido: {str(e)}"
        logger.error(
            f"❌ {error_msg}",
            extra={
                "file_path": excel_path,
                "error_type": "InvalidFileException",
                "error_message": str(e)
            }
        )
        raise InvalidExcelFileError(error_msg, file_path=excel_path) from e
        
    except PermissionError as e:
        error_msg = f"Sin permisos para leer el archivo: {str(e)}"
        logger.error(
            f"❌ {error_msg}",
            extra={
                "file_path": excel_path,
                "error_type": "PermissionError",
                "error_message": str(e)
            }
        )
        raise InvalidExcelFileError(error_msg, file_path=excel_path) from e
        
    except Exception as e:
        error_msg = f"Error inesperado al abrir archivo Excel: {str(e)}"
        logger.error(
            f"❌ {error_msg}",
            extra={
                "file_path": excel_path,
                "error_type": type(e).__name__,
                "error_message": str(e)
            },
            exc_info=True
        )
        raise InvalidExcelFileError(error_msg, file_path=excel_path) from e

    type_filter_upper = type_filter.upper()
    processed_sheets = 0
    skipped_sheets = 0
    processing_errors = []
    
    try:
        for sheet_name in wb.sheetnames:
            try:
                ws = wb[sheet_name]
                logger.info(f"📄 Procesando hoja: {sheet_name}")
                
                sheet_rules = _process_worksheet(ws, sheet_name, type_filter_upper)
                rules.extend(sheet_rules)
                processed_sheets += 1
                
            except (HeaderNotFoundError, MissingRequiredFieldsError) as e:
                # Errores de estructura de hoja - no críticos, continuar con otras hojas
                logger.warning(
                    f"⚠️ Hoja {sheet_name} omitida: {str(e)}",
                    extra={
                        "sheet_name": sheet_name,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    }
                )
                skipped_sheets += 1
                processing_errors.append({"sheet": sheet_name, "error": str(e), "type": type(e).__name__})
                continue
                
            except Exception as e:
                # Errores inesperados en hoja - log pero continuar
                logger.error(
                    f"❌ Error inesperado procesando hoja: {sheet_name}",
                    extra={
                        "sheet_name": sheet_name,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    exc_info=True
                )
                skipped_sheets += 1
                processing_errors.append({"sheet": sheet_name, "error": str(e), "type": type(e).__name__})
                continue
                
    finally:
        # Asegurar cierre del workbook
        if wb:
            try:
                wb.close()
            except Exception as e:
                logger.warning(f"Error cerrando workbook: {e}")

    elapsed_time = time.time() - start_time
    
    # Log final con resumen completo
    logger.info(
        f"✅ Proceso completado en {elapsed_time:.2f}s",
        extra={
            "total_rules": len(rules),
            "processed_sheets": processed_sheets,
            "skipped_sheets": skipped_sheets,
            "total_sheets": len(wb.sheetnames) if wb else 0,
            "type_filter": type_filter,
            "file_name": file_path.name,
            "processing_time": f"{elapsed_time:.2f}s",
            "processing_errors": processing_errors
        }
    )
    
    if len(rules) == 0:
        if processed_sheets == 0:
            logger.error("❌ No se pudo procesar ninguna hoja del archivo")
        else:
            logger.warning("⚠️ No se encontraron reglas válidas en el archivo")
    
    return rules


def _process_worksheet(ws: Worksheet, sheet_name: str, type_filter: str) -> List[RuleData]:
    """
    Procesa una hoja de Excel individual y extrae las reglas válidas.

    Args:
        ws (Worksheet): Hoja de Excel a procesar.
        sheet_name (str): Nombre de la hoja (para logging).
        type_filter (str): Tipo de regla a filtrar (en mayúsculas).

    Returns:
        List[RuleData]: Lista de reglas válidas de esta hoja.
        
    Raises:
        HeaderNotFoundError: Si no se encuentra el encabezado requerido.
        MissingRequiredFieldsError: Si faltan campos requeridos en el encabezado.
    """
    rules: List[RuleData] = []
    
    # Obtener información básica de la hoja con manejo de errores
    try:
        max_row = ws.max_row
        max_col = ws.max_column
    except Exception as e:
        logger.warning(f"⚠️ No se pudo obtener dimensiones de hoja {sheet_name}: {e}")
        max_row = max_col = "N/A"
    
    logger.debug(
        f"📊 Información de hoja: {sheet_name}",
        extra={
            "sheet_name": sheet_name,
            "max_rows": max_row,
            "max_columns": max_col,
            "dimension": f"{max_row}x{max_col}"
        }
    )
    
    try:
        rows = list(ws.iter_rows(values_only=True))
    except Exception as e:
        error_msg = f"Error al leer filas de la hoja {sheet_name}: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise ExcelProcessingError(error_msg, sheet_name=sheet_name) from e
    
    if not rows:
        raise HeaderNotFoundError("Hoja vacía", sheet_name=sheet_name)

    header_row_idx = _find_header_row_index(rows)
    if header_row_idx is None:
        raise HeaderNotFoundError(
            f"No se encontró encabezado con campos requeridos: {REQUIRED_EXCEL_FIELDS}",
            sheet_name=sheet_name
        )

    logger.debug(f"📍 Encabezado encontrado en fila {header_row_idx + 1} de {sheet_name}")

    headers = list(rows[header_row_idx])
    missing_fields = _get_missing_required_fields(headers)
    if missing_fields:
        raise MissingRequiredFieldsError(
            f"Faltan campos requeridos: {missing_fields}",
            missing_fields=missing_fields,
            sheet_name=sheet_name
        )

    mapped_headers = [EXCEL_FIELD_MAP.get(h, h) for h in headers]
    
    # Procesar filas de datos
    data_rows = rows[header_row_idx + 1:]
    valid_rules = 0
    invalid_rules = 0
    filtered_rules = 0
    empty_rows = 0
    validation_errors = []
    
    for row_idx, row in enumerate(data_rows, start=header_row_idx + 2):
        if _is_empty_row(row):
            empty_rows += 1
            continue

        try:
            rule = _create_rule_from_row(row, mapped_headers, type_filter, sheet_name, row_idx)
            if rule:
                rules.append(rule)
                valid_rules += 1
            else:
                # Verificar si fue filtrado por tipo
                row_dict = _create_row_dict(row, mapped_headers)
                if not _matches_type_filter(row_dict, type_filter):
                    filtered_rules += 1
                else:
                    invalid_rules += 1
                    
        except RuleValidationError as e:
            invalid_rules += 1
            validation_errors.append({
                "row": e.row_number,
                "error": str(e),
                "data": e.row_data
            })
            # Ya está loggeado en _create_rule_from_row
            
        except Exception as e:
            invalid_rules += 1
            logger.error(
                f"❌ Error inesperado procesando fila {row_idx} en {sheet_name}",
                extra={
                    "sheet_name": sheet_name,
                    "row_number": row_idx,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                exc_info=True
            )

    # Log detallado del procesamiento de la hoja
    logger.info(
        f"📋 Hoja {sheet_name} procesada",
        extra={
            "sheet_name": sheet_name,
            "valid_rules": valid_rules,
            "invalid_rules": invalid_rules,
            "filtered_rules": filtered_rules,
            "empty_rows": empty_rows,
            "total_data_rows": len(data_rows),
            "type_filter": type_filter,
            "validation_errors_count": len(validation_errors)
        }
    )
    
    if valid_rules == 0:
        logger.warning(f"⚠️ No se encontraron reglas válidas en hoja: {sheet_name}")
    
    return rules


def _create_rule_from_row(row: tuple, mapped_headers: List[str], type_filter: str, 
                         sheet_name: str, row_idx: int) -> Optional[RuleData]:
    """
    Crea un objeto RuleData a partir de una fila de datos.

    Args:
        row (tuple): Tupla con los valores de la fila.
        mapped_headers (List[str]): Lista de nombres de columnas mapeados.
        type_filter (str): Tipo de regla a filtrar.
        sheet_name (str): Nombre de la hoja (para logging).
        row_idx (int): Índice de la fila (para logging).

    Returns:
        Optional[RuleData]: Objeto RuleData si la fila es válida, None en caso contrario.
        
    Raises:
        RuleValidationError: Si hay errores en la validación de la regla.
    """
    try:
        # Crear diccionario de datos de la fila
        row_dict = _create_row_dict(row, mapped_headers)
        
        # Filtrar por tipo si está especificado
        if not _matches_type_filter(row_dict, type_filter):
            logger.debug(
                f"🔍 Regla filtrada por tipo en {sheet_name}, fila {row_idx}",
                extra={
                    "sheet_name": sheet_name,
                    "row_number": row_idx,
                    "rule_type": row_dict.get("type", "N/A"),
                    "expected_type": type_filter,
                    "rule_id": row_dict.get("id", "N/A")
                }
            )
            return None

        # Crear y validar objeto RuleData
        try:
            rule = RuleData(**row_dict)
        except TypeError as e:
            # Error de tipos en los parámetros del constructor
            raise RuleValidationError(
                f"Error en tipos de datos: {str(e)}",
                row_data=row_dict,
                row_number=row_idx,
                sheet_name=sheet_name
            ) from e
        except ValueError as e:
            # Error en valores (ej: campos requeridos faltantes)
            raise RuleValidationError(
                f"Error en valores: {str(e)}",
                row_data=row_dict,
                row_number=row_idx,
                sheet_name=sheet_name
            ) from e
        except Exception as e:
            # Otros errores de validación
            raise RuleValidationError(
                f"Error de validación: {str(e)}",
                row_data=row_dict,
                row_number=row_idx,
                sheet_name=sheet_name
            ) from e
        
        logger.debug(
            f"✅ Regla válida creada: {rule.id}",
            extra={
                "sheet_name": sheet_name,
                "row_number": row_idx,
                "rule_id": rule.id,
                "rule_type": rule.type,
                "criticality": getattr(rule, 'criticality', 'N/A')
            }
        )
        
        return rule
        
    except RuleValidationError:
        # Re-lanzar excepciones de validación ya manejadas
        raise
        
    except Exception as e:
        # Errores inesperados
        row_data = dict(zip(mapped_headers, row)) if len(mapped_headers) == len(row) else {"error": "datos_incompletos"}
        
        logger.warning(
            f"❌ Error inesperado creando regla en {sheet_name}, fila {row_idx}",
            extra={
                "sheet_name": sheet_name,
                "row_number": row_idx,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "row_data": row_data
            },
            exc_info=True
        )
        
        raise RuleValidationError(
            f"Error inesperado: {str(e)}",
            row_data=row_data,
            row_number=row_idx,
            sheet_name=sheet_name
        ) from e


def _create_row_dict(row: tuple, mapped_headers: List[str]) -> Dict[str, Any]:
    """
    Crea un diccionario con los datos de una fila, limpiando valores None y espacios.

    Args:
        row (tuple): Tupla con los valores de la fila.
        mapped_headers (List[str]): Lista de nombres de columnas mapeados.

    Returns:
        Dict[str, Any]: Diccionario con los datos de la fila.
    """
    row_dict = {}
    for header, value in zip(mapped_headers, row):
        if value is not None:
            # Limpiar strings, mantener otros tipos como están
            if isinstance(value, str):
                cleaned_value = value.strip()
                if cleaned_value:  # Solo agregar si no está vacío después de limpiar
                    row_dict[header] = cleaned_value
            else:
                row_dict[header] = value
    return row_dict


def _matches_type_filter(row_dict: Dict[str, Any], type_filter: str) -> bool:
    """
    Verifica si una fila coincide con el filtro de tipo especificado.

    Args:
        row_dict (Dict[str, Any]): Diccionario con los datos de la fila.
        type_filter (str): Tipo de regla a filtrar.

    Returns:
        bool: True si la fila coincide con el filtro.
    """
    row_type = row_dict.get("type", "").strip().upper()
    return row_type == type_filter


def _is_empty_row(row: tuple) -> bool:
    """
    Verifica si una fila está completamente vacía.

    Args:
        row (tuple): Tupla con los valores de la fila.

    Returns:
        bool: True si la fila está vacía.
    """
    return all(cell is None or (isinstance(cell, str) and not cell.strip()) for cell in row)


def _find_header_row_index(rows: List[tuple]) -> Optional[int]:
    """
    Busca la fila que contiene todos los campos requeridos.

    Args:
        rows (List[tuple]): Lista de filas del Excel.

    Returns:
        Optional[int]: Índice de la fila de encabezado, o None si no se encuentra.
    """
    required_fields_set = set(REQUIRED_EXCEL_FIELDS)
    
    logger.debug(
        f"🔍 Buscando encabezado",
        extra={
            "total_rows": len(rows),
            "required_fields": list(required_fields_set)
        }
    )
    
    for idx, row in enumerate(rows):
        if row is None:
            continue
            
        # Convertir valores de celda a strings y limpiar
        cell_values = set()
        for cell in row:
            if cell is not None:
                cell_str = str(cell).strip()
                if cell_str:
                    cell_values.add(cell_str)
        
        if required_fields_set.issubset(cell_values):
            logger.debug(
                f"✅ Encabezado encontrado en fila {idx + 1}",
                extra={
                    "row_index": idx,
                    "found_fields": list(cell_values),
                    "required_fields": list(required_fields_set)
                }
            )
            return idx
    
    logger.debug(
        f"❌ Encabezado no encontrado",
        extra={
            "searched_rows": len(rows),
            "required_fields": list(required_fields_set)
        }
    )
    return None


def _get_missing_required_fields(headers: List[Any]) -> List[str]:
    """
    Obtiene la lista de campos requeridos que faltan en los encabezados.

    Args:
        headers (List[Any]): Lista de nombres de columnas.

    Returns:
        List[str]: Lista de campos faltantes.
    """
    if not headers:
        return list(REQUIRED_EXCEL_FIELDS)
    
    # Convertir headers a strings y limpiar
    header_strings = set()
    for header in headers:
        if header is not None:
            header_str = str(header).strip()
            if header_str:
                header_strings.add(header_str)
    
    missing_fields = set(REQUIRED_EXCEL_FIELDS) - header_strings
    return list(missing_fields)


def _validate_required_fields(headers: List[Any]) -> bool:
    """
    Verifica que todos los campos requeridos estén presentes.

    Args:
        headers (List[Any]): Lista de nombres de columnas.

    Returns:
        bool: True si todos los campos requeridos están presentes.
    """
    missing_fields = _get_missing_required_fields(headers)
    return len(missing_fields) == 0


# Funciones de compatibilidad para mantener la interfaz original
def find_header_row_index(rows: list) -> Optional[int]:
    """
    Función de compatibilidad. Busca la fila que contiene todos los campos requeridos.
    
    Args:
        rows (list): Lista de filas del Excel.
    
    Returns:
        Optional[int]: Índice de la fila de encabezado, o None si no se encuentra.
    """
    try:
        return _find_header_row_index(rows)
    except Exception as e:
        logger.error(f"Error buscando encabezado: {e}", exc_info=True)
        return None


def validate_required_fields(headers: list) -> bool:
    """
    Función de compatibilidad. Verifica que todos los campos requeridos estén presentes.
    
    Args:
        headers (list): Lista de nombres de columnas.
    
    Returns:
        bool: True si todos los campos requeridos están presentes.
    """
    try:
        return _validate_required_fields(headers)
    except Exception as e:
        logger.error(f"Error validando campos requeridos: {e}", exc_info=True)
        return False


# Funciones de utilidad para manejo de errores
def handle_excel_processing_gracefully(func):
    """
    Decorador para manejo elegante de errores en funciones de procesamiento de Excel.
    
    Args:
        func: Función a decorar
        
    Returns:
        Función decorada con manejo de errores
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ExcelFileNotFoundError, InvalidExcelFileError) as e:
            # Errores críticos - re-lanzar
            raise
        except ExcelProcessingError as e:
            # Errores de procesamiento - log y retornar valor por defecto
            logger.error(f"Error de procesamiento: {e}", exc_info=True)
            return []
        except Exception as e:
            # Errores inesperados - log y re-lanzar como ExcelProcessingError
            logger.error(f"Error inesperado: {e}", exc_info=True)
            raise ExcelProcessingError(f"Error inesperado: {str(e)}") from e
    
    return wrapper


def is_recoverable_error(error: Exception) -> bool:
    """
    Determina si un error es recuperable y el procesamiento puede continuar.
    
    Args:
        error (Exception): Excepción a evaluar
        
    Returns:
        bool: True si el error es recuperable
    """
    recoverable_errors = (
        HeaderNotFoundError,
        MissingRequiredFieldsError,
        RuleValidationError
    )
    
    return isinstance(error, recoverable_errors)


def get_error_summary(errors: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Genera un resumen de errores para logging y debugging.
    
    Args:
        errors (List[Dict[str, Any]]): Lista de errores ocurridos
        
    Returns:
        Dict[str, Any]: Resumen de errores
    """
    if not errors:
        return {"total_errors": 0}
    
    error_types = {}
    for error in errors:
        error_type = error.get("type", "Unknown")
        error_types[error_type] = error_types.get(error_type, 0) + 1
    
    return {
        "total_rules": len(errors),
        "error_types": error_types,
        "most_common_error": max(error_types.items(), key=lambda x: x[1]) if error_types else None
    }