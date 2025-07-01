import pandas as pd
from typing import List
from app.models.rule_data import RuleData
from app.utils.logger import get_logger

logger = get_logger("excel-loader")

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
    Carga y filtra reglas de validación desde un archivo Excel (.xlsx).

    Este método recorre todas las hojas del archivo, localiza la fila de encabezados,
    valida que tenga las columnas mínimas requeridas, y convierte las filas válidas
    en objetos `RuleData`, filtrando por tipo.

    Args:
        excel_path (str): Ruta completa al archivo Excel (.xlsx) que contiene las reglas.
        type_filter (str, opcional): Tipo de regla a filtrar (estructura, contenido, semántica).

    Returns:
        List[RuleData]: Lista de reglas válidas cargadas desde el archivo.
    """
    rules: List[RuleData] = []

    try:
        with pd.ExcelFile(excel_path) as workbook:
            logger.info(f"Cargando reglas desde: {excel_path}")

            for sheet_name in workbook.sheet_names:
                try:
                    df = workbook.parse(sheet_name, header=None)

                    header_row_idx = find_header_row(df)
                    if header_row_idx is None:
                        logger.warning(
                            f"Hoja '{sheet_name}' omitida: encabezado no encontrado.")
                        continue

                    df.columns = df.iloc[header_row_idx]
                    df = df[(header_row_idx + 1):].reset_index(drop=True)

                    if not validate_required_fields(df.columns):
                        logger.warning(
                            f"Hoja '{sheet_name}' omitida: columnas faltantes.")
                        continue

                    added_count = process_sheet(df, type_filter, rules)
                    logger.info(
                        f"Hoja '{sheet_name}' → {added_count} reglas válidas cargadas.")
                except Exception as e:
                    logger.error(f"Error en hoja '{sheet_name}': {e}")

    except Exception as e:
        logger.error(f"Error al abrir el archivo Excel: {e}")
        return []

    logger.info(f"Total de reglas cargadas: {len(rules)}")
    return rules


def find_header_row(df: pd.DataFrame) -> int:
    """
    Busca la fila que contiene todos los encabezados requeridos.

    Args:
        df (pd.DataFrame): Hoja del Excel sin encabezados definidos.

    Returns:
        int: Índice de la fila que contiene los encabezados. Devuelve None si no se encuentra.
    """
    for i, row in df.iterrows():
        if all(field in row.values for field in REQUIRED_EXCEL_FIELDS):
            return i
    return None


def validate_required_fields(columns) -> bool:
    """
    Valida que la hoja contenga todas las columnas requeridas.

    Args:
        columns (List[str]): Lista de nombres de columnas detectadas en la hoja.

    Returns:
        bool: True si están presentes todas las columnas requeridas, False en caso contrario.
    """
    return all(field in columns for field in REQUIRED_EXCEL_FIELDS)


def process_sheet(df: pd.DataFrame, type_filter: str, rules: List[RuleData]) -> int:
    """
    Procesa una hoja del Excel, construyendo objetos `RuleData` y aplicando un filtro por tipo.

    Args:
        df (pd.DataFrame): DataFrame con las filas de reglas después de los encabezados.
        type_filter (RuleType): Tipo de regla que se desea filtrar (estructura, contenido, semántica).
        rules (List[RuleData]): Lista acumulativa donde se agregarán las reglas válidas.

    Returns:
        int: Cantidad de reglas válidas agregadas desde esta hoja.
    """
    count = 0

    for index, row in df.iterrows():
        try:
            rule_type = str(row["Tipo"]).strip().lower()
            if rule_type != type_filter:
                continue

            references = [
                ref.strip() for ref in str(row["Artefacto"]).split(";") if ref.strip()
            ]

            rule = RuleData(
                id=str(row["Id"]).strip(),
                description=str(row["Descripcion"]).strip(),
                type=rule_type,
                references=references,
                criticality=str(row.get("Criticidad", "media")).strip().lower()
            )

            rules.append(rule)
            count += 1
        except Exception as e:
            logger.warning(f"[Fila {index}] Error al construir la regla: {e}")

    return count
