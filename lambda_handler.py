# lambda_function.py

import json
import uuid
from typing import Dict, Any

from app.service.rules_synchronizer import sync_rules_from_github
from app.utils.logger import get_logger

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Handler principal de AWS Lambda para sincronización de reglas.
    
    Args:
        event: Evento que activa la ejecución.
        context: Contexto de ejecución de AWS Lambda.
        
    Returns:
        Dict: Respuesta HTTP estructurada.
    """
    # Generar ID único para esta ejecución
    execution_id = str(uuid.uuid4())[:8]
    
    # Log de inicio con contexto de Lambda
    logger.info(f"[{execution_id}] Lambda execution started", extra={
        'execution_id': execution_id,
        'request_id': context.aws_request_id,
        'function_name': context.function_name,
        'remaining_time': context.get_remaining_time_in_millis(),
        'event_source': event.get('source', 'unknown')
    })
    
    try:
        # Ejecutar sincronización usando el servicio
        result = sync_rules_from_github(execution_id)
        
        # Log de resultado
        logger.info(f"[{execution_id}] Lambda execution completed", extra={
            'execution_id': execution_id,
            'success': result.success,
            'rules_count': result.rules_count,
            'execution_time': result.execution_time,
            'remaining_time': context.get_remaining_time_in_millis()
        })
        
        # Respuesta exitosa
        return {
            "statusCode": result.status_code,
            "body": json.dumps({
                "message": result.message,
                "rules_count": result.rules_count,
                "success": result.success,
                "execution_id": result.execution_id,
                "execution_time": result.execution_time
            }, ensure_ascii=False),
            "headers": {
                "Content-Type": "application/json; charset=utf-8"
            }
        }
        
    except Exception as e:
        # Log de error crítico
        logger.exception(f"[{execution_id}] Critical error in lambda handler", extra={
            'execution_id': execution_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'remaining_time': context.get_remaining_time_in_millis()
        })
        
        # Respuesta de error
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": f"Error crítico: {str(e)}",
                "success": False,
                "execution_id": execution_id
            }, ensure_ascii=False),
            "headers": {
                "Content-Type": "application/json; charset=utf-8"
            }
        }

