import json
from typing import List
from app.models.rule_data import RuleData
from app.utils.logger import get_logger

logger = get_logger(__name__)

def load_rules_from_json(json_path: str) -> List[RuleData]:
    """Carga reglas desde un archivo JSON."""
    
    if not json_path:
        raise ValueError("Ruta del archivo no puede ser None")
    
    logger.info(f"Cargando reglas desde: {json_path}")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        rules_data = json.load(f)
    
    rules = []
    for rule_dict in rules_data:
        # references ya viene como string o null del JSON, no cambiar nada
        
        # Arreglar explanation: number -> string
        if rule_dict.get('explanation') is not None:
            rule_dict['explanation'] = str(rule_dict['explanation'])
        
        rules.append(RuleData(**rule_dict))
    
    logger.info(f"✅ {len(rules)} reglas cargadas")
    return rules