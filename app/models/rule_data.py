from typing import Optional, Dict, Any


class RuleData:
    """
    Representa una regla de validación cargada desde el Excel.
    """
    
    def __init__(self, 
                 id: str,
                 description: str,
                 type: str,
                 documentation: Optional[str] = None,
                 references: Optional[str] = None,
                 criticality: str = "media",
                 explanation: Optional[str] = None,
                 projects: Optional[str] = None,
                 **kwargs):
        """
        Inicializa una regla de validación.
        
        Args:
            id: Identificador único de la regla
            description: Descripción de la validación a realizar
            type: Tipo de regla (estructura, contenido, semántica, etc.)
            documentation: Nombre de la regla
            references: Artefactos o archivos asociados
            criticality: Nivel de criticidad (baja, media, alta)
            explanation: Detalle adicional o ejemplo de la regla
        """
        # Validar campos requeridos
        if not id:
            raise ValueError("id es requerido")
        if not description:
            raise ValueError("description es requerido") 
        if not type:
            raise ValueError("type es requerido")
        
        self.id = str(id)
        self.description = str(description)
        self.type = str(type)
        self.documentation = documentation
        self.references = references
        self.criticality = str(criticality)
        self.explanation = explanation
        self.projects = projects
        
        # Ignorar campos extra (como documentation del JSON)
        # pero no almacenarlos
    
    def summary(self) -> str:
        """
        Devuelve un resumen breve de la regla para logs o prompts IA.
        """
        return f"[{self.id}] {self.description} ({self.type}, {self.criticality})"
    
    def model_dump(self) -> Dict[str, Any]:
        """
        Convierte el objeto a diccionario (compatibilidad con Pydantic).
        """
        return {
            "id": self.id,
            "description": self.description,
            "documentation": self.documentation,
            "type": self.type,
            "references": self.references,
            "criticality": self.criticality,
            "explanation": self.explanation,
            "projects": self.projects
        }
    
    def dict(self) -> Dict[str, Any]:
        """
        Alias para model_dump() (compatibilidad con Pydantic v1).
        """
        return self.model_dump()
    
    def __repr__(self) -> str:
        return f"RuleData(id='{self.id}', type='{self.type}', criticality='{self.criticality}')"
    
    def __str__(self) -> str:
        return self.summary()