from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum


class RuleData(BaseModel):
    """
    Representa una regla de validación cargada desde el Excel.
    """
    id: str = Field(..., description="Identificador único de la regla.")
    description: str = Field(...,
                             description="Descripción de la validación a realizar.")
    documentation: Optional[str] = Field(...,
                      description="Nombre de la regla.") 

    type: str = Field(...,
                      description="Tipo de regla (estructura, contenido, semántica, etc.).")
    references: Optional[str] = Field(...,
                                  description="artefactos o archivos asociados.")
    criticality: str = Field(
        "media", description="Nivel de criticidad (baja, media, alta).")
    explanation: Optional[str] = Field(
        None, description="Detalle adicional o ejemplo de la regla.")

    def summary(self) -> str:
        """
        Devuelve un resumen breve de la regla para logs o prompts IA.
        """
        return f"[{self.id}] {self.description} ({self.type}, {self.criticality})"
