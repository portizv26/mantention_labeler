from typing import List, Optional, Any, Dict
from pydantic import BaseModel, ConfigDict, field_validator, FieldValidationInfo
from src.utils import normalize_name

FIELD_ALIAS_MAP: Dict[str, Dict[str, str]] = {
    "scheduled_type": {
        'T-09' : '',
        'T-10' : '',
        'T-11' : '',
        'T-12' : '',
        'T-13' : '',
        'T-14' : '',
        'T-15' : '',
        'T-16' : '',
        'T-17' : '',
        'T-18' : '',
        'T-24' : '',
        "Mantenimiento programado": "Programado",
        "Detencion programada": "Programado",
        'Mantenimiento preventivo': "Preventivo",
        'Mantenimiento preventivo programado': "Preventivo",
        'Programada': "Programado",
        'Mantencion programada': "Programado",
        'Inspeccion programada': "Programado",
        'Pm-express' : 'Programado',
        'Pm-550' : 'Pm-500',
        'Pm-400' : 'Pm-500',
        'Pm 250 horas': 'Pm-250',
        'Pm 500 horas': 'Pm-500',
        'Pm 1000 horas' : 'Pm-1000',
        'Pm 2000 horas' : 'Pm-2000',
        'Pm 4000 horas' : 'Pm-4000',
        'Pm de 250 horas' : "Pm-250",
        'Pm de 500 horas' : "Pm-500",
        'Pm de 1000 horas' : "Pm-1000",
        'Pm de 2000 horas' : "Pm-2000",
        'Pm de 4000 horas' : "Pm-4000",
        'Pm-250 horas': "Pm-250",
        'Pm-500 horas': "Pm-500",
        'Pm-1000 horas': "Pm-1000",
        'Pm-2000 horas': "Pm-2000",
        'Pm-4000 horas': "Pm-4000",
        'Preventivo pm-250' : 'Pm-250',
        'Preventivo pm-500' : 'Pm-500',
        'Preventivo pm-1000' : 'Pm-1000',
        'Preventivo pm-2000' : 'Pm-2000',
        'Preventivo pm-4000' : 'Pm-4000',
        'Ciclo de 250 horas': 'Pm-250',
        'Ciclo de 500 horas': 'Pm-500',
        'Ciclo de 1000 horas': 'Pm-1000',
        'Ciclo de 2000 horas': 'Pm-2000',
        'Ciclo de 4000 horas': 'Pm-4000',
        'Preventivo (pm-250)' : 'Pm-250',
        'Preventivo (pm-500)' : 'Pm-500',
        'Preventivo (pm-1000)' : 'Pm-1000',
        'Preventivo (pm-2000)' : 'Pm-2000',
        'Preventivo (pm-4000)' : 'Pm-4000',
        'Preventivo a 250 horas' : 'Pm-250',
        'Preventivo a 500 horas' : 'Pm-500',
        'Preventivo a 1000 horas' : 'Pm-1000',
        'Preventivo a 2000 horas' : 'Pm-2000',
        'Preventivo a 4000 horas' : 'Pm-4000',
        'Mantenimiento de 250 horas' : 'Pm-250',
        'Mantenimiento de 500 horas' : 'Pm-500',
        'Mantenimiento de 1000 horas' : 'Pm-1000',
        'Mantenimiento de 2000 horas' : 'Pm-2000',
        'Mantenimiento de 4000 horas' : 'Pm-4000',
        'Mantenimiento programado pm-250' : 'Pm-250',
        'Mantenimiento programado pm-500' : 'Pm-500',
        'Mantenimiento programado pm-1000' : 'Pm-1000',
        'Mantenimiento programado pm-2000' : 'Pm-2000',
        'Mantenimiento programado pm-4000' : 'Pm-4000',
        'Mantenimiento programado de 250 horas': 'Pm-250',
        'Mantenimiento programado de 500 horas': 'Pm-500',
        'Mantenimiento programado de 1000 horas' : 'Pm-1000',
        'Mantenimiento programado de 2000 horas' : 'Pm-2000',
        'Mantenimiento programado de 4000 horas' : 'Pm-4000',
        'Mantenimiento programado 250 horas' : 'Pm-250',
        'Mantenimiento programado 500 horas' : 'Pm-500',
        'Mantenimiento programado 1000 horas' : 'Pm-1000',
        'Mantenimiento programado 2000 horas' : 'Pm-2000',
        'Mantenimiento programado 4000 horas' : 'Pm-4000',
        "Relleno": 'Preventivo',
        'Relleno de fluidos' : 'Preventivo',
        'Mantenimiento regular':'Programado',
        'Preventivo programado':'Preventivo',
        'Inspeccion': 'Programado',
        'Reemplazo': 'Programado',
        'Reparacion programada': 'Programado',
        'Preventiva' : 'Preventivo',
        'Correctivo' : 'Correctivo',
        'Chequeo programado' : 'Programado',
        'Activacion sistema de control de traccion' : 'Programado',
        'Chequeo de presion y temperatura' : 'Preventivo',
        'Cambio de neumaticos' : 'Programado',
        'Pm' : 'Programado',
        'Parada programada' : 'Programado',
        'Mantenimiento programado preventivo' : 'Preventivo',
        'Mantenimiento operativo' : 'Programado',
        'Mantenimiento preventivo 1/3 de vida del motor' : '1/3 de vida',
        'Mantenimiento preventivo 1/2 de vida del motor' : '1/2 de vida',
        'Mantenimiento preventivo 1/3 de vida del transmision' : '1/3 de vida',
        'Mantenimiento preventivo 1/2 de vida del transmision' : '1/2 de vida',
        'Mantenimiento preventivo 1/3 de vida' : '1/3 de vida',
        'Mantenimiento preventivo 1/2 de vida' : '1/2 de vida',
        'Chequeo preventivo' : 'Preventivo',
        'Chequeo periodico' : 'Preventivo',
        'Correctivo preventivo' : 'Preventivo',
        'Correctivo programado' : 'Programado',
        'Logistica' : 'Programado',
        'Mantenimiento correctivo' : 'Correctivo',
        'Reparativa' : 'Correctivo',
        'Reparativo' : 'Correctivo',
        'Solicitud del operador' : 'Correctivo',
        'Programada - mantenimiento regular' : 'Programado',
        'Preventivo/predictivo' : 'Preventivo',
        'Pre-pm' : 'Programado',
        'Pre pm' : 'Programado'},
    "detention_type": {
        "falla grave": "Falla funcional",
        'Falla critica': 'Falla funcional',
        'Mantenimiento planificado': 'Programado',
        'Planificado': 'Programado',
        'Mantencion simple' : 'Operacional',
        'Mantencion general' : 'Operacional',
        'Mantencion sin clasificacion' : 'Operacional',
        'Mantenimiento simple' : 'Operacional',
        'Mantenimiento general' : 'Operacional',
        'Mantenimiento sin clasificacion' : 'Operacional',
        'Mantenimiento mixto' : 'Programado',
        'Ninguno' : 'No clasificado',
        'No clasificado' : 'No clasificado',
        'No clasificable' : 'No clasificado',
        'Sin clasificacion' : 'Operacional',
        'Mantenimiento critico' : 'Falla funcional',
        'No operacional' : 'Programado',
        'No aplica' : 'Operacional',
        'Ninguna' : 'Operacional',
        'Programado, preventivo' : 'Programado',
    },
    'job_type': {
        'Cambio' : 'Reemplazo',
        'Chequeo' : 'Inspeccion',
        'Desconexion' : 'Reemplazo',
        'Drenaje' : 'Relleno',
        'Inspeccion/relleno' : 'Inspeccion',
        'Instalacion' : 'Reemplazo',
        'Retiro' : 'Reemplazo',
        'Toma de muestra' : 'Inspeccion',
        'Toma de muestras' : 'Inspeccion',
        'Revision' : 'Inspeccion',
        'Reparacion, reemplazo' : 'Reemplazo',
        'Pruebas' : 'Inspeccion',
        'Observacion' : 'Inspeccion',
        'Dialisis' : 'Inspeccion',
        'Dilizado' : 'Inspeccion',
        'Corte' : 'Reparacion',
        'Ajuste' : 'Reparacion',
    },
    'system': {
        'General' : 'Equipo',
        'Neumatico' : 'Equipo',
        'Electrico' : 'Equipo',
        'No clasificado' : 'Equipo',
        'Iluminacion' : 'Equipo',
        'Hidrico' : 'Hidraulico',
    },
    'subsystem': {
        'Accesorios' : 'General',
        'Climatizacion' : 'General',
        'Estructura' : 'General',
        'Filtrado' : 'Lubricacion',
        'Fluido' : 'Lubricacion',
        'Inyectores': 'Motor',
        'Compresor' : 'Motor',
        'No clasificado' : 'General',
        'Turbos':'Aire',
        'Airea' : 'Aire',
        'Turbo': 'Aire',
        'Ventilador': 'Aire',
        'Ventilacion': 'Aire',
        'Admision': 'Aire',
        'Alimentacion': 'General',
        'Antenas': 'Electrico',
        'Canerias': 'Lubricacion',
        'Carroceria': 'General',
        'Diferencial (derecho)': 'Diferencial',
        'Suministro de combustible': 'Combustible',
        'Control de combustible': 'Combustible',
        
    },  
    'piece' : {
        'Desconocida' : '',
        'Liquido en deposito de agua' : '',
        'Matrices' : '',
        'N/a' : '',
        'Ninguna' : '',
        'No se identificaron actividades relevantes.' : '',
        'No se registraron actividades especificas.' : '',
        'Porta teclado de luces' : '',
        'Sellos de puertas' : "",
        'Vigas, escaleras y barandas' : '',
        'Sistema de aire acondicionado' : 'Aire acondicionado',
        'Sistema electrico aire acondicionado' : 'Aire acondicionado',
        'Arnes izquierdo de luz trocha' : 'Arnes izquierdo de luz',
        'Cabina en general' : 'Cabina',
        
        'Cojin y funda de asiento' : 'Asiento',
        'Conectores' : 'Conector',
        
        'Suspensiones y sellos de espejos' : 'Espejos',
        'Espejos y vidrios' : 'Espejos',
        'Filtros de motor, etc.' : 'Filtros de motor',
        'Flexibles y conectores' : 'Flexibles',
        'Flexibles de levante superior' : 'Flexibles',
        'Filtro' : 'Filtros',
        'Filtro de aire' : 'Filtros de aire',
        
        'Llanta (posicion 1)' : 'Llanta posicion 1',
        'Llanta (posicion 2)' : 'Llanta posicion 2',
        'Llanta (posicion 3)' : 'Llanta posicion 3',
        'Llanta (posicion 4)' : 'Llanta posicion 4',
        'Llanta (posicion 5)' : 'Llanta posicion 5',
        'Llanta (posicion 6)' : 'Llanta posicion 6',
        'Llanta (posicion 7)' : 'Llanta posicion 7',
        'Llanta (posicion 8)' : 'Llanta posicion 8',
        'Manguera (desde orbitrol hasta valvula de direccion)' : 'Manguera',
        'Ambas masas' : 'Masas',
        'Neumatico posicion 1' : 'Neumaticos posicion 1',
        'Neumatico posicion 2' : 'Neumaticos posicion 2',
        'Neumatico posicion 3' : 'Neumaticos posicion 3',
        'Neumatico posicion 4' : 'Neumaticos posicion 4',
        'Neumatico posicion 5' : 'Neumaticos posicion 5',
        'Neumatico posicion 6' : 'Neumaticos posicion 6',
        'Neumatico posicion 7' : 'Neumaticos posicion 7',
        'Neumatico posicion 8' : 'Neumaticos posicion 8',
        'Posicion 1' : 'Neumaticos posicion 1',
        'Posicion 2' : 'Neumaticos posicion 2',
        'Posicion 3' : 'Neumaticos posicion 3',
        'Posicion 4' : 'Neumaticos posicion 4',
        'Posicion 5' : 'Neumaticos posicion 5',
        'Posicion 6' : 'Neumaticos posicion 6',
        'Posicion 7' : 'Neumaticos posicion 7',
        'Posicion 8' : 'Neumaticos posicion 8',
        'Radio musical' : 'Radio',
        'Radio de comunicacion' : 'Radio',
        'Sistema de frenos' : 'Frenos',
        'Tk de combustible' : 'Tanque de combustible',
        'Tanque (tk) de combustible' : 'Tanque de combustible',
        
        'Auto deslizante' : 'Autodeslizante',
        'Bakin' : 'Baking',
        
    }
}


class NormalizedModel(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        # any other global config you use
    )
    
    @field_validator("*", mode="before")
    def _normalize_all_strings(cls, v: Any) -> Any:
        if isinstance(v, str):
            return normalize_name(v)
        return v
    
    @field_validator("*", mode="after")
    @classmethod
    def _apply_alias_map(
        cls, v: Any, info: FieldValidationInfo
    ) -> Any:
        # only remap strings for fields that have an alias dict
        if isinstance(v, str):
            mapping = FIELD_ALIAS_MAP.get(info.field_name)
            if mapping:
                # v is already normalized at this point
                v = mapping.get(v, v)
                
                if 'piece' in info.field_name:
                    # Special case for pieces, we want to ensure they are not empty
                    if 'Cable' in v:
                        v = 'Cable'
                        
                return v 
                
        return v

# ---------- First pass -------------------------------------------------

class hasRelevantActivities(NormalizedModel):
    flag: bool

class MaintenanceType(NormalizedModel):
    is_scheduled: bool
    scheduled_type: Optional[str] = None
    
class SimpleSummary(NormalizedModel):
    summary: str

class SimpleJob(NormalizedModel):
    piece: str
    job_type: str
    comment: str
    ot_number: Optional[str] = None
    liters: Optional[int] = None

class ListSimpleJob(NormalizedModel):
    jobs: List[SimpleJob]

class ComponentHierarchy(NormalizedModel):
    system: str
    subsystem: str
    component: str
    is_critical: bool = False
    detail : Optional[str] = None
    
class PieceComponentMapping(NormalizedModel):
    piece: str
    hierarchy: ComponentHierarchy
    
class ListPieceComponentMapping(NormalizedModel):
    component_mapping: List[PieceComponentMapping]

class SimpleMaintenanceRecord(NormalizedModel):
    is_scheduled: bool
    scheduled_type: Optional[str] = None
    summary: str
    jobs: List[SimpleJob]
    component_mapping: List[PieceComponentMapping]
    

# ---------- Component detection & criticity ----------------------------

class EvaluationCriticity(NormalizedModel):
    isCritic: bool

class CriticityEvaluation(NormalizedModel):
    job_type: str
    summary: str
    criticity: str

class MaintenanceRecordSupervised(NormalizedModel):
    detention_type: str
    summary: str
    
# ---------- Second-level supervised models -----------------------------

class Job(NormalizedModel):
    system: str
    subsystem: str
    component: str
    detail: Optional[str] = None
    job_type: str
    job_comment: str
    criticity: str
    critical_change: bool
    ot_number: Optional[str] = None
    liters: Optional[int] = None

class MaintenanceRecord(NormalizedModel):
    detention_type: str
    is_scheduled: bool
    scheduled_type: Optional[str] = None

    has_inspection: bool
    has_refill: bool
    has_repair: bool
    has_replacement: bool
    has_other: bool
    has_critical_change: bool

    summary: str
    jobs: List[Job]

from datetime import datetime

class FinalMaintenanceRecord(NormalizedModel):
    unit_id : str
    start_time: str
    end_time: str
    
    detention_type: str
    is_scheduled: bool
    scheduled_type: Optional[str] = None

    has_inspection: bool
    has_refill: bool
    has_repair: bool
    has_replacement: bool
    has_other: bool
    has_critical_change: bool

    summary: str
    jobs: List[Job]