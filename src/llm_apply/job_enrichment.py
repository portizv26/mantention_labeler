import pandas as pd
from typing import List

from src.schemas import (
    SimpleJob, 
    Job,
    ComponentHierarchy, 
    CriticityEvaluation, 
    PieceComponentMapping,
    EvaluationCriticity
    )

from src.utils import (
    call_llm, 
    call_llm_structured,
    CLIENT,
    MODEL,
    )
import src.prompts as P

from concurrent.futures import ThreadPoolExecutor

def _evaluate_criticity(job_type:str, critical_component:bool, summary:str) -> CriticityEvaluation:
    """
    - Si el trabajo es de inspeccion, se debe considerar como de criticidad baja.
    - Si el trabajo es de relleno, se debe considerar como de criticidad media.
    - Si no es ninguna de las anteriores, se debe evaluar si el componente es crítico o no. 
    - Si no es critico, es de criticidad baja, sin importar el tipo de trabajo.
    - Si es critico:
    - Si el trabajo de reparación es de criticidad media.
    - Si el trabajo de reemplazo es de criticidad alta.
    """
    if "inspec" in job_type.lower():
        cr = "Baja"
    elif "rell" in job_type.lower():
        cr = "Media"
    else:
        if not critical_component:
            cr = "Baja"
        else:
            if "repar" in job_type.lower():
                cr = "Media"
            else:
                obs = f'El trabajo es de tipo {job_type}.\nEl trabajo realizado es: {summary}'
                # summary -> critical_summary
                critical_summary = call_llm(
                    client=CLIENT,
                    model=MODEL,
                    system_prompt=P.job_cleaning_prompts["EvalSystem"],
                    user_prompts=[P.job_cleaning_prompts["EvalUser"], obs]
                )
                # critical_summary -> EvaluationCriticity
                evaluation = call_llm_structured(
                    client=CLIENT,
                    model=MODEL,
                    system_prompt=P.job_cleaning_prompts["EvalSystemStructured"],
                    user_prompts=[P.job_cleaning_prompts["EvalUserStructured"], critical_summary],
                    response_format=EvaluationCriticity
                )
                
                cr = "Alta" if evaluation.isCritic else "Media"
                
    return CriticityEvaluation(
        job_type=job_type,
        summary=summary,
        criticity=cr,
    )
    
def _review_job(job: SimpleJob, component_mapping: dict) -> Job:
    piece = job.piece
    job_type = job.job_type
    comment = job.comment
    
    # ---- component hierarchy -----------------------------------------
    comp = component_mapping.get(piece)
    if not comp or not isinstance(comp, (tuple, list)) or len(comp) != 5:        
        raise ValueError(f"No mapping for piece {piece!r}")
    system, subsystem, component, critical_component, detail = comp

    # ---- criticity evaluation ---------------------------------------
    
    crit = _evaluate_criticity(job_type, critical_component, comment)
    if not isinstance(crit, CriticityEvaluation):
        raise ValueError(f"Criticity evaluation failed for job {job!r}")
    
    critical_change = (crit.criticity == 'Alta')
    
    return Job(
        system=system,
        subsystem=subsystem,
        component=component,
        detail=detail,
        job_type=crit.job_type,
        job_comment=crit.summary,
        criticity=crit.criticity,
        critical_change=critical_change,
        ot_number=job.ot_number,
        liters=job.liters,
    )

def review_jobs(
    simple_jobs: List[SimpleJob],
    component_mapping: dict,
) -> List[Job]:
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda job: _review_job(job, component_mapping), simple_jobs))
    return results
