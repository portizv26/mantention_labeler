from typing import List, Tuple
import pandas as pd


from src.schemas import SimpleMaintenanceRecord, MaintenanceRecord, MaintenanceRecordSupervised
from src.llm_apply.job_enrichment import review_jobs
from src.utils import (
    map_parallel, 
    call_llm, 
    call_llm_structured, 
    timeit,
    CLIENT,
    MODEL,
    MAX_WORKERS)

import src.prompts as P


def _activity_flags(jobs):
    types = {j.job_type.lower() for j in jobs}
    rank = {"baja": 0, "media": 1, "alta": 2}
    max_r = max((rank.get(j.criticity.lower(), 0) for j in jobs), default=0)
    return {
        "has_inspection": "inspeccion" in types,
        "has_refill": "relleno" in types,
        "has_repair": "reparacion" in types,
        "has_replacement": "reemplazo" in types,
        "has_other": len(types) > 4,
        "has_critical_change": any(j.critical_change for j in jobs),
        "max_criticity": {0: "baja", 1: "media", 2: "alta"}[max_r],
    }
    
def _evaluate_detention_type(activities_flags: dict, scheduled_info: dict) -> str:
    if activities_flags["has_critical_change"]:
        return 'Falla Funcional'
    else:
        if scheduled_info["is_scheduled"]:
            if "Preventivo" in scheduled_info["scheduled_type"]:
                return 'Preventivo'
            else:
                return 'Programado'
        else:
            if (activities_flags["has_inspection"] or activities_flags["has_refill"]) and not (activities_flags["has_repair"] or activities_flags["has_replacement"] or activities_flags["has_other"]):
                return 'Operativo'
            else:
                if activities_flags["max_criticity"] == "baja":
                    return 'Operativo'
                else:
                    return 'Falla Menor'


@timeit("review_times.json")
def _review_maintenance_record(pair : Tuple[int, SimpleMaintenanceRecord]) -> Tuple[int, MaintenanceRecord]:
    row_idx, record = pair
    # record.component_mapping is a list of PieceComponentMapping i should transform to a dict
    component_mapping = {
        m.piece: (
            m.hierarchy.system,
            m.hierarchy.subsystem,
            m.hierarchy.component,
            m.hierarchy.is_critical,
            m.hierarchy.detail
        )
        for m in record.component_mapping
    }
    
    if len(record.jobs) == 0:
        return MaintenanceRecord(
            detention_type="",
            is_scheduled=record.is_scheduled,
            scheduled_type=record.scheduled_type,
            has_inspection=False,
            has_refill=False,
            has_repair=False,
            has_replacement=False,
            has_other=False,
            has_critical_change=False,
            summary="",
            jobs=[],
        )

    jobs = review_jobs(record.jobs, component_mapping)
    # ---- activity flags + summary ------------------------------------
    activities_flags = _activity_flags(jobs)
    scheduled_info = {
        "is_scheduled": record.is_scheduled,
        "scheduled_type": record.scheduled_type
    }

    det_type = _evaluate_detention_type(activities_flags, scheduled_info)

    return MaintenanceRecord(
        detention_type=det_type,
        is_scheduled=record.is_scheduled,
        scheduled_type=record.scheduled_type,
        has_inspection=activities_flags["has_inspection"],
        has_refill=activities_flags["has_refill"],
        has_repair=activities_flags["has_repair"],
        has_replacement=activities_flags["has_replacement"],
        has_other=activities_flags["has_other"],
        has_critical_change=activities_flags["has_critical_change"],
        summary=record.summary,
        jobs=jobs,
    )

def generate_records(
    records: List[SimpleMaintenanceRecord], max_workers: int = MAX_WORKERS
) -> List[MaintenanceRecord]:
    indexed_records = list(enumerate(records))  # [(row_number, record)]
        
    return map_parallel(_review_maintenance_record, indexed_records, max_workers)
