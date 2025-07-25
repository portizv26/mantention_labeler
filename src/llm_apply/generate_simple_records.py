from typing import List, Tuple
import pandas as pd

from src.schemas import (
    SimpleMaintenanceRecord, 
    PieceComponentMapping,
    ComponentHierarchy, 
    MaintenanceType,
    SimpleSummary,
    ListSimpleJob,
    ListPieceComponentMapping,
    hasRelevantActivities, SimpleJob
    )

from src.utils import (
    call_llm, 
    call_llm_structured, 
    map_parallel, 
    store_in_txt,
    timeit,
    MAX_WORKERS,
    CLIENT,
    MODEL,
    know_pieces
    )
import src.prompts as P
import os

forbiden_pieces = ["perno", "golilla", "calugas", "goma", "tuerca", "cojin", "camas", "valvulas", "funda",
                   "flexibles", 'mantenimiento', 'ecm', 'pieza', 'flexible', 'area', 'codo', 'caneria', 'cano',
                   '--', 'zona', 'abrazadera', 'almohadilla', 'testeo', 'regleta', 'camion', 'accesorio',
                   'unidad', 'dispositivo', 'equipo', 'estacion', 'huerta', 'logistica', 'maquina', 'no ',
                   'platina', 'implementos', 'inspeccion'
                   ]


def check_forbiden_pieces(piece: str) -> bool:
    """
    Check if the piece contains any forbidden words.
    Returns True if the piece is valid (does not contain forbidden words), False otherwise.
    """
    if piece is None:
        return False
    for word in forbiden_pieces:
        if word in piece.lower():
            return False
    return True


def review_joblist(joblist: ListSimpleJob) -> ListSimpleJob:
    """
    Review the job list to ensure it contains valid jobs.
    If no jobs are present, return an empty ListSimpleJob.
    """
    if not joblist.jobs:
        print("No jobs found in the job list. Returning empty job list.")
        return ListSimpleJob(jobs=[])

    final_jobs = []
    for job in joblist.jobs:
        piece = job.piece.strip() if job.piece else None
        
        if check_forbiden_pieces(piece) == False:
            print(f"Forbidden piece found: {piece}. Skipping job.")
            continue
        else:
            newJob = SimpleJob(
                piece=piece,
                job_type=job.job_type.strip() if job.job_type else None,
                comment=job.comment.strip() if job.comment else None,
                ot_number=job.ot_number.strip() if job.ot_number else None,
                liters=job.liters if job.liters is not None else None,
            )
            final_jobs.append(newJob)
            

    finalJobsList = ListSimpleJob(jobs=final_jobs)
    return finalJobsList

def ensure_piece_mappings(parsed: SimpleMaintenanceRecord, component_summary: str) -> SimpleMaintenanceRecord:
    pieces_in_jobs = {job.piece for job in parsed.jobs}
    pieces_in_mapping = {mapping.piece for mapping in parsed.component_mapping}
    missing_pieces = pieces_in_jobs - pieces_in_mapping

    # Add missing mappings
    for piece in missing_pieces:
        if piece in know_pieces:
            print(f"Using known mapping for piece: {piece}")
            hierarchy = ComponentHierarchy(**know_pieces[piece])
        else:
            print(f"Piece without mapping found: {piece}")
            obs = (
                f'La pieza en la que te debes centrar es :"{piece}". '
                f'El resumen del trabajo es: "{component_summary}". '
                "Por favor, proporciona la jerarquía de componentes para esta pieza."
            )
            hierarchy = call_llm_structured(
                client=CLIENT,
                model=MODEL,
                system_prompt=P.simple_prompts["SystemComponentMapping"],
                user_prompts=[P.simple_prompts["UserComponentMappingEx"], obs],
                response_format=ComponentHierarchy
            )
        parsed.component_mapping.append(PieceComponentMapping(piece=piece, hierarchy=hierarchy))


    # Set is_critical to False for certain components
    keywords = ("filtro", "culata", "acumulador", "cilindro", "manguera", "rotocamara", 'perno', 'tornillo', 'tuerca', 'codo', 'cabezal', 'aceite', 'caneria', 'tanque')
    for mapping in parsed.component_mapping:
        comp_lower = mapping.hierarchy.component.lower()
        if any(word in comp_lower for word in keywords):
            mapping.hierarchy.is_critical = False

    return parsed

# Insert a newline every 150 characters in observation for readability
def insert_newlines(text, every=170):
    return '\n'.join([text[i:i+every] for i in range(0, len(text), every)])

@timeit("generate_times.json")
def _generate_maintenance_record_single(pair: Tuple[int, str]) -> SimpleMaintenanceRecord:
    row_idx, observation = pair
    fname_txt = f"observation_{row_idx}.txt"
    content = ''
    if len(observation) < 40:
        content += f"\nObservation: {observation}\n"
        store_in_txt(fname_txt, content)
        
        return SimpleMaintenanceRecord(
            is_scheduled=False, scheduled_type=None, summary="", jobs=[], component_mapping=[]
        )

    # free‐text → summary
    text_summary = call_llm(
        client=CLIENT,
        model=MODEL,
        system_prompt=P.simple_prompts["SystemFreeToSummary"],
        user_prompts=[P.simple_prompts["UserFreeToSummary"], 
                      P.simple_prompts["UserExampleFreeToSummary"],
                      P.simple_prompts["AssistantExampleFreeToSummary"],
                      P.simple_prompts["UserExample2FreeToSummary"],
                      P.simple_prompts["AssistantExample2FreeToSummary"],
                      observation],
    )


    formatted_observation = insert_newlines(observation, every=150)
    content += f"\n\nObservation: {formatted_observation}\n"
    content += f"\n\nText Summary: {text_summary}\n"
    
    # summary -> hasRelevantActivities
    flagActivities = call_llm_structured(
        client=CLIENT,
        model=MODEL,
        system_prompt=P.simple_prompts["SystemRelevantActivities"],
        user_prompts=[P.simple_prompts["UserRelevantActivities"], text_summary],
        response_format=hasRelevantActivities
    )
    
    if flagActivities.flag == False:
        # If no relevant activities, return empty record
        print(f"No relevant activities found in observation {row_idx}. Returning empty record.")
        content += "\n\nNo relevant activities found.\n"
        store_in_txt(fname_txt, content)
        return SimpleMaintenanceRecord(
            is_scheduled=False, scheduled_type=None, summary="", jobs=[], component_mapping=[]
        )
    
    # observation -> MaintenanceType
    mant_type: MaintenanceType = call_llm_structured(
        client=CLIENT,
        model=MODEL,
        system_prompt=P.simple_prompts["SystemMaintenanceType"],
        user_prompts=[P.simple_prompts["UserMaintenanceType"], observation],
        response_format=MaintenanceType
    )
    
    # summary -> text_summary_cleaned
    text_summary = call_llm(
        client=CLIENT,
        model=MODEL,
        system_prompt=P.simple_prompts["SystemCleanSummary"],
        user_prompts=[P.simple_prompts["UserCleanSummary"],
                      P.simple_prompts["UserExampleCleanSummary"],  
                      P.simple_prompts["AssistantExampleCleanSummary"],  
                      text_summary],
    )
    content += f"\n\nText Summary Cleaned: {text_summary}\n"
    
    # summary → shortened summary
    shortened_summary = call_llm_structured(
        client=CLIENT,
        model=MODEL,
        system_prompt=P.simple_prompts["SystemShortened"],
        user_prompts=[P.simple_prompts["UserShortened"], text_summary],
        response_format=SimpleSummary
    )
    
    # summary -> JobList
    joblist = call_llm_structured(
        client=CLIENT,
        model=MODEL,
        system_prompt=P.simple_prompts["SystemJobs"],
        user_prompts=[P.simple_prompts["UserJobs"], text_summary],
        response_format=ListSimpleJob
    )
    # Review the job list
    joblist = review_joblist(joblist)
    pieces_in_jobs = [job.piece for job in joblist.jobs]
    
    if len(joblist.jobs) == 0:
        print(f"No valid jobs found in observation {row_idx}. Returning empty record.")
        content += "\n\nNo valid jobs found.\n"
        store_in_txt(fname_txt, content)
        return SimpleMaintenanceRecord(
            is_scheduled=False, scheduled_type=None, summary="", jobs=[], component_mapping=[]
        )
    
    # summary -> component_summary
    component_summary = call_llm(
        client=CLIENT,
        model=MODEL,
        system_prompt=P.simple_prompts["SystemComponentSummary"],
        user_prompts=[P.simple_prompts["UserComponentSummary"], text_summary],
    )
    content += f"\n\nComponent Summary: {component_summary}\n"
    store_in_txt(fname_txt, content)
    
    extra_text = f'Centrate principalmente en las siguientes piezas: {", ".join(pieces_in_jobs)}.\n'
    # component_summary -> PieceComponentMapping
    component_mapping = call_llm_structured(
        client=CLIENT,
        model=MODEL,
        system_prompt=P.simple_prompts["SystemComponentMapping"],
        user_prompts=[P.simple_prompts["UserComponentMapping"], component_summary, extra_text],
        response_format=ListPieceComponentMapping
    )
    
    # summary → structured
    parsed = SimpleMaintenanceRecord(
        is_scheduled=mant_type.is_scheduled,
        scheduled_type=mant_type.scheduled_type,
        summary=shortened_summary.summary,
        jobs= joblist.jobs,
        component_mapping= component_mapping.component_mapping
    )

    parsed = ensure_piece_mappings(parsed, component_summary)

    return parsed

def generate_maintenance_records(
    observations: pd.Series, 
    max_workers: int = MAX_WORKERS
) -> List[SimpleMaintenanceRecord]:
    inputs = list(observations.items())  # [(index, observation), ...]
    return map_parallel(_generate_maintenance_record_single, inputs, max_workers)
    