import unicodedata
import os
from openai import OpenAI
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Iterable, List, TypeVar, Callable, Sequence
import time
import logging
import json
import threading
import datetime
import functools

_timing_lock = threading.Lock()

T = TypeVar("T")
R = TypeVar("R")

load_dotenv()

MAX_WORKERS = os.cpu_count() or 1
CLIENT = OpenAI()
MODEL = "gpt-4o-mini"
MODEL_REASON = "o4-mini"

# --------------------------------------------------------------------- #
# Text helpers
# --------------------------------------------------------------------- #
def normalize_name(text: str) -> str:
    """
    Remove accents, strip, lowercase + capitalize first letter.
    Examples
    --------
    >>> normalize_name("  MOTÓR  ")
    'Motor'
    """
    nfkd = unicodedata.normalize("NFKD", text or "")
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    return ascii_only.strip().lower().capitalize()


def skip_logistics(JobObject : list) -> list:
    """
    Skip logistics jobs from the list of SimpleJob objects.
    """
    return [job for job in JobObject if job.job_type.lower() != "logistica"]


# --------------------------------------------------------------------- #
# Concurrency helpers
# --------------------------------------------------------------------- #
def map_parallel(
    fn: Callable[[T], R],
    items: Sequence[T],
    max_workers: int = MAX_WORKERS,
) -> List[R]:
    """
    Simple thread-pool map that preserves order.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(fn, items))

# --------------------------------------------------------------------- #
# LLM helpers
# --------------------------------------------------------------------- #

def call_llm(client, model, system_prompt, user_prompts):
    messages = [{"role": "system", "content": system_prompt}] + [{"role": "user", "content": up} for up in user_prompts]
    return client.chat.completions.create(model=model, messages=messages).choices[0].message.content.strip()

def call_llm_structured(client, model, system_prompt, user_prompts, response_format):
    messages = [{"role": "system", "content": system_prompt}] + [{"role": "user", "content": up} for up in user_prompts]
    if model == MODEL_REASON:
        # For reasoning models, we use a different endpoint
        return client.beta.chat.completions.parse(
            model=model,
            messages=messages,
            response_format=response_format,
            reasoning_effort='low',
        ).choices[0].message.parsed
        
    else:
        return client.beta.chat.completions.parse(
            model=model,
            messages=messages,
            response_format=response_format,
        ).choices[0].message.parsed

# --------------------------------------------------------------------- #
# Logging helpers
# --------------------------------------------------------------------- #
def get_log_dir():
    """
    Lazy-load the log directory from the LOG_DIR env var
    (falling back to logs/default if unset), and ensure it exists.
    """
    log_dir = os.environ.get("LOG_DIR", os.path.join("logs", "default"))
    os.makedirs(log_dir, exist_ok=True)
    return log_dir

def get_logger(fname: str) -> logging.Logger:
    """
    Return a logger that writes to <LOG_DIR>/<fname>.
    """
    log_dir = get_log_dir()
    log_path = os.path.join(log_dir, fname)
    logger = logging.getLogger(log_path)
    logger.setLevel(logging.INFO)

    # avoid double-adding handlers
    if not any(
        isinstance(h, logging.FileHandler) and
        os.path.abspath(h.baseFilename) == os.path.abspath(log_path)
        for h in logger.handlers
    ):
        fh = logging.FileHandler(log_path)
        fh.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        logger.addHandler(fh)

    return logger

def store_in_txt(fname:str, content: str):
    """
    Store content in a text file at <LOG_DIR>/<fname>.
    If the file already exists, append to it.
    """
    log_dir = get_log_dir()
    out_path = os.path.join(log_dir, fname)
    with open(out_path, "a", encoding="utf-8") as f:
        f.write(content + "\n")
    # print(f"Stored content in {out_path}")

def timeit(json_fname: str):
    """
    Measure runtime, append an entry to LOG_DIR/json_fname, and
    print "row X ran in Y.s" if we detect a row-index arg.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            # --- detect row index if passed as first positional arg ---
            row_idx = None
            if args:
                first = args[0]
                # case 1: single int
                if isinstance(first, int):
                    row_idx = first
                # case 2: tuple where first element is int
                elif (
                    isinstance(first, tuple) 
                    and len(first) > 0 
                    and isinstance(first[0], int)
                ):
                    row_idx = first[0]
                # case 3: tuple with 2 strings -> row_idx = concat of both
                elif (
                    isinstance(first, tuple) 
                    and len(first) == 2 
                    and all(isinstance(x, str) for x in first)
                ):
                    row_idx = f"{first[0]}_{first[1]}"
                    
            # --- build JSON entry ---
            start = time.perf_counter()
            result = fn(*args, **kwargs)
            elapsed = round(time.perf_counter() - start, 2)

            entry = {
                "timestamp": datetime.datetime.now().isoformat(),
                "elapsed_s": elapsed,
                "function" : fn.__name__
            }
            if row_idx is not None:
                entry["row"] = row_idx
                

            # --- write to JSON file ---
            out_path = os.path.join(get_log_dir(), json_fname)
            with _timing_lock:
                if os.path.exists(out_path):
                    with open(out_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                else:
                    data = []
                data.append(entry)
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

            # --- print for notebook/CLI ---
            if row_idx is not None:
                print(f"{fn.__name__!r} row {row_idx} ran in {elapsed}s")
            else:
                print(f"{fn.__namxe__!r} ran in {elapsed}s")

            return result
        return wrapper
    return decorator

# --------------------------------------------------------------------- #
# know_pieces
# --------------------------------------------------------------------- #

know_pieces = {
    '' : {
        'system': 'Sin especificar',
        'subsystem': 'Sin especificar',
        'component': 'Sin especificar',
        'is_critical': False,
        'detail': None
    },
    'Aceite hidraulico' : {
        'system': 'Hidraulico',
        'subsystem': 'Fluido',
        'component': 'Aceite',
        'is_critical': False,
        'detail': 'Aceite hidraulico'
    },
    'Aceite de transmision' : {
        'system': 'Tren de fuerza',
        'subsystem': 'Transmision',
        'component': 'Aceite',
        'is_critical': False,
        'detail': 'Aceite de transmision'
    },
    'Acumuladores de direccion' : {
        'system': 'Direccion',
        'subsystem': 'General',
        'component': 'Acumuladores',
        'is_critical': False,
        'detail': 'Acumuladores de direccion'
    },
    'Aire acondicionado' : {
        'system': 'Equipo',
        'subsystem': 'Cabina',
        'component': 'Aire acondicionado',
        'is_critical': False,
        'detail': None
    },
    'Alternador' : {
        'system': 'Motor',
        'subsystem': 'Electrico',
        'component': 'Alternador',
        'is_critical': False,
        'detail': None
    },
    'Antenas' : {
        'system': 'Equipo',
        'subsystem': 'Cabina',
        'component': 'Antena',
        'is_critical': False,
        'detail': None
    },
    'Arnes izquierdo de luz' : {
        'system': 'Equipo',
        'subsystem': 'Cabina',
        'component': 'Arnes de luz',
        'is_critical': False,
        'detail': 'Arnes izquierdo de luz'
    },
    'Asientos' : {
        'system': 'Equipo',
        'subsystem': 'Cabina',
        'component': 'Asiento',
        'is_critical': False,
        'detail': None
    },
    'Bateria' : {
        'system': 'Equipo',
        'subsystem': 'General',
        'component': 'Bateria',
        'is_critical': False,
        'detail': None
    },
    'Cabina' : {
        'system': 'Equipo',
        'subsystem': 'Cabina',
        'component': 'Cabina',
        'is_critical': False,
        'detail': None
    },
    'Cable' : {
        'system': 'Equipo',
        'subsystem': 'Electrico',
        'component': 'Cable',
        'is_critical': False,
        'detail': None
    },
    'Conector' : {
        'system': 'Equipo',
        'subsystem': 'Electrico',
        'component': 'Conector',
        'is_critical': False,
        'detail': None
    },
    'Direccion' : {
        'system': 'Direccion',
        'subsystem': 'Direccion',
        'component': 'Direccion',
        'is_critical': False,
        'detail': None
    },
    'Ducto de enfriamiento de transmision' : {
        'system': 'Tren de fuerza',
        'subsystem': 'Transmision',
        'component': 'Ducto de enfriamiento',
        'is_critical': False,
        'detail': 'Ducto de enfriamiento de transmision'
    },
    'Equipo' : {
        'system': 'Equipo',
        'subsystem': 'General',
        'component': 'Equipo',
        'is_critical': False,
        'detail': None
    },
    'Espejos':{
        'system': 'Equipo',
        'subsystem': 'Cabina',
        'component': 'Espejos',
        'is_critical': False,
        'detail': None
    },
    'Filtros' : {
        'system': 'Hidraulico',
        'subsystem': 'General',
        'component': 'Filtro',
        'is_critical': False,
        'detail': None
    },
    'Filtros de aire' : {
        'system': 'Equipo',
        'subsystem': 'Cabina',
        'component': 'Filtro',
        'is_critical': False,
        'detail': 'Filtro de aire'
    },
    'Filtros de cabina' : {
        'system': 'Equipo',
        'subsystem': 'Cabina',
        'component': 'Filtro',
        'is_critical': False,
        'detail': 'Filtro de cabina'
    },
    'Filtros de diferencial' : {
        'system': 'Tren de fuerza',
        'subsystem': 'Diferencial',
        'component': 'Filtro',
        'is_critical': False,
        'detail': 'Filtro de diferencial'
    },
    'Filtros de motor' : {
        'system': 'Motor',
        'subsystem': 'Lubricacion',
        'component': 'Filtro',
        'is_critical': False,
        'detail': 'Filtro de motor'
    },
    'Flexibles' : {
        'system': 'Equipo',
        'subsystem': 'General',
        'component': 'Flexible',
        'is_critical': False,
        'detail': None
    },
    'Foco inferior derecho' : {
        'system': 'Equipo',
        'subsystem': 'Electrico',
        'component': 'Foco',
        'is_critical': False,
        'detail': 'Foco inferior derecho'
    },
    'Frenos' : {
        'system': 'Frenado',
        'subsystem': 'Frenos',
        'component': 'Freno',
        'is_critical': True,
        'detail': None
    },
    'Lineas de refrigeracion de turbos' : {
        'system': 'Motor',
        'subsystem': 'Aire',
        'component': 'Refrigeracion de turbo',
        'is_critical': False,
        'detail': 'Linea de refrigeracion de turbos',
    },
    'Llantas' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Llanta',
        'is_critical': False,
        'detail': None
    },
    'Llantas posicion 1' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Llanta',
        'is_critical': False,
        'detail': 'Posicion 1',
    },
    'Llantas posicion 2' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Llanta',
        'is_critical': False,
        'detail': 'Posicion 2',
    },
    'Llantas posicion 3' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Llanta',
        'is_critical': False,
        'detail': 'Posicion 3',
    },
    'Llantas posicion 4' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Llanta',
        'is_critical': False,
        'detail': 'Posicion 4',
    },
    'Llantas posicion 5' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Llanta',
        'is_critical': False,
        'detail': 'Posicion 5',
    },
    'Llantas posicion 6' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Llanta',
        'is_critical': False,
        'detail': 'Posicion 6',
    },
    'Llantas posicion 7' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Llanta',
        'is_critical': False,
        'detail': 'Posicion 7',
    },
    'Llantas posicion 8' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Llanta',
        'is_critical': False,
        'detail': 'Posicion 8',
    },
    'Mandos finales' : {
        'system': 'Tren de fuerza',
        'subsystem': 'Diferencial',
        'component': 'Mando final',
        'is_critical': True,
        'detail': None
    },
    'Mangueras' : {
        'system': 'Hidraulico',
        'subsystem': 'General',
        'component': 'Manguera',
        'is_critical': False,
        'detail': None
    },
    'Mangueras de acumuladores de direccion' : {
        'system': 'Direccion',
        'subsystem': 'General',
        'component': 'Mangueras de acumuladores',
        'is_critical': False,
        'detail': 'Mangueras de acumuladores de direccion'
    },
    'Manguera de enfriamiento de freno' : {
        'system': 'Frenado',
        'subsystem': 'General',
        'component': 'Manguera de enfriamiento',
        'is_critical': False,
        'detail': 'Manguera de enfriamiento de freno'
    },
    'Manguera de refrigeracion del turbo' : {
        'system': 'Motor',
        'subsystem': 'Refrigeracion',
        'component': 'Manguera de refrigeracion',
        'is_critical': False,
        'detail': 'Manguera de refrigeracion del turbo'
    },
    'Manifolds de inyectores' : {
        'system': 'Motor',
        'subsystem': 'Combustible',
        'component': 'Manifold de inyectores',
        'is_critical': False,
        'detail': None
    },
    'Masas' : {
        'system': 'Tren de fuerza',
        'subsystem': 'Diferencial',
        'component': 'Masas',
        'is_critical': False,
        'detail': None
    },
    'Motor' : {
        'system': 'Motor',
        'subsystem': 'Motor',
        'component': 'Motor',
        'is_critical': True,
        'detail': None
    },
    'Neumaticos' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Neumatico',
        'is_critical': True,
        'detail': None
    },
    'Neumatico posicion 1' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Neumatico',
        'is_critical': True,
        'detail': 'Posicion 1',
    },
    'Neumatico posicion 2' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Neumatico',
        'is_critical': True,
        'detail': 'Posicion 2',
    },
    'Neumatico posicion 3' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Neumatico',
        'is_critical': True,
        'detail': 'Posicion 3',
    },
    'Neumatico posicion 4' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Neumatico',
        'is_critical': True,
        'detail': 'Posicion 4',
    },
    'Neumatico posicion 5' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Neumatico',
        'is_critical': True,
        'detail': 'Posicion 5',
    },
    'Neumatico posicion 6' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Neumatico',
        'is_critical': True,
        'detail': 'Posicion 6',
    },
    'Neumatico posicion 7' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Neumatico',
        'is_critical': True,
        'detail': 'Posicion 7',
    },
    'Neumatico posicion 8' : {
        'system': 'Equipo',
        'subsystem': 'Neumaticos',
        'component': 'Neumatico',
        'is_critical': True,
        'detail': 'Posicion 8',
    },
    'Radio' : {
        'system': 'Equipo',
        'subsystem': 'Cabina',
        'component': 'Radio',
        'is_critical': False,
        'detail': None
    },
    'Rejillas magneticas de la transmision' : {
        'system': 'Tren de fuerza',
        'subsystem': 'Transmision',
        'component': 'Rejilla magnetica',
        'is_critical': False,
        'detail': 'Rejillas magneticas de la transmision'
    },
    'Sistemas digitales' : {
        'system': 'Equipo',
        'subsystem': 'Electrico',
        'component': 'Sistema digital',
        'is_critical': False,
        'detail': None
    },
    'Suspensiones delanteras' : {
        'system': 'Equipo',
        'subsystem': 'Suspensiones',
        'component': 'Suspension',
        'is_critical': True,
        'detail': 'Suspension delantera',
    },
    'Tapon de masas diferenciales' : {
        'system': 'Tren de fuerza',
        'subsystem': 'Diferencial',
        'component': 'Tapon de masas',
        'is_critical': False,
        'detail': 'Tapon de masas diferenciales'
    },
    'Tanque de combustible' : {
        'system': 'Motor',
        'subsystem': 'Combustible',
        'component': 'Tanque',
        'is_critical': True,
        'detail': 'Tanque de combustible'
    },
    'Tercera viga tolva' : {
            'system': 'Equipo',
            'subsystem': 'Tolva',
            'component': 'Viga',
            'is_critical': False,
            'detail': 'Tercera viga de tolva'
        },
    'Vigas soporte' : {
        'system': 'Equipo',
        'subsystem': 'General',
        'component': 'Viga',
        'is_critical': False,
        'detail': 'Vigas soporte',
    },
}
