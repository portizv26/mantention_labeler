import pandas as pd
from datetime import timedelta
import os
from typing import List, Dict, Any
import json

d_cols = {
    'Equipos': 'UnitId',
    'Fecha_Actual': 'Date',
    'Hora_de_Detención': 'hour_in',
    'Hora_de_Entrega': 'hour_out',
    'Equipos': 'UnitId',
    'Tiempo_FS': 'time_FS',
    'Sistema': 'System',
    'Sub_Sistemas': 'Subsystem',
    'Tipo_de_Detención': 'type_detention',
    'Trabajo_Ejecutado': 'observation'
}

def _get_file_extension(file_path: str) -> str:
    """Return the lowercase file extension for a given file path."""
    return os.path.splitext(file_path)[1].lower()


def _read_excel(file_path: str, nheader: int = None) -> pd.DataFrame:
    """Read an Excel file, optionally skipping rows."""
    if nheader is not None:
        return pd.read_excel(file_path, header=nheader)
    return pd.read_excel(file_path)


def _read_csv(file_path: str, nheader: int = None, encoding: str = 'latin1') -> pd.DataFrame:
    """Read a CSV file, optionally skipping rows."""
    if nheader is not None:
        return pd.read_csv(file_path, encoding=encoding, header=nheader)
    return pd.read_csv(file_path, encoding=encoding)


def _ensure_equipos_column(df: pd.DataFrame, read_func, file_path: str) -> pd.DataFrame:
    """If 'Equipos' column is missing, retry reading with skiprows=8."""
    if "Equipos" not in df.columns:
        df = read_func(file_path, skiprows=9)
    return df


def read_data(file_path: str) -> pd.DataFrame:
    """
    Read data from an Excel or CSV file and return a DataFrame.
    This function supports both Excel (.xls, .xlsx) and CSV (.csv) file formats.
    If the file format is not supported, it raises a ValueError.
    If the 'Equipos' column is not found, it tries reading again skipping the first 8 rows.
    Args:
        file_path (str): The path to the file to read.
    Returns:
        pd.DataFrame: The DataFrame containing the data from the file.
    """
    ext = _get_file_extension(file_path)
    if ext in (".xls", ".xlsx"):
        df = _read_excel(file_path)
        df = _ensure_equipos_column(df, _read_excel, file_path)
    elif ext == ".csv":
        df = _read_csv(file_path)
        df = _ensure_equipos_column(df, _read_csv, file_path)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")
    return df


def process_data_sctructure(df: pd.DataFrame, column_name_dictionary: dict, years: int, week: int) -> pd.DataFrame:
    """
    Process the data structure of the DataFrame.
    In order to prepare the DataFrame for further analysis, this function performs the following steps:
    1. Drops duplicate rows.
    2. Renames columns based on the provided dictionary.
    3. Filters rows based on specific conditions for 'UnitId' and 'System'.
    4. Sorts the DataFrame by 'UnitId' and 'Date'.
    5. Converts 'Date' to datetime format.
    6. Converts 'hour_in' and 'hour_out' to timedelta format.
    7. Adjusts 'hour_out' if it is less than 'hour_in' by adding a day.
    8. Combines 'Date' with 'hour_in' and 'hour_out' to create 'start_time' and 'end_time'.
    9. Converts 'start_time' and 'end_time' to datetime format.
    10. Filters rows by year and week of 'start_time' if parameters are provided.
    11. Selects specific columns to keep in the DataFrame.
    12. Sorts the DataFrame by 'UnitId' and 'start_time'.
    13. Resets the index of the DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame to process.
        column_name_dictionary (dict): A dictionary mapping old column names to new column names.
        years (int, optional): Year to filter by.
        week (int, optional): Week number to filter by.
        
    Returns:
        pd.DataFrame: The processed DataFrame with renamed columns.
    """
    df.drop_duplicates(inplace=True) # Step 1: Drop duplicate rows
    df.rename(columns=column_name_dictionary, inplace=True) # Step 2: Rename columns
    df = df[df.UnitId.isin(['T_09', 'T_11', 'T_12', 'T_13', 'T_14', 'T_15', 'T_16', 'T_17', 'T_18', 'T_24'])] # Step 3: Filter rows by 'UnitId'
    df = df[df.System != 'T_Sin trabajos'] # Step 3: Filter rows by 'System'
    df.sort_values(by=['UnitId', 'Date'], inplace=True) # Step 4: Sort by 'UnitId' and 'Date'

    # proper datetime format
    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y') # Step 5: Convert 'Date' to datetime format
    df['hour_in'] = pd.to_timedelta(df['hour_in'].astype(str)) # Step 6: Convert 'hour_in' to timedelta format
    df['hour_out'] = pd.to_timedelta(df['hour_out'].astype(str)) # Step 6: Convert 'hour_out' to timedelta format

    # add a day to hour_out if it is less than hour_in
    df.loc[df['hour_out'] < df['hour_in'], 'hour_out'] += timedelta(days=1) # Step 7: Adjust 'hour_out' if it is less than 'hour_in'

    # Step 8: Combine 'Date' with 'hour_in' and 'hour_out' to create 'start_time' and 'end_time'
    df['start_time'] = df['Date'] + df['hour_in'] 
    df['end_time'] = df['Date'] + df['hour_out']

    # Step 9: Convert 'start_time' and 'end_time' to datetime format
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])

    # Step 10: Filter by year and week if provided
    if years is not None and week is not None:
        df.loc[:, 'WeekYear'] = df['start_time'].dt.strftime('%Y-%U')
        df = df[df['WeekYear'] == f"{years}-{week:02d}"]  # Filter by year and week
        df.drop(columns=['WeekYear'], inplace=True)  # Remove the temporary 'WeekYear' column

    # Step 11: Select specific columns to keep in the DataFrame
    df = df[['UnitId', 'Date', 'start_time', 'end_time', 'time_FS', 'System', 'Subsystem', 'type_detention', 'observation']]

    df.sort_values(by=['UnitId', 'start_time'], inplace=True) # Step 12: Sort by 'UnitId' and 'start_time'
    df.reset_index(drop=True, inplace=True) # Step 13: Reset the index of the DataFrame
    
    if len(df) == 0:
        raise ValueError("No data available for the specified year and week.")
    
    else:
        return df


def clean_comments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the data from the DetencionesV2.xlsx file.
    
    Args:
        df (pd.DataFrame): The DataFrame to process.
        column_name_dictionary (dict): A dictionary mapping old column names to new column names.
        
    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    # Replace "T_" with "" in System
    df['System'] = df['System'].str.replace('T_', '')

    # Fix typos in Subsystem
    df['Subsystem'] = df['Subsystem'].str.replace('T_', '') 
    df['Subsystem'] = df['Subsystem'].str.replace('  ', ' ') 
    df['Subsystem'] = df['Subsystem'].str.replace('Electrico', 'Electrico') 
    df['Subsystem'] = df['Subsystem'].str.replace('Tenperatura', 'Temperatura') 
    df['Subsystem'] = df['Subsystem'].str.replace('T°', 'Temperatura') 
    df['Subsystem'] = df['Subsystem'].str.replace('Bba.', 'Bomba') 
    df['Subsystem'] = df['Subsystem'].str.replace('Tk', 'TK') 

    # observation in lower
    df['observation'] = df['observation'].str.lower()

    # Delete ´ from observation
    tildes = {'á':'a', 'é':'e', 'í':'i', 'ó':'o', 'ú':'u', 'ñ':'n', 'ü':'u'}
    for t in tildes:
        df['observation'] = df['observation'].str.replace(t, tildes[t])
        
    # Fix position references
    for n in [1,2,3,4,5,6]:
        df['observation'] = df['observation'].str.replace(f'pos-{n}', f'posicion {n}')
        df['observation'] = df['observation'].str.replace(f'pos.{n}', f'posicion {n}')
        df['observation'] = df['observation'].str.replace(f'pos {n}', f'posicion {n}')
        df['observation'] = df['observation'].str.replace(f'pos-0{n}', f'posicion {n}')
        df['observation'] = df['observation'].str.replace(f'pos 0{n}', f'posicion {n}')
        df['observation'] = df['observation'].str.replace(f'pos0{n}', f'posicion {n}')
        df['observation'] = df['observation'].str.replace(f'pos.0{n}', f'posicion {n}')
        df['observation'] = df['observation'].str.replace(f'pos. {n}', f'posicion {n}')
        df['observation'] = df['observation'].str.replace(f'pos. 0{n}', f'posicion {n}')
        
    # Delete names from observation
    df.loc[:, 'observation'] = df['observation'].str.replace("j.agulera", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.aguilera", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. aguilera", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.aguilera", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. aguilera", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jose aguilera", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.alfaro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.alfaro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("carlos alfaro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. alfaro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("d.alvares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.alvares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.alvarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("carlos aguirre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("carlo aguirre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. aguirre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.aguirre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c- aguirre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c-aguirre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.aguirre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("fco.aguirre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("fco. aguirre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.aguirre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.aguirre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("v.alarcon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("v. alarcon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("cesar araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("cesar a.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("victor alarcon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. alburquenque", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.alburquenque", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("d.alvarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("d. alvarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a. alvarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("andres alvarez","")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.alavarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.lavarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s.alvarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s. alvarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("carlos araya carmona", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("k.araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l. araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("sebastian araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("sebastian araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("sebastian.araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jose araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g. araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s.araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s,araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s, araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s. araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. araya", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jose a.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jose.a", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.astudillo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.atudillo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.cerda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p. astudillo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p astudillo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("claudo b", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("claudio b", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("claudio c", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("marco barraza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. barraza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.barraza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.barraza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.barraz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.berna", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i.bernal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i. bernal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("ignacio bernal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i.brernal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.bertin", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.bravo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.bravo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.brones", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.briones", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("lenin briones", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.bozo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. bozo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.bugeño", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. bugeño", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.bugueño", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. bugueño", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c bugueño", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("barbara c.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("alejandro c", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("gonzalo c", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("cristian c", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("exequiel c", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("michel c", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.cabrera", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. cabrera", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.castillo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.castillo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.campos", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.campo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.casanga", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.casanga", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.casang", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.castro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("marco carmona", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("crisitan casanga", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.calderon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. calderon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("matias calderon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. calderon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.claudio calderon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("claudio calderon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("francisco carvajal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("claudio caldero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.calderon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.calderron", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. calderon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.campillay", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.carvajal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.carvajal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.carvajal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f. carvajal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.carbajal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("fco.carvajal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("fco. carvajal", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. carrizo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.carrizo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.carmona", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.castro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.castillo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.cerda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.cerda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.cerda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.ceda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.verda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g. cerda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.cerda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.cerda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("v.chuy", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("v.chy", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("v. chuy", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("v.chuvy", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.coba", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i.cofre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i. cofre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.concha", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n. contreras", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s.contreras", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. contreras", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.contreras", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.contreras", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.contreras", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.contrera", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n. contreas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.contrera", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.correa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i.cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l. cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("b.cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("b. cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.cortes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.corte", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.cuello", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a cuello", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a. cuello", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.cuello", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.cuello", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. cuello", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("raul.c", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.diaz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.dias", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("d.diaz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("d-diaz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.duarte", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("felipe e", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("alejandro e", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("ramon e/", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("ramon e", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.echeverria", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.echeverria", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r. echeverria", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.echevarria", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.enriquez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.escobar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.esobar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("alejando escobar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a. escobar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.eloisa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o.espinoza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o.espinosa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o.espinisa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.espinoza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m-espinoza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. espinoza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.espinioza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.espiniza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.espinosa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.espinoza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f. espinosa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f. espinoza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f. espinoza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.errazuriz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("fco. errazuriz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f. errazuriz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.errazuris", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o.fernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o. fernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.fernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r-fernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r. fernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.galvez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("pedro gavez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("pedro galvez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i.gallardo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.gallardo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.gajardo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. gajardo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.gallardo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.gatica", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jorge guerrero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("miguel g", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c, galleguillos", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.galleguillos", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.galleguillos", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.galleguillo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("carlos galleguillos", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("raul g.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.gatica", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r. gatica", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.gamboa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m,gamboa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.godoy", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.godoy", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g. godoy", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e. godoy", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.godoy", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.godoy", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e. gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e gonzales", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a. gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("edo.gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.gonzales", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. gonzales", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o.gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o. gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.gonzalez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.guerrero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.guerrero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("navia guerrero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.guerrero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. guerrero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.guerrero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.guerrero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.fuentes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.hecheverria", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("cristian h", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("d.hernanez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("d.hernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("d. hernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.hernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.hernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("U.hernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("u.hernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("u.hernandes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("u. hernandez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("u.hernadez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("v.herrera", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("edo-henriquez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("edo. henriquez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.henrique", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("eduardo henriquez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("edo.henriquez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.henriquez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e. henriquez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.henriquez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. henriquez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jonathan henriquez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("crisitan honores", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.honores", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c,honores", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. honores", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("b.juares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("b.juarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.juarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("brayan juarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.juica", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("y.jimenes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("y.jimenez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("y. jimenes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("y. jimenez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.jofre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l. jofre", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.labra", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f. labra", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("fco . labra", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.laferte", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.larraguibel", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.larraquibel", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f-larraquibel", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.leon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.lenin", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.lemus", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.leiva", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l-leiva", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l,leiva", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("v.leon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.loisa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r. loisa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.loiza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r. loiza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.loaiza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r. loaiza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.lopes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.lopez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. lopez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a loza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.loza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.maluenda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r. maluenda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("k.madariaga", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.maluenda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.marin", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n. marin", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("nicolas marin", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.martinez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e. martinez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.meza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i.miranda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i. miranda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.montero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("k.mondaga", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("k. mondaga", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. montero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("k.morata", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("k. morata", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.morales", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. morales", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.muñoz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c-muñoz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. muñoz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("cristian m.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("cristian muñoz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.muñoz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.muños", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.muños", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c muños", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c muñoz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.muñoz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i.muñoz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f. muñoz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m . munizaga", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. munizaga", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("manuel munizaga", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.munizaga", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.mumizaga", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.nilo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.nilo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.nuñez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.nuñez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.nuñez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.nuñes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.nuñez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r. nuñez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("fco.nuñez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("fco. nuñez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("marcelo nuñez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("rodrigo nuñez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("cristian o.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a. ochoa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.ochoa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.ochoa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e. ochoa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("francisco ogalde", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.oizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("b. olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c, olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("luis olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r. olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("rodrigo olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.ogalde", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.ogalde", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("v.olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("b.olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("b.oliovares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r. olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("john olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. olivares", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o.ortega", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o. ortega", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.ortis", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.ortiz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. oritz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. otiz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. ortiz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jose ortiz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("luis pasten", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jorge perez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.pasten", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.pasten", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.pasten", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o.pasten", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o. pasten", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.pasten", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s.pereira", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("h.perez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("h. perez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("hector perez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.perez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g. perez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.perez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jorge p", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jorge perez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j,perez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("pablo pinto", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jose pinto", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.pinto", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.pinto", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("paola pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e. pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e, pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e-pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e- pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g. pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("alvaro pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("alvaro pzarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("w.pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("w. pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.pizarro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.plaza", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.portilla", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.poltilla", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("w.portilla", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.ponce", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. ponce", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.quintana", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o.rtega", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.ramirez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jp.ramirez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.ramos", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("J.rebolledo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.rebolledo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. rebolledo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("juan r.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("juan r", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.reyes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e. reyes", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.rodriguez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("mario rodriguez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.roja", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.roja", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s.rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s-rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s. rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("edo.rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s.romero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s. romero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s romero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.rubilar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("luis santander", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.santanader","")
    df.loc[:, 'observation'] = df['observation'].str.replace("sergio santiago", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s. santiago", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s.santiago", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s. satiago", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("abel s", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.salina", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.salinas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.saldivar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i.saldivar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i. saldivar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i.saldivia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i. saldivia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.saldivar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j saldivar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("d.sanchez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.sanches", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.sanchez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.sanchez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n. sanchez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.satander", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l. santander", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.santader", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.santander", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("n.santander", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.salinas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("b.salinas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.salinas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.salfate", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("J.salfate", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.segovia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.segovia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f. segovia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.segovia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p. segovia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("camilo segunda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.segunda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. segunda", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.segura", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. segura", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.segura", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.segura", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.saud", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.sierra", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.soto", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. soto", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j soto", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("jonathan soto", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("alejandreo.s", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("alejandro s", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("alejandro.s", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.rapia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.tapia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.thenoux", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p. tapia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("l.santander", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a.salazar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("a. salazar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i saldivar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("i.saldivar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o.salazar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("pablo tapia","")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.tapia","")
    df.loc[:, 'observation'] = df['observation'].str.replace("i.tello", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("philippe t", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("phillippe t", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("phillipe t", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.torty", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f. torty", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f. torti", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f-torti", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.torti", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("claudo u/", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("claudo u", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("g.ugarte", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.ugueño", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.urbina", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e.ulloa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.urrutia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r. urrutia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("u.urrutia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("u. urrutia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r-urrutia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("e. ulloa", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.valero", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.valdivia", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.varas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f. varas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("fdo.varas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("fdo. varas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.vega", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c. vega", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("f.vega", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("felipe vega","")
    df.loc[:, 'observation'] = df['observation'].str.replace("p.vega", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("p. vega", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.veliz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("mauro veliz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("mauricio veliz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("marcelo veliz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.veliz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.veliz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("h.veliz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("h. veliz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m. veliz", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s.vergara", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("s. vergara", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("o.villanueva", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j.zapata", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("j. zapata", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("h.zambra", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("h. zambra", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("h.zabra", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("m.zambra", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("sm.zambra", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("r.zepeda", "") 
    df.loc[:, 'observation'] = df['observation'].str.replace("f.zepeda", "") 
    df.loc[:, 'observation'] = df['observation'].str.replace("f zepeda", "") 
    df.loc[:, 'observation'] = df['observation'].str.replace("f.zeleda", "") 
    df.loc[:, 'observation'] = df['observation'].str.replace("d.zepeda", "") 
    df.loc[:, 'observation'] = df['observation'].str.replace("v.zepeda", "") 

    df.loc[:, 'observation'] = df['observation'].str.replace("ot-ot", "ot.")
    df.loc[:, 'observation'] = df['observation'].str.replace("ok.huerta", "ok.")
    df.loc[:, 'observation'] = df['observation'].str.replace("scobar", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("mant.mina", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(".huerta", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace("ok.3duck", "ok.")
    df.loc[:, 'observation'] = df['observation'].str.replace(".3duck", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace("(mauricio )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(backlog)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(obs.)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("+2 aprendices", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("2 aprendices", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("barray", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(".nilo.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(". rojas", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(",alvarez", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("personal finning", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("ampos-", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("alderon", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(obs.)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(obs)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(observacion)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("( observacion )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(observacion.)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(" s,", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("----", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(" . ,", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace("---", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(". -.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("   ", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("  ", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(-).", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(-)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("( -)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(- )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("( – ).", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("( – )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("--.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("--", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("- -", "-")
    df.loc[:, 'observation'] = df['observation'].str.replace("  -  -  ", "-")
    df.loc[:, 'observation'] = df['observation'].str.replace("()", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("().", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(  )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("., .", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("-s", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("()g&g", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("--", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(.-)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("( - )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(s-)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(s)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(  -)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("( )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("--", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(".--", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace(".-", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace("( -- )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(". -n", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(". .", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace("..", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace(".--", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace(".- .", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace(".-.", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace(", .",".")
    df.loc[:, 'observation'] = df['observation'].str.replace("( –– .",".")
    df.loc[:, 'observation'] = df['observation'].str.replace(".--",".")
    df.loc[:, 'observation'] = df['observation'].str.replace(".-",".")
    df.loc[:, 'observation'] = df['observation'].str.replace(".-",".")
    df.loc[:, 'observation'] = df['observation'].str.replace("- - -", "-")
    df.loc[:, 'observation'] = df['observation'].str.replace("( –– ).", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace("(.)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(, , e, )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(",.", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace(".,", ".")
    df.loc[:, 'observation'] = df['observation'].str.replace("alderon- luis s. onores", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("shitch", "switch")
    df.loc[:, 'observation'] = df['observation'].str.replace("n2", "nitrogeno")
    df.loc[:, 'observation'] = df['observation'].str.replace("quedando ok.-","")
    df.loc[:, 'observation'] = df['observation'].str.replace("quedando ok.","")
    df.loc[:, 'observation'] = df['observation'].str.replace("quedando operativo.","")
    df.loc[:, 'observation'] = df['observation'].str.replace("quedando operativo","")
    df.loc[:, 'observation'] = df['observation'].str.replace(", equipo ok.","")
    df.loc[:, 'observation'] = df['observation'].str.replace(", equipo ok","")
    df.loc[:, 'observation'] = df['observation'].str.replace("equipo operativo.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(".equipo operativo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(". equipo operativo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("se entrega equipo operativo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("queda en oficina","")
    df.loc[:, 'observation'] = df['observation'].str.replace(", en observacion","")
    df.loc[:, 'observation'] = df['observation'].str.replace("se chequea equipo completo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("se chequean nivele ok","")
    df.loc[:, 'observation'] = df['observation'].str.replace("trabaja terminado", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("con fecha", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("se restrige acceso con cinta de peligro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("se realiza orden y aseo a area de trabajo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("- se-","")
    df.loc[:, 'observation'] = df['observation'].str.replace("( – s)", "")  
    df.loc[:, 'observation'] = df['observation'].str.replace("(-– )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(" (-– )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(" (-–- )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("-–", "-")
    df.loc[:, 'observation'] = df['observation'].str.replace("-ed", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("-fc–", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("-z–", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("-g–", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("c.s.i.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(" csi", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("csi ", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(" ks", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("ks ", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("k&s ", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(" s,", ",")
    df.loc[:, 'observation'] = df['observation'].str.replace("7i-5423", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("10w-", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("restex", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("se realiza levantamiento de mangueras adicionales para cambio de motor diesel. mangueras sistema enfriamiento freno, direccion y neumatico.", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("lavado de equipo por contaminacion devido a eliminacion de fuga de aceite por joke de transmision", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("personal de", "personal")
    df.loc[:, 'observation'] = df['observation'].str.replace("personal g y g", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("chequeo visual de componentes, chequeo de cortes en gomas y retiro de piedras", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(z)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(movil)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(puchos)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(quedan en meson)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(primero)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(funcionando correctamente)", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("equipo operativo", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("color oscuro", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(", ok ", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(", , ,", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(", , ", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(", ,", "")
    df.loc[:, 'observation'] = df['observation'].str.replace(" _ ", " ")
    df.loc[:, 'observation'] = df['observation'].str.replace("---", " ")
    df.loc[:, 'observation'] = df['observation'].str.replace("--", " ")
    df.loc[:, 'observation'] = df['observation'].str.replace("(, )", "")
    df.loc[:, 'observation'] = df['observation'].str.replace("(sin avance)", " ")
    df.loc[:, 'observation'] = df['observation'].str.replace("(nuevos)", " ")
    df.loc[:, 'observation'] = df['observation'].str.replace("alazar", "")
    
    df.loc[:, 'observation'] = df['observation'].str.replace('a/c', 'aire acondicionado')
    df.loc[:, 'observation'] = df['observation'].str.replace('bba', 'bomba')
    df.loc[:, 'observation'] = df['observation'].str.replace('mfi', 'mando final izquierdo')
    df.loc[:, 'observation'] = df['observation'].str.replace('3 patito', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('tres patito', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('3 pato', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('tres pato', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('3duck', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('3 duck', '') 
    df.loc[:, 'observation'] = df['observation'].str.replace('tres duck', '') 
    df.loc[:, 'observation'] = df['observation'].str.replace('tresduck', '') 
    df.loc[:, 'observation'] = df['observation'].str.replace('harness', 'arnes')
    df.loc[:, 'observation'] = df['observation'].str.replace('harnes', 'arnes')
    df.loc[:, 'observation'] = df['observation'].str.replace('b-tag', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('b_tag', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('bytag', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('by-tag', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('btag', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('b tag', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('bitag', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('block', 'bloque')
    df.loc[:, 'observation'] = df['observation'].str.replace('suspencion', 'suspension')
    df.loc[:, 'observation'] = df['observation'].str.replace('personalcontinua', 'personal continua')
    df.loc[:, 'observation'] = df['observation'].str.replace('personal k&s', '')
    df.loc[:, 'observation'] = df['observation'].str.replace(' scl', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('n.o', 'numero')
    df.loc[:, 'observation'] = df['observation'].str.replace('n°', 'numero')
    df.loc[:, 'observation'] = df['observation'].str.replace('n.°', 'numero')
    df.loc[:, 'observation'] = df['observation'].str.replace('g&g', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('gyg', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('cojon', 'cojin')
    df.loc[:, 'observation'] = df['observation'].str.replace('serchap', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('de sci,', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('de finning,', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('de huerta,', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('de sci', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('de finning', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('de huerta,', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('huerta', ',')
    df.loc[:, 'observation'] = df['observation'].str.replace('quedando ok', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('3 meses', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('chequeo en taller', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('chequeo preventivo en taller', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('quedando el equipo operativo', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('estacion de servicio, linea de v/v venteo', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('se procede al lavado completo del equipo mina', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('se procede al lavado completo del equipo', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('posterior al retiro de grasa', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('equipo sube a taller para mantencion programada de', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('equipo sube a taller para mantencion programada', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('equipo sube a taller', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('operador deja equipo fuera de servicio por falla en suspension asiento', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('r134a', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('realiza lavado completo', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('se sube equipo a taller', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('se realiza orden y aseo', '')
    df.loc[:, 'observation'] = df['observation'].str.replace("se realiza retiro de componentes a loza de lavado", '')
    df.loc[:, 'observation'] = df['observation'].str.replace('se realiza ingreso de equipo a loza', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('orden del area', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('queda en losa de lavado para limpiar y enviar a reparar', '')
    df.loc[:, 'observation'] = df['observation'].str.replace('queda en losa de lavado para limpiar', '')
    df['observation'] = df['observation'].str.replace('_x000d_', '')
    
    df.loc[:, 'observation'] = df['observation'].str.replace("   ", " ")
    df.loc[:, 'observation'] = df['observation'].str.replace("   ", " ")
    df.loc[:, 'observation'] = df['observation'].str.replace("  ", " ")
    df.loc[:, 'observation'] = df['observation'].str.replace("  ", " ")
    df.loc[:, 'observation'] = df['observation'].str.replace("  ", " ")
    df.loc[:, 'observation'] = df['observation'].str.replace("  ", " ")
    df.loc[:, 'observation'] = df['observation'].str.replace("  ", " ")

    df.drop_duplicates(inplace=True)

    return df


def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the DataFrame to remove unwanted observations and clean up the data.
    Parameters:
    df (pd.DataFrame): Input DataFrame with maintenance records.
    Returns:
    pd.DataFrame: Filtered and cleaned DataFrame.
    """
    df.loc[:,'observation_length'] = df.observation.apply(lambda x: len(x))
    df.loc[df.observation_length < 40, 'observation'] = ""
    df.loc[df.observation == 'se sube equipo a taller mm para lavado e incio pm 2000 hrs.e retira componentes para mantencion', 'observation'] = ""
    df.loc[df['observation'].str.contains('no se realizan trabajos', case=False), 'observation'] = ""
    df.loc[(df.observation_length <=400) & (df.observation.str.contains('olor', case=False)), 
           'observation'] = ""
    df.loc[(df.observation_length <=400) & (df.observation_length >= 40) &
            (~df.observation.str.contains('aceite', case=False)) &
            (~df.observation.str.contains('ajuste', case=False)) &
            (~df.observation.str.contains('alarmas', case=False)) &
            (~df.observation.str.contains('alternador', case=False)) &
            (~df.observation.str.contains('aprete', case=False)) &
            (~df.observation.str.contains('arreglo', case=False)) &
            (~df.observation.str.contains('cambio', case=False)) &
            (~df.observation.str.contains('conectores de diferencial', case=False)) &
            (~df.observation.str.contains('dañ', case=False)) &
            (~df.observation.str.contains('desengrasa', case=False)) &
            (~df.observation.str.contains('diferencial', case=False)) &
            (~df.observation.str.contains('drenaje', case=False)) &
            (~df.observation.str.contains('espejo', case=False)) &
            (~df.observation.str.contains('falla', case=False)) &
            (~df.observation.str.contains('fuga', case=False)) &
            (~df.observation.str.contains('instala', case=False)) &
            (~df.observation.str.contains('motor', case=False)) &
            (~df.observation.str.contains('neumatico', case=False)) &
            (~df.observation.str.contains('pinchado', case=False)) &
            (~df.observation.str.contains('purga', case=False)) &
            (~df.observation.str.contains('regulariza', case=False)) &
            (~df.observation.str.contains('rellen', case=False)) &
            (~df.observation.str.contains('repara', case=False)) &
            (~df.observation.str.contains('reset', case=False)) &
            (~df.observation.str.contains('retir', case=False)) &
            (~df.observation.str.contains('reubica', case=False)) &
            (~df.observation.str.contains('retocamara', case=False)) &
            (~df.observation.str.contains('rotocamara', case=False)), 
            'observation'] = ""

    df.loc[:,'observation_length_new'] = df.observation.apply(lambda x: len(x))
    df.loc[:, 'isMantention'] = df.apply(lambda x: f'Si ({x['Subsystem']}).' if x['type_detention'] == 'Programada' else 'No.', axis=1)
    df.loc[:, 'empty_obs'] = df['observation'].apply(lambda x: True if x == "" else False)
    df.loc[:, 'observation'] = df.apply(lambda x: f'Mantenimiento Programado.\n{x["observation"]}' if x['isMantention']!='No.' else  f'{x["observation"]}' , axis=1)

    df.reset_index(drop=True, inplace=True)
    
    return df


def read_and_process_data(file_path: str, year: int, week: int) -> pd.DataFrame:
    """
    Read and process the maintenance data from the specified file path.
    Parameters:
        file_path (str): Path to the input file containing maintenance records.
        years (int): Number of years to consider in the data.
        weeks (int): Number of weeks to consider in the data.
    Returns:
        pd.DataFrame: Processed DataFrame with maintenance records.
    """
    
    df = read_data(file_path)
    df = process_data_sctructure(df, d_cols, year, week)
    df = clean_comments(df)
    df = filter_data(df)
    
    return df


def save_data(file_path: str, df: pd.DataFrame) -> None:
    """
    Save the processed DataFrame to the specified file path.
    
    Parameters:
    file_path (str): Path to save the output file.
    df (pd.DataFrame): DataFrame to save.
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    ext = os.path.splitext(file_path)[1].lower()
    if ext in (".xls", ".xlsx"):
        df.to_excel(file_path, index=False)
    elif ext == ".csv":
        df.to_csv(file_path, index=False, encoding='latin1')
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def save_results(
    records: list,
    year: str,
    week: str,
    out_dir: str = "results"
) -> str:
    """
    Serialize a list of Pydantic MaintenanceRecord objects to JSON.
    Files end up in <out_dir>/maintenance_records_<year>_<week>.json
    """
    os.makedirs(out_dir, exist_ok=True)
    fn = f"maintenance_records_{year}_{week}.json"
    out_path = os.path.join(out_dir, fn)

    # If these are Pydantic models, use .model_dump(); otherwise .dict() or vars()
    payload = [r.model_dump() for r in records]  

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return out_path
