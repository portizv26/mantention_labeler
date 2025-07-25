import os
import argparse
from src.utils import timeit
from src.schemas import FinalMaintenanceRecord

def setup_log_dir(year: str, week: str):
    """
    Create logs/<year>/week_<week> and expose it via LOG_DIR.
    """
    log_dir = os.path.join("logs", year, f"week_{week}")
    os.makedirs(log_dir, exist_ok=True)
    os.environ["LOG_DIR"] = log_dir
    
def _assign_final_records(records, df):
    """
    Assigns final records to the DataFrame based on the row index.
    """
    final_records = []
    for idx, record in enumerate(records):
        row = df.iloc[idx]
        
        final_record = FinalMaintenanceRecord(
            unit_id=row["UnitId"],
            start_time=str(row["start_time"]),
            end_time=str(row["end_time"]),
            
            detention_type=record.detention_type,
            is_scheduled=record.is_scheduled,
            scheduled_type=record.scheduled_type,
            
            has_inspection=record.has_inspection,
            has_refill=record.has_refill,
            has_repair=record.has_repair,
            has_replacement=record.has_replacement,
            has_other=record.has_other,
            has_critical_change=record.has_critical_change,
            
            summary=record.summary,
            jobs=record.jobs
        )
        final_records.append(final_record)
    
    return final_records    
    


@timeit("full_cycle.json")
def excecute_labeler(year: str, week: str):
    """
    Run the weekly maintenance_labeler pipeline for the given year and ISO-week.
    """
    # 1) Setup logging/timing folder
    setup_log_dir(year, week)
    print(f'Year: {year}, Week: {week}')

    # 2) Import downstream modules only after LOG_DIR is set
    from src.data_handler import read_and_process_data, save_results, save_data
    from src.llm_apply.generate_simple_records import generate_maintenance_records
    from src.llm_apply.record_summarization import generate_records

    # 3) Load the input excel for that week
    print('Loading data... ⏳')
    excel_path_in = os.path.join(
        "data",
        "to_process",
        f"maintenance_data_{year}-{week}.xlsx"
    )
    df = read_and_process_data(excel_path_in, year, week)

    # 4) Run your LLM-based transformations and save the results
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            print('Generating simple records... ⏳')
            simple_records = generate_maintenance_records(df["observation"])
            save_results(simple_records, year, week, "jsondata/simple_records")
            print('Simple records generated! ✅')

            # 5) Persist the outputs however you like
            print('Generating final records... ⏳')
            records = generate_records(simple_records)
            save_results(records, year, week, "jsondata/records")
            
            
            final_records = _assign_final_records(records, df)
            save_results(final_records, year, week, "jsondata/final_records")
            print('Final records generated! ✅')
            break
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt == max_retries:
                print("Max retries reached. Exiting.")
                raise
            else:
                print("Retrying...")
         
    # 5) Save the processed DataFrame to an Excel file       
    excel_path_out = os.path.join(
        "data",
        "processed",
        f"maintenance_records_{year}-{week}.xlsx"
    )
    save_data(file_path=excel_path_out, df=df)
    print(f'Data processed loaded! ( {df.shape[0]}  rows )✅')
    
    
    
def _cli():
    p = argparse.ArgumentParser(
        description="Run the maintenance_labeler pipeline by year & week"
    )
    p.add_argument("--year",  required=True, help="YYYY (e.g. 2025)")
    p.add_argument("--week",  required=True, help="ISO week number 01–53")
    args = p.parse_args()
    excecute_labeler(args.year, args.week)

if __name__ == "__main__":
    _cli()
