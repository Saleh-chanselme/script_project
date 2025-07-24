# pipelines/pipeline_scraping.py

from pathlib import Path
import pandas as pd

from get_data.scraping_data import scrape_books
from process_data.process_scraping_data import convert_types
from database.insert_data import insert_to_database

def run_pipeline_scraping(pages: int, db_name: str):
    """
    1) Scrape pages → raw DataFrame
    2) Write Data/books_infos.csv
    3) Process types → cleaned DataFrame
    4) Write Data/clean_scraping_data.csv
    5) Load Data/books_infos.csv into `{db_name}.db` and verify insertion
    """
    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / "Data"
    data_dir.mkdir(exist_ok=True)

    # 1 & 2: Scrape and save raw CSV
    print("Start scraping:")
    books_list = scrape_books(pages)
    df_raw = pd.DataFrame(books_list)
    raw_csv = data_dir / "books_infos.csv"
    df_raw.to_csv(raw_csv, index=False)
    print(f"Raw data saved to {raw_csv}")
    print("#" * 50)

    # 3 & 4: Process and save cleaned CSV
    print("Start processing data:")
    df_clean = convert_types(df_raw)
    clean_csv = data_dir / "clean_scraping_data.csv"
    df_clean.to_csv(clean_csv, index=False)
    print(f"Cleaned data saved to {clean_csv}")
    print("#" * 50)

    # 5: Insert into DB and capture row count
    print("Start loading into database:")
    inserted = insert_to_database("Data/books_infos.csv", db_name)
    print(f"Database load complete – {inserted} rows now in `{db_name}.db`")
    print("#" * 50)
    print("Pipeline completed successfully!")

