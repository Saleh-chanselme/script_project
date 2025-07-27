
# database/insert_data.py

import pandas as pd
from pathlib import Path
import sqlite3

def insert_to_database(csv_rel_path: str, db_name: str) -> int:
    """
    Read CSV at `csv_rel_path` (relative to project root),
    insert into SQLite `{db_name}.db`, and return the row count.
    """
    project_root = Path(__file__).resolve().parent.parent
    csv_path = project_root / csv_rel_path
    db_path  = project_root / f"{db_name}.db"

    # Load the CSV
    df_books = pd.read_csv(csv_path)

    # Connect and write to SQL (replace existing table)
    conn = sqlite3.connect(db_path)
    df_books.to_sql("books_informations", conn, if_exists="replace", index=False)
    conn.commit()

    # Verify row count
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM books_informations")
    count = cursor.fetchone()[0]
    conn.close()

    print(f"Inserted {count} records into database file: {db_path}")
    return count


