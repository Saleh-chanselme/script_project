# book_pipeline/google_books.py
import os
import logging
import requests
import pandas as pd
import sqlite3


def fetch_books(query: str ,
                max_result : int=40,
                filter_books : str = "paid-ebooks",
                orderby: str = "relevance") -> list[dict] :
    
        # URL de l'API Google Books
    url = "https://www.googleapis.com/books/v1/volumes"

    # Dictionnaire de paramètres pour la requête
    params = {
        "q": query,
        "filter": filter_books,
        "maxResults": max_result,
        "orderBy": orderby
    }
    # Request the Google Books API
    response = requests.get(url, params = params)
    # Check response status code
    response.raise_for_status
    print(response)
    print(response.url)
    # Retrieve the heart of the answer
    data_books_raw = response.json()
    # Afficher les données récupérées
    print(data_books_raw)
    # Afficher le type de la variable data_books_raw
    print(type(data_books_raw))
    # Retrieve the data related to the books using the 'items' key
    data_books = data_books_raw.get('items')
    return data_books
###################################################

def normalize_books(raw_items: list[dict]) -> pd.DataFrame:
    """
    Converts raw API items into a DataFrame with columns: title, price, rating.
    """
    records = []
    for item in raw_items:
        vol = item.get("volumeInfo", {})
        sale = item.get("saleInfo", {})
        records.append({
            "title": vol.get("title", "No Title"),
            "price": sale.get("listPrice", {}).get("amount", None),
            "rating": vol.get("averageRating", None)
        })
    return pd.DataFrame(records)


def clean_books(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows with NaN in price or rating, resets index,
    and adds an 'availability' column set to False.
    """
    df_clean = df.dropna(subset=["price", "rating"]).reset_index(drop=True)
    df_clean["availability"] = False
    return df_clean


def save_to_db(df: pd.DataFrame,
               db_path: str,
               table_name: str = "books_informations") -> None:
    """
    Saves the DataFrame to the specified SQLite database table using sqlite3.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # create table if it doesn't exist
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            title TEXT,
            price REAL,
            rating REAL,
            availability INTEGER
        )
    """)

    # prepare insert statement
    rows = df[["title", "price", "rating", "availability"]].values.tolist()
    cursor.executemany(
        f"INSERT INTO {table_name} (title, price, rating, availability) VALUES (?, ?, ?, ?)",
        rows
    )

    conn.commit()
    conn.close()
    logging.info("Wrote %d rows to table '%s' in %s.", len(rows), table_name, db_path)


if __name__ == "__main__":
    # 1) Fetch books from the API
    raw_items = fetch_books(query="food", max_results=40)
    logging.info("Fetched %d raw items.", len(raw_items))

    # 2) Normalize the raw data into a DataFrame
    df = normalize_books(raw_items)
    logging.info("Normalized into DataFrame with %d rows.", len(df))

    # 3) Clean the DataFrame (drop NaNs, reset index, add availability)
    clean_df = clean_books(df)
    logging.info("After cleaning: %d rows remain.", len(clean_df))

    # 4) Save cleaned data to the database
    db_path = os.getenv("DATABASE_PATH", "book_store.db")
    save_to_db(clean_df, db_path)

    # 5) Confirm total row count in the table
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM books_informations")
    total = cursor.fetchone()[0]
    conn.close()
    logging.info("Total rows now in DB: %d", total)
