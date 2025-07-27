
# process_data/process_scraping_data.py

import pandas as pd

def convert_availability(value: str) -> bool:
    """Convert availability text to a boolean."""
    return "In stock" in value

def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert DataFrame columns to proper types:
      - title → string
      - price → float (strip symbols)
      - availability → bool
      - rating → int (map One→1 … Five→5)
    """
    df = df.copy()
    df["title"] = df["title"].astype(str)
    df["price"] = (
        df["price"]
        .str.replace("£", "", regex=False)
        .str.replace("Â", "", regex=False)
        .str.strip()
        .astype(float)
    )
    df["availability"] = df["availability"].apply(convert_availability)
    ratings_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
    df["rating"] = df["rating"].map(ratings_map).fillna(0).astype(int)
    return df
