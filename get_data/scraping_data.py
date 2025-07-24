# get_data/scraping_data.py

import requests
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd

def get_books_html(url: str) -> BeautifulSoup:
    """Fetch the HTML content of a page and return a BeautifulSoup object."""
    response = requests.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.content, "html.parser")

def extract_title(book: BeautifulSoup) -> str:
    """Extract the title of a book."""
    return book.h3.a["title"]

def extract_price(book: BeautifulSoup) -> str:
    """Extract the price of a book."""
    return book.find(class_="price_color").text.strip()

def extract_rating(book: BeautifulSoup) -> str:
    """Extract the star rating of a book."""
    classes = book.find(class_="star-rating")["class"]
    return classes[1]  # e.g. 'Three'

def extract_availability(book: BeautifulSoup) -> str:
    """Extract availability info of a book."""
    return book.select_one(".availability").text.strip()

def extract_book_info(book: BeautifulSoup) -> dict:
    """Combine all book info into a dictionary."""
    return {
        "title": extract_title(book),
        "price": extract_price(book),
        "rating": extract_rating(book),
        "availability": extract_availability(book),
    }

def scrape_books(pages: int) -> list[dict]:
    """Scrape `pages` pages and return a list of book‑info dicts."""
    all_books = []
    base_url = "http://books.toscrape.com/catalogue/page-{}.html"
    for page in range(1, pages + 1):
        url = base_url.format(page)
        soup = get_books_html(url)
        articles = soup.find_all("article", class_="product_pod")
        for art in articles:
            all_books.append(extract_book_info(art))
    return all_books

if __name__ == "__main__":
    books = scrape_books(50)
    df_books = pd.DataFrame(books)
    print(f"Number of books scraped: {len(df_books)}")
    out = Path("Data")
    out.mkdir(exist_ok=True)
    csv_path = out / "books_infos.csv"
    df_books.to_csv(csv_path, index=False)
    print(f"Saved raw data to {csv_path}")
