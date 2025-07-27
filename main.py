
# main.py

from pipelines.pipeline_scraping import run_pipeline_scraping

from google_books_API.google_books_API import fetch_books

# Get user input from terminal
query = input(" Enter search keyword: ")
max_result = int(input("Max results (e.g. 40): "))
filter_books = input("Filter (ebooks/free-ebooks/paid-ebooks): ")
orderby = input("Order (relevance/newest): ")

# Call function with user inputs
books = fetch_books(query, max_result, filter_books, orderby)

# Just show how many books we got
print(f"\n Fetched {len(books)} books!")

def main():
    print("Bonjour")
    try:
        run_pipeline_scraping(50, "book_store")
    except Exception as e:
        print(f"Une erreur est survenue: {e}")

if __name__ == "__main__":
    main()