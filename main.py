
# main.py

from pipelines.pipeline_scraping import run_pipeline_scraping

def main():
    print("Bonjour")
    try:
        run_pipeline_scraping(50, "book_store")
    except Exception as e:
        print(f"Une erreur est survenue: {e}")

if __name__ == "__main__":
    main()
