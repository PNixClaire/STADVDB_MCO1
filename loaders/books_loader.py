import pandas as pd
import sqlite3, os, re
from sqlalchemy import create_engine, text
from datetime import datetime, date
from config import PG_URL, BOOKS_PATH

DW_SCHEMA = "dw_books_movies" #our star schema where the data will be loaded

def _derive_movie_id_source_from_imdb(imdb_id: str | None) -> str | None:
    #movie natural key from IMDb so we can upsert easily
    if not imdb_id or not isinstance(imdb_id, str):
        return None

    m = re.search(r"tt?(\d+)", imdb_id.strip())
    return m.group(1) if m else None

def _coerce_date(val) -> date | None:
    if val is None:
        return None
    try:

        if isinstance(val, (int, float)) and 1800 < val < 2100:
             # Handle integer years
            return date(int(val), 1, 1)
        
        dt = pd.to_datetime(val, errors="coerce")
        if pd.isna(dt):
            # Try parsing just the year if full parse fails
            m = re.search(r"(\d{4})", str(val))
            if m:
                return date(int(m.group(1)), 1, 1)
            return None
        return dt.date()
    except Exception:
        return None

#date
def _date_to_sk(d: date) -> int:
    # YYYYMMDD as int
    return d.year * 10000 + d.month * 100 + d.day

def _safe_float(v, lo=None, hi=None):
    try:
        f = float(v)
        if lo is not None and f < lo: return None
        if hi is not None and f > hi: return None
        return f
    except Exception:
        return None
    
def _safe_int(v):
        try:
            return int(float(v))
        except Exception:
            return None 

#load the data from the source --> eto lang nabago
def load_dw_from_books_csv():
    """
    Loads data from the books.csv file into Dim_Book and backfills
    measures into Fact_Book_Adaptation.
    """
    print("\nStarting books.csv load...")
    if not BOOKS_PATH or not os.path.exists(BOOKS_PATH):
        print(f"books.csv not found: {BOOKS_PATH}")
        return

    pg = create_engine(PG_URL)
    
    #Read CSV 
    try:
        # skip bad lines
        books_df = pd.read_csv(BOOKS_PATH, on_bad_lines='skip') 
    except Exception as e:
        print(f"Error reading CSV file at {BOOKS_PATH}: {e}")
        return
        
    books_df.columns = [c.strip().lower() for c in books_df.columns]

    # search for the relevant cols
    if 'bookid' not in books_df.columns:
        print("Error: 'bookID' column not found. Check CSV header.")
        print(f"Found columns: {books_df.columns.tolist()}")
        return

    print(f"Loaded {len(books_df)} rows from books.csv")

    fact_update_data = [] # To store data for fact update
    processed_books = 0
    updated_facts = 0

    with pg.begin() as c:
        c.execute(text(f"SET search_path TO {DW_SCHEMA}"))
        print("Upserting data into Dim_Book...")

        # Upsert Dim_Book 
        for _, r in books_df.iterrows():
            book_id_source = r.get('bookid')
            if pd.isna(book_id_source):
                continue
            
            # Clean book_id_source
            book_id_source = str(_safe_int(book_id_source))
            if not book_id_source:
                continue
            
            pub_date = _coerce_date(r.get("publication_date"))
            
            # Prioritize isbn13, fall back to isbn
            isbn = r.get('isbn13')
            if pd.isna(isbn):
                isbn = r.get('isbn')
            
            # Clean language code (e.g., 'en-US' -> 'EN')
            lang = r.get('language_code')
            if pd.notna(lang):
                lang = str(lang).upper().split('-')[0][:3]
            else:
                lang = None

            try:
                res = c.execute(text("""
                    INSERT INTO Dim_Book
                        (Book_ID_Source, ISBN, Title, Author, Publisher, Publication_Date, Language_Code, Num_Pages)
                    VALUES
                        (:src, :isbn, :title, :author, :pub, :pub_date, :lang, :pages)
                    ON CONFLICT (Book_ID_Source) DO UPDATE SET
                        ISBN = EXCLUDED.ISBN,
                        Title = EXCLUDED.Title,
                        Author = EXCLUDED.Author,
                        Publisher = EXCLUDED.Publisher,
                        Publication_Date = EXCLUDED.Publication_Date,
                        Language_Code = EXCLUDED.Language_Code,
                        Num_Pages = EXCLUDED.Num_Pages
                    RETURNING Book_SK
                """), {
                    "src": book_id_source,
                    "isbn": str(isbn) if pd.notna(isbn) else None,
                    "title": (r.get("title") or f"Book {book_id_source}")[:500],
                    "author": (r.get("authors") or None)[:500] if pd.notna(r.get("authors")) else None,
                    "pub": (r.get("publisher") or None)[:255] if pd.notna(r.get("publisher")) else None,
                    "pub_date": pub_date,
                    "lang": lang,
                    "pages": _safe_int(r.get("num_pages"))
                }).scalar()

                if res is None: # Get SK if a conflict happened but did not return
                    res = c.execute(text("SELECT Book_SK FROM Dim_Book WHERE Book_ID_Source=:src"), {"src": book_id_source}).scalar()
                
                if res is not None:
                    book_sk = int(res)
                    processed_books += 1
                    
                    #Prepare Fact Table Update Data 
                    fact_update_data.append({
                        "bsk": book_sk,
                        "bavg": _safe_float(r.get("average_rating"), lo=0, hi=5),
                        "brc": _safe_int(r.get("ratings_count")),
                        "btrc": _safe_int(r.get("text_reviews_count"))
                    })
            
            except Exception as e:
                print(f"Error processing bookID {book_id_source}: {e}")
                continue 

        print(f"Processed {processed_books} rows for Dim_Book.")

        # Batch Update Fact_Book_Adaptation 
        if fact_update_data:
            print(f"Backfilling {len(fact_update_data)} fact rows with new book measures...")
            
            # Create a temporary table
            c.execute(text("""
                CREATE TEMPORARY TABLE temp_book_measures (
                    book_sk INT PRIMARY KEY,
                    b_avg_rating DECIMAL(4, 2),
                    b_ratings_count INT,
                    b_text_reviews_count INT
                ) ON COMMIT DROP;
            """))
            
            # Insert data
            c.execute(text("""
                INSERT INTO temp_book_measures (book_sk, b_avg_rating, b_ratings_count, b_text_reviews_count)
                VALUES (:bsk, :bavg, :brc, :btrc)
            """), fact_update_data)
            
            # join to temp table
            result = c.execute(text("""
                UPDATE Fact_Book_Adaptation f SET
                    Book_Average_Rating = t.b_avg_rating,
                    Book_Ratings_Count = t.b_ratings_count,
                    Book_Text_Reviews_Count = t.b_text_reviews_count
                FROM temp_book_measures t
                WHERE f.Book_SK = t.book_sk; -- <-- FIXED: Overwrite existing data
            """))
            updated_facts = result.rowcount
            
    print("books.csv load complete.")
    print(f"  Books upserted into Dim_Book: {processed_books}")
    print(f"  Fact rows backfilled with measures: {updated_facts}")