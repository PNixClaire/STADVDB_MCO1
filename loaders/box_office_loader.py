import pandas as pd
import sqlite3, os, re
from sqlalchemy import create_engine, text
from datetime import datetime, date
from config import PG_URL, BOX_OFFICE_PATH

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

def _clean_currency(val) -> float | None:
    """Removes $ and , from currency strings and converts to float."""
    if pd.isna(val):
        return None
    try:
        s = str(val).replace("$", "").replace(",", "")
        return _safe_float(s)
    except Exception:
        return None
    
#load the data from the source --> eto lang nabago
def load_dw_from_box_office():
    """
    Loads data from the top box office CSV.
    This enriches existing Dim_Movie and Fact_Book_Adaptation rows.
    It matches movies based on Title and Release Year.
    """
    print("\nStarting Box Office (CSV) load...")
    if not BOX_OFFICE_PATH or not os.path.exists(BOX_OFFICE_PATH):
        print(f"Box office CSV not found: {BOX_OFFICE_PATH}")
        return

    pg = create_engine(PG_URL)
    
    try:
        # Load the CSV
        df = pd.read_csv(BOX_OFFICE_PATH)
    except Exception as e:
        print(f"Error reading CSV file at {BOX_OFFICE_PATH}: {e}")
        return

    col_map = {}
    for col in df.columns:
        c = col.lower().strip()
        if c.startswith('movie'): col_map[col] = 'movie'
        elif c.startswith('release da'): col_map[col] = 'release_date'
        elif c.startswith('distributor'): col_map[col] = 'distributor'
        elif c.startswith('genre'): col_map[col] = 'genre'
        elif c.startswith('2025 gros'): col_map[col] = 'gross'
        elif c.startswith('tickets sol'): col_map[col] = 'tickets'
    
    df = df.rename(columns=col_map)

    # Check for essential columns
    required = ['movie', 'release_date', 'gross', 'tickets']
    if not all(col in df.columns for col in required):
        print(f"Error: Missing required columns. Found: {df.columns.tolist()}")
        print(f"Expected to find: {required}")
        return

    print(f"Loaded {len(df)} rows from box office CSV.")

    updated_dims = 0
    updated_facts = 0
    skipped_rows = 0

    with pg.begin() as c:
        c.execute(text(f"SET search_path TO {DW_SCHEMA}"))

        for _, r in df.iterrows():
            title = r.get('movie')
            release_date = _coerce_date(r.get('release_date'))
            
            if pd.isna(title) or not release_date:
                skipped_rows += 1
                continue 

            release_year = release_date.year
            
            # Find the Movie_SK using the composite key of title + year
            movie_sk = c.execute(text("""
                SELECT Movie_SK FROM Dim_Movie
                WHERE Movie_Title_Source = :title AND Release_Year = :year
            """), {"title": title, "year": release_year}).scalar()
            
            if movie_sk:
                #Update Dim_Movie
                c.execute(text("""
                    UPDATE Dim_Movie SET
                        Distributor = COALESCE(:dist, Distributor),
                        Genre = COALESCE(:genre, Genre)
                    WHERE Movie_SK = :sk
                """), {
                    "dist": r.get('distributor'),
                    "genre": r.get('genre'),
                    "sk": movie_sk
                })
                updated_dims += 1
                
                #Update Fact_Book_Adaptation
                gross_val = _clean_currency(r.get('gross'))
                tickets_val = _safe_int(str(r.get('tickets')).replace(",", ""))

                res = c.execute(text("""
                    UPDATE Fact_Book_Adaptation SET
                        Box_Office_Gross = :gross,
                        Tickets_Sold = :tickets
                    WHERE Movie_SK = :sk
                """), {
                    "gross": gross_val,
                    "tickets": tickets_val,
                    "sk": movie_sk
                })
                
                if res.rowcount > 0:
                    updated_facts += 1
            else:
                skipped_rows += 1

    print("Box Office (CSV) load complete.")
    print(f"  Rows processed: {len(df)}")
    print(f"  Movies found & dimensions updated: {updated_dims}")
    print(f"  Fact rows updated with financials: {updated_facts}")
    print(f"  Movies skipped (not found in DW): {skipped_rows}")