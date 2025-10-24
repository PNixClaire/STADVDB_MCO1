import pandas as pd
import sqlite3, os, re, io
from sqlalchemy import create_engine, text
from datetime import datetime, date
from config import PG_URL, IMDB_PATH

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
    
def load_dw_from_imdb_actors():
    """
    Loads actor data from the large IMDB names.basics.tsv file.
    It reads the file in chunks and performs a bulk "upsert"
    into Dim_Actor for high performance.
    """
    print("\nStarting IMDB Actor (TSV) load...")
    if not IMDB_PATH or not os.path.exists(IMDB_PATH):
        print(f"IMDB names.basics.tsv not found: {IMDB_PATH}")
        return

    pg = create_engine(PG_URL)
    
    # relevant columns
    use_cols = ["nconst", "primaryName", "birthYear", "primaryProfession"]
    total_rows_processed = 0
    chunk_num = 0

    try:
        # Read the large TSV file in chunks of 100,000
        chunk_iter = pd.read_csv(
            IMDB_PATH,
            sep='\t',        # Tab-separated file
            na_values='\\N',   # IMDB uses '\N' for NULL
            usecols=use_cols,
            chunksize=100000
        )

        for chunk_df in chunk_iter:
            chunk_num += 1
            
            # clean chunk - rename columns to matcg dw
            chunk_df = chunk_df.rename(columns={
                "nconst": "actor_id_source",
                "primaryName": "name",
                "birthYear": "birth_year",
                "primaryProfession": "primary_profession"
            })

            

            # Drop rows where the 'name' is null -> violates NOT NULL constraint in dw
            original_count = len(chunk_df)
            chunk_df = chunk_df.dropna(subset=['name'])
            dropped_rows = original_count - len(chunk_df)
            if dropped_rows > 0:
                print(f"    Dropped {dropped_rows} rows from chunk {chunk_num} due to missing actor name.")
            # Apply cleaning functions
            chunk_df['primary_profession'] = chunk_df['primary_profession'].apply(
                lambda x: str(x)[:255] if pd.notna(x) else None
            )

            # bulk upsert
            try:
                with pg.connect() as conn:
                    with conn.begin() as trans:

                        conn.execute(text(f"SET search_path TO {DW_SCHEMA}"))

                        raw_conn = conn.connection
                        
                        conn.execute(text("""
                            CREATE TEMPORARY TABLE temp_actors (
                                actor_id_source VARCHAR(100),
                                name VARCHAR(255),
                                birth_year INT,
                                primary_profession VARCHAR(255)
                            ) ON COMMIT DROP;
                        """))

                        # Create an in-memory "file" -temp
                        buffer = io.StringIO()
                        chunk_df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N', float_format='%.0f')
                        buffer.seek(0) # Rewind the "file" to the beginning

                        #copy
                        with raw_conn.cursor() as cursor:
                            cursor.copy_expert(
                                "COPY temp_actors FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')",
                                buffer
                            )
                        
                        #upsert chunk
                        result = conn.execute(text("""
                            INSERT INTO Dim_Actor (Actor_ID_Source, Name, Birth_Year, Primary_Profession)
                            SELECT
                                actor_id_source,
                                name,
                                birth_year,
                                primary_profession
                            FROM temp_actors
                            ON CONFLICT (Actor_ID_Source) DO UPDATE SET
                                Name = EXCLUDED.Name,
                                Birth_Year = EXCLUDED.Birth_Year,
                                Primary_Profession = EXCLUDED.Primary_Profession;
                        """))
                        
                        rows_in_chunk = len(chunk_df)
                        total_rows_processed += rows_in_chunk
                        print(f"  Processed chunk {chunk_num} ({rows_in_chunk} rows). Total processed: {total_rows_processed}")
            
            except Exception as e:
                print(f"Error processing chunk {chunk_num}: {e}")
                print("Skipping this chunk and continuing...")
                continue

    except Exception as e:
        print(f"Fatal error reading CSV at {IMDB_PATH}: {e}")
        return

    print("IMDB Actor (TSV) load complete.")
    print(f"  Total rows processed from file: {total_rows_processed}")