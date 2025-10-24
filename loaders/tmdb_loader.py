import pandas as pd
import os, re, io, requests, time
from sqlalchemy import create_engine, text
from datetime import datetime, date
from config import PG_URL, TMDB_API_KEY

DW_SCHEMA = "dw_books_movies" #our star schema where the data will be loaded

#ENDPOINTS
FIND_URL = "https://api.themoviedb.org/3/find/{external_id}"
MOVIE_URL = "https://api.themoviedb.org/3/movie/{tmdb_id}"
PERSON_URL = "https://api.themoviedb.org/3/person/{person_id}"

def _derive_movie_id_source_from_imdb(imdb_id: str | None) -> str | None:
    #movie natural key from IMDb so we can upsert easily
    if not imdb_id or not isinstance(imdb_id, str):
        return None
    # --- MODIFIED: Handle 'tt' prefix or just the number ---
    m = re.search(r"tt?(\d+)", imdb_id.strip())
    return m.group(1) if m else None

def _coerce_date(val) -> date | None:
    if val is None:
        return None
    try:
        # --- MODIFIED: Handle various date formats, including just year ---
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
            return int(float(v)) # --- MODIFIED: Cast via float to handle "123.0" ---
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
    
API_HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_API_KEY}"
}

def _get_tmdb_id(external_id, external_source_type):
    """
    Finds the TMDb ID from an IMDb ID using the /find endpoint.
    external_source_type can be 'imdb_id' for movies or actors.
    """
    if not external_id:
        return None
    
    # Add 'tt' prefix for movies, 'nm' for actors if missing
    if external_source_type == 'imdb_id':
        if not external_id.startswith('tt') and not external_id.startswith('nm'):
            # This is a guess; movies use 'tt', actors use 'nm'
            # We'll rely on the source type to be correct
            if len(external_id) > 8: # nm1234567
                external_id = f"nm{external_id.zfill(7)}"
            else:
                external_id = f"tt{external_id.zfill(7)}" # tt1234567
    
    try:
        params = {'external_source': 'imdb_id'}
        response = requests.get(
            FIND_URL.format(external_id=external_id), 
            params=params, 
            headers=API_HEADERS 
        )
        response.raise_for_status()
        data = response.json()
        
        results = data.get('movie_results') if external_source_type == 'imdb_id' else data.get('person_results')
        if results:
            return results[0].get('id')
    except requests.exceptions.RequestException as e:
        print(f"  API Error finding {external_id}: {e}")
    
    return None


def load_dynamic_movie_data():
    """
    Loops through all movies in Dim_Movie, fetches dynamic data from TMDb,
    and updates Dim_Movie and Fact_Book_Adaptation.
    """
    print("\nStarting Dynamic Movie Data (API) load...")
    if not TMDB_API_KEY or TMDB_API_KEY == "PASTE_YOUR_API_KEY_HERE":
        print("Error: TMDB_API_KEY not set. Skipping load.")
        return

    pg = create_engine(PG_URL)
    movies_to_check = []
    movies_updated = 0
    facts_updated = 0

    with pg.connect() as c: # Use .connect() for better control
        # Get all movies from our DW
        result = c.execute(text(f"SELECT Movie_SK, Movie_ID_Source FROM {DW_SCHEMA}.Dim_Movie"))
        movies_to_check = result.fetchall()
        print(f"Found {len(movies_to_check)} movies in Dim_Movie to update.")
        
        for movie_sk, movie_id_source in movies_to_check:
            # 1. Find TMDb ID
            tmdb_id = _get_tmdb_id(movie_id_source, 'imdb_id')
            if not tmdb_id:
                # print(f"  Skipping Movie_SK {movie_sk}: Could not find TMDb ID for {movie_id_source}")
                continue

            # 2. Call MOVIES > Details endpoint
            try:
                response = requests.get(
                    MOVIE_URL.format(tmdb_id=tmdb_id), 
                    headers=API_HEADERS
                )
                response.raise_for_status()
                data = response.json()
                
                # We got data! Start a transaction to update
                with c.begin():
                    c.execute(text(f"SET search_path TO {DW_SCHEMA}"))
                    
                    # 3. Update Dim_Movie with new popularity
                    pop = _safe_float(data.get('popularity'))
                    c.execute(text("""
                        UPDATE Dim_Movie
                        SET TMDb_Popularity = :pop
                        WHERE Movie_SK = :sk
                    """), {"pop": pop, "sk": movie_sk})
                    movies_updated += 1
                    
                    # 4. Update Fact_Book_Adaptation with new financials
                    budget = _safe_int(data.get('budget'))
                    revenue = _safe_int(data.get('revenue'))
                    profit = (revenue - budget) if budget is not None and revenue is not None else None
                    roi = (profit / budget * 100) if profit is not None and budget is not None and budget > 0 else None

                    c.execute(text("""
                        UPDATE Fact_Book_Adaptation SET
                            Production_Budget = :budget,
                            Box_Office_Gross = :revenue,
                            Profit = :profit,
                            ROI = :roi
                        WHERE Movie_SK = :sk
                    """), {
                        "budget": budget,
                        "revenue": revenue,
                        "profit": profit,
                        "roi": roi,
                        "sk": movie_sk
                    })
                    facts_updated += 1
                
                # Be polite to the API
                time.sleep(0.1)
                
            except requests.exceptions.RequestException as e:
                print(f"  Error processing Movie_SK {movie_sk} (TMDb ID {tmdb_id}): {e}")
                continue
            
            if movies_updated % 100 == 0:
                print(f"  ...updated {movies_updated} movies and {facts_updated} fact rows...")

    print("Dynamic Movie Data load complete.")
    print(f"  Total movies updated: {movies_updated}")
    print(f"  Total fact rows updated: {facts_updated}")


# --- LOADER 2: FOR ACTORS ---
def load_dynamic_actor_data():
    """
    Loops through actors *who are in our movies* and gets their
    popularity score from TMDb.
    """
    print("\nStarting Dynamic Actor Data (API) load...")
    if not TMDB_API_KEY or TMDB_API_KEY == "PASTE_YOUR_API_KEY_HERE":
        print("Error: TMDB_API_KEY not set. Skipping load.")
        return
        
    pg = create_engine(PG_URL)
    actors_to_check = []
    actors_updated = 0

    with pg.connect() as c:
        # Get only the actors linked to our movies
        query = text(f"""
            SELECT DISTINCT
                a.Actor_SK,
                a.Actor_ID_Source
            FROM {DW_SCHEMA}.Dim_Actor a
            JOIN {DW_SCHEMA}.Bridge_Movie_Actor b ON a.Actor_SK = b.Actor_SK
        """)
        result = c.execute(query)
        actors_to_check = result.fetchall()
        print(f"Found {len(actors_to_check)} linked actors in Bridge_Movie_Actor to update.")
        
        for actor_sk, actor_id_source in actors_to_check:
            # 1. Find TMDb ID for the person
            tmdb_id = _get_tmdb_id(actor_id_source, 'imdb_id')
            if not tmdb_id:
                # print(f"  Skipping Actor_SK {actor_sk}: Could not find TMDb ID for {actor_id_source}")
                continue
                
            # 2. Call PEOPLE > Details endpoint
            try:
                response = requests.get(
                    PERSON_URL.format(person_id=tmdb_id), 
                    headers=API_HEADERS)
                response.raise_for_status()
                data = response.json()
                
                pop = _safe_float(data.get('popularity'))
                
                # 3. Update Dim_Actor
                with c.begin():
                    c.execute(text(f"SET search_path TO {DW_SCHEMA}"))
                    c.execute(text("""
                        UPDATE Dim_Actor
                        SET Popularity_Score = :pop
                        WHERE Actor_SK = :sk
                    """), {"pop": pop, "sk": actor_sk})
                    actors_updated += 1
                
                # Be polite to the API
                time.sleep(0.1)

            except requests.exceptions.RequestException as e:
                print(f"  Error processing Actor_SK {actor_sk} (TMDb ID {tmdb_id}): {e}")
                continue
            
            if actors_updated % 100 == 0:
                print(f"  ...updated {actors_updated} actors...")

    print("Dynamic Actor Data load complete.")
    print(f"  Total actors updated with popularity: {actors_updated}")