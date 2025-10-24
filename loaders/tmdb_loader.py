import pandas as pd
import os, re, io, requests, time
from sqlalchemy import create_engine, text
from datetime import datetime, date
from config import PG_URL, TMDB_API_KEY

DW_SCHEMA = "dw_books_movies" #our star schema where the data will be loaded

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
    
    
# --- THIS FUNCTION NOW HAS RETRY LOGIC ---
# --- FUNCTION TO REPLACE (in tmdb_loader.py) ---
def _get_tmdb_id(external_id, external_source_type):
    """
    Finds the TMDb ID from an IMDb ID, with retries and better debugging.
    """
    if not external_id:
        print("  _get_tmdb_id: Received empty external_id.") # Debug
        return None

    original_id = external_id # Keep original for logging

    # Add 'tt' prefix for movies, 'nm' for actors if missing
    if external_source_type == 'imdb_id':
        if not external_id.startswith('tt') and not external_id.startswith('nm'):
            if len(str(external_id)) >= 7 and str(external_id).isdigit():
                 external_id = f"nm{external_id.zfill(7)}"
            else:
                 external_id = f"tt{external_id.zfill(7)}"
        elif external_id.startswith('tt') and len(external_id) < 9:
             external_id = f"tt{external_id[2:].zfill(7)}"
        elif external_id.startswith('nm') and len(external_id) < 9:
             external_id = f"nm{external_id[2:].zfill(7)}"

    params = {'api_key': TMDB_API_KEY, 'external_source': 'imdb_id'}
    url_to_call = FIND_URL.format(external_id=external_id) # --- Store URL for logging ---

    for attempt in range(3):
        try:
            # --- ADDED: Print URL before calling ---
            print(f"    _get_tmdb_id Attempt {attempt+1}: Calling URL: {url_to_call} with params: {params}")

            response = requests.get(
                url_to_call,
                params=params,
                timeout=15
            )

            # --- ADDED: Print status code and raw response text ---
            print(f"    _get_tmdb_id Response Status: {response.status_code}")
            # Only print first 200 chars in case response is huge HTML error page
            print(f"    _get_tmdb_id Response Text (first 200 chars): {response.text[:200]}")

            response.raise_for_status() # Raise error if 4xx or 5xx
            data = response.json()

            results_key = 'person_results' if external_source_type == 'imdb_id' and external_id.startswith('nm') else 'movie_results'
            results = data.get(results_key)

            if results:
                tmdb_id = results[0].get('id')
                print(f"    _get_tmdb_id Success: Found TMDb ID {tmdb_id} for {original_id}") # Debug
                return tmdb_id
            else:
                print(f"    _get_tmdb_id Failed: API returned success, but no results found for {original_id}") # Debug
                return None # Found no results

        except requests.exceptions.RequestException as e:
            print(f"  Attempt {attempt + 1} failed for {original_id}: {e}")
            if attempt < 2:
                wait_time = (attempt + 1) * 5
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"  Max retries reached for {original_id}. Skipping.")
        # --- ADDED: Catch potential JSON decode error ---
        except requests.exceptions.JSONDecodeError as e:
             print(f"  Attempt {attempt + 1} failed for {original_id}: Could not decode JSON response. Error: {e}")
             # Don't retry JSON errors, likely bad response format
             return None


    print(f"  _get_tmdb_id Failed: All retries failed for {original_id}.") # Debug
    return None # All retries failed

def load_dynamic_movie_data():
    print("\nStarting Dynamic Movie Data (API) load...")
    if not TMDB_API_KEY or TMDB_API_KEY == "PASTE_YOUR_API_KEY_HERE":
        print("Error: TMDB_API_KEY not set. Skipping load.")
        return

    pg = create_engine(PG_URL)
    movies_to_check = []
    movies_updated = 0
    facts_updated = 0

    with pg.connect() as c: 
        result = c.execute(text(f"SELECT Movie_SK, Movie_ID_Source FROM {DW_SCHEMA}.Dim_Movie"))
        movies_to_check = result.fetchall()
        print(f"Found {len(movies_to_check)} movies in Dim_Movie to update.")
        
        for movie_sk, movie_id_source in movies_to_check:
            tmdb_id = _get_tmdb_id(movie_id_source, 'imdb_id')
            if not tmdb_id:
                continue

            try:
                # --- ADDED RETRY LOGIC ---
                data = None
                for attempt in range(3):
                    try:
                        params = {'api_key': TMDB_API_KEY}
                        response = requests.get(
                            MOVIE_URL.format(tmdb_id=tmdb_id), 
                            params=params, 
                            timeout=15 # Increased timeout
                        )
                        response.raise_for_status()
                        data = response.json()
                        break # Success! Break the retry loop
                    except requests.exceptions.RequestException as e:
                        print(f"  Attempt {attempt + 1} failed for Movie_SK {movie_sk}: {e}")
                        if attempt < 2:
                            wait_time = (attempt + 1) * 5
                            print(f"  Retrying in {wait_time} seconds...")
                            time.sleep(wait_time)
                        else:
                            raise # Max retries reached, re-raise the exception
                
                if data is None:
                    print(f"  Skipping Movie_SK {movie_sk} after failed retries.")
                    continue 
                # --- END OF RETRY LOGIC ---
                
                with c.begin():
                    c.execute(text(f"SET search_path TO {DW_SCHEMA}"))
                    
                    pop = _safe_float(data.get('popularity'))
                    c.execute(text("""
                        UPDATE Dim_Movie
                        SET TMDb_Popularity = :pop
                        WHERE Movie_SK = :sk
                    """), {"pop": pop, "sk": movie_sk})
                    # movies_updated count happens implicitly below if fact update succeeds

                    budget = _safe_int(data.get('budget'))
                    revenue = _safe_int(data.get('revenue'))
                    profit = (revenue - budget) if budget is not None and revenue is not None else None
                    roi = (profit / budget * 100) if profit is not None and budget is not None and budget > 0 else None

                    # Only count update if fact update succeeds as well
                    fact_res = c.execute(text("""
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
                    
                    # Check if the fact table was actually updated
                    if fact_res.rowcount > 0:
                         movies_updated += 1 # Count movie update only if fact existed
                         facts_updated += 1
                    # If fact update didn't happen (rowcount=0), maybe still update Dim_Movie popularity?
                    # Current logic only increments if fact is updated. Adjust if needed.

                time.sleep(0.2) # Slightly increased polite delay
                
            except Exception as e: # Catch the error if retries fail
                print(f"  Failed to process Movie_SK {movie_sk} after 3 attempts. Skipping. Error: {e}")
                continue
            
            if movies_updated > 0 and movies_updated % 100 == 0:
                print(f"  ...updated {movies_updated} movies and {facts_updated} fact rows...")

    print("Dynamic Movie Data load complete.")
    print(f"  Total movies updated (in Dim_Movie and Fact): {movies_updated}")
    print(f"  Total fact rows updated: {facts_updated}")


# --- LOADER 2: FOR ACTORS (NOW HAS RETRY LOGIC) ---
# --- LOADER 2: FOR ACTORS (MODIFIED FOR DEBUGGING) ---
def load_dynamic_actor_data():
    print("\nStarting Dynamic Actor Data (API) load...")
    if not TMDB_API_KEY or TMDB_API_KEY == "PASTE_YOUR_API_KEY_HERE":
        print("Error: TMDB_API_KEY not set. Skipping load.")
        return

    pg = create_engine(PG_URL)
    actors_to_check = []
    actors_updated = 0

    try: # --- ADDED try block for initial query ---
        print("Connecting to database to fetch actors...")
        with pg.connect() as c:
            query = text(f"""
                SELECT DISTINCT
                    a.Actor_SK,
                    a.Actor_ID_Source
                FROM {DW_SCHEMA}.Dim_Actor a
                JOIN {DW_SCHEMA}.Bridge_Movie_Actor b ON a.Actor_SK = b.Actor_SK
                WHERE a.Actor_ID_Source IS NOT NULL
            """)
            result = c.execute(query)
            actors_to_check = result.fetchall()
            print(f"Found {len(actors_to_check)} linked actors in Bridge_Movie_Actor to update.")
    except Exception as e: # --- ADDED exception handling ---
         print(f"!!! DATABASE ERROR fetching actors: {e}")
         return # Stop if we can't get the actor list

    processed_count = 0
    with pg.connect() as c: # Re-establish connection for the loop
        for actor_sk, actor_id_source in actors_to_check:
            processed_count += 1
            print(f"\nProcessing actor {processed_count}/{len(actors_to_check)}: SK={actor_sk}, ID={actor_id_source}") # --- ADDED ---

            print(f"  Attempting to find TMDb ID for {actor_id_source}...") # --- ADDED ---
            tmdb_id = _get_tmdb_id(actor_id_source, 'imdb_id')
            print(f"  _get_tmdb_id returned: {tmdb_id}") # --- ADDED ---

            if not tmdb_id:
                print(f"  Skipping Actor_SK {actor_sk} - TMDb ID not found.") # --- ADDED ---
                continue

            try:
                # (Keep the existing retry logic inside here)
                data = None
                for attempt in range(3):
                    try:
                        print(f"    Attempt {attempt + 1}: Calling PEOPLE API for TMDb ID {tmdb_id}...") # --- ADDED ---
                        params = {'api_key': TMDB_API_KEY}
                        response = requests.get(
                            PERSON_URL.format(person_id=tmdb_id),
                            params=params,
                            timeout=15
                        )
                        print(f"    API response status: {response.status_code}") # --- ADDED ---
                        response.raise_for_status()
                        data = response.json()
                        print(f"    Successfully got data for TMDb ID {tmdb_id}.") # --- ADDED ---
                        break
                    except requests.exceptions.RequestException as e:
                        print(f"  Attempt {attempt + 1} failed for Actor_SK {actor_sk} (IMDb: {actor_id_source}): {e}")
                        if attempt < 2:
                            wait_time = (attempt + 1) * 5
                            print(f"  Retrying in {wait_time} seconds...")
                            time.sleep(wait_time)
                        else:
                            raise

                if data is None:
                    print(f"  Skipping Actor_SK {actor_sk} after failed retries.")
                    continue

                pop = _safe_float(data.get('popularity'))
                print(f"    Popularity score found: {pop}") # --- ADDED ---

                with c.begin():
                    print(f"    Updating database for Actor_SK {actor_sk}...") # --- ADDED ---
                    c.execute(text(f"SET search_path TO {DW_SCHEMA}"))
                    c.execute(text("""
                        UPDATE Dim_Actor
                        SET Popularity_Score = :pop
                        WHERE Actor_SK = :sk
                    """), {"pop": pop, "sk": actor_sk})
                    actors_updated += 1
                    print(f"    Database update successful for Actor_SK {actor_sk}.") # --- ADDED ---

                time.sleep(0.2)

            except Exception as e:
                print(f"  Failed to process Actor_SK {actor_sk} (IMDb: {actor_id_source}) after 3 attempts. Skipping. Error: {e}")
                continue

            if processed_count % 100 == 0:
                 # --- MODIFIED log message slightly ---
                print(f"\n--- Progress: Processed {processed_count}/{len(actors_to_check)} actors (updated {actors_updated}) ---\n")

    print("\nDynamic Actor Data load complete.")
    print(f"  Total actors processed: {processed_count}")
    print(f"  Total actors updated with popularity: {actors_updated}")