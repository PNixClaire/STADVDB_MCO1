import pandas as pd
import os, re, io, requests, time
from sqlalchemy import create_engine, text
from datetime import datetime, date
from config import PG_URL, TMDB_API_KEY

DW_SCHEMA = "dw_books_movies" #our star schema where the data will be loaded

#endpoints
FIND_URL = "https://api.themoviedb.org/3/find/{external_id}"
MOVIE_URL = "https://api.themoviedb.org/3/movie/{tmdb_id}"
PERSON_URL = "https://api.themoviedb.org/3/person/{person_id}"

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
    
    
def _get_tmdb_id(external_id, find_type): # find_type is 'movie' or 'person'
    """
    Finds the TMDb ID from an IMDb ID, with retries and strict type checking.
    """
    if not external_id:
        print("  _get_tmdb_id: Received empty external_id.")
        return None

    original_id = external_id

    #check if actor or movie
    if find_type == 'movie':
        if external_id.startswith('nm'):
            print(f"  _get_tmdb_id: ID {external_id} is a PERSON ID, but we are looking for a MOVIE. Skipping.")
            return None 
        if not external_id.startswith('tt'):
            external_id = f"tt{external_id.zfill(7)}"
            
    elif find_type == 'person':
        if external_id.startswith('tt'):
            print(f"  _get_tmdb_id: ID {external_id} is a MOVIE ID, but we are looking for a PERSON. Skipping.")
            return None 
        if not external_id.startswith('nm'):
            external_id = f"nm{external_id.zfill(7)}"
   

    params = {'api_key': TMDB_API_KEY, 'external_source': 'imdb_id'}
    url_to_call = FIND_URL.format(external_id=external_id)

    for attempt in range(3):
        try:
            print(f"    _get_tmdb_id Attempt {attempt+1}: Calling URL: {url_to_call}")
            response = requests.get(
                url_to_call,
                params=params,
                timeout=15
            )
            print(f"    _get_tmdb_id Response Status: {response.status_code}")
            print(f"    _get_tmdb_id Response Text (first 200 chars): {response.text[:200]}")

            response.raise_for_status()
            data = response.json()

            #seachr for movie or person
            if find_type == 'movie':
                results = data.get('movie_results')
            else: 
                results = data.get('person_results')
           

            if results:
                tmdb_id = results[0].get('id')
                print(f"    _get_tmdb_id Success: Found TMDb ID {tmdb_id} for {original_id}")
                return tmdb_id
            else:
                print(f"    _get_tmdb_id Failed: API returned success, but no {find_type} results found for {original_id}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"  Attempt {attempt + 1} failed for {original_id}: {e}")
            if attempt < 2:
                wait_time = (attempt + 1) * 5
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"  Max retries reached for {original_id}. Skipping.")
        except requests.exceptions.JSONDecodeError as e:
             print(f"  Attempt {attempt + 1} failed for {original_id}: Could not decode JSON response. Error: {e}")
             return None

    print(f"  _get_tmdb_id Failed: All retries failed for {original_id}.")
    return None

def load_dynamic_movie_data():
    print("\nStarting Dynamic Movie Data (API) load...")
    if not TMDB_API_KEY or TMDB_API_KEY == "PASTE_YOUR_API_KEY_HERE":
        print("Error: TMDB_API_KEY not set. Skipping load.")
        return

    pg = create_engine(PG_URL)
    movies_to_check = []
    movies_updated = 0
    facts_updated = 0

    #get list of movies to chekc
    try:
        print("Connecting to database to fetch movies...")
        with pg.connect() as c: 
            result = c.execute(text(f"SELECT movie_sk, movie_id_source FROM {DW_SCHEMA}.\"dim_movie\""))        
            movies_to_check = result.fetchall()
        print(f"Found {len(movies_to_check)} movies in dim_movie to update.")
    except Exception as e:
         print(f"!!! DATABASE ERROR fetching movies: {e}")
         return # Stop if we can't get the movie list
    
    #process
    processed_count = 0
    with pg.connect() as c: 
        for movie_sk, movie_id_source in movies_to_check:
            processed_count += 1
            
            #Find TMDb ID
            tmdb_id = _get_tmdb_id(movie_id_source, 'movie') # Use 'movie' type
            if not tmdb_id:
                print(f"  Skipping Movie_SK {movie_sk} - TMDb ID not found or was not a movie.")
                continue

            #Call MOVIES > Details endpoint
            try:
                data = None
                for attempt in range(3):
                    try:
                        params = {'api_key': TMDB_API_KEY}
                        response = requests.get(
                            MOVIE_URL.format(tmdb_id=tmdb_id), 
                            params=params, 
                            timeout=15
                        )
                        response.raise_for_status()
                        data = response.json()
                        break 
                    except requests.exceptions.RequestException as e:
                        print(f"  Attempt {attempt + 1} failed for Movie_SK {movie_sk}: {e}")
                        if attempt < 2:
                            wait_time = (attempt + 1) * 5
                            print(f"  Retrying in {wait_time} seconds...")
                            time.sleep(wait_time)
                        else:
                            raise 
                
                if data is None:
                    print(f"  Skipping Movie_SK {movie_sk} after failed retries.")
                    continue 
                
                
                with c.begin(): 
                    c.execute(text(f"SET search_path TO {DW_SCHEMA}"))

                time.sleep(0.2) 
                
            except Exception as e: 
                print(f"  Failed to process Movie_SK {movie_sk} after 3 attempts. Skipping. Error: {e}")
                continue
            
            if processed_count % 100 == 0:
                print(f"  ...processed {processed_count}/{len(movies_to_check)} movies (updated {facts_updated} fact rows)...")

    print("Dynamic Movie Data load complete.")
    print(f"  Total movies processed: {processed_count}")
    print(f"  Total fact rows updated: {facts_updated}")



def load_dynamic_actor_data():
    print("\nStarting Dynamic Actor Data (API) load...")
    if not TMDB_API_KEY or TMDB_API_KEY == "PASTE_YOUR_API_KEY_HERE":
        print("Error: TMDB_API_KEY not set. Skipping load.")
        return

    pg = create_engine(PG_URL)
    actors_to_check = []
    actors_updated = 0

    try: 
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
    except Exception as e: 
         print(f"!!! DATABASE ERROR fetching actors: {e}")
         return 

    processed_count = 0
    with pg.connect() as c: # Re-establish connection for the loop
        for actor_sk, actor_id_source in actors_to_check:
            processed_count += 1
            print(f"\nProcessing actor {processed_count}/{len(actors_to_check)}: SK={actor_sk}, ID={actor_id_source}") 

            print(f"  Attempting to find TMDb ID for {actor_id_source}...") 
            tmdb_id = _get_tmdb_id(actor_id_source, 'person')
            print(f"  _get_tmdb_id returned: {tmdb_id}")

            if not tmdb_id:
                print(f"  Skipping Actor_SK {actor_sk} - TMDb ID not found.") 
                continue

            try:
            
                data = None
                for attempt in range(3):
                    try:
                        print(f"    Attempt {attempt + 1}: Calling PEOPLE API for TMDb ID {tmdb_id}...") 
                        params = {'api_key': TMDB_API_KEY}
                        response = requests.get(
                            PERSON_URL.format(person_id=tmdb_id),
                            params=params,
                            timeout=15
                        )
                        print(f"    API response status: {response.status_code}") 
                        response.raise_for_status()
                        data = response.json()
                        print(f"    Successfully got data for TMDb ID {tmdb_id}.") 
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
                print(f"    Popularity score found: {pop}") 

                with c.begin():
                    print(f"    Updating database for Actor_SK {actor_sk}...") 
                    c.execute(text(f"SET search_path TO {DW_SCHEMA}"))
                    c.execute(text("""
                        UPDATE Dim_Actor
                        SET Popularity_Score = :pop
                        WHERE Actor_SK = :sk
                    """), {"pop": pop, "sk": actor_sk})
                    actors_updated += 1
                    print(f"    Database update successful for Actor_SK {actor_sk}.") 

                time.sleep(0.2)

            except Exception as e:
                print(f"  Failed to process Actor_SK {actor_sk} (IMDb: {actor_id_source}) after 3 attempts. Skipping. Error: {e}")
                continue

            if processed_count % 100 == 0:
                print(f"\n--- Progress: Processed {processed_count}/{len(actors_to_check)} actors (updated {actors_updated}) ---\n")

    print("\nDynamic Actor Data load complete.")
    print(f"  Total actors processed: {processed_count}")
    print(f"  Total actors updated with popularity: {actors_updated}")