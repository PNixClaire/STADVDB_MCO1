import pandas as pd
import sqlite3, os, re
from sqlalchemy import create_engine, text
from datetime import datetime, date
from config import PG_URL, BOOKS_FILM_REVIEW_PATH

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

#load the data from the source
def load_dw_from_bfr():
    if not BOOKS_FILM_REVIEW_PATH or not os.path.exists(BOOKS_FILM_REVIEW_PATH):
        print(f"books_films_reviews not found: {BOOKS_FILM_REVIEW_PATH}")
        return

    pg = create_engine(PG_URL)
    src = sqlite3.connect(BOOKS_FILM_REVIEW_PATH)

    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table'", src
    )["name"].str.lower().tolist()
    print("SQLite tables:", ", ".join(sorted(tables)))

    # load books
    books_df = pd.DataFrame()
    if "books" in tables:
        books_df = pd.read_sql_query("SELECT * FROM books", src)
        books_df.columns = [c.strip().lower() for c in books_df.columns]
    elif "book_data_cleaned" in tables: 
        books_df = pd.read_sql_query("SELECT * FROM book_data_cleaned", src)
        books_df.columns = [c.strip().lower() for c in books_df.columns]


    # movies
    movies_df = pd.DataFrame()
    for t in ["movies", "movie_overall_data"]:
        if t in tables:
            movies_df = pd.read_sql_query(f"SELECT * FROM {t}", src)
            movies_df.columns = [c.strip().lower() for c in movies_df.columns]
            if not movies_df.empty:
                print(f"Loaded movie metadata from: {t}")
                break

    tmdb_to_imdb = {}
    if "tmdb_to_imdb_id_mapping" in tables:
        map_df = pd.read_sql_query("SELECT * FROM tmdb_to_imdb_id_mapping", src)
        map_df.columns = [c.strip().lower() for c in map_df.columns]
        tmdb_col = "tmdbid" if "tmdbid" in map_df.columns else "tmdb_id"
        imdb_col = "imdbid" if "imdbid" in map_df.columns else "imdb_id"
        
        if tmdb_col in map_df.columns and imdb_col in map_df.columns:
            for _, r in map_df.iterrows():
                t = r.get(tmdb_col)
                i = r.get(imdb_col)
                if pd.notna(t) and pd.notna(i):
                    tmdb_to_imdb[int(t)] = str(i)

    if not movies_df.empty:
        tmdb_col = "tmdbid" if "tmdbid" in movies_df.columns else "tmdb_id"
        imdb_col = "imdbid" if "imdbid" in movies_df.columns else "imdb_id"

        if tmdb_col in movies_df.columns and imdb_col in movies_df.columns:
            for _, r in movies_df.iterrows():
                t = r.get(tmdb_col)
                i = r.get(imdb_col)
                if pd.notna(t) and pd.notna(i):
                    tmdb_to_imdb[int(t)] = str(i)
    

    imdb_to_genre = {}
    if "movie_genres" in tables:
        movie_genres_df = pd.read_sql_query("SELECT * FROM movie_genres", src)
        movie_genres_df.columns = [c.strip().lower() for c in movie_genres_df.columns]
        if not movie_genres_df.empty and "imdbid" in movie_genres_df.columns and "genre" in movie_genres_df.columns:
            # Get the numeric key
            movie_genres_df["imdb_key"] = movie_genres_df["imdbid"].apply(
                lambda x: _derive_movie_id_source_from_imdb(str(x)) if pd.notna(x) else None
            )
            # Group by the numeric key and join genres
            genre_groups = movie_genres_df.groupby("imdb_key")["genre"].apply(lambda x: ", ".join(x.astype(str).unique()))
            imdb_to_genre = genre_groups.to_dict()
            print(f"Loaded {len(imdb_to_genre)} movie genre mappings")

    people_df = pd.DataFrame()
    imdb_to_director = {}
    actors_data = [] # Will store tuples of (imdb_key, actor_id, actor_name, role)
    unique_actors = {} # Will store {actor_id: actor_name}
    
    if "movie_actor_director" in tables:
        people_df = pd.read_sql_query("SELECT * FROM movie_actor_director", src)
        people_df.columns = [c.strip().lower() for c in people_df.columns]
        
        movie_id_col = "tconst" if "tconst" in people_df.columns else "imdbid"
        person_id_col = "nconst" if "nconst" in people_df.columns else "person_id"
        name_col = "primaryname" if "primaryname" in people_df.columns else "name"
        
        for _, r in people_df.iterrows():
            imdb_key = _derive_movie_id_source_from_imdb(r.get(movie_id_col))
            person_id = r.get(person_id_col)
            name = r.get(name_col)
            category = r.get("category")
            
            if not imdb_key or not person_id or not name:
                continue

            if category == 'director':
                if imdb_key not in imdb_to_director:
                    imdb_to_director[imdb_key] = name
            
            elif category in ('actor', 'actress'):
                role = r.get("role") or r.get("characters") 
                actors_data.append((imdb_key, person_id, name, role))
                if person_id not in unique_actors:
                    unique_actors[person_id] = name
        
        print(f"Loaded {len(imdb_to_director)} director mappings")
        print(f"Loaded {len(unique_actors)} unique actors")


    #book to movie link table
    links_df = pd.DataFrame()
    prefers = ["wiki_book_movie_ids_matching", "booksmovies"]
    for t in prefers:
        if t in tables:
            links_df = pd.read_sql_query(f"SELECT * FROM {t}", src)
            links_df.columns = [c.strip().lower() for c in links_df.columns]
            print(f"Loaded link data from: {t}")
            break
    if links_df.empty:
        print("No mapping table (booksmovies/wiki_book_movie_ids_matching) found. Exiting.")
        src.close()
        return
    
    imdb_to_movie = {}
    if not movies_df.empty:
        for _, r in movies_df.iterrows():
            raw_imdb = r.get("imdbid")
            key = _derive_movie_id_source_from_imdb(str(raw_imdb)) if pd.notna(raw_imdb) else None
            if not key:
                continue
            
            release_date = _coerce_date(r.get("release_date") or r.get("year"))

            imdb_to_movie[key] = {
                "imdb_id": str(raw_imdb),
                "title": r.get("title") or r.get("original_title") or r.get("full_name"),
                "release_date": release_date,
                "vote_average": _safe_float(r.get("averagerating") or r.get("rating_average") or r.get("vote_average"), lo=0, hi=10),
                "vote_count": _safe_int(r.get("numvotes") or r.get("vote_count")),
                "distributor": r.get("distributor") if "distributor" in movies_df.columns else None,
                "genre": imdb_to_genre.get(key), 
                "director": imdb_to_director.get(key), 
                "budget": _safe_int(r.get("budget")), 
                "revenue": _safe_int(r.get("revenue")), 
            }

    # book measures
    book_measures = {}
    if not books_df.empty:
        for _, r in books_df.iterrows():
    
            src_id = (r.get("goodreads_book_id") or r.get("book_id"))
            if pd.isna(src_id): 
                continue
            src_id = str(int(src_id)) if not isinstance(src_id, str) else src_id.strip()
            
            pub_date = _coerce_date(r.get("publication_date") or r.get("year"))
            
            book_measures[src_id] = {
                "title": r.get("title"),
                "authors": r.get("author") or r.get("authors"), 
                "isbn": r.get("isbn"),
                "pub_date": pub_date,
                "language_code": (r.get("language_code") or None),
                "num_pages": _safe_int(r.get("length") or r.get("num_pages")), 
                "avg_rating": _safe_float(r.get("avg_rating") or r.get("average_rating"), lo=0, hi=5), 
                "ratings_count": _safe_int(r.get("rating_count") or r.get("ratings_count")), 
                "text_reviews_count": _safe_int(r.get("review_count") or r.get("work_text_reviews_count") or r.get("text_reviews_count")),
            }

    # Normalize links
    norm_links = []
    for _, r in links_df.iterrows():
        # book id
        bid = None
     
        for cand in ["goodreads_book_id", "book_id", "id_goodreads"]:
            if cand in links_df.columns and pd.notna(r.get(cand)):
                bid = str(int(r[cand])) if not isinstance(r[cand], str) else r[cand].strip()
                break
        if not bid:
            continue

        imdb_key = None
        # common imdb columns

        for cand in ["imdb_id", "movie_imdb_id", "imdb", "ttid", "imdbid"]:
            if cand in links_df.columns and pd.notna(r.get(cand)):
                imdb_key = _derive_movie_id_source_from_imdb(str(r[cand]))
                if imdb_key:
                    break

        # If only tmdb is present, try to map it
        if not imdb_key:
        
            for cand in ["tmdb_id", "movie_tmdb_id", "tmdb", "tmdbid"]:
                if cand in links_df.columns and pd.notna(r.get(cand)):
                    try:
                        tmdb_id = int(r[cand])
                        imdb_id = tmdb_to_imdb.get(tmdb_id)
                        if imdb_id:
                            imdb_key = _derive_movie_id_source_from_imdb(imdb_id)
                    except Exception:
                        pass
                    if imdb_key:
                        break

        if not imdb_key:
            continue 

        norm_links.append((bid, imdb_key))
    

    norm_links = sorted(list(set(norm_links)))

    print(f"Link rows discovered: {len(norm_links)}")

    # If zero links
    if len(norm_links) == 0:
        print("No links were matched. Check which ID columns exist:")
        print(f"  Link table ({prefers[0]}) columns:", list(links_df.columns))
        print(f"  Movie table ({'movies' if not movies_df.empty else 'N/A'}) columns:", list(movies_df.columns))
        src.close()
        print("DW load complete: 0 rows inserted.")
        return

    # load into dw
    with pg.begin() as c:
        c.execute(text(f"SET search_path TO {DW_SCHEMA}"))

        book_src_to_sk = {}
        movie_src_to_sk = {}

        actor_src_to_sk = {}
        inserted_facts = 0
        inserted_bridge = 0

        # Dim_Book
        for book_src_id in sorted({b for b, _ in norm_links}):
            bm = book_measures.get(book_src_id, {})
            res = c.execute(text("""
                INSERT INTO Dim_Book
                    (Book_ID_Source, ISBN, Title, Author, Publisher, Publication_Date, Language_Code, Num_Pages)
                VALUES
                    (:src, :isbn, :title, :author, NULL, :pub_date, :lang, :pages)
                ON CONFLICT (Book_ID_Source) DO UPDATE SET
                    ISBN = EXCLUDED.ISBN,
                    Title = EXCLUDED.Title,
                    Author = EXCLUDED.Author,
                    Publication_Date = EXCLUDED.Publication_Date,
                    Language_Code = EXCLUDED.Language_Code,
                    Num_Pages = EXCLUDED.Num_Pages
                RETURNING Book_SK
            """), {
                "src": book_src_id,
                "isbn": bm.get("isbn"),
                "title": (bm.get("title") or f"Book {book_src_id}")[:500],
                "author": (bm.get("authors") or None)[:500] if bm.get("authors") else None,
                "pub_date": bm.get("pub_date"),
                "lang": (str(bm.get("language_code")).upper()[:3] if bm.get("language_code") else None),
                "pages": bm.get("num_pages")
            }).scalar()
            if res is None:
                res = c.execute(text("SELECT Book_SK FROM Dim_Book WHERE Book_ID_Source=:src"), {"src": book_src_id}).scalar()
            
            if res is not None:
                book_src_to_sk[book_src_id] = int(res)

        # Dim_Movie
        for movie_src_id in sorted({m for _, m in norm_links}):
            mm = imdb_to_movie.get(movie_src_id, {})
            
     
            genre_val = mm.get("genre") or imdb_to_genre.get(movie_src_id)
            director_val = mm.get("director") or imdb_to_director.get(movie_src_id)
            
        
            res = c.execute(text("""
                INSERT INTO Dim_Movie
                    (Movie_ID_Source, Movie_Title_Source, Release_Date, Release_Year, Distributor, Genre, Director)
                VALUES
                    (:src, :title, :rdate, :ryear, :dist, :genre, :director)
                ON CONFLICT (Movie_ID_Source) DO UPDATE SET
                    Movie_Title_Source = EXCLUDED.Movie_Title_Source,
                    Release_Date       = EXCLUDED.Release_Date,
                    Release_Year       = EXCLUDED.Release_Year,
                    Distributor        = EXCLUDED.Distributor,
                    Genre              = EXCLUDED.Genre,
                    Director           = EXCLUDED.Director
                RETURNING Movie_SK
            """), {
                "src": movie_src_id,
                "title": (mm.get("title") or f"Movie tt{movie_src_id}")[:500],
                "rdate": mm.get("release_date"),
                "ryear": (mm["release_date"].year if mm.get("release_date") else None),
                "dist":  mm.get("distributor"),
                "genre": genre_val[:100] if genre_val else None,        
                "director": director_val[:255] if director_val else None, 
            }).scalar()

            if res is None:
                res = c.execute(text("SELECT Movie_SK FROM Dim_Movie WHERE Movie_ID_Source=:src"), {"src": movie_src_id}).scalar()
            
            if res is not None:
                movie_src_to_sk[movie_src_id] = int(res)

        # Dim_Date for release dates
        for movie_src_id, msk in movie_src_to_sk.items():
            mm = imdb_to_movie.get(movie_src_id, {})
            rdate = mm.get("release_date")
            if rdate:
                dsk = _date_to_sk(rdate) # Use helper function
                c.execute(text("""
                    INSERT INTO Dim_Date (Date_SK, Full_Date, Year, Month, Month_Name, Quarter, Day_of_Week)
                    VALUES (:sk, :d, EXTRACT(YEAR FROM :d)::INT, EXTRACT(MONTH FROM :d)::INT,
                            TO_CHAR(:d, 'Month'), CONCAT('Q', EXTRACT(QUARTER FROM :d)::INT),
                            TO_CHAR(:d, 'Day'))
                    ON CONFLICT (Date_SK) DO NOTHING
                """), {"sk": dsk, "d": rdate})

        #dim actor
        for actor_src_id, actor_name in unique_actors.items():
            res = c.execute(text("""
                INSERT INTO Dim_Actor (Actor_ID_Source, Name)
                VALUES (:src_id, :name)
                ON CONFLICT (Actor_ID_Source) DO UPDATE SET
                    Name = EXCLUDED.Name
                RETURNING Actor_SK
            """), {"src_id": actor_src_id, "name": actor_name[:255]}).scalar()
            
            if res is None:
                res = c.execute(text("SELECT Actor_SK FROM Dim_Actor WHERE Actor_ID_Source=:src_id"), {"src_id": actor_src_id}).scalar()
            
            if res is not None:
                actor_src_to_sk[actor_src_id] = int(res)
     
        for imdb_key, actor_id, actor_name, role in actors_data:
            msk = movie_src_to_sk.get(imdb_key)
            ask = actor_src_to_sk.get(actor_id)
            
            if msk and ask:
                c.execute(text("""
                    INSERT INTO Bridge_Movie_Actor (Movie_SK, Actor_SK, Role)
                    VALUES (:msk, :ask, :role)
                    ON CONFLICT (Movie_SK, Actor_SK) DO NOTHING
                """), {"msk": msk, "ask": ask, "role": (role[:100] if role else None)})
                inserted_bridge += 1


        # Fact
        for book_src_id, movie_src_id in norm_links:
            bsk = book_src_to_sk.get(book_src_id)
            msk = movie_src_to_sk.get(movie_src_id)
            if not bsk or not msk:
                continue

            bm = book_measures.get(book_src_id, {})
            mm = imdb_to_movie.get(movie_src_id, {})

            rdate = mm.get("release_date")
            date_sk = _date_to_sk(rdate) if rdate else None
            
      
            budget = mm.get("budget")
            revenue = mm.get("revenue")
            profit = (revenue - budget) if budget is not None and revenue is not None else None
            roi = (profit / budget * 100) if profit is not None and budget is not None and budget != 0 else None

      
            c.execute(text("""
                INSERT INTO Fact_Book_Adaptation
                    (Book_SK, Movie_SK, Movie_Release_Date_SK,
                     Box_Office_Gross, Tickets_Sold, Production_Budget, Profit, ROI,
                     Book_Average_Rating, Book_Ratings_Count, Book_Text_Reviews_Count,
                     Movie_Average_Rating, Movie_Review_Count)
                VALUES
                    (:bsk, :msk, :dsk,
                     :revenue, NULL, :budget, :profit, :roi,
                     :bavg, :brc, :btrc,
                     :mavg, :mrc)
                ON CONFLICT (Book_SK, Movie_SK) DO NOTHING
            """), {
                "bsk": bsk, "msk": msk, "dsk": date_sk,
                "revenue": revenue,
                "budget": budget,
                "profit": profit,
                "roi": roi,
                "bavg": bm.get("avg_rating"),
                "brc": bm.get("ratings_count"),
                "btrc": bm.get("text_reviews_count"),
                "mavg": mm.get("vote_average"),
                "mrc": mm.get("vote_count"),
            })
            inserted_facts += 1

    src.close()
    print("DW load complete:")
    print(f"  Books upserted (distinct Book_ID_Source): {len(book_src_to_sk)}")
    print(f"  Movies upserted (distinct IMDb ids):     {len(movie_src_to_sk)}")
    print(f"  Actors upserted (distinct Actor_ID):    {len(actor_src_to_sk)}")
    print(f"  Movie-Actor bridge rows inserted:         {inserted_bridge}")
    print(f"  Fact rows inserted/updated:               {inserted_facts}")