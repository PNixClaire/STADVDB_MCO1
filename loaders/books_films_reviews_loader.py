# loaders/books_films_reviews_loader.py
from sqlalchemy import create_engine, text
import sqlite3, os, pandas as pd, re
from datetime import datetime
from config import PG_URL, BOOKS_FILM_REVIEW_PATH

def _derive_movie_id_from_imdb(imdb_id: str | None):
    if not imdb_id or not isinstance(imdb_id, str):
        return None
    m = re.search(r"tt(\d+)", imdb_id)
    return int(m.group(1)) if m else None

def _coerce_date(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return pd.to_datetime(val, errors="coerce").date()
    except Exception:
        return None

# map textual Goodreads-style ratings to numeric (1..5)
TEXT_RATING_MAP = {
    "did not like it": 1.0,
    "it was ok": 2.0,
    "liked it": 3.0,
    "really liked it": 4.0,
    "it was amazing": 5.0,
}

def _coerce_book_rating(val):
    # numeric? great
    try:
        v = float(val)
        return v if 0 <= v <= 5 else None
    except Exception:
        pass
    # text label?
    if isinstance(val, str):
        return TEXT_RATING_MAP.get(val.strip().lower())
    return None

def _coerce_movie_rating(val):
    # Accept 0–10 numeric, or 1–5 -> scale to 10 if you want. We'll just store what we get.
    try:
        v = float(val)
        return v if 0 <= v <= 10 else None
    except Exception:
        return None

def load_books_films_reviews():
    if not BOOKS_FILM_REVIEW_PATH or not os.path.exists(BOOKS_FILM_REVIEW_PATH):
        print(f"books_films_reviews not found: {BOOKS_FILM_REVIEW_PATH}")
        return

    engine = create_engine(PG_URL)
    src = sqlite3.connect(BOOKS_FILM_REVIEW_PATH)
    cur = src.cursor()
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    print("books_films_reviews tables:", ", ".join(sorted(tables)))

    with engine.begin() as c:
        c.execute(text("SET search_path TO source_books_movies"))

    # ------------------------
    # 1) MOVIES (load first)
    # ------------------------
    movies_df = None
    for t in ["movies", "movie_overall_data"]:
        if t in tables:
            movies_df = pd.read_sql_query(f"SELECT * FROM {t}", src)
            if not movies_df.empty:
                movies_df.columns = [c.strip().lower() for c in movies_df.columns]
                break
    if movies_df is not None and not movies_df.empty:
        with engine.begin() as c:
            inserted = 0
            for _, r in movies_df.iterrows():
                imdb_id = r.get("imdb_id")
                movie_id = _derive_movie_id_from_imdb(imdb_id) \
                           if pd.notna(imdb_id) else None
                # fallbacks if no imdb_id
                if movie_id is None:
                    # sometimes there’s an 'id' that *is* imdb numeric
                    if "id" in movies_df.columns:
                        try:
                            movie_id = int(r["id"])
                        except Exception:
                            pass
                if movie_id is None:
                    # skip movies we can't key
                    continue

                title = r.get("title") if pd.notna(r.get("title")) else r.get("original_title")
                rel   = _coerce_date(r.get("release_date"))
                vote_avg = r.get("vote_average")
                vote_cnt = r.get("vote_count")
                imdb_rating = r.get("imdb_rating")
                imdb_votes  = r.get("imdb_votes")

                c.execute(text("""
                    INSERT INTO movies
                      (movie_id, imdb_id, title, release_date, vote_average, vote_count,
                       imdb_rating, imdb_votes)
                    VALUES
                      (:movie_id, :imdb_id, :title, :release_date, :vote_average, :vote_count,
                       :imdb_rating, :imdb_votes)
                    ON CONFLICT (movie_id) DO UPDATE SET
                      imdb_id=EXCLUDED.imdb_id,
                      title=EXCLUDED.title,
                      release_date=EXCLUDED.release_date,
                      vote_average=EXCLUDED.vote_average,
                      vote_count=EXCLUDED.vote_count,
                      imdb_rating=EXCLUDED.imdb_rating,
                      imdb_votes=EXCLUDED.imdb_votes;
                """), {
                    "movie_id": int(movie_id),
                    "imdb_id": None if pd.isna(imdb_id) else str(imdb_id),
                    "title": None if pd.isna(title) else str(title),
                    "release_date": None if rel is None else str(rel),
                    "vote_average": None if pd.isna(vote_avg) else float(vote_avg),
                    "vote_count": None if pd.isna(vote_cnt) else int(vote_cnt),
                    "imdb_rating": None if pd.isna(imdb_rating) else float(imdb_rating),
                    "imdb_votes": None if pd.isna(imdb_votes) else int(imdb_votes),
                })
                inserted += 1
        print(f"books_films_reviews → movies upserts: {inserted}")
    else:
        print("No movies table content found; continuing.")

def _existing_ids(conn, table, id_col):
    rows = conn.execute(text(f"SELECT {id_col} FROM {table}")).fetchall()
    return set(r[0] for r in rows)

    # ------------------------
    # 2) BOOKS
    # ------------------------
    if "books" in tables:
        df = pd.read_sql_query("SELECT * FROM books", src)
        if not df.empty:
            col = df.columns
            keep = {}
            for k in ["book_id","title","authors","average_rating","language_code","isbn","isbn13",
                      "ratings_count","work_text_reviews_count","original_publication_year"]:
                if k in col: keep[k]=k
            mini = df[list(keep.values())].copy()
            mini.columns = list(keep.keys())
            with engine.begin() as c:
                for _, r in mini.iterrows():
                    c.execute(text("""
                        INSERT INTO books
                          (book_id, title, authors, average_rating, language_code, isbn, isbn13,
                           ratings_count, work_text_reviews_count, original_publication_year)
                        VALUES
                          (:book_id,:title,:authors,:avg,:lang,:isbn,:isbn13,:rc,:trc,:opy)
                        ON CONFLICT (book_id) DO NOTHING
                    """), {
                        "book_id": r.get("book_id"),
                        "title": r.get("title"),
                        "authors": r.get("authors"),
                        "avg": None if pd.isna(r.get("average_rating")) else float(r.get("average_rating")),
                        "lang": r.get("language_code"),
                        "isbn": r.get("isbn"),
                        "isbn13": None if pd.isna(r.get("isbn13")) else str(r.get("isbn13")),
                        "rc": None if pd.isna(r.get("ratings_count")) else int(r.get("ratings_count")),
                        "trc": None if pd.isna(r.get("work_text_reviews_count")) else int(r.get("work_text_reviews_count")),
                        "opy": None if pd.isna(r.get("original_publication_year")) else int(r.get("original_publication_year")),
                    })
        print(f"books_films_reviews → books upserts: {0 if df.empty else len(df)}")

    # ------------------------
    # 3) BOOK REVIEWS  (map text → numeric)
    # ------------------------
    if "book_reviews" in tables:
        br = pd.read_sql_query("SELECT * FROM book_reviews", src)
    if not br.empty:
        br.columns = [c.strip().lower() for c in br.columns]
        # choose rating source then coerce
        if "rating" in br.columns:
            rating_series = br["rating"].apply(_coerce_book_rating)
        elif "stars" in br.columns:
            rating_series = br["stars"].apply(_coerce_book_rating)
        else:
            rating_series = pd.Series([None]*len(br), index=br.index)

        with engine.begin() as c:
            valid_books = _existing_ids(c, "books", "book_id")   # <-- preload once
            inserted = skipped = 0
            for idx, r in br.iterrows():
                bid = pd.to_numeric(r.get("book_id"), errors="coerce")
                if pd.isna(bid) or int(bid) not in valid_books:
                    skipped += 1
                    continue

                rating = rating_series.loc[idx]
                rating_param = None if rating is None or pd.isna(rating) else float(rating)
                txt   = r.get("review_text")
                title = r.get("review_title")
                helpc = r.get("helpful_count")
                dt    = _coerce_date(r.get("review_date"))

                c.execute(text("""
                    INSERT INTO book_reviews
                      (book_id, rating, review_text, review_title, helpful_count, review_date)
                    VALUES
                      (:bid, :rating, :txt, :title, :help, :dt)
                """), {
                    "bid": int(bid),
                    "rating": rating_param,
                    "txt": None if pd.isna(txt) else str(txt),
                    "title": None if pd.isna(title) else str(title),
                    "help": None if pd.isna(helpc) else int(helpc),
                    "dt": None if dt is None else str(dt),
                })
                inserted += 1
        print(f"books_films_reviews → book_reviews inserts: {inserted} (skipped {skipped} orphans)")
    else:
        br = None
        
    # ------------------------
    # 4) MOVIE REVIEWS (requires movie in table)
    # ------------------------
    if "movie_reviews" in tables:
        mr = pd.read_sql_query("SELECT * FROM movie_reviews", src)
    if not mr.empty:
        mr.columns = [c.strip().lower() for c in mr.columns]
        with engine.begin() as c:
            valid_movies = _existing_ids(c, "movies", "movie_id")  # <-- preload once
            inserted = skipped = 0
            for _, r in mr.iterrows():
                # resolve to our movie_id
                mid = None
                if "imdb_id" in mr.columns and pd.notna(r.get("imdb_id")):
                    mid = _derive_movie_id_from_imdb(str(r["imdb_id"]))
                elif "movie_id" in mr.columns and pd.notna(r.get("movie_id")):
                    mid = int(pd.to_numeric(r["movie_id"], errors="coerce"))
                elif "tmdb_id" in mr.columns and pd.notna(r.get("tmdb_id")):
                    # if you store tmdb_id in movies, you can map here; otherwise skip
                    pass

                if mid is None or mid not in valid_movies:
                    skipped += 1
                    continue

                rating = None
                if "rating" in mr.columns:
                    rating = _coerce_movie_rating(r["rating"])
                elif "stars" in mr.columns:
                    rating = _coerce_movie_rating(r["stars"])
                rating_param = None if rating is None or pd.isna(rating) else float(rating)

                txt   = r.get("review_text")
                title = r.get("review_title")
                helpc = r.get("helpful_count")
                dt    = _coerce_date(r.get("review_date"))

                c.execute(text("""
                    INSERT INTO movie_reviews
                      (movie_id, rating, review_text, review_title, helpful_count, review_date)
                    VALUES
                      (:mid, :rating, :txt, :title, :help, :dt)
                """), {
                    "mid": int(mid),
                    "rating": rating_param,
                    "txt": None if pd.isna(txt) else str(txt),
                    "title": None if pd.isna(title) else str(title),
                    "help": None if pd.isna(helpc) else int(helpc),
                    "dt": None if dt is None else str(dt),
                })
                inserted += 1
        print(f"books_films_reviews → movie_reviews inserts: {inserted} (skipped {skipped} orphans)")
    else:
        mr = None
    # ------------------------
    # 5) BOOK ↔ MOVIE LINKS
    # ------------------------
    mapped = False
    if "booksmovies" in tables:
        df = pd.read_sql_query("SELECT * FROM booksmovies", src)
        if not df.empty:
            df.columns = [c.strip().lower() for c in df.columns]
            bid_col = "book_id" if "book_id" in df.columns else ("goodreads_book_id" if "goodreads_book_id" in df.columns else None)
            mid_col = "imdb_id" if "imdb_id" in df.columns else None
            if bid_col and mid_col:
                with engine.begin() as c:
                    links = 0
                    for _, r in df.iterrows():
                        bid = pd.to_numeric(r[bid_col], errors="coerce")
                        mid = _derive_movie_id_from_imdb(str(r[mid_col]))
                        if pd.isna(bid) or mid is None:
                            continue
                        c.execute(text("""
                            INSERT INTO book_movie_adaptations (book_id, movie_id, adaptation_type)
                            VALUES (:bid, :mid, 'direct')
                            ON CONFLICT (book_id, movie_id) DO NOTHING
                        """), {"bid": int(bid), "mid": int(mid)})
                        links += 1
                print(f"books_films_reviews → book_movie_adaptations linked: {links}")
                mapped = True

    if not mapped and "wiki_book_movie_ids_matching" in tables:
        df = pd.read_sql_query("SELECT * FROM wiki_book_movie_ids_matching", src)
        if not df.empty:
            df.columns = [c.strip().lower() for c in df.columns]
            if "goodreads_book_id" in df.columns and "imdb_id" in df.columns:
                with engine.begin() as c:
                    links = 0
                    for _, r in df.iterrows():
                        bid = pd.to_numeric(r["goodreads_book_id"], errors="coerce")
                        mid = _derive_movie_id_from_imdb(str(r["imdb_id"]))
                        if pd.isna(bid) or mid is None:
                            continue
                        c.execute(text("""
                            INSERT INTO book_movie_adaptations (book_id, movie_id, adaptation_type)
                            VALUES (:bid, :mid, 'direct')
                            ON CONFLICT (book_id, movie_id) DO NOTHING
                        """), {"bid": int(bid), "mid": int(mid)})
                        links += 1
                print(f"books_films_reviews → wiki links: {links}")

    src.close()
    print("Done: books_films_reviews -> staging.")
