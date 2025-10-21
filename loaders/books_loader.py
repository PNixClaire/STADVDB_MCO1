# loaders/books_loader.py
from sqlalchemy import create_engine, text
import pandas as pd, os, re
from config import PG_URL, BOOKS_PATH

def _read_books(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"Books file not found: {path}")

    ext = os.path.splitext(path)[1].lower()
    common_kwargs = dict(dtype={
        "book_id": "Int64",
        "isbn": "string",
        "isbn13": "string",
        "title": "string",
        "authors": "string",
        "language_code": "string",
        "average_rating": "float64",
        "ratings_count": "Int64",
        "text_reviews_count": "Int64",
        "publication_date": "string",
    })

    if ext in [".csv", ".txt"]:
        # robust CSV read: handles quotes/embedded commas; skip truly bad lines
        return pd.read_csv(
            path,
            engine="python",
            sep=",",
            quotechar='"',
            escapechar="\\",
            on_bad_lines="skip",
            **common_kwargs
        )
    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(path, **common_kwargs)
    else:
        raise ValueError(f"Unsupported Goodreads file type: {ext}")

def load_books():
    path = BOOKS_PATH
    if not path or not os.path.exists(path):
        print(f"Books file not found: {path}")
        return

    engine = create_engine(PG_URL)
    df = _read_books(path)

    # 🔧 Normalize headers: strip, lowercase, remove BOMs
    df.columns = [str(c).strip().lower().replace("\ufeff", "") for c in df.columns]

    # If your file uses slight variants, fix them here
    aliases = {
        "bookid": "book_id",
        "text_reviews_count": "work_text_reviews_count",  # our staging column
        "publication_date ": "publication_date",
    }
    df = df.rename(columns={k: v for k, v in aliases.items() if k in df.columns})

    # Normalize column names the dataset actually uses
    rename_map = {
        "book_id": "book_id",
        "title": "title",
        "authors": "authors",
        "average_rating": "average_rating",
        "language_code": "language_code",
        "isbn": "isbn",
        "isbn13": "isbn13",
        "ratings_count": "ratings_count",
        "work_text_reviews_count": "work_text_reviews_count",
        "publication_date": "publication_date",
    }
    df = df.rename(columns=rename_map)

    # Derive original_publication_year from publication_date (if present)
    if "publication_date" in df.columns:
        df["original_publication_year"] = pd.to_datetime(
            df["publication_date"], errors="coerce"
        ).dt.year
    else:
        df["original_publication_year"] = pd.NA

    keep = [
        "book_id","title","authors","average_rating","language_code",
        "isbn","isbn13","ratings_count","work_text_reviews_count",
        "original_publication_year"
    ]
    for k in keep:
        if k not in df.columns:
            df[k] = pd.NA

    # 🔧 Coerce book_id to numeric so NA/garbage becomes NaN (and gets dropped)
    df["book_id"] = pd.to_numeric(df["book_id"], errors="coerce")

    books_df = df[keep].dropna(subset=["book_id"]).copy()
    books_df["book_id"] = books_df["book_id"].astype(int)

    print("after-rename rows =", len(df))
    print("to-insert rows    =", len(books_df))

    with engine.begin() as c:
        c.execute(text("SET search_path TO source_books_movies"))

        # Upsert books
        rows = books_df.to_dict(orient="records")
        for r in rows:
            c.execute(text("""
                INSERT INTO books
                  (book_id, title, authors, average_rating, language_code, isbn, isbn13,
                   ratings_count, work_text_reviews_count, original_publication_year)
                VALUES
                  (:book_id, :title, :authors, :avg, :lang, :isbn, :isbn13,
                   :rc, :trc, :opy)
                ON CONFLICT (book_id) DO UPDATE SET
                  title                     = EXCLUDED.title,
                  authors                   = EXCLUDED.authors,
                  average_rating            = EXCLUDED.average_rating,
                  language_code             = EXCLUDED.language_code,
                  isbn                      = EXCLUDED.isbn,
                  isbn13                    = EXCLUDED.isbn13,
                  ratings_count             = EXCLUDED.ratings_count,
                  work_text_reviews_count   = EXCLUDED.work_text_reviews_count,
                  original_publication_year = EXCLUDED.original_publication_year;
            """), {
                "book_id": int(r["book_id"]),
                "title": None if pd.isna(r["title"]) else str(r["title"]),
                "authors": None if pd.isna(r["authors"]) else str(r["authors"]),
                "avg":   None if pd.isna(r["average_rating"]) else float(r["average_rating"]),
                "lang":  None if pd.isna(r["language_code"]) else str(r["language_code"]),
                "isbn":  None if pd.isna(r["isbn"]) else str(r["isbn"]),
                "isbn13":None if pd.isna(r["isbn13"]) else str(r["isbn13"]),
                "rc":    None if pd.isna(r["ratings_count"]) else int(r["ratings_count"]),
                "trc":   None if pd.isna(r["work_text_reviews_count"]) else int(r["work_text_reviews_count"]),
                "opy":   None if pd.isna(r["original_publication_year"]) else int(r["original_publication_year"]),
            })

        # Build authors + mapping from the 'authors' column (comma-separated)
        author_by_name = {}
        for r in rows:
            if pd.isna(r["authors"]):
                continue
            raw = str(r["authors"])

            # split on comma OR slash OR semicolon (and collapse whitespace)
            parts = re.split(r"\s*(?:,|/|;)\s*", raw)
            # normalize + drop empties + dedupe while preserving order
            seen = set()
            names = []
            for p in parts:
                n = p.strip()
                if not n:
                    continue
                if n.lower() in seen:
                    continue
                seen.add(n.lower())
                # hard cap just in case—PG now TEXT, but being tidy is fine
                names.append(n[:500])

            for order, name in enumerate(names, start=1):
                aid = author_by_name.get(name)
                if aid is None:
                    aid = c.execute(text("""
                        INSERT INTO book_authors (author_name)
                        VALUES (:n)
                        ON CONFLICT (author_name) DO NOTHING
                        RETURNING author_id
                    """), {"n": name}).scalar()
                    if aid is None:
                        aid = c.execute(
                            text("SELECT author_id FROM book_authors WHERE author_name=:n"),
                            {"n": name}
                        ).scalar()
                    author_by_name[name] = aid
                c.execute(text("""
                    INSERT INTO book_author_mapping (book_id, author_id, author_order)
                    VALUES (:bid, :aid, :ord)
                    ON CONFLICT DO NOTHING
                """), {"bid": int(r["book_id"]), "aid": int(aid), "ord": order})

    print("Done: Goodreads -> books, book_authors, book_author_mapping")

