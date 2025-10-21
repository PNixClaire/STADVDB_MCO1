from sqlalchemy import create_engine, text
import pandas as pd, os
from config import PG_URL, IMDB_DIR

def _read_tsv(path, usecols=None):
    return pd.read_csv(path, sep="\t", na_values="\\N", usecols=usecols, dtype=str)

def load_imdb():
    if not IMDB_DIR or not os.path.isdir(IMDB_DIR):
        print(f"IMDB_DIR not found: {IMDB_DIR}")
        return
    engine = create_engine(PG_URL)
    with engine.begin() as c:
        c.execute(text("SET search_path TO source_books_movies"))

    # people
    nb_path = os.path.join(IMDB_DIR, "name.basics.tsv")
    if os.path.exists(nb_path):
        nb = _read_tsv(nb_path, ["nconst","primaryName"])
        with engine.begin() as c:
            for _, r in nb.iterrows():
                c.execute(text("""
                  INSERT INTO people (imdb_id, name)
                  VALUES (:imdb, :name)
                  ON CONFLICT (imdb_id) DO UPDATE SET name=EXCLUDED.name;
                """), {"imdb": r["nconst"], "name": r["primaryName"]})
        print(f"people upserts: {len(nb)}")

    # ratings (update existing movies by imdb_id)
    tr_path = os.path.join(IMDB_DIR, "title.ratings.tsv")
    if os.path.exists(tr_path):
        tr = _read_tsv(tr_path, ["tconst","averageRating","numVotes"])
        with engine.begin() as c:
            for _, r in tr.iterrows():
                c.execute(text("""
                  UPDATE movies SET imdb_rating=:r, imdb_votes=:v
                  WHERE imdb_id=:imdb
                """), {"r": float(r["averageRating"]), "v": int(r["numVotes"]), "imdb": r["tconst"]})
        print(f"ratings applied: {len(tr)}")

    # alternate titles
    ta_path = os.path.join(IMDB_DIR, "title.akas.tsv")
    if os.path.exists(ta_path):
        ta = _read_tsv(ta_path, ["titleId","title","region","language","types","attributes","isOriginalTitle"])
        with engine.begin() as c:
            for _, r in ta.iterrows():
                movie_id = c.execute(text("SELECT movie_id FROM movies WHERE imdb_id=:x"), {"x": r["titleId"]}).scalar()
                if not movie_id: 
                    continue
                c.execute(text("""
                  INSERT INTO movie_alternate_titles (movie_id, title, region, language, types, attributes, is_original_title)
                  VALUES (:mid, :t, :r, :l, :ty, :attr, :orig)
                """), {
                    "mid": movie_id, "t": r["title"], "r": r["region"], "l": r["language"],
                    "ty": r["types"], "attr": r["attributes"],
                    "orig": None if pd.isna(r["isOriginalTitle"]) else (r["isOriginalTitle"] == "1")
                })
        print(f"alternate titles inserted (linked by imdb_id)")

    # principals (cast)
    tp_path = os.path.join(IMDB_DIR, "title.principals.tsv")
    if os.path.exists(tp_path):
        tp = _read_tsv(tp_path, ["tconst","nconst","category","job","characters","ordering"])
        cast = tp[tp["category"].isin(["actor","actress","self"])].copy()
        with engine.begin() as c:
            for _, r in cast.iterrows():
                movie_id = c.execute(text("SELECT movie_id FROM movies WHERE imdb_id=:x"), {"x": r["tconst"]}).scalar()
                person_id = c.execute(text("SELECT person_id FROM people WHERE imdb_id=:x"), {"x": r["nconst"]}).scalar()
                if movie_id and person_id:
                    c.execute(text("""
                      INSERT INTO movie_cast (movie_id, person_id, character_name, cast_order, billing_position, is_lead_role)
                      VALUES (:mid, :pid, :char, :ord, NULL, :lead)
                    """), {
                        "mid": movie_id, "pid": person_id,
                        "char": r.get("characters"),
                        "ord": None if pd.isna(r["ordering"]) else int(float(r["ordering"])),
                        "lead": (not pd.isna(r["ordering"]) and int(float(r["ordering"])) <= 2)
                    })
        print("movie_cast linked by imdb_id")

    # crew
    tc_path = os.path.join(IMDB_DIR, "title.crew.tsv")
    if os.path.exists(tc_path):
        tc = _read_tsv(tc_path, ["tconst","directors","writers"])
        with engine.begin() as c:
            for _, r in tc.iterrows():
                mid = c.execute(text("SELECT movie_id FROM movies WHERE imdb_id=:x"), {"x": r["tconst"]}).scalar()
                if not mid: 
                    continue
                for role, col in [("Director","directors"),("Writer","writers")]:
                    vals = [] if pd.isna(r[col]) else str(r[col]).split(",")
                    for nconst in vals:
                        pid = c.execute(text("SELECT person_id FROM people WHERE imdb_id=:x"), {"x": nconst}).scalar()
                        if pid:
                            c.execute(text("""
                              INSERT INTO movie_crew (movie_id, person_id, job, department, is_key_creative)
                              VALUES (:mid, :pid, :job, :dept, true)
                              ON CONFLICT (movie_id, person_id, job) DO NOTHING;
                            """), {"mid": mid, "pid": pid, "job": role, "dept": role+" Dept"})
        print("movie_crew linked by imdb_id")

    print("IMDb load complete (no new movies created).")
