import time, json, requests
from sqlalchemy import create_engine, text
from config import PG_URL, TMDB_API_KEY

BASE = "https://api.themoviedb.org/3"
S = requests.Session()

def _get(path, **params):
    from config import TMDB_API_KEY
    headers = {}
    if TMDB_API_KEY and TMDB_API_KEY.startswith("eyJ"):
        # v4 access token
        headers["Authorization"] = f"Bearer {TMDB_API_KEY}"
    else:
        # v3 api key fallback
        params["api_key"] = TMDB_API_KEY
    r = S.get(f"{BASE}{path}", params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()
def load_tmdb():
    if not TMDB_API_KEY:
        print("TMDB_API_KEY missing in .env")
        return

    engine = create_engine(PG_URL)
    # Seed IDs to fetch: pick any movies already inserted (e.g., via SQLite/Goodreads mapping) that have tmdb_id
    with engine.begin() as c:
        c.execute(text("SET search_path TO source_books_movies"))
        rows = c.execute(text("""
            SELECT DISTINCT tmdb_id FROM movies WHERE tmdb_id IS NOT NULL
            UNION
            SELECT DISTINCT movie_id FROM movie_reviews WHERE movie_id IS NOT NULL
            LIMIT 200
        """)).fetchall()
    ids = [r[0] for r in rows] or [603, 155, 597]

    for i, mid in enumerate(ids, 1):
        try:
            core    = _get(f"/movie/{mid}")
            credits = _get(f"/movie/{mid}/credits")

            with engine.begin() as c:
                c.execute(text("SET search_path TO source_books_movies"))
                # movies.upsert (movie_id := tmdb_id)
                c.execute(text("""
                INSERT INTO movies
                (movie_id, tmdb_id, imdb_id, title, original_title, original_language, overview, tagline,
                 status, release_date, runtime, budget, revenue, popularity, vote_average, vote_count,
                 adult, video, homepage, poster_path, backdrop_path)
                VALUES
                (:id, :id, :imdb, :t, :ot, :lang, :ov, :tg, :st, :rd, :rt, :bud, :rev, :pop, :va, :vc,
                 :ad, :vid, :home, :pp, :bp)
                ON CONFLICT (movie_id) DO UPDATE SET
                  title=EXCLUDED.title, original_title=EXCLUDED.original_title,
                  original_language=EXCLUDED.original_language, overview=EXCLUDED.overview,
                  tagline=EXCLUDED.tagline, status=EXCLUDED.status, release_date=EXCLUDED.release_date,
                  runtime=EXCLUDED.runtime, budget=EXCLUDED.budget, revenue=EXCLUDED.revenue,
                  popularity=EXCLUDED.popularity, vote_average=EXCLUDED.vote_average, vote_count=EXCLUDED.vote_count,
                  adult=EXCLUDED.adult, video=EXCLUDED.video, homepage=EXCLUDED.homepage,
                  poster_path=EXCLUDED.poster_path, backdrop_path=EXCLUDED.backdrop_path, updated_at=now();
                """), {
                    "id": core["id"], "imdb": (core.get("imdb_id") or None),
                    "t": core.get("title"), "ot": core.get("original_title"),
                    "lang": core.get("original_language"), "ov": core.get("overview"), "tg": core.get("tagline"),
                    "st": core.get("status"), "rd": core.get("release_date"), "rt": core.get("runtime"),
                    "bud": core.get("budget"), "rev": core.get("revenue"), "pop": core.get("popularity"),
                    "va": core.get("vote_average"), "vc": core.get("vote_count"), "ad": core.get("adult"),
                    "vid": core.get("video"), "home": core.get("homepage"), "pp": core.get("poster_path"),
                    "bp": core.get("backdrop_path")
                })
                # genres + link
                for g in core.get("genres", []):
                    c.execute(text("""
                      INSERT INTO genres (genre_id, genre_name, media_type)
                      VALUES (:gid, :name, 'movie')
                      ON CONFLICT (genre_id) DO UPDATE SET genre_name=EXCLUDED.genre_name;
                    """), {"gid": g["id"], "name": g["name"]})
                    c.execute(text("""
                      INSERT INTO movie_genres (movie_id, genre_id, is_primary_genre)
                      VALUES (:mid, :gid, false)
                      ON CONFLICT DO NOTHING;
                    """), {"mid": core["id"], "gid": g["id"]})
                # people + cast/crew
                for p in credits.get("cast", []) + credits.get("crew", []):
                    c.execute(text("""
                      INSERT INTO people (person_id, tmdb_id, name, gender, popularity, profile_path)
                      VALUES (:id, :id, :name, :gender, :pop, :photo)
                      ON CONFLICT (person_id) DO UPDATE SET name=EXCLUDED.name, gender=EXCLUDED.gender,
                        popularity=EXCLUDED.popularity, profile_path=EXCLUDED.profile_path;
                    """), {"id": p["id"], "name": p.get("name"), "gender": p.get("gender"),
                             "pop": p.get("popularity"), "photo": p.get("profile_path")})
                for cs in credits.get("cast", []):
                    c.execute(text("""
                      INSERT INTO movie_cast (movie_id, person_id, character_name, cast_order, billing_position, is_lead_role)
                      VALUES (:mid, :pid, :char, :ord, :bill, :lead)
                    """), {"mid": mid, "pid": cs["id"], "char": cs.get("character"),
                             "ord": cs.get("order"), "bill": cs.get("billing"), "lead": (cs.get("order", 99) <= 2)})
                for cr in credits.get("crew", []):
                    c.execute(text("""
                      INSERT INTO movie_crew (movie_id, person_id, job, department, is_key_creative)
                      VALUES (:mid, :pid, :job, :dept, :key)
                      ON CONFLICT (movie_id, person_id, job) DO NOTHING;
                    """), {"mid": mid, "pid": cr["id"], "job": cr.get("job"), "dept": cr.get("department"),
                             "key": cr.get("job") in ("Director","Writer","Producer")})
            print(f"{i}/{len(ids)} {mid} {core.get('title')}")
            time.sleep(0.35)
        except Exception as e:
            print(f"{mid}: {e}")
            time.sleep(1)
