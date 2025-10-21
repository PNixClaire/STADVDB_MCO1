from sqlalchemy import create_engine, text
import pandas as pd, os, glob, re
from config import PG_URL, NUMBERS_DAILY_DIR

def _filename_date(path):
    m = re.search(r'(\d{4}-\d{2}-\d{2})', os.path.basename(path))
    return m.group(1) if m else None

def load_numbers_daily():
    if not NUMBERS_DAILY_DIR or not os.path.isdir(NUMBERS_DAILY_DIR):
        print(f"NUMBERS_DAILY_DIR not found: {NUMBERS_DAILY_DIR}")
        return
    files = sorted(glob.glob(os.path.join(NUMBERS_DAILY_DIR, "*.csv")))
    if not files:
        print(f"No CSV files in {NUMBERS_DAILY_DIR}")
        return

    engine = create_engine(PG_URL)
    total = 0
    for path in files:
        try:
            df = pd.read_csv(path)
            cols = {c: c.strip().lower().replace(' ', '_') for c in df.columns}
            df.rename(columns=cols, inplace=True)
            d = _filename_date(path)
            df['chart_date'] = pd.to_datetime(d).dt.date
            with engine.begin() as c:
                c.execute(text("SET search_path TO source_books_movies"))
                for _, r in df.iterrows():
                    title = r.get('movie_title') or r.get('title')
                    if not title: 
                        continue
                    movie_id = c.execute(text("""
                        SELECT movie_id FROM movies
                        WHERE lower(title)=lower(:t)
                        ORDER BY movie_id LIMIT 1
                    """), {"t": title}).scalar()
                    c.execute(text("""
                    INSERT INTO box_office_daily
                    (movie_id, chart_date, distributor, rank_position, daily_gross, percent_change_yesterday,
                     percent_change_last_week, theaters, per_theater_average, total_gross_to_date, days_in_release)
                    VALUES
                    (:movie_id, :chart_date, :dist, :rank, :gross, :yd, :lw, :theaters, :pta, :total, :days)
                    ON CONFLICT (movie_id, chart_date) DO UPDATE SET
                      distributor=EXCLUDED.distributor,
                      rank_position=EXCLUDED.rank_position,
                      daily_gross=EXCLUDED.daily_gross,
                      percent_change_yesterday=EXCLUDED.percent_change_yesterday,
                      percent_change_last_week=EXCLUDED.percent_change_last_week,
                      theaters=EXCLUDED.theaters,
                      per_theater_average=EXCLUDED.per_theater_average,
                      total_gross_to_date=EXCLUDED.total_gross_to_date,
                      days_in_release=EXCLUDED.days_in_release;
                    """), {
                        "movie_id": movie_id,
                        "chart_date": r['chart_date'],
                        "dist": r.get('distributor'),
                        "rank": r.get('rank') or r.get('rank_position'),
                        "gross": r.get('gross') or r.get('daily_gross'),
                        "yd": r.get('%yd') or r.get('percent_change_yesterday'),
                        "lw": r.get('%lw') or r.get('percent_change_last_week'),
                        "theaters": r.get('theaters'),
                        "pta": r.get('per_theater') or r.get('per_theater_average'),
                        "total": r.get('total_gross') or r.get('total_gross_to_date'),
                        "days": r.get('days_in_release')
                    })
            total += len(df)
            print(f"{os.path.basename(path)}: {len(df)} rows")
        except Exception as e:
            print(f"{os.path.basename(path)}: {e}")
    print(f"🎉 Daily charts loaded: {total} rows total.")
