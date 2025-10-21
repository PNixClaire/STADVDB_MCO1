from sqlalchemy import create_engine, text
import pandas as pd, glob, os
from config import PG_URL, WEEKENDS_DIR

def load_weekends():
    if not WEEKENDS_DIR or not os.path.isdir(WEEKENDS_DIR):
        print(f"WEEKENDS_DIR not found: {WEEKENDS_DIR}")
        return
    files = sorted(glob.glob(os.path.join(WEEKENDS_DIR, "weekend_summary_*.xlsx")))
    if not files:
        print(f"No weekend_summary_*.xlsx files under {WEEKENDS_DIR}")
        return

    engine = create_engine(PG_URL)
    total = 0
    for path in files:
        try:
            df = pd.read_excel(path)
            cols = {c: c.strip().lower().replace(' ', '_') for c in df.columns}
            df.rename(columns=cols, inplace=True)

            df['weekend_date'] = pd.to_datetime(df.get('date'), errors='coerce').dt.date
            df['calendar_year'] = pd.to_datetime(df.get('date'), errors='coerce').dt.year
            keep = ['weekend_date','calendar_year','week_no','overall_gross','num_releases']
            d2 = df[[c for c in keep if c in df.columns]].dropna(subset=['weekend_date'])

            with engine.begin() as c:
                c.execute(text("SET search_path TO source_books_movies"))
                for _, r in d2.iterrows():
                    c.execute(text("""
                    INSERT INTO box_office_weekends (weekend_date, calendar_year, week_no, total_gross, num_releases)
                    VALUES (:d, :y, :w, :g, :n)
                    ON CONFLICT (weekend_date) DO UPDATE SET
                      calendar_year=EXCLUDED.calendar_year,
                      week_no=EXCLUDED.week_no,
                      total_gross=EXCLUDED.total_gross,
                      num_releases=EXCLUDED.num_releases;
                    """), {
                        "d": r["weekend_date"],
                        "y": int(r["calendar_year"]) if pd.notna(r["calendar_year"]) else None,
                        "w": int(r["week_no"]) if pd.notna(r["week_no"]) else None,
                        "g": int(r["overall_gross"]) if ('overall_gross' in r and pd.notna(r["overall_gross"])) else None,
                        "n": int(r["num_releases"]) if ('num_releases' in r and pd.notna(r["num_releases"])) else None
                    })
            total += len(d2)
            print(f"{os.path.basename(path)}: {len(d2)} rows")
        except Exception as e:
            print(f"{os.path.basename(path)}: {e}")
    print(f"Weekend summaries loaded: {total} rows total.")
