import argparse
from loaders.books_loader import load_dw_from_books_csv
from loaders.books_films_reviews_loader import load_dw_from_bfr
from loaders.tmdb_loader import load_tmdb
from loaders.imdb_loader import load_imdb
from loaders.weekend_summary_loader import load_weekends
from loaders.daily_chart_loader import load_numbers_daily

def main():
    p = argparse.ArgumentParser("Staging ETL Orchestrator (fits schema)")
    p.add_argument("--books", action="store_true")
    p.add_argument("--bfr", action="store_true")
    p.add_argument("--tmdb", action="store_true")
    p.add_argument("--imdb", action="store_true")
    p.add_argument("--weekends", action="store_true")
    p.add_argument("--numbers-daily", action="store_true")
    p.add_argument("--all", action="store_true")
    args = p.parse_args()

    if args.all or args.books: load_dw_from_books_csv()
    if args.all or args.bfr:   load_dw_from_bfr()
    if args.all or args.tmdb:           load_tmdb()
    if args.all or args.imdb:           load_imdb()
    if args.all or args.weekends:       load_weekends()
    if args.all or args.numbers_daily:  load_numbers_daily()

if __name__ == "__main__":
    main()
