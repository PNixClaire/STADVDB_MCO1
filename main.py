import argparse
from loaders.books_loader import load_dw_from_books_csv
from loaders.books_films_reviews_loader import load_dw_from_bfr
from loaders.tmdb_loader import load_tmdb
from loaders.imdb_loader import load_imdb
from loaders.weekend_summary_loader import load_weekends
from loaders.box_office_loader import load_dw_from_box_office

def main():
    p = argparse.ArgumentParser("Staging ETL Orchestrator (fits schema)")
    p.add_argument("--books", action="store_true")
    p.add_argument("--bfr", action="store_true")
    p.add_argument("--boxOffice", action="store_true")
    
    p.add_argument("--tmdb", action="store_true")
    p.add_argument("--imdb", action="store_true")
    p.add_argument("--weekends", action="store_true")

    p.add_argument("--all", action="store_true")
    args = p.parse_args()

    if args.all or args.books: load_dw_from_books_csv()
    if args.all or args.bfr:   load_dw_from_bfr()
    if args.all or args.boxOffice:  load_dw_from_box_office()
    if args.all or args.tmdb:           load_tmdb()
    if args.all or args.imdb:           load_imdb()
    if args.all or args.weekends:       load_weekends()
  

if __name__ == "__main__":
    main()
