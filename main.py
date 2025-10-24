import argparse
from loaders.books_loader import load_dw_from_books_csv
from loaders.books_films_reviews_loader import load_dw_from_bfr
from loaders.box_office_loader import load_dw_from_box_office
from loaders.imdb_loader import load_dw_from_imdb_actors

from loaders.tmdb_loader import load_dynamic_movie_data, load_dynamic_actor_data


def main():
    p = argparse.ArgumentParser("Staging ETL Orchestrator (fits schema)")
    p.add_argument("--books", action="store_true")
    p.add_argument("--bfr", action="store_true")
    p.add_argument("--boxOffice", action="store_true")
    p.add_argument("--imdb", action="store_true")


    p.add_argument("--tmdb", action="store_true")

    p.add_argument("--all", action="store_true")
    args = p.parse_args()

    if args.all or args.books:          load_dw_from_books_csv()
    if args.all or args.bfr:            load_dw_from_bfr()
    if args.all or args.boxOffice:      load_dw_from_box_office()
    if args.all or args.imdb:           load_dw_from_imdb_actors()

    if args.all or args.tmdb:          
        load_dynamic_actor_data() 
        load_dynamic_movie_data()
  

if __name__ == "__main__":
    main()
