-- ============================================================================
-- POSTGRES-READY: UNIFIED BOOKS & MOVIES SOURCE SCHEMA (STAGING LAYER)
-- Optimized for: Adaptation Analysis, ROI Tracking, Success Correlation
-- Notes:
--   * Uses a dedicated schema: source_books_movies
--   * Uses GENERATED ... AS IDENTITY instead of SERIAL
--   * Renamed some columns to avoid confusion with reserved-ish names:
--       - box_office_weekends.year       -> calendar_year
--       - box_office_weekends.week_number-> week_no
--   * Keep INTEGER PRIMARY KEY where IDs come from the source (no auto-gen)
-- ============================================================================

-- Create schema and set search_path
CREATE SCHEMA IF NOT EXISTS source_books_movies;
SET search_path TO source_books_movies;

-- (Optional) Make things repeatable during development:
-- DROP TABLES in correct order if re-running. Comment out in production.
-- DO $$ BEGIN
--   EXECUTE 'DROP TABLE IF EXISTS movie_collection_mapping         CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS movie_collections                CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS movie_popularity_snapshots       CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS book_popularity_snapshots        CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS distributors                     CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS box_office_performance           CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS box_office_daily                 CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS box_office_weekends              CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS tv_reviews                       CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS movie_reviews                    CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS book_reviews                     CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS users                            CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS movie_spoken_languages           CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS spoken_languages                 CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS movie_production_countries       CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS production_countries             CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS movie_production_companies       CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS production_companies             CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS tv_genres                        CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS movie_genres                     CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS genres                           CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS tv_cast                          CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS movie_crew                       CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS movie_cast                       CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS people                           CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS book_tv_adaptations              CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS book_movie_adaptations           CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS tv_series                        CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS movie_alternate_titles           CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS movies                           CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS book_author_mapping              CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS book_authors                     CASCADE';
--   EXECUTE 'DROP TABLE IF EXISTS books                            CASCADE';
-- END $$;

-- ============================================================================
-- BOOKS DOMAIN (Goodreads Dataset)
-- ============================================================================

CREATE TABLE books (
    book_id INTEGER PRIMARY KEY, -- provided by source
    goodreads_book_id BIGINT UNIQUE,
    best_book_id BIGINT,
    work_id BIGINT,
    books_count INTEGER,
    isbn VARCHAR(13),
    isbn13 VARCHAR(13),
    authors TEXT,
    original_publication_year INTEGER,
    original_title TEXT,
    title TEXT NOT NULL,
    language_code VARCHAR(10),
    average_rating DECIMAL(3,2),
    ratings_count INTEGER,
    work_ratings_count INTEGER,
    work_text_reviews_count INTEGER,
    ratings_1 INTEGER,
    ratings_2 INTEGER,
    ratings_3 INTEGER,
    ratings_4 INTEGER,
    ratings_5 INTEGER,
    image_url TEXT,
    small_image_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE book_authors (
    author_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    author_name VARCHAR(255) NOT NULL UNIQUE,
    total_books INTEGER DEFAULT 0,
    avg_book_rating DECIMAL(3,2),
    total_ratings_received INTEGER DEFAULT 0,
    author_popularity_score DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE book_author_mapping (
    book_id INTEGER REFERENCES books(book_id) ON DELETE CASCADE,
    author_id INTEGER REFERENCES book_authors(author_id) ON DELETE CASCADE,
    author_order INTEGER,
    PRIMARY KEY (book_id, author_id)
);

-- ============================================================================
-- MOVIES DOMAIN (TMDB API & IMDb TSV)
-- ============================================================================

CREATE TABLE movies (
    movie_id INTEGER PRIMARY KEY, -- provided by source
    tmdb_id INTEGER UNIQUE,
    imdb_id VARCHAR(20) UNIQUE,
    title VARCHAR(500) NOT NULL,
    original_title VARCHAR(500),
    original_language VARCHAR(10),
    overview TEXT,
    tagline TEXT,
    status VARCHAR(50),
    release_date DATE,
    runtime INTEGER,
    budget BIGINT,
    revenue BIGINT,
    popularity DECIMAL(10,3),
    vote_average DECIMAL(3,1),
    vote_count INTEGER,
    -- IMDb ratings supplement
    imdb_rating DECIMAL(3,1),
    imdb_votes INTEGER,
    adult BOOLEAN DEFAULT FALSE,
    video BOOLEAN DEFAULT FALSE,
    homepage TEXT,
    poster_path TEXT,
    backdrop_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE movie_alternate_titles (
    akas_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    region VARCHAR(10),
    language VARCHAR(10),
    types TEXT,
    attributes TEXT,
    is_original_title BOOLEAN
);

CREATE TABLE tv_series (
    series_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tmdb_id INTEGER UNIQUE,
    imdb_id VARCHAR(20) UNIQUE,
    name VARCHAR(500) NOT NULL,
    original_name VARCHAR(500),
    original_language VARCHAR(10),
    overview TEXT,
    tagline TEXT,
    status VARCHAR(50),
    first_air_date DATE,
    last_air_date DATE,
    number_of_seasons INTEGER,
    number_of_episodes INTEGER,
    popularity DECIMAL(10,3),
    vote_average DECIMAL(3,1),
    vote_count INTEGER,
    homepage TEXT,
    poster_path TEXT,
    backdrop_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- CRITICAL: BOOK-MOVIE / BOOK-TV ADAPTATION MAPPING
-- ============================================================================

CREATE TABLE book_movie_adaptations (
    adaptation_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    book_id INTEGER REFERENCES books(book_id) ON DELETE CASCADE,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    adaptation_type VARCHAR(50) DEFAULT 'direct',
    adaptation_year INTEGER,
    time_gap_years INTEGER,
    fidelity_score DECIMAL(3,1),
    marketing_budget BIGINT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(book_id, movie_id)
);

CREATE TABLE book_tv_adaptations (
    adaptation_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    book_id INTEGER REFERENCES books(book_id) ON DELETE CASCADE,
    series_id INTEGER REFERENCES tv_series(series_id) ON DELETE CASCADE,
    adaptation_type VARCHAR(50) DEFAULT 'direct',
    adaptation_year INTEGER,
    time_gap_years INTEGER,
    number_of_seasons_adapted INTEGER,
    fidelity_score DECIMAL(3,1),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(book_id, series_id)
);

-- ============================================================================
-- PEOPLE DOMAIN (Cast & Crew from TMDB & IMDb TSV)
-- ============================================================================

CREATE TABLE people (
    person_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tmdb_id INTEGER UNIQUE,
    imdb_id VARCHAR(20) UNIQUE,
    name VARCHAR(255) NOT NULL,
    also_known_as TEXT[], -- PG array type
    biography TEXT,
    birthday DATE,
    deathday DATE,
    place_of_birth VARCHAR(255),
    gender SMALLINT,
    popularity DECIMAL(10,3),
    profile_path TEXT
);

CREATE TABLE movie_cast (
    cast_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    person_id INTEGER REFERENCES people(person_id) ON DELETE CASCADE,
    character_name VARCHAR(500),
    cast_order INTEGER,
    billing_position INTEGER,
    is_lead_role BOOLEAN DEFAULT FALSE
);

CREATE TABLE movie_crew (
    crew_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    person_id INTEGER REFERENCES people(person_id) ON DELETE CASCADE,
    job VARCHAR(100),
    department VARCHAR(100),
    is_key_creative BOOLEAN DEFAULT FALSE,
    UNIQUE(movie_id, person_id, job)
);

CREATE TABLE tv_cast (
    cast_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    series_id INTEGER REFERENCES tv_series(series_id) ON DELETE CASCADE,
    person_id INTEGER REFERENCES people(person_id) ON DELETE CASCADE,
    character_name VARCHAR(500),
    cast_order INTEGER,
    is_lead_role BOOLEAN DEFAULT FALSE,
    UNIQUE(series_id, person_id, character_name)
);

-- ============================================================================
-- GENRES & CLASSIFICATIONS
-- ============================================================================

CREATE TABLE genres (
    genre_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL UNIQUE,
    media_type VARCHAR(20) NOT NULL CHECK (media_type IN ('movie', 'tv', 'both'))
);

CREATE TABLE movie_genres (
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    genre_id INTEGER REFERENCES genres(genre_id) ON DELETE CASCADE,
    is_primary_genre BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE tv_genres (
    series_id INTEGER REFERENCES tv_series(series_id) ON DELETE CASCADE,
    genre_id INTEGER REFERENCES genres(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (series_id, genre_id)
);

CREATE TABLE production_companies (
    company_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tmdb_id INTEGER UNIQUE,
    name VARCHAR(255) NOT NULL,
    logo_path TEXT,
    origin_country VARCHAR(10)
);

CREATE TABLE movie_production_companies (
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    company_id INTEGER REFERENCES production_companies(company_id) ON DELETE CASCADE,
    is_lead_studio BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (movie_id, company_id)
);

CREATE TABLE production_countries (
    country_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    iso_3166_1 VARCHAR(2) UNIQUE NOT NULL,
    country_name VARCHAR(100) NOT NULL
);

CREATE TABLE movie_production_countries (
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    country_id INTEGER REFERENCES production_countries(country_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, country_id)
);

CREATE TABLE spoken_languages (
    language_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    iso_639_1 VARCHAR(2) UNIQUE NOT NULL,
    language_name VARCHAR(100) NOT NULL,
    english_name VARCHAR(100)
);

CREATE TABLE movie_spoken_languages (
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    language_id INTEGER REFERENCES spoken_languages(language_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, language_id)
);

-- ============================================================================
-- REVIEWS DOMAIN (Books/Movies Reviews)
-- ============================================================================

CREATE TABLE users (
    user_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_active TIMESTAMP
);

CREATE TABLE book_reviews (
    review_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    book_id INTEGER REFERENCES books(book_id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(user_id) ON DELETE SET NULL,
    rating DECIMAL(2,1) CHECK (rating >= 0 AND rating <= 5),
    review_text TEXT,
    review_title VARCHAR(500),
    helpful_count INTEGER DEFAULT 0,
    verified_purchase BOOLEAN DEFAULT FALSE,
    review_date DATE,
    sentiment_score DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE movie_reviews (
    review_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(user_id) ON DELETE SET NULL,
    rating DECIMAL(2,1) CHECK (rating >= 0 AND rating <= 10),
    review_text TEXT,
    review_title VARCHAR(500),
    helpful_count INTEGER DEFAULT 0,
    spoiler BOOLEAN DEFAULT FALSE,
    review_date DATE,
    sentiment_score DECIMAL(3,2),
    is_critic_review BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE tv_reviews (
    review_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    series_id INTEGER REFERENCES tv_series(series_id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(user_id) ON DELETE SET NULL,
    rating DECIMAL(2,1) CHECK (rating >= 0 AND rating <= 10),
    review_text TEXT,
    review_title VARCHAR(500),
    helpful_count INTEGER DEFAULT 0,
    spoiler BOOLEAN DEFAULT FALSE,
    review_date DATE,
    sentiment_score DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- BOX OFFICE DOMAIN (Weekend & Daily Box Office Charts)
-- ============================================================================

CREATE TABLE box_office_weekends (
    weekend_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    weekend_date DATE NOT NULL,
    calendar_year INTEGER,
    week_no INTEGER,
    total_gross BIGINT,
    num_releases INTEGER,
    num_1_release INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(weekend_date)
);

CREATE TABLE box_office_daily (
    daily_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    chart_date DATE NOT NULL,
    distributor VARCHAR(255),
    rank_position INTEGER,
    daily_gross BIGINT,
    percent_change_yesterday DECIMAL(5,2),
    percent_change_last_week DECIMAL(5,2),
    theaters INTEGER,
    per_theater_average DECIMAL(12,2),
    total_gross_to_date BIGINT,
    days_in_release INTEGER,
    UNIQUE(movie_id, chart_date)
);

CREATE TABLE box_office_performance (
    performance_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    weekend_id INTEGER REFERENCES box_office_weekends(weekend_id) ON DELETE CASCADE,
    rank_position INTEGER,
    weekend_gross BIGINT,
    theaters INTEGER,
    per_theater_average DECIMAL(12,2),
    total_gross BIGINT,
    week_no INTEGER,
    percent_change DECIMAL(5,2),
    distributor VARCHAR(255)
);

CREATE TABLE distributors (
    distributor_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    distributor_name VARCHAR(255) NOT NULL UNIQUE,
    country VARCHAR(100),
    total_releases INTEGER DEFAULT 0,
    avg_opening_weekend BIGINT,
    success_rate DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- TRENDING & POPULARITY TRACKING
-- ============================================================================

CREATE TABLE book_popularity_snapshots (
    snapshot_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    book_id INTEGER REFERENCES books(book_id) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,
    ratings_count INTEGER,
    average_rating DECIMAL(3,2),
    reviews_count INTEGER,
    popularity_rank INTEGER,
    trending_score DECIMAL(10,2),
    UNIQUE(book_id, snapshot_date)
);

CREATE TABLE movie_popularity_snapshots (
    snapshot_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,
    vote_count INTEGER,
    vote_average DECIMAL(3,1),
    popularity DECIMAL(10,3),
    box_office_total BIGINT,
    trending_score DECIMAL(10,2),
    UNIQUE(movie_id, snapshot_date)
);

-- ============================================================================
-- COLLECTIONS & WATCHLISTS
-- ============================================================================

CREATE TABLE movie_collections (
    collection_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tmdb_id INTEGER UNIQUE,
    name VARCHAR(500) NOT NULL,
    overview TEXT,
    poster_path TEXT,
    backdrop_path TEXT,
    total_box_office BIGINT,
    avg_rating DECIMAL(3,1)
);

CREATE TABLE movie_collection_mapping (
    collection_id INTEGER REFERENCES movie_collections(collection_id) ON DELETE CASCADE,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    collection_order INTEGER,
    PRIMARY KEY (collection_id, movie_id)
);

-- Helpful indexes for joins (FKs don’t auto-index in PG; add selectively)
CREATE INDEX IF NOT EXISTS idx_bam_book ON book_author_mapping(book_id);
CREATE INDEX IF NOT EXISTS idx_bam_author ON book_author_mapping(author_id);
CREATE INDEX IF NOT EXISTS idx_bma_book  ON book_movie_adaptations(book_id);
CREATE INDEX IF NOT EXISTS idx_bma_movie ON book_movie_adaptations(movie_id);
CREATE INDEX IF NOT EXISTS idx_mc_movie  ON movie_cast(movie_id);
CREATE INDEX IF NOT EXISTS idx_mc_person ON movie_cast(person_id);
CREATE INDEX IF NOT EXISTS idx_mcr_movie ON movie_crew(movie_id);
CREATE INDEX IF NOT EXISTS idx_mcr_person ON movie_crew(person_id);
CREATE INDEX IF NOT EXISTS idx_mg_movie  ON movie_genres(movie_id);
CREATE INDEX IF NOT EXISTS idx_mg_genre  ON movie_genres(genre_id);
CREATE INDEX IF NOT EXISTS idx_bod_movie ON box_office_daily(movie_id);
CREATE INDEX IF NOT EXISTS idx_bod_date  ON box_office_daily(chart_date);
CREATE INDEX IF NOT EXISTS idx_bop_movie ON box_office_performance(movie_id);
CREATE INDEX IF NOT EXISTS idx_bop_week  ON box_office_performance(weekend_id);
