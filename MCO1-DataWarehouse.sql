-- We decided to create a star Schema using PostgreSQL
-- Data warehouse: dw_books_adaptations

CREATE SCHEMA IF NOT EXISTS dw_books_adaptations;
SET search_path TO dw_books_adaptations;

-- --------------------------------------------------------------------------
-- DIMENSIONS
-- --------------------------------------------------------------------------

--DATE
CREATE TABLE dim_date (
    date_key           INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_date          DATE NOT NULL UNIQUE,
    calendar_year      INT,
    quarter_no         INT,
    month_no           INT,
    month_name         VARCHAR(15),
    week_no            INT,
    day_of_month       INT,
    day_of_week        INT,       -- 1=Mon ... 7=Sun
    day_name           VARCHAR(10),
    is_weekend         BOOLEAN
);

--BOOK [from books_movies_review]
CREATE TABLE dim_book (
    book_key           INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_book_id     INTEGER,                  
    title              VARCHAR(500),
    genre_primary      VARCHAR(100),          
    language_code      VARCHAR(10),
    publication_year   INT,
    average_rating     DECIMAL(3,2),
    ratings_count      INT
);

--AUTHOR 
CREATE TABLE dim_author (
    author_key         INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_author_id   INTEGER,                 
    author_name        VARCHAR(255),
    total_books        INT,
    avg_book_rating    DECIMAL(3,2),
    total_ratings      INT
);

--MOVIE
CREATE TABLE dim_movie (
    movie_key          INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_movie_id    INTEGER,                  
    title              VARCHAR(500),
    release_year       INT,
    runtime_minutes    INT,
    imdb_rating        DECIMAL(3,1),
    tmdb_vote_average  DECIMAL(3,1),
    tmdb_popularity    DECIMAL(10,3),
    status             VARCHAR(50)
);

--ACTOR
CREATE TABLE dim_actor (
    actor_key          INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_person_id   INTEGER,                 
    actor_name         VARCHAR(255),
    gender             SMALLINT,
    popularity         DECIMAL(10,3),
    total_movies       INT,
    avg_movie_rating   DECIMAL(3,1)
);

--DISTRIBUTOR
CREATE TABLE dim_distributor (
    distributor_key    INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_distributor_id INTEGER,              
    distributor_name   VARCHAR(255),
    country            VARCHAR(100),
    avg_opening_weekend BIGINT,
    success_rate       DECIMAL(5,2)
);

--GENRE
CREATE TABLE dim_genre (
    genre_key          INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_genre_id    INTEGER,
    genre_name         VARCHAR(100),
    media_type         VARCHAR(20)               -- 'movie' | 'tv' | 'both'
);

--INDEXES [just to make sure there's no duplicates]
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_book_source      ON dim_book(source_book_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_author_source    ON dim_author(source_author_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_movie_source     ON dim_movie(source_movie_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_distributor_src  ON dim_distributor(source_distributor_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_genre_source     ON dim_genre(source_genre_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_actor_source     ON dim_actor(source_person_id);

-- --------------------------------------------------------------------------
-- FACT: Adaptation [book -> movie]
-- MEASURES: budget, revenue, roi, ratings, popularity, fidelity, gap years
-- --------------------------------------------------------------------------
CREATE TABLE fact_adaptation (
    adaptation_key       INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Foreign keys to dims
    book_key             INT REFERENCES dim_book(book_key),
    author_key           INT REFERENCES dim_author(author_key),
    movie_key            INT REFERENCES dim_movie(movie_key),
    distributor_key      INT REFERENCES dim_distributor(distributor_key),
    primary_genre_key    INT REFERENCES dim_genre(genre_key),
    release_date_key     INT REFERENCES dim_date(date_key),

    -- Degenerate / descriptors
    adaptation_type      VARCHAR(50),            -- 'direct', 'inspired', etc.

    -- Measures
    budget               BIGINT,
    revenue              BIGINT,
    roi                  DECIMAL(12,6),          -- (revenue - budget) / budget
    profit               BIGINT,                 -- revenue - budget
    imdb_rating          DECIMAL(3,1),           -- snapshot at load
    tmdb_vote_average    DECIMAL(3,1),
    tmdb_popularity      DECIMAL(10,3),
    avg_book_rating      DECIMAL(3,2),
    adaptation_gap_years INT,                    -- movie_release_year - publication_year
    fidelity_score       DECIMAL(3,1),

    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_adapt_book      ON fact_adaptation(book_key);
CREATE INDEX IF NOT EXISTS idx_fact_adapt_movie     ON fact_adaptation(movie_key);
CREATE INDEX IF NOT EXISTS idx_fact_adapt_author    ON fact_adaptation(author_key);
CREATE INDEX IF NOT EXISTS idx_fact_adapt_genre     ON fact_adaptation(primary_genre_key);
CREATE INDEX IF NOT EXISTS idx_fact_adapt_date      ON fact_adaptation(release_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_adapt_distrib   ON fact_adaptation(distributor_key);

-- --------------------------------------------------------------------------
-- FACT (Bridge): ADAPTATION x ACTOR [to see if actor's popularity == adaptation success]
-- --------------------------------------------------------------------------

CREATE TABLE fact_adaptation_actor (
    adaptation_actor_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    adaptation_key       INT REFERENCES fact_adaptation(adaptation_key) ON DELETE CASCADE,
    actor_key            INT REFERENCES dim_actor(actor_key),

    is_lead_role         BOOLEAN,
    billing_position     INT,
    cast_order           INT,

    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(adaptation_key, actor_key)          
);

CREATE INDEX IF NOT EXISTS idx_faa_adaptation ON fact_adaptation_actor(adaptation_key);
CREATE INDEX IF NOT EXISTS idx_faa_actor      ON fact_adaptation_actor(actor_key);

-- --------------------------------------------------------------------------
-- FACT: BOX OFFICE BY WEEKEND (time series)
-- --------------------------------------------------------------------------
CREATE TABLE fact_box_office_week (
    box_office_week_key  INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    movie_key            INT REFERENCES dim_movie(movie_key),
    weekend_date_key     INT REFERENCES dim_date(date_key),

    rank_position        INT,
    weekend_gross        BIGINT,
    theaters             INT,
    per_theater_average  DECIMAL(12,2),
    total_gross_to_date  BIGINT,
    week_no              INT,
    distributor_key      INT REFERENCES dim_distributor(distributor_key),

    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (movie_key, weekend_date_key)
);

CREATE INDEX IF NOT EXISTS idx_fboweek_movie ON fact_box_office_week(movie_key);
CREATE INDEX IF NOT EXISTS idx_fboweek_date  ON fact_box_office_week(weekend_date_key);
CREATE INDEX IF NOT EXISTS idx_fboweek_dist  ON fact_box_office_week(distributor_key);
