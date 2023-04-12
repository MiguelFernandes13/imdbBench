DROP TABLE IF EXISTS Title;
DROP TABLE IF EXISTS Genre;
DROP TABLE IF EXISTS TitleGenre;
DROP TABLE IF EXISTS TitleAkas;
DROP TABLE IF EXISTS Name;
DROP TABLE IF EXISTS NamePrimaryProfession;
DROP TABLE IF EXISTS Profession;
DROP TABLE IF EXISTS NameKnownForTitles;
DROP TABLE IF EXISTS TitlePrincipals;
DROP TABLE IF EXISTS Category;
DROP TABLE IF EXISTS Job;
DROP TABLE IF EXISTS TitlePrincipalsCharacters;
DROP TABLE IF EXISTS TitleCrew;
DROP TABLE IF EXISTS TitleEpisode;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS UserGenre;
DROP TABLE IF EXISTS UserList;
DROP TABLE IF EXISTS UserHistory;

CREATE TABLE Title (
    id varchar PRIMARY KEY,
    title_type varchar,
    primary_title varchar,
    original_title varchar,
    is_adult boolean,
    start_year integer,
    end_year integer,
    runtime_minutes integer
);

CREATE TABLE Genre (
    id integer PRIMARY KEY,
    name varchar
);

CREATE TABLE TitleGenre (
    title_id varchar,
    genre_id integer,
    PRIMARY KEY (title_id, genre_id)
);

CREATE TABLE TitleAkas (
    title_id varchar,
    ordering integer,
    title varchar,
    region varchar,
    language varchar,
    PRIMARY KEY (title_id, ordering)
);

CREATE TABLE Name (
    id varchar PRIMARY KEY,
    primary_name varchar,
    birth_year integer,
    death_year integer
);

CREATE TABLE NamePrimaryProfession (
    name_id varchar,
    profession_id integer,
    PRIMARY KEY (name_id, profession_id)
);

CREATE TABLE Profession (
    id integer PRIMARY KEY,
    name varchar
);

CREATE TABLE NameKnownForTitles (
    name_id varchar,
    title_id varchar,
    PRIMARY KEY (name_id, title_id)
);

CREATE TABLE TitlePrincipals (
    title_id varchar,
    ordering integer,
    name_id varchar,
    category_id integer,
    job_id integer,
    PRIMARY KEY (title_id, ordering)
);

CREATE TABLE Category (
    id integer PRIMARY KEY,
    name varchar
);

CREATE TABLE Job (
    id integer PRIMARY KEY,
    name varchar
);

CREATE TABLE TitlePrincipalsCharacters (
    title_id varchar,
    name_id varchar,
    name varchar,
    PRIMARY KEY (title_id, name_id, name)
);

CREATE TABLE TitleCrew (
    title_id varchar,
    name_id varchar,
    type varchar,
    PRIMARY KEY (title_id, name_id, type)
);

CREATE TABLE TitleEpisode (
    title_id varchar PRIMARY KEY,
    parent_title_id varchar,
    season_number integer,
    episode_number integer
);

CREATE TABLE Users (
    id integer NOT NULL PRIMARY KEY,
    email varchar NOT NULL,
    password_hash varchar NOT NULL,
    name varchar NOT NULL,
    country_code varchar(2) NOT NULL
);

CREATE TABLE UserGenre (
    user_id integer,
    genre_id integer,
    PRIMARY KEY (user_id, genre_id)
);

CREATE TABLE UserList (
    user_id integer NOT NULL,
    title_id varchar NOT NULL,
    created_date timestamp NOT NULL,
    PRIMARY KEY (user_id, title_id)
);

CREATE TABLE UserHistory (
    user_id integer NOT NULL,
    title_id varchar NOT NULL,
    duration_seen integer NOT NULL,
    first_seen timestamp NOT NULL,
    last_seen timestamp NOT NULL,
    rating integer,
    PRIMARY KEY (user_id, title_id)
);
