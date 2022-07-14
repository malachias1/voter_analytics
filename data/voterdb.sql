CREATE TABLE IF NOT EXISTS precinct_details
(
    id              integer primary key,
    county_code     text not null,
    precinct_id     text not null,
    precinct_name   text
);

CREATE INDEX IF NOT EXISTS precinct_details_county_idx ON precinct_details (county_code);

CREATE TABLE IF NOT EXISTS contest_class
(
    id integer primary key,
    election_date  text not null,
    contest        text not null,
    category       text not null,
    canonical_name text not null,
    type           text not null,
    subcategory    text,
    party          text,
    is_question    bool not null,
    ambiguous      bool not null
);

CREATE INDEX IF NOT EXISTS election_results_county_idx ON contest_class (election_date);
CREATE INDEX IF NOT EXISTS contest_class_idx ON contest_class (election_date, category, subcategory);

CREATE TABLE IF NOT EXISTS election_result_details
(
    id integer primary key,
    election_date  text not null,
    county_code         text not null,
    contest        text not null,
    choice         text not null,
    party          text,
    is_question    bool not null,
    precinct_name  text not null,
    vote_type      text not null,
    votes          integer not null
);

CREATE INDEX IF NOT EXISTS election_result_details_county_idx ON election_result_details (election_date, county_code);

CREATE TABLE IF NOT EXISTS election_results
(
    id integer primary key,
    election_date  text not null,
    county_code         text not null,
    contest        text not null,
    choice         text not null,
    party          text,
    is_question    bool not null,
    precinct_name  text not null,
    votes          integer not null
);

CREATE INDEX IF NOT EXISTS election_results_county_idx ON election_results (election_date, county_code);
CREATE INDEX IF NOT EXISTS election_results_contest_idx ON election_results (election_date, contest);


CREATE TABLE IF NOT EXISTS election_results_over_under
(
    id integer primary key,
    election_date     text not null,
    contest         text not null,
    county_code     text not null,
    precinct_name   text not null,
    overvotes           integer not null,
    undervotes           integer not null
);

CREATE INDEX IF NOT EXISTS election_results_county_over_under_idx ON election_results_over_under (election_date, county_code);

CREATE INDEX IF NOT EXISTS election_results_contest_over_under_idx ON election_results_over_under (election_date, contest);

CREATE TABLE IF NOT EXISTS voter_history
(
    voter_id     text not null,
    date         text not null,
    type         text not null,
    party        text,
    county_id    text not null,
    absentee     int  not null,
    provisional  int  not null,
    supplemental int  not null,
    primary key (voter_id, date, type)
);

CREATE INDEX IF NOT EXISTS VOTER_HISTORY_DATE_IDX ON voter_history (date);

CREATE TABLE IF NOT EXISTS voter_name
(
    voter_id    text primary key,
    last_name   text not null,
    first_name  text not null,
    middle_name text,
    name_suffix text,
    name_title  text
);

CREATE INDEX IF NOT EXISTS VOTER_LAST_NAME_FIRST_NAME_IDX ON voter_name (last_name, first_name);

CREATE TABLE IF NOT EXISTS residence_address
(
    address_id   integer primary key,
    county_code  text not null,
    house_number text not null,
    street_name  text not null,
    apt_no       text,
    city         text not null,
    state        text not null,
    zipcode      text not null,
    plus4        text,
    lat          real,
    lon          real
);

CREATE TABLE IF NOT EXISTS voter_search
(
    voter_id    text primary key,
    address_id integer not null,
    last_name   text not null,
    first_name  text not null,
    middle_name text,
    house_number text not null,
    zipcode      text not null
);

CREATE INDEX IF NOT EXISTS VOTER_SEARCH_IDX ON voter_search (last_name, house_number, zipcode);

CREATE TABLE IF NOT EXISTS address_voter
(
    voter_id   text primary key,
    address_id integer not null
);

CREATE TABLE IF NOT EXISTS mailing_address
(
    address_id    integer primary key,
    house_number  text not null,
    street_name   text not null,
    apt_no        text,
    city          text not null,
    state         text not null,
    zipcode       text not null,
    plus4         text,
    address_line2 text,
    address_line3 text,
    country       text
);

CREATE TABLE IF NOT EXISTS mailing_address_voter
(
    voter_id   text primary key,
    address_id integer not null
);


CREATE TABLE IF NOT EXISTS voter_status
(
    voter_id          text primary key,
    status            text not null,
    status_reason     text,
    date_added        text not null,
    date_changed      text not null,
    registration_date text not null,
    last_contact_date text not null
);

CREATE TABLE IF NOT EXISTS voter_demographics
(
    voter_id      text primary key,
    race_id       text    not null,
    gender        text    not null,
    year_of_birth integer not null
);

CREATE TABLE IF NOT EXISTS voter_precinct
(
    voter_id text primary key,
    precinct_id integer not null
);

CREATE TABLE IF NOT EXISTS voter_cng
(
    voter_id text primary key,
    cng text not null
);

CREATE INDEX IF NOT EXISTS voter_cng_idx ON voter_cng (cng);

CREATE TABLE IF NOT EXISTS voter_sen
(
    voter_id text primary key,
    sen text not null
);

CREATE INDEX IF NOT EXISTS voter_sen_idx ON voter_sen (sen);

CREATE TABLE IF NOT EXISTS voter_hse
(
    voter_id text primary key,
    hse text not null
);

CREATE INDEX IF NOT EXISTS voter_hse_idx ON voter_hse (hse);

CREATE TABLE IF NOT EXISTS precinct_summary
(
    precinct_id integer primary key,
    total integer not null,
    AP          integer not null,
    AI          integer not null,
    HP          integer not null,
    BH          integer not null,
    OT          integer not null,
    U           integer not null,
    WH          integer not null,
    S           integer not null,
    B           integer not null,
    GX          integer not null,
    M           integer not null,
    GZ          integer not null,
    WH_F_S      integer not null,
    WH_F_B      integer not null,
    WH_F_GX     integer not null,
    WH_F_M      integer not null,
    WH_F_GZ     integer not null,
    WH_M_S      integer not null,
    WH_M_B      integer not null,
    WH_M_GX     integer not null,
    WH_M_M      integer not null,
    WH_M_GZ     integer not null,
    BH_F_S      integer not null,
    BH_F_B      integer not null,
    BH_F_GX     integer not null,
    BH_F_M      integer not null,
    BH_F_GZ     integer not null,
    BH_M_S      integer not null,
    BH_M_B      integer not null,
    BH_M_GX     integer not null,
    BH_M_M      integer not null,
    BH_M_GZ     integer not null,
    U_F_S       integer not null,
    U_F_B       integer not null,
    U_F_GX      integer not null,
    U_F_M       integer not null,
    U_F_GZ      integer not null,
    U_M_S       integer not null,
    U_M_B       integer not null,
    U_M_GX      integer not null,
    U_M_M       integer not null,
    U_M_GZ      integer not null,
    OT_F_S      integer not null,
    OT_F_B      integer not null,
    OT_F_GX     integer not null,
    OT_F_M      integer not null,
    OT_F_GZ     integer not null,
    OT_M_S      integer not null,
    OT_M_B      integer not null,
    OT_M_GX     integer not null,
    OT_M_M      integer not null,
    OT_M_GZ     integer not null,
    HP_F_S      integer not null,
    HP_F_B      integer not null,
    HP_F_GX     integer not null,
    HP_F_M      integer not null,
    HP_F_GZ     integer not null,
    HP_M_S      integer not null,
    HP_M_B      integer not null,
    HP_M_GX     integer not null,
    HP_M_M      integer not null,
    HP_M_GZ     integer not null,
    AI_F_S      integer not null,
    AI_F_B      integer not null,
    AI_F_GX     integer not null,
    AI_F_M      integer not null,
    AI_F_GZ     integer not null,
    AI_M_S      integer not null,
    AI_M_B      integer not null,
    AI_M_GX     integer not null,
    AI_M_M      integer not null,
    AI_M_GZ     integer not null,
    AP_F_S      integer not null,
    AP_F_B      integer not null,
    AP_F_GX     integer not null,
    AP_F_M      integer not null,
    AP_F_GZ     integer not null,
    AP_M_S      integer not null,
    AP_M_B      integer not null,
    AP_M_GX     integer not null,
    AP_M_M      integer not null,
    AP_M_GZ     integer not null
);

create table cng_map
(
    id          integer primary key,
    area         REAL,
    district     TEXT,
    population   INTEGER,
    ideal_value  REAL,
    geometry_wkb TEXT,
    center_wkb   TEXT
);

CREATE INDEX IF NOT EXISTS cng_map_idx ON cng_map (district);
