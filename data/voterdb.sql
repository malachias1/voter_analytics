CREATE TABLE IF NOT EXISTS county_details
(
    county_code     text not null,
    county_fp       text not null,
    county_name     text not null
);

CREATE TABLE IF NOT EXISTS precinct_details
(
    id              text primary key,
    year            integer not null,
    county_code     text not null,
    precinct_id     text not null,
    precinct_name   text not null
);

CREATE TABLE IF NOT EXISTS election_results
(
    county_code     text not null,
    precinct_id     text not null,
    precinct_name   text not null,
    votes           integer not null,
    election_date   text not null,
    contest         text not null,
    choice          text not null,
    vote_type       text not null,
    is_question     text not null,
    incumbent       text not null,
    party       text not null
);

CREATE INDEX IF NOT EXISTS election_results_idx ON election_results (election_date, contest);

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
    address_id integer not null,
    voter_id   text    not null,
    primary key (address_id, voter_id)
);

CREATE INDEX IF NOT EXISTS ADDRESS_VOTER_VOTER_IDX ON address_voter (voter_id);

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
    address_id integer not null,
    voter_id   text    not null,
    primary key (address_id, voter_id)
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


CREATE TABLE IF NOT EXISTS address_land_district
(
    address_id    integer primary key,
    land_district text not null
);

CREATE TABLE IF NOT EXISTS address_land_lot
(
    address_id integer primary key,
    land_lot   text not null
);

CREATE TABLE IF NOT EXISTS address_precinct_id
(
    address_id  integer primary key,
    precinct_id text not null
);

CREATE TABLE IF NOT EXISTS address_city_precinct_id
(
    address_id       integer primary key,
    city_precinct_id text not null
);

CREATE TABLE IF NOT EXISTS address_cng
(
    address_id integer primary key,
    cng        text not null
);

CREATE TABLE IF NOT EXISTS address_sen
(
    address_id integer primary key,
    sen        text not null
);

CREATE TABLE IF NOT EXISTS address_hse
(
    address_id integer primary key,
    hse        text not null
);

CREATE TABLE IF NOT EXISTS address_jud
(
    address_id integer primary key,
    jud        text not null
);

CREATE TABLE IF NOT EXISTS address_com
(
    address_id integer primary key,
    com        text not null
);

CREATE TABLE IF NOT EXISTS address_sch
(
    address_id integer primary key,
    sch        text not null
);

CREATE TABLE IF NOT EXISTS address_county_districta_name
(
    address_id            integer primary key,
    county_districta_name text not null
);

CREATE TABLE IF NOT EXISTS address_county_districta_value
(
    address_id             integer primary key,
    county_districta_value text not null
);

CREATE TABLE IF NOT EXISTS address_county_districtb_name
(
    address_id            integer primary key,
    county_districtb_name text not null
);

CREATE TABLE IF NOT EXISTS address_county_districtb_value
(
    address_id             integer primary key,
    county_districtb_value text not null
);

CREATE TABLE IF NOT EXISTS address_municipal_name
(
    address_id     integer primary key,
    municipal_name text not null
);

CREATE TABLE IF NOT EXISTS address_municipal_code
(
    address_id     integer primary key,
    municipal_code text not null
);

CREATE TABLE IF NOT EXISTS address_ward_city_council_name
(
    address_id             integer primary key,
    ward_city_council_name text not null
);

CREATE TABLE IF NOT EXISTS address_ward_city_council_code
(
    address_id             integer primary key,
    ward_city_council_code text not null
);

CREATE TABLE IF NOT EXISTS address_city_school_district_name
(
    address_id                integer primary key,
    city_school_district_name text not null
);

CREATE TABLE IF NOT EXISTS address_city_school_district_value
(
    address_id                 integer primary key,
    city_school_district_value text not null
);

CREATE TABLE IF NOT EXISTS address_city_dista_name
(
    address_id      integer primary key,
    city_dista_name text not null
);

CREATE TABLE IF NOT EXISTS address_city_dista_value
(
    address_id       integer primary key,
    city_dista_value text not null
);

CREATE TABLE IF NOT EXISTS address_city_distb_name
(
    address_id      integer primary key,
    city_distb_name text not null
);

CREATE TABLE IF NOT EXISTS address_city_distb_value
(
    address_id       integer primary key,
    city_distb_value text not null
);

CREATE TABLE IF NOT EXISTS address_city_distc_name
(
    address_id      integer primary key,
    city_distc_name text not null
);

CREATE TABLE IF NOT EXISTS address_city_distc_value
(
    address_id       integer primary key,
    city_distc_value text not null
);

CREATE TABLE IF NOT EXISTS address_city_distd_name
(
    address_id      integer primary key,
    city_distd_name text not null
);

CREATE TABLE IF NOT EXISTS address_city_distd_value
(
    address_id       integer primary key,
    city_distd_value text not null
);

CREATE TABLE IF NOT EXISTS address_district_combo
(
    address_id     integer primary key,
    district_combo text not null
);
