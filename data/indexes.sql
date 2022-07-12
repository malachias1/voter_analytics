drop index if exists contest_class_idx;
create index if not exists contest_class_idx on contest_class (election_date, category, subcategory);

drop index if exists election_results_county_idx;
drop index if exists election_results_contest_idx;
create index if not exists election_results_county_idx on election_results (election_date, county_code);
create index if not exists election_results_contest_idx on election_results (election_date, contest);

drop index if exists election_result_details_county_idx;
create index if not exists election_result_details_county_idx on election_result_details (election_date, county_code);

drop index if exists precinct_details_county_idx;
create index if not exists precinct_details_county_idx on precinct_details (county_code);

drop index if exists voter_history_date_idx;
create index if not exists voter_history_date_idx on voter_history (date);

drop index if exists voter_last_name_first_name_idx;
create index if not exists voter_last_name_first_name_idx on voter_name (last_name, first_name);

drop index if exists voter_search_idx;
create index if not exists voter_search_idx on voter_search (last_name, house_number, zipcode);

drop index if exists voter_cng_idx;
create index if not exists voter_cng_idx on voter_cng (cng);

drop index if exists voter_sen_idx;
create index if not exists voter_sen_idx on voter_sen (sen);

drop index if exists voter_hse_idx;
create index if not exists voter_hse_idx on voter_hse (hse);

drop index if exists voter_precinct_idx;
create index if not exists voter_precinct_idx on voter_precinct (precinct_id);

