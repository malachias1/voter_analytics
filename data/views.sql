create or replace view voter_precinct_details as
    select a.voter_id,
           b.id as precinct_id,
           b.county_code as precinct_detail_county_code,
           b.precinct_id as precinct_detail_id,
           b.precinct_name as precinct_detail_name
    from voter_precinct as a
    join precinct_details as b on a.precinct_id = b.id
