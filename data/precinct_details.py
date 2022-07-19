import pandas as pd
from psycopg2.extras import execute_values
from data.voterdb import VoterDb


class PrecinctDetails(VoterDb):

    @property
    def count(self):
        result = self.fetchone(f"select count(*) from precinct_details")
        return result[0]

    def get_details(self, county_code):
        results = self.fetchall(f"""
            select id, county_code, precinct_id, precinct_name
                from precinct_details where county_code = '{county_code}'
        """)
        return pd.DataFrame.from_records(results, columns=['id', 'county_code',
                                                           'name', 'description'])

    @property
    def next_precinct_id(self):
        result = self.fetchone(f"select max(id) from precinct_details")
        if result[0] is not None:
            return result[0] + 1
        return 0

    # -------------------------------------------------------------------------
    # Ingest and Update Methods
    # -------------------------------------------------------------------------

    def build_details(self, df):
        # Run a brief integrity check
        check = df.county_code.unique()
        if len(check) > 1:
            raise ValueError('More than one county code in voter list!')
        precinct_details = df[['county_code', 'precinct_id']].drop_duplicates()
        next_id = self.next_precinct_id
        return precinct_details.assign(id=range(next_id, next_id + len(precinct_details.index)))

    def delete_precincts(self, county_code):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(f"""
                    delete from precinct_details where county_code ='{county_code}'
                """)

    def insert_details(self, df):
        with self.con:
            with self.con.cursor() as cur:
                records = df[['id', 'county_code', 'precinct_id']].to_records(index=False)
                execute_values(cur, f"""
                        insert into precinct_details (id, county_code, precinct_id)
                            values %s
                """, records)
