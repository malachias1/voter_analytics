import pandas as pd
from psycopg2.extras import execute_values
from data.voterdb import VoterDb
from data.residence_address_management import ResidenceAddressManagement
from data.mailing_address_management import MailingAddressManagement


class VoterDetails(VoterDb):
    def __init__(self):
        super().__init__()
        self.ram = ResidenceAddressManagement()
        self.mam = MailingAddressManagement()

    @property
    def voter_names(self):
        results = self.fetchall(f"""
                    select voter_id, last_name, first_name, middle_name, 
                        name_suffix, name_title
                        from voter_name
                """)
        return pd.DataFrame.from_records(results, columns=['voter_id', 'last_name', 'first_name',
                                                           'middle_name', 'name_suffix', 'name_title'])

    def get_cng(self, voter_id):
        result = self.fetchone(f"select cng from voter_cng where voter_id='{voter_id}'")
        return result[0]

    def get_demographics(self, voter_id):
        cur = self.cursor()
        cur.execute(f"""
            select voter_id, race_id, gender, year_of_birth 
                from voter_demographics 
                where voter_id='{voter_id}'
        """)
        result = cur.fetchall()
        if len(result) > 0:
            return pd.DataFrame.from_records(result,
                                             columns=['voter_id', 'race_id',
                                                      'gender', 'year_of_birth'])
        return None

    def get_hse(self, voter_id):
        result = self.fetchone(f"select hse from voter_hse where voter_id='{voter_id}'")
        return result[0]

    def get_name_details(self, voter_id):
        cur = self.cursor()
        cur.execute(f"""
            select voter_id, last_name, first_name, middle_name, name_suffix, name_title 
                from voter_name 
                where voter_id='{voter_id}'
        """)
        result = cur.fetchone()
        if result is not None:
            return pd.DataFrame.from_records([result], columns=['voter_id', 'last_name', 'first_name',
                                                                'middle_name', 'name_suffix', 'name_title'])
        return None

    def get_mailing_address(self, voter_id):
        return self.mam.get_voter_address(voter_id)

    def get_residence_address(self, voter_id):
        return self.ram.get_voter_address(voter_id)

    def get_sen(self, voter_id):
        result = self.fetchone(f"select sen from voter_sen where voter_id='{voter_id}'")
        return result[0]

    def get_precinct_details(self, voter_id):
        cur = self.cursor()
        cur.execute(f"""
            select b.id as id,
                   b.county_code as county_code,
                   b.precinct_id as name,
                   b.precinct_name as description
                from voter_precinct as a
                join precinct_details as b 
                on a.precinct_id = b.id
                where a.voter_id = '{voter_id}'
        """)
        results = cur.fetchall()
        return pd.DataFrame.from_records(results, columns=['id', 'county_code', 'name', 'description'])

    def get_precinct_id(self, voter_id):
        result = self.fetchone(f"select precinct_id from voter_precinct where voter_id='{voter_id}'")
        return result[0]

    def voter_search(self, first_name, last_name, house_number, zipcode):
        cur = self.cursor()
        cur.execute(f"""
            select 
                address_id, voter_id, last_name, first_name, middle_name, house_number, zipcode 
            from voter_search where last_name=%s and house_number=%s and zipcode=%s""",
                    (last_name, house_number, zipcode))
        results = cur.fetchall()
        df = pd.DataFrame.from_records(results, columns=['address_id', 'voter_id', 'last_name', 'first_name',
                                                         'middle_name', 'house_number', 'zipcode'])
        return pd.concat([df[df.first_name == first_name], df[df.middle_name == first_name]], ignore_index=True)

    # -------------------------------------------------------------------------
    # Ingest and Update Methods
    # -------------------------------------------------------------------------

    def ingest_name(self, df):
        self.delete_voters_from_table(df.voter_id, 'voter_name')
        df = df.rename(columns={'middle_maiden_name': 'middle_name'})
        with self.con:
            with self.cursor() as cur:
                execute_values(cur, f"""
                      insert into voter_name (voter_id, last_name, first_name, 
                                               middle_name, name_suffix, name_title)
                      values %s
                    """, df[['voter_id', 'last_name', 'first_name',
                             'middle_name', 'name_suffix', 'name_title']].to_records(index=False))

    def ingest_status(self, df):
        self.delete_voters_from_table(df.voter_id, 'voter_status')
        df = df.rename(columns={'voter_status': 'status'})
        with self.con:
            with self.cursor() as cur:
                execute_values(cur, f"""
                          insert into voter_status (voter_id, status, status_reason, date_added,
                                            date_changed, registration_date, last_contact_date)
                          values %s
                        """, df[['voter_id', 'status', 'status_reason', 'date_added',
                                 'date_changed', 'registration_date', 'last_contact_date']].to_records(index=False))

    def ingest_demographics(self, df):
        self.delete_voters_from_table(df.voter_id, 'voter_demographics')
        with self.con:
            with self.cursor() as cur:
                execute_values(cur, f"""
                          insert into voter_demographics (voter_id, race_id, gender, year_of_birth)
                          values %s
                        """, df[['voter_id', 'race_id', 'gender', 'year_of_birth']].to_records(index=False))

    def ingest_cng(self, df):
        df = df[['voter_id', 'cng']]
        df = df.assign(cng=df.cng.apply(lambda x: f'{int(x):03d}'))
        self.delete_voters_from_table(df, 'voter_cng')
        with self.con:
            with self.cursor() as cur:
                execute_values(cur, f"""
                          insert into voter_cng (voter_id, cng)
                          values %s
                        """, df[['voter_id', 'cng']].to_records(index=False))

    def ingest_hse(self, df):
        df = df[['voter_id', 'hse']]
        df = df.assign(hse=df.hse.apply(lambda x: f'{int(x):03d}'))
        self.delete_voters_from_table(df, 'voter_hse')
        with self.con:
            with self.cursor() as cur:
                execute_values(cur, f"""
                          insert into voter_hse (voter_id, hse)
                          values %s
                        """, df[['voter_id', 'hse']].to_records(index=False))

    def ingest_sen(self, df):
        df = df[['voter_id', 'sen']]
        df = df.assign(sen=df.sen.apply(lambda x: f'{int(x):03d}'))
        self.delete_voters_from_table(df, 'voter_sen')
        with self.con:
            with self.cursor() as cur:
                execute_values(cur, f"""
                          insert into voter_sen (voter_id, sen)
                          values %s
                        """, df[['voter_id', 'sen']].to_records(index=False))

    def build_search_table(self):
        results = self.fetchall(f"""
            select av.address_id, av.voter_id, vn.last_name, vn.first_name, vn.middle_name,
                ra.house_number, ra.zipcode
                from address_voter as av
                join voter_name as vn
                on av.voter_id = vn.voter_id
                join residence_address ra on av.address_id = ra.address_id
            """)
        return pd.DataFrame.from_records(results, columns=['address_id', 'voter_id', 'last_name',
                                                           'first_name', 'middle_name', 'house_number',
                                                           'zipcode'])

    def replace_search_table(self, df):
        with self.con:
            with self.cursor() as cur:
                cur.execute('truncate from voter_search')
                execute_values(cur, f"""
                          insert into voter_search (address_id, voter_id, last_name, first_name, middle_name, house_number, zipcode) 
                          values %s
                        """, df[['address_id', 'voter_id', 'last_name',
                                 'first_name', 'middle_name', 'house_number',
                                 'zipcode']].to_records(index=False))
