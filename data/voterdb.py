from pathlib import Path
import sqlite3 as sql
import pandas as pd
from data.pathes import Pathes


class VoterDb(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        p = Path(self.root_dir, 'voter.db')
        self.con = sql.connect(p)

    @property
    def mailing_address_voter(self):
        return pd.read_sql_query(f"select * from mailing_address_voter", self.con)

    @property
    def mailing_addresses(self):
        df = pd.read_sql_query(f"select * from mailing_address", self.con)
        return self.add_address_key(df, self.as_mailing_address_key)

    @property
    def precinct_details(self):
        return pd.read_sql_query(f"select * from precinct_details", self.con)

    @property
    def precinct_summary(self):
        return pd.read_sql_query(f"select * from precinct_summary", self.con)

    @property
    def next_mailing_address_id(self):
        df = pd.read_sql_query(f"select max(address_id) from mailing_address", self.con)
        if len(df) > 0 and df.iloc[0, 0] is not None:
            return df.iloc[0, 0] + 1
        return 0

    @property
    def next_precinct_id(self):
        df = pd.read_sql_query(f"select max(id) from precinct_details", self.con)
        if len(df) > 0 and df.iloc[0, 0] is not None:
            return df.iloc[0, 0] + 1
        return 0

    @property
    def next_residence_address_id(self):
        df = pd.read_sql_query(f"select max(address_id) from residence_address", self.con)
        if len(df) > 0 and df.iloc[0, 0] is not None:
            return df.iloc[0, 0] + 1
        return 0

    @property
    def residence_addresses(self):
        df = pd.read_sql_query(f"select * from residence_address", self.con)
        return self.add_address_key(df, self.as_residence_address_key)

    @property
    def voter_cng(self):
        return pd.read_sql_query(f"select voter_id, cng from voter_cng", self.con)

    @property
    def voter_demographics(self):
        return pd.read_sql_query(f"select * from voter_demographics", self.con)

    @property
    def voter_hse(self):
        return pd.read_sql_query(f"select voter_id, hse from voter_hse", self.con)

    @property
    def voter_mailing_address(self):
        return pd.read_sql_query(f"select * from mailing_address_voter", self.con)

    @property
    def voter_names(self):
        return pd.read_sql_query(f"select * from voter_name", self.con)

    @property
    def voter_precinct(self):
        return pd.read_sql_query(f"select * from voter_precinct", self.con)

    @property
    def voter_residence_address(self):
        return pd.read_sql_query(f"select * from address_voter", self.con)

    @property
    def voter_sen(self):
        return pd.read_sql_query(f"select voter_id, sen from voter_sen", self.con)

    @property
    def voter_status(self):
        return pd.read_sql_query(f"select * from voter_status", self.con)

    def initialize(self):
        """
        Execute schema DDL. The method is idempotent.

        :return: None
        """
        schema_path = Path(Path(__file__).parent, 'voterdb.sql')
        with schema_path.open('r') as f:
            schema_ddl = f.read()
            self.run_script(schema_ddl)

    @classmethod
    def as_residence_address_key(cls, row, pos=0):
        address = f'{row[pos + 1]} {row[pos + 2]}'
        if len(row[pos + 3]) > 0:
            address += f' #{row[pos + 3]}'
        address += f'; {row[pos + 4]}'
        address += f' {row[pos + 5]}'
        address += f' {row[pos + 6]}'
        if len(row[pos + 7]) > 0:
            address += f'-{row[pos + 7]}'
        return address

    @classmethod
    def as_mailing_address_key(cls, row, pos=0):
        if len(row[pos + 4]) == 0:
            return ''
        address = f'{row[pos + 0]} {row[pos + 1]}'
        if len(row[pos + 2]) > 0:
            address += f' #{row[pos + 2]}'
        address += f'; {row[pos + 3]}'
        address += f' {row[pos + 4]}'
        address += f' {row[pos + 5]}'
        if len(row[pos + 6]) > 0:
            address += f'-{row[pos + 6]}'
        if len(row[pos + 7]) > 0:
            address += f'; {row[pos + 7]}'
        if len(row[pos + 8]) > 0:
            address += f'; {row[pos + 8]}'
        if len(row[pos + 9]) > 0:
            address += f'; {row[pos + 9]}'
        return address

    @classmethod
    def add_address_key(cls, df, func):
        if len(df) > 0:
            df = df.assign(key=df.apply(func, axis=1, pos=1))
        else:
            columns = list(df.columns)
            columns.append('key')
            df = pd.DataFrame(columns=columns)
        return df

    def get_residence_address(self, address_id):
        return pd.read_sql_query(f"select * from residence_address where address_id={address_id}", self.con)

    def get_residence_address_for_voter(self, voter_id):
        address_id = self.get_residence_address_id_for_voter(voter_id)
        if address_id:
            return self.get_residence_address(address_id)
        return None

    def get_residence_address_id_for_voter(self, voter_id):
        df = pd.read_sql_query(f"select address_id,voter_id from address_voter where voter_id='{voter_id}'", self.con)
        if len(df) > 0:
            return df.iloc[0, 0]
        return None

    def get_residence_addresses_for_county(self, county_code):
        df = pd.read_sql_query(f"select * from residence_address where county_code='{county_code}'", self.con)
        return self.add_address_key(df, self.as_residence_address_key)

    def get_voter_cng(self, voter_id):
        df = pd.read_sql_query(f"select * from voter_cng where voter_id={voter_id}", self.con)
        if len(df) > 0:
            return df.cng.iloc[0]
        return None

    def get_voter_hse(self, voter_id):
        df = pd.read_sql_query(f"select * from voter_hse where voter_id={voter_id}", self.con)
        if len(df) > 0:
            return df.cng.iloc[0]
        return None

    def get_voter_sen(self, voter_id):
        df = pd.read_sql_query(f"select * from voter_sen where voter_id={voter_id}", self.con)
        if len(df) > 0:
            return df.cng.iloc[0]
        return None

    def get_voter_precinct_id(self, voter_id):
        df = pd.read_sql_query(f"select * from voter_sen where voter_id={voter_id}", self.con)
        if len(df) > 0:
            return df.cng.iloc[0]
        return None

    def get_voter_by_name(self, first_name, last_name):
        params = (first_name, last_name)
        return pd.read_sql_query(f"select * from voter_name where ?=first_name and ?=last_name",
                                 self.con, params=params)

    def get_voter_by_middle_name(self, middle_name, last_name):
        params = (middle_name, last_name)
        return pd.read_sql_query(f"select * from voter_name where ?=middle_name and ?=last_name",
                                 self.con, params=params)

    def get_voter_by_last_name(self, last_name):
        params = (last_name,)
        return pd.read_sql_query(f"select * from voter_name where ?=last_name",
                                 self.con, params=params)

    def get_voter_demographics_for_voter(self, voter_id):
        df = pd.read_sql_query(f"select * from voter_demographics where voter_id='{voter_id}'", self.con)
        if len(df) > 0:
            return df.iloc[0]
        return None

    def get_voter_name_details(self, voter_id):
        df = pd.read_sql_query(f"select * from voter_name where voter_id='{voter_id}'", self.con)
        if len(df.index) == 1:
            return df
        return None

    def get_voter_demographics_details(self, voter_id):
        df = pd.read_sql_query(f"select * from voter_demographics where voter_id='{voter_id}'", self.con)
        if len(df.index) == 1:
            return df
        return None

    def rebuild_search_table(self):
        cur = self.con.cursor()
        cur.execute('DELETE FROM voter_search;')
        self.con.commit()
        df1 = pd.read_sql_query(f"select * from address_voter", self.con)
        df2 = pd.read_sql_query(f"select voter_id, last_name, first_name, middle_name from voter_name", self.con)
        df3 = pd.read_sql_query(f"select address_id, house_number, zipcode from residence_address", self.con)
        df = df1.merge(df2, how='inner', on=['voter_id'])
        df = df.merge(df3, how='inner', on=['address_id'])
        # note the order of address_id and voter_id
        stmt = f"""
        replace into voter_search 
            (address_id, voter_id, last_name, first_name, middle_name, house_number, zipcode) 
        values (?,?,?,?,?,?,?)"""
        cur = self.con.cursor()
        df.apply(lambda row: cur.execute(stmt, (row[0], row[1], row[2], row[3], row[4], row[5], row[6])), axis=1)
        self.con.commit()

    def voter_search(self, first_name, last_name, house_number, zipcode):
        df = pd.read_sql_query(f"""
            select 
                address_id, voter_id, last_name, first_name, middle_name, house_number, zipcode 
            from voter_search where last_name=? and house_number=? and zipcode=?""",
                               self.con, params=(last_name, house_number, zipcode))
        return pd.concat([df[df.first_name == first_name], df[df.middle_name == first_name]], ignore_index=True)

    def voter_history_for_date(self, year, month, day):
        date = f'{year}{month:02d}{day:02d}'
        return pd.read_sql_query(f"select * from voter_history where date='{date}'", self.con)

    def get_voter_history_summary(self, limit=None):
        stmt = f"select * from voter_history_summary"
        if limit is not None:
            stmt += f' limit {limit}'
        return pd.read_sql_query(stmt, self.con, dtype="string")

    def get_voter_score(self):
        return pd.read_sql_query(f"select * from voter_score", self.con)

    def delete_election_result(self, election_date, county):
        cur = self.con.cursor()
        cur.execute(f"delete from election_results where election_date='{election_date}' and county='{county}'")
        self.con.commit()

    def get_election_results(self, election_date):
        return pd.read_sql_query(f"select * from election_results where election_date='{election_date}'", self.con)

    def delete_election_result_over_under(self, election_date, county):
        cur = self.con.cursor()
        cur.execute(
            f"delete from election_results_over_under where election_date='{election_date}' and county='{county}'")
        self.con.commit()

    def get_election_results_over_under(self, election_date):
        return pd.read_sql_query(f"select * from election_results_over_under where election_date='{election_date}'",
                                 self.con)

    def insert_multiple_mailing_address(self, df):
        df = df[['address_id', 'house_number',
                 'street_name', 'apt_no', 'city',
                 'state', 'zipcode', 'plus4',
                 'address_line2', 'address_line3', 'country']]
        stmt = f"""
        insert into mailing_address (address_id, house_number, street_name,
                                     apt_no, city, state, zipcode, plus4,
                                     address_line2, address_line3, country)
        values (?,?,?,?,?,?,?,?,?,?,?)
        """
        self.executemany(stmt, df.values.tolist())

    def insert_multiple_residence_address(self, df):
        df = df[['address_id', 'county_code', 'house_number',
                 'street_name', 'apt_no', 'city',
                 'state', 'zipcode', 'plus4']]
        stmt = f"""
        insert into residence_address (address_id, county_code, house_number, street_name,
                                       apt_no, city, state, zipcode, plus4) 
        values (?,?,?,?,?,?,?,?,?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_precinct_details(self, df):
        """
        Update precinct details for a county. Raise an
        error if multiple counties are present in df.
        Purge precincts. Put colums in correct order.

        :param df:
        :return:
        """
        df = df[['id', 'county_code', 'precinct_id']]
        self.check_county_code_integrity(df)
        county_code = df.county_code.unique()[0]
        pruge_script = f"""
        delete from precinct_details where county_code='{county_code}';
        """
        self.run_script(pruge_script)
        stmt = f"""insert into precinct_details (id, county_code, precinct_id)
                   values (?, ?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_cng(self, df):
        df = df[['voter_id', 'cng']]
        stmt = f"""
            replace into voter_cng (voter_id, cng)
            values (?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_demographics(self, df):
        df = df[['voter_id', 'race_id', 'gender', 'year_of_birth']]
        stmt = f"""
            replace into voter_demographics (voter_id, race_id, gender, year_of_birth)
            values (?, ?, ?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_hse(self, df):
        df = df[['voter_id', 'hse']]
        stmt = f"""
            replace into voter_hse (voter_id, hse)
            values (?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_mailing_address(self, df):
        df = df[['voter_id', 'address_id']]
        stmt = f'replace into mailing_address_voter (voter_id, address_id) values (?,?)'
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_name(self, df):
        df = df[['voter_id', 'last_name', 'first_name',
                 'middle_name', 'name_suffix', 'name_title']]
        stmt = f"""
          replace into voter_name (voter_id, last_name, first_name, 
                                   middle_name, name_suffix, name_title)
          values (?, ?, ?, ?, ?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_precinct(self, df):
        df = df[['voter_id', 'precinct_id']]
        # values = df.apply(lambda row: row.tolist(), axis=1)
        stmt = f"""
          replace into voter_precinct (voter_id, precinct_id) 
          values (?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_residence_address(self, df):
        df = df[['voter_id', 'address_id']]
        stmt = f'replace into address_voter (voter_id, address_id) values (?,?)'
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_sen(self, df):
        df = df[['voter_id', 'sen']]
        stmt = f"""
            replace into voter_sen (voter_id, sen)
            values (?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_status(self, df):
        df = df[['voter_id', 'status', 'status_reason', 'date_added',
                 'date_changed', 'registration_date', 'last_contact_date']]
        stmt = f"""
            replace into voter_status (voter_id, status, status_reason, date_added,
                                       date_changed, registration_date, last_contact_date) 
            values (?, ?, ?, ?, ?, ?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_precinct_summary(self, df):
        values = df.to_dict(orient='records')
        truncate_script = f"""
        delete from precinct_summary;
        """
        self.run_script(truncate_script)

        stmt = f"""
        insert into precinct_summary (
            precinct_id, total, AP, AI, HP, BH, OT, U, WH, S, B, GX, M, GZ, WH_F_S, WH_F_B, 
            WH_F_GX, WH_F_M, WH_F_GZ, WH_M_S, WH_M_B, WH_M_GX, WH_M_M, WH_M_GZ, BH_F_S, 
            BH_F_B, BH_F_GX, BH_F_M, BH_F_GZ, BH_M_S, BH_M_B, BH_M_GX, BH_M_M, BH_M_GZ, 
            U_F_S, U_F_B, U_F_GX, U_F_M, U_F_GZ, U_M_S, U_M_B, U_M_GX, U_M_M, U_M_GZ, 
            OT_F_S, OT_F_B, OT_F_GX, OT_F_M, OT_F_GZ, OT_M_S, OT_M_B, OT_M_GX, OT_M_M, 
            OT_M_GZ, HP_F_S, HP_F_B, HP_F_GX, HP_F_M, HP_F_GZ, HP_M_S, HP_M_B, HP_M_GX, 
            HP_M_M, HP_M_GZ, AI_F_S, AI_F_B, AI_F_GX, AI_F_M, AI_F_GZ, AI_M_S, AI_M_B, AI_M_GX, 
            AI_M_M, AI_M_GZ, AP_F_S, AP_F_B, AP_F_GX, AP_F_M, AP_F_GZ, AP_M_S, AP_M_B, AP_M_GX, 
            AP_M_M, AP_M_GZ
        )
        values(
            :precinct_id, :total, :AP, :AI, :HP, :BH, :OT, :U, :WH, :S, :B, :GX, :M, :GZ, 
            :WH_F_S, :WH_F_B, :WH_F_GX, :WH_F_M, :WH_F_GZ, :WH_M_S, :WH_M_B, :WH_M_GX, 
            :WH_M_M, :WH_M_GZ, :BH_F_S, :BH_F_B, :BH_F_GX, :BH_F_M, :BH_F_GZ, :BH_M_S, 
            :BH_M_B, :BH_M_GX, :BH_M_M, :BH_M_GZ, :U_F_S, :U_F_B, :U_F_GX, :U_F_M, :U_F_GZ, 
            :U_M_S, :U_M_B, :U_M_GX, :U_M_M, :U_M_GZ, :OT_F_S, :OT_F_B, :OT_F_GX, :OT_F_M, 
            :OT_F_GZ, :OT_M_S, :OT_M_B, :OT_M_GX, :OT_M_M, :OT_M_GZ, :HP_F_S, :HP_F_B, :HP_F_GX, 
            :HP_F_M, :HP_F_GZ, :HP_M_S, :HP_M_B, :HP_M_GX, :HP_M_M, :HP_M_GZ, :AI_F_S, :AI_F_B, 
            :AI_F_GX, :AI_F_M, :AI_F_GZ, :AI_M_S, :AI_M_B, :AI_M_GX, :AI_M_M, :AI_M_GZ, :AP_F_S, 
            :AP_F_B, :AP_F_GX, :AP_F_M, :AP_F_GZ, :AP_M_S, :AP_M_B, :AP_M_GX, :AP_M_M, :AP_M_GZ
        )
        """
        self.executemany(stmt, values)

    def run_script(self, script):
        cur = self.con.cursor()
        for stmt in script.split(';'):
            # print(stmt)
            cur.execute(stmt)
        self.con.commit()

    def executemany(self, stmt, values):
        cur = self.con.cursor()
        cur.executemany(stmt, values)
        self.con.commit()

    @classmethod
    def check_county_code_integrity(cls, df):
        check = df.county_code.unique()
        if len(check) > 1:
            raise ValueError('More than one county code in voter list!')
