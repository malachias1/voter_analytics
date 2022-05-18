from pathlib import Path
import sqlite3 as sql
import pandas as pd
from data.pathes import Pathes


class VoterDb(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        p = Path(self.root_dir, 'voter.db')
        self.con = sql.connect(p)

    def initialize(self):
        """
        Execute schema DDL. The method is idempotent.

        :return: None
        """
        schema_path = Path(Path(__file__).parent, 'voterdb.sql')
        with schema_path.open('r') as f:
            schema_ddl = f.read()
        cur = self.con.cursor()
        for stmt in schema_ddl.split(';'):
            cur.execute(stmt)
        self.con.commit()

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

    def get_mailing_addresses(self):
        df = pd.read_sql_query(f"select * from mailing_address", self.con)
        return self.add_address_key(df, self.as_mailing_address_key)

    def get_mailing_address_id_max(self):
        df = pd.read_sql_query(f"select max(address_id) from mailing_address", self.con)
        if len(df) > 0 and df.iloc[0, 0] is not None:
            return df.iloc[0, 0]
        return 0

    def get_mailing_address_voter(self):
        return pd.read_sql_query(f"select * from mailing_address_voter", self.con)

    def get_residence_address(self, address_id):
        return pd.read_sql_query(f"select * from residence_address where address_id={address_id}", self.con)

    def get_residence_address_id_max(self):
        df = pd.read_sql_query(f"select max(address_id) from residence_address", self.con)
        if len(df) > 0 and df.iloc[0, 0] is not None:
            return df.iloc[0, 0]
        return 0

    def get_residence_address_voter(self):
        return pd.read_sql_query(f"select * from address_voter", self.con)

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

    def get_residence_addresses(self, county_code):
        df = pd.read_sql_query(f"select * from residence_address where county_code='{county_code}'", self.con)
        return self.add_address_key(df, self.as_residence_address_key)

    def get_voter_cng(self, voter_id):
        address_id = self.get_residence_address_id_for_voter(voter_id)
        if address_id:
            df = pd.read_sql_query(f"select * from address_cng where address_id={address_id}", self.con)
            if len(df) > 0:
                return df.cng.iloc[0]
        return None

    def get_voter_hse(self, voter_id):
        address_id = self.get_residence_address_id_for_voter(voter_id)
        if address_id:
            df = pd.read_sql_query(f"select * from address_hse where address_id={address_id}", self.con)
            if len(df) > 0:
                return df.hse.iloc[0]
        return None

    def get_voter_sen(self, voter_id):
        address_id = self.get_residence_address_id_for_voter(voter_id)
        if address_id:
            df = pd.read_sql_query(f"select * from address_sen where address_id={address_id}", self.con)
            if len(df) > 0:
                return df.sen.iloc[0]
        return None

    def get_voter_precinct_id(self, voter_id):
        address_id = self.get_residence_address_id_for_voter(voter_id)
        if address_id:
            df = pd.read_sql_query(f"select * from address_precinct_id where address_id={address_id}", self.con)
            if len(df) > 0:
                return df.precinct_id.iloc[0]
        return None

    def get_voter_names(self):
        return pd.read_sql_query(f"select * from voter_name", self.con)

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

    def get_voter_status(self):
        return pd.read_sql_query(f"select * from voter_status", self.con)

    def get_voter_demographics(self):
        return pd.read_sql_query(f"select * from voter_demographics", self.con)
