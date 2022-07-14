import pandas as pd
from data.voterdb import VoterDb


class AddressManagementBase(VoterDb):
    def __init__(self, address_table, address_voter_table):
        super().__init__()
        self.address_table = address_table
        self.address_voter_table = address_voter_table

    @property
    def address_count(self):
        result = self.fetchone(f"select count(*) from {self.address_table}")
        return result[0]

    @property
    def addresses(self):
        raise NotImplemented('addresses property is not implemented!')

    @property
    def columns(self):
        raise NotImplemented('columns property is not implemented!')

    @property
    def next_address_id(self):
        result = self.fetchone(f"select max(address_id) from {self.address_table}")
        if result[0] is not None:
            return result[0] + 1
        return 0

    @property
    def voter_address_count(self):
        result = self.fetchone(f"select count(*) from {self.address_voter_table}")
        return result[0]

    @property
    def voter_address(self):
        results = self.fetchall(f"""
            select voter_id, address_id
                from {self.address_voter_table}
        """)
        return pd.DataFrame.from_records(results, columns=[
            'voter_id', 'address_id'
        ])

    @classmethod
    def add_address_key(cls, df):
        pos = 1 if 'address_id' in df.columns else 0
        if len(df) > 0:
            df = df.assign(key=df.apply(cls.as_address_key, axis=1, pos=pos))
        else:
            columns = list(df.columns)
            columns.append('key')
            df = pd.DataFrame(columns=columns)
        return df

    @classmethod
    def as_address_key(cls, row, pos=0):
        raise NotImplemented('as_address_key property is not implemented!')

    def from_records(self, df):
        df = pd.DataFrame.from_records(df, columns=self.columns)
        return self.add_address_key(df)

    def get_address(cls, address_id):
        raise NotImplemented('get_address method is not implemented!')

    def get_address_for_voter_id(self, voter_id):
        result = self.fetchone(f"""
            select address_id from {self.address_voter_table} where voter_id='{voter_id}'
        """)
        if result[0] is None:
            return None
        return self.get_address(result[0])

    def truncate(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(f"""
                    truncate {self.address_voter_table}, {self.address_table};
                """)
