import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb
from pathlib import Path


class IngestCountyDetails(Pathes):
    COL_NAMES = ['county_name',
                 'county_code',
                 'county_fp'
                 ]
    COL_TYPES = {'county_name': str,
                 'county_code': str,
                 'county_fp': str
                 }

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    @property
    def con(self):
        return self.db.con

    def read_csv(self, path):
        df = pd.read_csv(Path(path).expanduser(), dtype=self.COL_TYPES)
        return df

    def ingest_county_details(self, path):
        df = self.read_csv(path)
        stmt = f'replace into county_details (county_name, county_code, county_fp) values (?, ?, ?)'
        cur = self.con.cursor()
        df.apply(lambda row: cur.execute(stmt, [row[i] for i in range(0, 3)]), axis=1)
        self.con.commit()
