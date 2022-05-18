import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb


class IngestVoterHistory(Pathes):
    FIELD_WIDTH = [3, 8, 8, 3, 2, 1, 1, 1]
    COL_NAMES = ['county_id',
                 'voter_id',
                 'date',
                 'type',
                 'party',
                 'absentee',
                 'provisional',
                 'supplemental'
                 ]
    COL_TYPES = {'county_id': str,
                 'voter_id': str,
                 'date': str,
                 'type': str,
                 'party': str,
                 'absentee': bool,
                 'provisional': bool,
                 'supplemental': bool
                 }

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    @property
    def con(self):
        return self.db.con

    def read_csv(self, year):
        p = self.voter_history_path(year)
        return pd.read_fwf(p, widths=self.FIELD_WIDTH, header=None, names=self.COL_NAMES,
                           dtype=self.COL_TYPES, true_values=['Y'], false_values=['N'])

    def ingest_voter_history_year(self, year):
        df = self.read_csv(year)
        stmt = f'replace into voter_history (county_id, voter_id, date, type, party, ' \
               f'absentee, provisional, supplemental) values (?, ?, ?, ?, ?, ?, ?, ?)'
        cur = self.con.cursor()
        df.apply(lambda row: cur.execute(stmt, [row[i] for i in range(0, 8)]), axis=1)
        self.con.commit()

    def ingest_voter_history(self):
        for p in self.voter_history_dir.glob(r'\d\d\d\d.txt'):
            self.ingest_voter_history_year(int(p.name[:4]))
