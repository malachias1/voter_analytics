from django.db import models
from data.voterdb import VoterDb
from data.pathes import Pathes
from psycopg2.extras import execute_values
from pathlib import Path
import pandas as pd


class VotingHistoryManager(models.Manager):
    @property
    def db(self):
        try:
            return self.db_
        except AttributeError:
            self.db_ = VoterDb()
        return self.db_

    def dates(self):
        return [x.h_date for x in self.distinct('h_date')]

    def years(self):
        return [x.h_date for x in self.distinct('h_year')]

    def get_history(self, county_code, date):
        results = [(x.voter_id, x.h_date, x.type, x.party,
                    x.absentee, x.provisional, x.supplemental)
                   for x in self.filter(county_code=county_code, h_date=date)]

        return pd.DataFrame.from_records(results, columns=['voter_id', 'date', 'type',
                                                           'party', 'absentee',
                                                           'provisional', 'supplemental'])

    # -------------------------------------------------------------------------
    # Ingest and Update Methods
    # -------------------------------------------------------------------------

    FIELD_WIDTH = [3, 8, 8, 3, 2, 1, 1, 1]
    COL_NAMES = ['county_code',
                 'voter_id',
                 'date',
                 'type',
                 'party',
                 'absentee',
                 'provisional',
                 'supplemental'
                 ]
    COL_TYPES = {'county_code': str,
                 'voter_id': str,
                 'date': str,
                 'type': str,
                 'party': str,
                 'absentee': bool,
                 'provisional': bool,
                 'supplemental': bool
                 }

    def delete_year(self, year):
        with self.db.con:
            with self.db.cursor() as cur:
                cur.execute(f"""
                    delete from voter_history where h_year = {year}
                """)

    def read_csv(self, year, root_dir=None, alt_dir=None):
        pathes = Pathes(root_dir)
        if alt_dir is not None:
            p = Path(alt_dir, f'{year}.TXT').expanduser()
        else:
            p = pathes.voter_history_path(year)
        df = pd.read_fwf(p, widths=self.FIELD_WIDTH, header=None, names=self.COL_NAMES,
                         dtype=self.COL_TYPES, true_values=['Y'], false_values=['N'])
        df.loc[df.type.isna(), 'type'] = '999'
        df = df.assign(year=year)
        return df

    def ingest_year(self, year, root_dir=None, alt_dir=None):
        self.delete_year(year)
        df = self.read_csv(year, root_dir=root_dir, alt_dir=alt_dir)
        with self.db.con:
            with self.db.cursor() as cur:
                execute_values(cur, f"""
                      insert into voter_history (voter_id, h_year, h_date, type, party, 
                        county_code, absentee, provisional, supplemental)
                      values %s
                    """, df[['voter_id', 'year', 'date',
                             'type', 'party', 'county_code',
                             'absentee', 'provisional', 'supplemental']].to_records(index=False))


class VoterHistory(models.Model):
    voter_id = models.TextField()
    h_year = models.IntegerField()
    h_date = models.TextField()
    type = models.TextField()
    party = models.TextField(blank=True, null=True)
    county_code = models.TextField()
    absentee = models.BooleanField()
    provisional = models.BooleanField()
    supplemental = models.BooleanField()

    objects = VotingHistoryManager()

    class Meta:
        indexes = [
            models.Index(fields=['voter_id']),
            models.Index(fields=['h_year']),
            models.Index(fields=['h_date']),
        ]
        db_table = 'voter_history'

