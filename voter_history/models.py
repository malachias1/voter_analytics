from django.db import models
from data.pathes import Pathes
from pathlib import Path
import pandas as pd


class VotingHistoryManager(models.Manager):
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

    def get_for(self, election_date, voter_ids):
        h_date = election_date.strftime('%Y%m%d')
        k = self.filter(h_date=h_date, voter_id__in=voter_ids)
        return pd.DataFrame.from_records([x.as_record for x in k])

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
        self.filter(h_year=year).delete()
        df = self.read_csv(year, root_dir=root_dir, alt_dir=alt_dir)
        records = []
        for i in df.index:
            row = df.loc[i]
            records.append(VoterHistory(
                voter_id=row.voter_id,
                h_year=row.h_year,
                h_date=row.h_date,
                type=row.type,
                party=row.party,
                county_code=row.county_code,
                absentee=row.absentee,
                provisional=row.provisional,
                supplemental=row.supplemental
            ))
        VoterHistory.objects.bulk_create(records, batch_size=10000)


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

    @property
    def as_record(self):
        return {
            'voter_id': self.voter_id,
            'h_year': self.h_year,
            'h_date': self.h_date,
            'type': self.type,
            'party': self.party,
            'county_code': self.county_code,
            'absentee': self.absentee,
            'provisional': self.provisional,
            'supplemental': self.supplemental,
        }

    class Meta:
        indexes = [
            models.Index(fields=['voter_id']),
            models.Index(fields=['h_year']),
            models.Index(fields=['h_date']),
        ]
        db_table = 'voter_history'

