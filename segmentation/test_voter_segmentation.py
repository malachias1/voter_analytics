import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

import numpy as np
import pandas as pd
from segmentation.voter_segmentation import VoterSegmentation
from hse_map.models import HseMap
from datetime import datetime
from voter.models import ListEdition


class TestVoterSegmentation(unittest.TestCase):
    VOTER_HISTORY_COLUMNS = ['voter_id', 'date', 'type', 'party',
                             'county_code', 'absentee', 'provisional',
                             'supplemental']
    D_20110520 = datetime.strptime('20110520', '%Y%m%d')
    D_20190520 = datetime.strptime('20190520', '%Y%m%d')
    D_20140520 = datetime.strptime('20140520', '%Y%m%d')
    D_20201103 = datetime.strptime('20201103', '%Y%m%d')
    SIMPLE_HISTORY = pd.DataFrame.from_records([
        ['01', D_20140520, 'P', 'R', '033', 0, 0, 0],
        ['01', D_20201103, 'G', 'G', '060', 0, 0, 0],
        ['02', D_20140520, 'P', 'D', '033', 0, 0, 0],
        ['03', D_20140520, 'P', 'D', '033', 0, 0, 0],
        ['02', D_20201103, 'G', 'G', '067', 0, 0, 0],
        ['04', D_20201103, 'G', 'G', '067', 0, 0, 0]
    ], columns=VOTER_HISTORY_COLUMNS)

    DATE_ADDED = pd.DataFrame.from_records([
        ['02', D_20110520],
        ['01', D_20110520],
        ['04', D_20190520]
    ], columns=['voter_id', 'date_added'])

    def setUp(self):
        edition_date = datetime.strptime('2022-08-05', '%Y-%m-%d')
        edition = ListEdition.objects.get(date=edition_date)
        self.hd51 = HseMap.objects.get(district='051')
        self.hd51.edition = edition
        self.sut = VoterSegmentation(self.hd51.voters)

    def test_get_county_info(self):
        df = self.sut.get_county_info(self.SIMPLE_HISTORY)
        self.assertEqual(4, len(df.index))
        self.assertEqual('060', df[df.voter_id == '01'].iloc[0, 1])
        self.assertEqual('067', df[df.voter_id == '02'].iloc[0, 1])
        self.assertEqual('033', df[df.voter_id == '03'].iloc[0, 1])

    def test_get_first_last(self):
        df = self.sut.get_first_last(self.SIMPLE_HISTORY)
        self.assertEqual(4, len(df.index))
        self.assertEqual(self.D_20140520, df[df.voter_id == '01'].iloc[0, 1])
        self.assertEqual(self.D_20201103, df[df.voter_id == '01'].iloc[0, 2])
        self.assertEqual(self.D_20140520, df[df.voter_id == '02'].iloc[0, 1])
        self.assertEqual(self.D_20201103, df[df.voter_id == '02'].iloc[0, 2])
        self.assertEqual(self.D_20140520, df[df.voter_id == '03'].iloc[0, 1])
        self.assertEqual(self.D_20140520, df[df.voter_id == '03'].iloc[0, 2])

    def test_add_missing_records(self):
        df = self.sut.add_missing_records(self.SIMPLE_HISTORY)
        self.assertEqual(8, len(df.index))
        self.assertEqual(2, len(df[df.voter_id == '01'].index))
        self.assertEqual(2, len(df[df.voter_id == '02'].index))
        self.assertEqual(2, len(df[df.voter_id == '03'].index))
        self.assertEqual(1, len(df[(df.voter_id == '03') & (df.date == self.D_20140520)].index))
        self.assertEqual(1, len(df[(df.voter_id == '03') & (df.date == self.D_20201103)].index))
        self.assertEqual('DP', df[(df.voter_id == '03') & (df.date == self.D_20140520)].reset_index().loc[0, 'party'])
        self.assertEqual('XG', df[(df.voter_id == '03') & (df.date == self.D_20201103)].reset_index().loc[0, 'party'])
        self.assertEqual('GG', df[(df.voter_id == '02') & (df.date == self.D_20201103)].reset_index().loc[0, 'party'])

    def test_pivot(self):
        df_first_last = self.sut.get_first_last(self.SIMPLE_HISTORY)
        df = self.sut.add_missing_records(self.SIMPLE_HISTORY)
        df = self.sut.pivot(df, df_first_last, self.DATE_ADDED)
        self.assertIsNotNone(df)
        self.assertEqual('GG', df.at[3, '2020-11-03'])
        self.assertTrue(np.isnan(df.at[3, '2014-05-20']))
        self.assertEqual('GG', df.at[3, '2020-11-03'])
        self.assertEqual('--', df.at[2, '2020-11-03'])
        print(df)

    def test_add_county(self):
        df_first_last = self.sut.get_first_last(self.SIMPLE_HISTORY)
        df_county = self.sut.get_county_info(self.SIMPLE_HISTORY)
        df = self.sut.add_missing_records(self.SIMPLE_HISTORY)
        df = self.sut.pivot(df, df_first_last, self.DATE_ADDED)
        df = self.sut.add_county(df, df_county)
        self.assertIsNotNone(df)
        self.assertEqual('060', df.at[0, 'county_code'])
        self.assertEqual('067', df.at[1, 'county_code'])
        self.assertEqual('033', df.at[2, 'county_code'])
        self.assertEqual('067', df.at[3, 'county_code'])

    def test_compute_ops(self):
        vhs = self.sut.history_summary()
        vhs = self.sut.compute_ops(vhs)
        self.assertIsNotNone(vhs)
        print(vhs)

    # def test_date_added(self):
    #     df = self.sut.get_date_added()
    #     self.assertIsNotNone(df)
    #     self.assertEqual(0, df.isna().sum().sum())

    # def test_history_for_election_date(self):
    #     df = self.sut.history_for_election_date('20180522', True)
    #     self.assertEqual(1, len(df.date.unique()))
    #     self.assertEqual(0, df.isna().sum().sum())
    #
    #
    # def test_history(self):
    #     df = self.sut.history()
    #     self.assertEqual(9, len(df.date.unique()))
    #     self.assertEqual(0, df.isna().sum().sum())
    #     print(df)


if __name__ == '__main__':
    unittest.main()
