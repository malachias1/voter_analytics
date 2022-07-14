import unittest
import pandas as pd
import numpy as np
from util.search import VoterMatch

import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()


class TestVoterMatch(unittest.TestCase):
    COLUMNS = ['zipcode', 'contact_name', 'first_name', 'last_name', 'street_address']
    TEST_RECORD = {'zipcode': '30022',
                   'contact_name': 'Kaaryn Walker',
                   'first_name': 'Kaaryn',
                   'last_name': 'Walker',
                   'street_address': '115 Weedon Ct'}

    def setUp(self):
        self.root_dir = '~/Documents/data'
        self.sut = VoterMatch()

    def test_name_isna(self):
        record = self.TEST_RECORD.copy()
        record['name'] = np.NaN
        df = pd.DataFrame(columns=self.COLUMNS, data=[record])
        df = self.sut.match_df(df, None)
        self.assertEqual('02543836', df.voter_id.iloc[0])

    def test_successful_search(self):
        df = pd.DataFrame(columns=self.COLUMNS, data=[self.TEST_RECORD])
        df = self.sut.match_df(df, None)
        self.assertEqual('02543836', df.voter_id.iloc[0])


if __name__ == '__main__':
    unittest.main()
