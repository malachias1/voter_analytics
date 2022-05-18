import unittest
import pandas as pd
import numpy as np
from util.search import VoterMatch


class TestVoterMatch(unittest.TestCase):
    COLUMNS = ['zipcode', 'name', 'first_name', 'last_name', 'street_address']
    TEST_RECORD = {'zipcode': '30022',
                   'name': 'Kaaryn Walker',
                   'first_name': 'Kaaryn',
                   'last_name': 'Walker',
                   'street_address': '115 Weedon Ct'}

    def setUp(self):
        self.root_dir = '~/Documents/data'
        self.sut = VoterMatch(self.root_dir)

    def test_name_isna(self):
        record = self.TEST_RECORD.copy()
        record['name'] = np.NaN
        df = pd.DataFrame(columns=self.COLUMNS, data=[record])
        m, u = self.sut.match(df)
        self.assertEqual(0, len(m))
        self.assertEqual(1, len(u))

    def test_successful_search(self):
        df = pd.DataFrame(columns=self.COLUMNS, data=[self.TEST_RECORD])
        m, u = self.sut.match(df)
        print(m)
        print(u)
        self.assertEqual(1, len(m))
        self.assertEqual(0, len(u))


if __name__ == '__main__':
    unittest.main()
