import unittest
from pathlib import Path
import pandas as pd
import sqlite3 as sql
from segmentation.voter_segmentation import VoterSegmentation
from data.voterdb import VoterDb


class TestVoterSegmentation(unittest.TestCase):
    def setUp(self):
        self.root_dir = '~/Documents/data'
        self.sut = VoterSegmentation(self.root_dir)

    def test_history_for_election_date(self):
        df = self.sut.history_for_election_date('20180522', True)
        self.assertEqual(1, len(df.date.unique()))
        self.assertEqual(0, df.isna().sum().sum())

    def test_history(self):
        df = self.sut.history()
        # self.assertEqual(9, len(df.date.unique()))
        # self.assertEqual(0, df.isna().sum().sum())
        print(df)


if __name__ == '__main__':
    unittest.main()
