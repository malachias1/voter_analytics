import unittest
from pathlib import Path
import pandas as pd
import sqlite3 as sql
from data.election_contest_identifier import ElectionContestIdentifer
from data.voterdb import VoterDb
from elections.elections import Elections


class TestElectionContestIdentifier(unittest.TestCase):
    def setUp(self):
        self.root_dir = '~/Documents/data'
        voter_db = VoterDb(self.root_dir)
        self.sut = ElectionContestIdentifer()

    def test_classify(self):
        e = Elections(self.root_dir)
        df = e.get_results('2022-05-24')
        contests = df.contest.unique()
        for c in contests:
            self.sut.classify(c)


if __name__ == '__main__':
    unittest.main()
