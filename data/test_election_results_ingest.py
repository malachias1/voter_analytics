import unittest
from pathlib import Path
import pandas as pd
import sqlite3 as sql
from data.election_results_ingest import IngestElectionResults
from data.voterdb import VoterDb


class TestIngestElectionResults(unittest.TestCase):
    def setUp(self):
        self.root_dir = '../test_resources'
        self.voter_db_path = Path('../test_resources/ga/voter.db').expanduser()
        self.voter_db_path.unlink(missing_ok=True)
        voter_db = VoterDb(self.root_dir)
        voter_db.initialize()
        self.sut = IngestElectionResults(self.root_dir)

    def test_read_xml(self):
        root = self.sut.read_xml('../test_resources/ga/politics/raw_elections/033/cobb_20220524.xml')
        contests = list(root.findall("Contest"))
        self.assertTrue(len(contests) > 0)

    def test_raw_tally(self):
        root = self.sut.read_xml('../test_resources/ga/politics/raw_elections/033/cobb_20220524.xml')
        df = self.sut.raw_tally(root)
        self.assertTrue(len(df.index) > 0)
