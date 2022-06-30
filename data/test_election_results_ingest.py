import unittest
from pathlib import Path
from data.election_results_ingest import IngestElectionResults
from data.voterdb import VoterDb
import xml.etree.ElementTree as Et


class TestIngestElectionResults(unittest.TestCase):
    def setUp(self):
        self.root_dir = '../test_resources'
        self.voter_db_path = Path('../test_resources/ga/voter.db').expanduser()
        self.voter_db_path.unlink(missing_ok=True)
        self.db = VoterDb(self.root_dir)
        self.db.initialize()
        self.sut = IngestElectionResults(self.root_dir)

    def test_ingest(self):
        self.sut.ingest('../test_resources/ga/politics/elections')
        df = self.db.election_result_details
        self.assertEqual(57, len(df.contest.unique()))

    def test_count(self):
        self.sut.ingest('../test_resources/ga/politics/elections')
        df1 = self.db.election_result_details
        df2 = self.db.over_under_votes
        self.assertEqual(6846, len(df1.index)+len(df2.index))

    def test_votes_details(self):
        self.sut.ingest('../test_resources/ga/politics/elections')
        df = self.db.election_result_details
        df = df[df.contest == 'REP - US SENATE']
        df = df[df.choice == 'GARY W. BLACK']
        df = df[df.vote_type == 'E']
        df = df[df.precinct_name == 'HINTON']
        self.assertEqual(1, len(df.index))
        self.assertEqual(40, df.votes.iloc[0])

    def test_votes(self):
        self.sut.ingest('../test_resources/ga/politics/elections')
        df = self.db.election_results
        df = df[df.contest == 'REP - US SENATE']
        df = df[df.choice == 'GARY W. BLACK']
        df = df[df.precinct_name == 'HINTON']
        self.assertEqual(1, len(df.index))
        self.assertEqual(45, df.votes.iloc[0])

    def test_rebuild(self):
        self.sut.ingest('../test_resources/ga/politics/elections')
        df = self.db.election_results
        df = df[df.contest == 'REP - US SENATE']
        df = df[df.choice == 'GARY W. BLACK']
        df = df[df.precinct_name == 'HINTON']
        self.assertEqual(1, len(df.index))
        self.assertEqual(45, df.votes.iloc[0])



