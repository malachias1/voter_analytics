import unittest
from pathlib import Path
from voter_history_ingest import IngestVoterHistory
from data.voterdb import VoterDb


class TestVoterHistoryIngest(unittest.TestCase):
    @property
    def con(self):
        return self.voter_db.con

    def setUp(self):
        self.root_dir = '../test_resources'
        self.voter_db_path = Path('../test_resources/ga/voter.db').expanduser()
        self.voter_db_path.unlink(missing_ok=True)
        self.voter_db = VoterDb(self.root_dir)
        self.voter_db.initialize()
        self.sut = IngestVoterHistory(self.root_dir)

    def test_ingest_voter_history_year_check_count(self):
        with Path('../test_resources/ga/politics/voter_history/2022.txt').open('r') as f:
            x = f.readlines()
        self.sut.ingest_voter_history_year(2022)
        cur = self.con.cursor()
        n = cur.execute('SELECT count(*) FROM voter_history;').fetchone()[0]
        self.assertEqual(len(list(x)), n)

    def test_ingest_voter_history_year_check_values(self):
        self.sut.ingest_voter_history_year(2022)
        cur = self.con.cursor()
        result = cur.execute('SELECT voter_id, date, type, party, county_id, absentee, provisional, supplemental '
                             ' FROM voter_history;').fetchone()
        self.assertEqual('101', result[4])
        self.assertEqual('10453093', result[0])
        self.assertEqual('20220524', result[1])
        self.assertEqual('005', result[2])
        self.assertEqual(None, result[3])
        self.assertEqual(True, result[5])
        self.assertEqual(False, result[6])
        self.assertEqual(False, result[7])


if __name__ == '__main__':
    unittest.main()
