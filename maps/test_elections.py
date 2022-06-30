import unittest
from maps.elections import PrimarySummary


class TestMaps(unittest.TestCase):
    def setUp(self):
        self.root_dir = '~/Documents/data'
        self.sut = PrimarySummary(self.root_dir, '2022-05-24', category='us_house', subcategory='007')

    def test_voter_query(self):
        self.assertEqual("select voter_id from voter_cng where cng='007'", self.sut.voter_query.strip())

    def test_election_results(self):
        df = self.sut.election_results
        self.assertIsNotNone(df)
        self.assertTrue(len(df.index) > 0)

    def test_voters(self):
        df = self.sut.voters
        self.assertIsNotNone(df)
        self.assertTrue(len(df.index) > 0)

    def test_voter_precincts(self):
        df = self.sut.voter_precincts
        self.assertIsNotNone(df)
        self.assertTrue(len(df.index) > 0)

    def test_precincts(self):
        df = self.sut.precincts
        self.assertIsNotNone(df)
        self.assertTrue(len(df.index) > 0)

    def test_precinct_boundaries(self):
        df = self.sut.precinct_boundaries
        self.assertIsNotNone(df)
        self.assertTrue(len(df.index) > 0)

    def test_blockgroup_boundaries(self):
        df = self.sut.blockgroup_boundaries
        self.assertIsNotNone(df)
        self.assertTrue(len(df.index) > 0)

    def test_sync(self):
        df1 = self.sut.precincts[['county_code', 'precinct_id']]
        df2 = self.sut.boundaries[['county_code', 'precinct_id', 'precinct_name']]
        df3 = self.sut.election_results[['county_code', 'precinct_name']].drop_duplicates()
        df4 = df3.merge(df2, on=['county_code', 'precinct_name'], how='left')
        df5 = df4[df4.precinct_id.isna()]
        df6 = df5.merge(self.sut.election_results, on=['county_code', 'precinct_name'], how='left')
        print(df6[['contest', 'precinct_name', 'votes']])
