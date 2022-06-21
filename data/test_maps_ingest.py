import unittest
import pandas as pd
from data.voterdb import VoterDb
from data.maps_ingest import IngestUSHouseMaps, IngestGAHouseMaps, IngestGASenateMaps, IngestVTDMaps, IngestCountyMaps


class TestIngestMaps(unittest.TestCase):
    def setUp(self):
        self.root_dir = '../test_resources'
        self.db = VoterDb(self.root_dir)

    def test_read_cng_map(self):
        sut = IngestUSHouseMaps(self.root_dir)
        dmaps = sut.read_maps()
        self.assertEqual(14, len(dmaps.index))

    def test_ingest_cng_map(self):
        sut = IngestUSHouseMaps(self.root_dir)
        sut.ingest()
        dmaps = pd.read_sql_query(f'select * from {sut.tablename}', self.db.con)
        self.assertEqual(14, len(dmaps.index))

    def test_read_hse_map(self):
        sut = IngestGAHouseMaps(self.root_dir)
        dmaps = sut.read_maps()
        self.assertEqual(180, len(dmaps.index))

    def test_ingest_hse_map(self):
        sut = IngestGAHouseMaps(self.root_dir)
        sut.ingest()
        dmaps = pd.read_sql_query(f'select * from {sut.tablename}', self.db.con)
        self.assertEqual(180, len(dmaps.index))

    def test_read_sen_map(self):
        sut = IngestGASenateMaps(self.root_dir)
        dmaps = sut.read_maps()
        self.assertEqual(56, len(dmaps.index))

    def test_ingest_sen_map(self):
        sut = IngestGASenateMaps(self.root_dir)
        sut.ingest()
        dmaps = pd.read_sql_query(f'select * from {sut.tablename}', self.db.con)
        self.assertEqual(56, len(dmaps.index))

    def test_read_vtd_map(self):
        sut = IngestVTDMaps(self.root_dir)
        dmaps = sut.read_maps()
        self.assertEqual(2654, len(dmaps.index))

    def test_ingest_vtd_map(self):
        sut = IngestVTDMaps(self.root_dir)
        sut.ingest()
        dmaps = pd.read_sql_query(f'select * from {sut.tablename}', self.db.con)
        self.assertEqual(2654, len(dmaps.index))

    def test_read_county_map(self):
        sut = IngestCountyMaps(self.root_dir)
        dmaps = sut.read_maps()
        self.assertEqual(159, len(dmaps.index))

    def test_ingest_county_map(self):
        sut = IngestCountyMaps(self.root_dir)
        sut.ingest()
        dmaps = pd.read_sql_query(f'select * from {sut.tablename}', self.db.con)
        self.assertEqual(159, len(dmaps.index))


if __name__ == '__main__':
    unittest.main()
