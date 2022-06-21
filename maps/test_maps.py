import unittest
from data.voterdb import VoterDb
from data.maps_ingest import IngestUSHouseMaps, IngestGAHouseMaps, IngestGASenateMaps
from maps.district_maps import CngMap, HseMap, SenMap


class TestMaps(unittest.TestCase):
    def setUp(self):
        self.root_dir = '../test_resources'
        self.db = VoterDb(self.root_dir)
        IngestUSHouseMaps(self.root_dir).ingest()
        IngestGAHouseMaps(self.root_dir).ingest()
        IngestGASenateMaps(self.root_dir).ingest()

    def test_cng_maps(self):
        sut = CngMap(self.root_dir)
        x = sut.maps
        self.assertEqual(14, len(x))

    def test_hse_maps(self):
        sut = HseMap(self.root_dir)
        x = sut.maps
        self.assertEqual(180, len(x))

    def test_sen_maps(self):
        sut = SenMap(self.root_dir)
        x = sut.maps
        self.assertEqual(56, len(x))


if __name__ == '__main__':
    unittest.main()
