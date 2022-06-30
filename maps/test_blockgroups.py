import unittest
from maps.blockgroups import BlockGroupMaps, MedianIncome


class TestBlockGroupMaps(unittest.TestCase):
    def setUp(self):
        self.root_dir = '~/Documents/data'
        self.sut = BlockGroupMaps(self.root_dir)

    def test_get_blockgroup_boundaries(self):
        gdf = self.sut.get_blockgroup_boundaries(['033'])
        self.assertTrue(len(gdf.index) > 0)


class TestMedianIncome(unittest.TestCase):
    def setUp(self):
        self.root_dir = '~/Documents/data'
        self.sut = MedianIncome(self.root_dir)

    def test_get_blockgroup_boundaries(self):
        gdf = self.sut.get_median_income(['033'])
        self.assertTrue(len(gdf.index) > 0)


if __name__ == '__main__':
    unittest.main()
