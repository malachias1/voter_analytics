import unittest
from util.addresses import StreetNameNormalizer


class TestAddresses(unittest.TestCase):
    def setUp(self):
        self.sut = StreetNameNormalizer()

    def test_normalize_case_1(self):
        self.assertEqual('LONOX PT NE', self.sut.normalize('Lonox Point N.E.'))

    def test_normalize_case_2(self):
        self.assertEqual('WILLOW BROOK DR', self.sut.normalize('Willow Brook Drive'))

    def test_normalize_case_3(self):
        self.assertEqual('WILLOW BROOK DR NE', self.sut.normalize('Willow Brook Drive N.E.'))

    def test_normalize_case_4(self):
        self.assertEqual('WILLOW', self.sut.normalize('Willow'))

    def test_normalize_case_5(self):
        self.assertEqual('WILLOW NE', self.sut.normalize('Willow N.E.'))

    def test_normalize_case_6(self):
        self.assertEqual('NE', self.sut.normalize('N.E.'))

    def test_is_street_address_case_1(self):
        self.assertTrue(self.sut.is_street_address('123 Aylesbury Dr'))

    def test_is_street_address_case_2(self):
        self.assertFalse(self.sut.is_street_address('P.O. Box 4'))


if __name__ == '__main__':
    unittest.main()
