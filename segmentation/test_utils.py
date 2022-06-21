import unittest
import pandas as pd
from segmentation.utils import categorize_age


class TestUtils(unittest.TestCase):
    def test_categorize_age(self):
        years = pd.Series([1933, 1955, 1968, 1983, 2001, 0, 1946, 1965, 1981, 1997, 1945, 1964, 1980, 1996])
        gen = categorize_age(years)
        self.assertEqual('S', gen[0])
        self.assertEqual('B', gen[1])
        self.assertEqual('GX', gen[2])
        self.assertEqual('M', gen[3])
        self.assertEqual('GZ', gen[4])
        self.assertEqual('B', gen[6])
        self.assertEqual('GX', gen[7])
        self.assertEqual('M', gen[8])
        self.assertEqual('GZ', gen[9])
        self.assertEqual('S', gen[10])
        self.assertEqual('B', gen[11])
        self.assertEqual('GX', gen[12])
        self.assertEqual('M', gen[13])


if __name__ == '__main__':
    unittest.main()
