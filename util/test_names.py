import unittest
from util.names import NameNormalizer


class TestNameNormalizer(unittest.TestCase):
    def setUp(self):
        self.sut = NameNormalizer()

    def test_standardize_to_level_1_case_1(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_1('John Morris'))

    def test_standardize_to_level_1_case_2(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_1('John "jack" Morris'))

    def test_standardize_to_level_1_case_3(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_1('John (jack) Morris'))

    def test_standardize_to_level_1_case_4(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_1('John, Morris'))

    def test_standardize_to_level_1_case_5(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_1('John,Morris'))

    def test_standardize_to_level_1_case_6(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_1('John. Morris'))

    def test_standardize_to_level_1_case_7(self):
        self.assertEqual('JOHN', self.sut.standarize_name_to_level_1('John.'))

    def test_standardize_to_level_2_case_1(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_2('Dr. John Morris'))

    def test_standardize_to_level_2_case_2(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_2('DR John Morris'))

    def test_standardize_to_level_2_case_3(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_2('DR John Morris Sr.'))

    def test_standardize_to_level_2_case_4(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_2('Mr. John Morris Jr'))

    def test_standardize_to_level_2_case_5(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_2('Ms. John Morris II'))

    def test_standardize_to_level_2_case_6(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_2('Mrs. John Morris III'))

    def test_standardize_to_level_2_case_7(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_2('Senator John Morris IV'))

    def test_standardize_to_level_2_case_8(self):
        self.assertEqual('JOHN', self.sut.standarize_name_to_level_2('Senator John'))

    def test_standardize_to_level_2_case_9(self):
        self.assertEqual('MORRIS', self.sut.standarize_name_to_level_2('Morris IV'))

    def test_standardize_to_level_2_case_10(self):
        self.assertEqual('JOHN MORRIS', self.sut.standarize_name_to_level_2('Senator T. John Morris IV'))

    def test_standardize_to_level_3_case_1(self):
        self.assertEqual(('JOHN MORRIS',), self.sut.standarize_name_to_level_3('Senator John Morris IV'))

    def test_standardize_to_level_3_case_2(self):
        self.assertEqual(('JOHN MORRIS', 'KATE MORRIS'),
                         self.sut.standarize_name_to_level_3('Senator John and Kate Morris'))

    def test_standardize_to_level_3_case_3(self):
        self.assertEqual(('JOHN MORRIS', 'KATE MORRIS'),
                         self.sut.standarize_name_to_level_3('Senator John & Kate Morris'))

    def test_standardize_to_level_3_case_4(self):
        self.assertEqual(('JOHN', 'KATE'),
                         self.sut.standarize_name_to_level_3('John and Kate'))


if __name__ == '__main__':
    unittest.main()
