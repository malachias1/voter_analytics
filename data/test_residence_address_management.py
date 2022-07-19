import re
import unittest
import pandas as pd
from data.residence_address_management import ResidenceAddressManagement
from data.voter_list import VoterList
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()


class TestResidenceAddressManagement(unittest.TestCase):
    ROOT_DIR = '~/Documents/data'
    COUNTY_CODE = '036'
    DF = VoterList(ROOT_DIR).read_csv(COUNTY_CODE)

    def setUp(self):
        self.sut = ResidenceAddressManagement()

    def test_as_residence_address_key_no_apt_no_plus4(self):
        row = pd.Series(data=('033', '1234', 'HAPPY ST', '', 'EVANS', 'GA', '12345', ''))
        key = self.sut.as_address_key(row)
        self.assertEqual('1234 HAPPY ST; EVANS GA 12345', key)

    def test_as_residence_address_key_apt_no_plus4(self):
        row = pd.Series(data=('033', '1234', 'HAPPY ST', '12', 'EVANS', 'GA', '12345', ''))
        key = self.sut.as_address_key(row)
        self.assertEqual('1234 HAPPY ST #12; EVANS GA 12345', key)

    def test_as_residence_address_key_apt_plus4(self):
        row = pd.Series(data=('033', '1234', 'HAPPY ST', '12', 'EVANS', 'GA', '12345', '2020'))
        key = self.sut.as_address_key(row)
        self.assertEqual('1234 HAPPY ST #12; EVANS GA 12345-2020', key)

    def test_as_residence_address_key_apt_plus4_pos_1(self):
        row = pd.Series(data=('0', '033', '1234', 'HAPPY ST', '12', 'EVANS', 'GA', '12345', '2020'))
        key = self.sut.as_address_key(row, pos=1)
        self.assertEqual('1234 HAPPY ST #12; EVANS GA 12345-2020', key)

    def test_address_count(self):
        self.assertTrue(0 < self.sut.count)

    def test_voter_address_count(self):
        self.assertTrue(0 < self.sut.voter_address_count)

    def voter_address_content_test(self, addresses):
        sut = addresses.iloc[0]
        first_address_id = sut.address_id
        # Get first voter id with this address_id
        address_voter = self.sut.voter_address
        # There could be multiple voters at this address
        voter_id = address_voter[address_voter.address_id == first_address_id].voter_id.iat[0]
        # Get voter record
        standard = self.DF[self.DF.voter_id == voter_id].iloc[0]
        self.assertEqual(voter_id, standard.voter_id)
        self.assertEqual(sut.house_number, standard.residence_house_number)
        self.assertEqual(sut.street_name, standard.residence_street_name)
        self.assertEqual(sut.apt_no, standard.residence_apt_unit_nbr)
        self.assertEqual(sut.city, standard.residence_city)
        m = re.match(r'(\d{5})-?(\d{4})?', standard.residence_zipcode)
        zipcode = m.group(1) if m is not None else ''
        plus4 = m.group(2) if m is not None and m.group(2) is not None else ''
        self.assertEqual(sut.plus4, plus4)
        self.assertEqual(sut.zipcode, zipcode)

    def test_voter_addresses(self):
        """
        Make sure voter has residence address.
        """
        df = self.sut.voter_address
        voter_id = self.DF.voter_id.iloc[0]
        row = df[df.voter_id == voter_id]
        self.assertTrue(len(row.index) == 1, "Voter should have a residence address!")

    def test_get_address_for_voter_id(self):
        """
        Make sure voter has a valid linked address.
        """
        voter_with_apt_and_plus4 = self.DF[(self.DF.residence_apt_unit_nbr != '') &
                                           self.DF.residence_zipcode.str.contains(r'\d{5}-?\d{4}')]
        voter_id = voter_with_apt_and_plus4.voter_id.iloc[0]
        address = self.sut.get_address_for_voter_id(voter_id)
        self.voter_address_content_test(address)
