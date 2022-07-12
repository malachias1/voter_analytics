import re
import unittest
import pandas as pd
from data.mailing_address_management import MailingAddressManagement
from data.voter_list import VoterList
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()


class TestMailingAddressManagement(unittest.TestCase):
    ROOT_DIR = '~/Documents/data'
    COUNTY_CODE = '036'
    DF = VoterList(ROOT_DIR).read_csv(COUNTY_CODE)

    def setUp(self):
        self.sut = MailingAddressManagement()

    def test_as_mailing_address_key_apt_plus4(self):
        row = pd.Series(data=('1234', 'HAPPY ST', '12', 'EVANS', 'GA', '12345', '2020', 'A', 'B', 'USA'))
        key = self.sut.as_address_key(row)
        self.assertEqual('1234 HAPPY ST #12; EVANS GA 12345-2020; A; B; USA', key)

    def test_address_count(self):
        self.assertTrue(0 < self.sut.address_count)

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
        self.assertEqual(sut.house_number, standard.mail_house_nbr)
        self.assertEqual(sut.street_name, standard.mail_street_name)
        self.assertEqual(sut.apt_no, standard.mail_apt_unit_nbr)
        self.assertEqual(sut.city, standard.mail_city)
        m = re.match(r'(\d{5})-?(\d{4})?', standard.mail_zipcode)
        zipcode = m.group(1) if m is not None else ''
        plus4 = m.group(2) if m is not None and m.group(2) is not None else ''
        self.assertEqual(sut.plus4, plus4)
        self.assertEqual(sut.zipcode, zipcode)
        self.assertEqual(sut.address_line2, standard.mail_address_2)
        self.assertEqual(sut.address_line3, standard.mail_address_3)
        self.assertEqual(sut.country, standard.mail_country)

    def test_voter_addresses(self):
        """
        Make sure voter with no mailing address
        (if different from residence) has no mailing address
        data.
        """
        df = self.sut.voter_address
        no_mailing_addresses = self.DF[self.DF.mail_house_nbr == '']
        voter_id = no_mailing_addresses.voter_id.iloc[0]
        row = df[df.voter_id == voter_id]
        self.assertTrue(len(row.index) == 0, "Voter should not have a mailing address!")

    def test_get_address_for_voter_id(self):
        """
        Make sure voter has a valid linked address.
        """
        has_mailing_addresses = self.DF[(self.DF.mail_apt_unit_nbr != '') &
                                        self.DF.mail_zipcode.str.contains(r'\d{5}-?\d{4}')]
        voter_id = has_mailing_addresses.voter_id.iloc[0]
        address = self.sut.get_address_for_voter_id(voter_id)
        self.voter_address_content_test(address)
