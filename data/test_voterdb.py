import re
import unittest
import pandas as pd
from data.voterdb import VoterDb
from ingest.voter_list_ingest import IngestVoterList
from random import randint

import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()


class TestVoterDb(unittest.TestCase):
    ROOT_DIR = '~/Documents/data'
    COUNTY_CODE = '036'
    IVL = IngestVoterList(ROOT_DIR)
    DF = IVL.read_csv(COUNTY_CODE)

    def setUp(self):
        self.sut = VoterDb()

    def test_get_voter_names_not_empty(self):
        """
        Make sure get_voter_names is not empty.
        """

        df = self.sut.voter_names
        self.assertTrue(len(df.index) > 0)

    def test_get_voter_status_not_empty(self):
        """
        Make sure get_voter_status is not empty.
        """

        df = self.sut.voter_status
        self.assertTrue(len(df.index) > 0)

    def test_get_voter_demographics_not_empty(self):
        """
        Make sure get_voter_demographics is not empty.
        """

        df = self.sut.voter_demographics
        self.assertTrue(len(df.index) > 0)

    def test_get_address_voter_not_empty(self):
        """
        Make sure get_address_voter is not empty.
        """

        df = self.sut.voter_residence_address
        self.assertTrue(len(df.index) > 0)

    def test_get_mailing_address_voter_not_empty(self):
        """
        Make sure get_mailing_address_voter is not empty.
        """

        df = self.sut.mailing_address_voter
        self.assertTrue(len(df.index) > 0)

    # def test_address_voter_content_case_1(self):
    #     df = self.sut.voter_residence_address
    #     df = df[df.]
    #     voter_record = self.df.iloc[1]

    # def test_get_residence_addresses_content_case_1(self):
    #     """
    #     Make sure get_residence_addresses returns correct values.
    #     """
    #
    #     df = self.sut.get_residence_addresses_for_county(self.county_code)
    #     voter_record = self.df.iloc[1]
    #
    #     df_1 = df[df.address_id == 790155]
    #     self.assertEqual(790155, df.iloc[0, 0])
    #     self.assertEqual('036', df.iloc[0, 1])
    #     self.assertEqual('1426', df.iloc[0, 2])
    #     self.assertEqual('AYLESBURY DR', df.iloc[0, 3])
    #     self.assertTrue(df.iloc[0, 4].isna())
    #     self.assertEqual('EVANS', df.iloc[0, 5])
    #     self.assertEqual('GA', df.iloc[0, 6])
    #     self.assertEqual('30809', df.iloc[0, 7])
    #     self.assertTrue(df.iloc[0, 8].isna())
    #     self.assertEqual('1234 HAPPY ST; EVANS GA 12345', df.iloc[0, len(df.columns) - 1])
    #
    # def test_get_mailing_addresses_not_empty(self):
    #     """
    #     Make sure get_mailing_addresses returns correct values.
    #     """
    #
    #     df = self.sut.mailing_addresses
    #     self.assertEqual(1, len(df))
    #     self.fail('need to find a usable voter id')
    #     self.assertTrue('key' in df.columns)
    #     self.assertEqual(0, df.iloc[0, 0])
    #     self.assertEqual('1234', df.iloc[0, 1])
    #     self.assertEqual('HAPPY ST', df.iloc[0, 2])
    #     self.assertEqual('12', df.iloc[0, 3])
    #     self.assertEqual('EVANS', df.iloc[0, 4])
    #     self.assertEqual('GA', df.iloc[0, 5])
    #     self.assertEqual('12345', df.iloc[0, 6])
    #     self.assertEqual('2020', df.iloc[0, 7])
    #     self.assertEqual('A', df.iloc[0, 8])
    #     self.assertEqual('B', df.iloc[0, 9])
    #     self.assertEqual('USA', df.iloc[0, 10])
    #     self.assertEqual('1234 HAPPY ST #12; EVANS GA 12345-2020; A; B; USA', df.iloc[0, 11])
    #
    # def test_get_voter_names_not_empty(self):
    #     """
    #     Make sure get_voter_names returns correct values.
    #     """
    #
    #     df = self.sut.voter_names
    #     self.assertEqual(1, len(df))
    #     self.assertEqual('0', df.iloc[0, 0])
    #     self.assertEqual('BOGGS', df.iloc[0, 1])
    #     self.assertEqual('WILLIAM', df.iloc[0, 2])
    #     self.assertEqual('BENTLEY', df.iloc[0, 3])
    #     self.assertEqual('IV', df.iloc[0, 4])
    #     self.assertEqual('DR', df.iloc[0, 5])
    #
    # def test_get_voter_status_not_empty(self):
    #     """
    #     Make sure get_voter_status returns correct values.
    #     """
    #
    #     df = self.sut.voter_status
    #     self.assertEqual(1, len(df))
    #     self.assertEqual('0', df.iloc[0, 0])
    #     self.assertEqual('I', df.iloc[0, 1])
    #     self.assertEqual('NCOA', df.iloc[0, 2])
    #     self.assertEqual('20000101', df.iloc[0, 3])
    #     self.assertEqual('20210101', df.iloc[0, 4])
    #     self.assertEqual('20210102', df.iloc[0, 5])
    #     self.assertEqual('20100102', df.iloc[0, 6])
    #
    # def test_get_voter_demographics_not_empty(self):
    #     """
    #     Make sure get_voter_demographics returns correct values.
    #     """
    #
    #     df = self.sut.voter_demographics
    #     self.assertEqual(1, len(df))
    #     self.assertEqual('0', df.iloc[0, 0])
    #     self.assertEqual('WH', df.iloc[0, 1])
    #     self.assertEqual('F', df.iloc[0, 2])
    #     self.assertEqual(2000, df.iloc[0, 3])
    #
    # def test_get_address_voter_not_empty(self):
    #     """
    #     Make sure get_address_voter returns correct values.
    #     """
    #
    #     df = self.sut.voter_residence_address
    #     self.assertEqual(1, len(df))
    #     self.assertEqual(0, df.iloc[0, 1])
    #     self.assertEqual('1235', df.iloc[0, 0])
    #
    # def test_get_mailing_address_voter_not_empty(self):
    #     """
    #     Make sure get_mailing_address_voter returns correct values.
    #     """
    #
    #     df = self.sut.mailing_address_voter
    #     self.assertEqual(1, len(df))
    #     self.assertEqual(0, df.iloc[0, 1])
    #     self.assertEqual('1235', df.iloc[0, 0])

    # def test_insert_multiple_residence_address(self):
    #     df = pd.DataFrame()
    #     df = df.assign(address_id=[x for x in range(0, 5000)])
    #     df = df.assign(county_code=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(house_number=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(street_name=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(apt_no=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(city=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(state=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(zipcode=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(plus4=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     start = time.perf_counter()
    #     self.sut.insert_multiple_residence_address(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     ra = self.sut.residence_addresses
    #     self.assertEqual(5000, len(ra.index))
    #     self.assertEqual(df.address_id.iloc[13], ra.address_id.iloc[13])
    #     self.assertEqual(df.county_code.iloc[13], ra.county_code.iloc[13])
    #     self.assertEqual(df.house_number.iloc[13], ra.house_number.iloc[13])
    #     self.assertEqual(df.street_name.iloc[13], ra.street_name.iloc[13])
    #     self.assertEqual(df.apt_no.iloc[13], ra.apt_no.iloc[13])
    #     self.assertEqual(df.city.iloc[13], ra.city.iloc[13])
    #     self.assertEqual(df.state.iloc[13], ra.state.iloc[13])
    #     self.assertEqual(df.zipcode.iloc[13], ra.zipcode.iloc[13])
    #     self.assertEqual(df.plus4.iloc[13], ra.plus4.iloc[13])

    # def test_insert_multiple_mailing_address(self):
    #     df = pd.DataFrame()
    #     df = df.assign(address_id=[x for x in range(0, 5000)])
    #     df = df.assign(house_number=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(street_name=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(apt_no=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(city=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(state=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(zipcode=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(plus4=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(address_line2=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(address_line3=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(country=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     start = time.perf_counter()
    #     self.sut.insert_multiple_mailing_address(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     ma = self.sut.mailing_addresses
    #     self.assertEqual(5000, len(ma.index))
    #     self.assertEqual(df.address_id.iloc[13], ma.address_id.iloc[13])
    #     self.assertEqual(df.house_number.iloc[13], ma.house_number.iloc[13])
    #     self.assertEqual(df.street_name.iloc[13], ma.street_name.iloc[13])
    #     self.assertEqual(df.apt_no.iloc[13], ma.apt_no.iloc[13])
    #     self.assertEqual(df.city.iloc[13], ma.city.iloc[13])
    #     self.assertEqual(df.state.iloc[13], ma.state.iloc[13])
    #     self.assertEqual(df.zipcode.iloc[13], ma.zipcode.iloc[13])
    #     self.assertEqual(df.plus4.iloc[13], ma.plus4.iloc[13])
    #     self.assertEqual(df.address_line2.iloc[13], ma.address_line2.iloc[13])
    #     self.assertEqual(df.address_line3.iloc[13], ma.address_line3.iloc[13])
    #     self.assertEqual(df.country.iloc[13], ma.country.iloc[13])
    #
    # def test_replace_multiple_precinct_details(self):
    #     df = pd.DataFrame()
    #     df = df.assign(id=[x for x in range(0, 5000)])
    #     df = df.assign(precinct_id=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(county_code='033')
    #     start = time.perf_counter()
    #     self.sut.replace_multiple_precinct_details(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     df1 = self.sut.precinct_details
    #     self.assertEqual(5000, len(df1.index))
    #     self.assertEqual(df.id.iloc[13], df1.id.iloc[13])
    #     self.assertEqual(df.precinct_id.iloc[13], df1.precinct_id.iloc[13])
    #
    # def test_replace_multiple_voter_cng(self):
    #     df = pd.DataFrame()
    #     df = df.assign(voter_id=[str(x) for x in range(0, 5000)])
    #     df = df.assign(cng=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     start = time.perf_counter()
    #     self.sut.replace_multiple_voter_cng(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     vc = self.sut.voter_cng
    #     self.assertEqual(5000, len(vc.index))
    #     self.assertEqual(df.voter_id.iloc[13], vc.voter_id.iloc[13])
    #     self.assertEqual(df.cng.iloc[13], vc.cng.iloc[13])
    #
    # def test_replace_multiple_voter_demographics(self):
    #     df = pd.DataFrame()
    #     df = df.assign(voter_id=[str(x) for x in range(0, 5000)])
    #     df = df.assign(race_id=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(gender=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(year_of_birth=[randint(1925, 2001) for _ in range(0, 5000)])
    #     start = time.perf_counter()
    #     self.sut.replace_multiple_voter_demographics(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     vd = self.sut.voter_demographics
    #     self.assertEqual(5000, len(vd.index))
    #     self.assertEqual(df.voter_id.iloc[13], vd.voter_id.iloc[13])
    #     self.assertEqual(df.race_id.iloc[13], vd.race_id.iloc[13])
    #     self.assertEqual(df.gender.iloc[13], vd.gender.iloc[13])
    #     self.assertEqual(df.year_of_birth.iloc[13], vd.year_of_birth.iloc[13])
    #
    # def test_replace_multiple_voter_hse(self):
    #     df = pd.DataFrame()
    #     df = df.assign(voter_id=[str(x) for x in range(0, 5000)])
    #     df = df.assign(hse=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     start = time.perf_counter()
    #     self.sut.replace_multiple_voter_hse(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     vc = self.sut.voter_hse
    #     self.assertEqual(5000, len(vc.index))
    #     self.assertEqual(df.voter_id.iloc[13], vc.voter_id.iloc[13])
    #     self.assertEqual(df.hse.iloc[13], vc.hse.iloc[13])
    #
    # def test_replace_multiple_voter_name(self):
    #     df = pd.DataFrame()
    #     df = df.assign(voter_id=[str(x) for x in range(0, 5000)])
    #     df = df.assign(last_name=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(first_name=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(middle_name=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(name_suffix=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(name_title=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     start = time.perf_counter()
    #     self.sut.replace_multiple_voter_name(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     vn = self.sut.voter_names
    #     self.assertEqual(5000, len(vn.index))
    #     self.assertEqual(df.voter_id.iloc[13], vn.voter_id.iloc[13])
    #     self.assertEqual(df.last_name.iloc[13], vn.last_name.iloc[13])
    #     self.assertEqual(df.first_name.iloc[13], vn.first_name.iloc[13])
    #     self.assertEqual(df.middle_name.iloc[13], vn.middle_name.iloc[13])
    #     self.assertEqual(df.name_suffix.iloc[13], vn.name_suffix.iloc[13])
    #     self.assertEqual(df.name_title.iloc[13], vn.name_title.iloc[13])
    #
    # def test_replace_multiple_voter_mailing_address(self):
    #     df = pd.DataFrame()
    #     df = df.assign(voter_id=[str(x) for x in range(0, 5000)])
    #     df = df.assign(address_id=[randint(0, 3) for _ in range(0, 5000)])
    #     start = time.perf_counter()
    #     self.sut.replace_multiple_voter_mailing_address(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     vm = self.sut.voter_mailing_address
    #     self.assertEqual(5000, len(vm.index))
    #     self.assertEqual(df.voter_id.iloc[13], vm.voter_id.iloc[13])
    #     self.assertEqual(df.address_id.iloc[13], vm.address_id.iloc[13])
    #
    # def test_replace_multiple_voter_precinct(self):
    #     df = pd.DataFrame()
    #     df = df.assign(voter_id=[str(x) for x in range(0, 5000)])
    #     df = df.assign(precinct_id=randint(0, 3))
    #     start = time.perf_counter()
    #     self.sut.replace_multiple_voter_precinct(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     vp = self.sut.voter_precinct
    #     self.assertEqual(5000, len(vp.index))
    #     self.assertEqual(df.voter_id.iloc[13], vp.voter_id.iloc[13])
    #     self.assertEqual(df.precinct_id.iloc[13], vp.precinct_id.iloc[13])
    #
    # def test_replace_multiple_voter_residence_address(self):
    #     df = pd.DataFrame()
    #     df = df.assign(voter_id=[str(x) for x in range(0, 5000)])
    #     df = df.assign(address_id=[randint(0, 3) for _ in range(0, 5000)])
    #     start = time.perf_counter()
    #     self.sut.replace_multiple_voter_residence_address(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     vr = self.sut.voter_residence_address
    #     self.assertEqual(5000, len(vr.index))
    #     self.assertEqual(df.voter_id.iloc[13], vr.voter_id.iloc[13])
    #     self.assertEqual(df.address_id.iloc[13], vr.address_id.iloc[13])
    #
    # def test_replace_multiple_voter_sen(self):
    #     df = pd.DataFrame()
    #     df = df.assign(voter_id=[str(x) for x in range(0, 5000)])
    #     df = df.assign(sen=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     start = time.perf_counter()
    #     self.sut.replace_multiple_voter_sen(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     vs = self.sut.voter_sen
    #     self.assertEqual(5000, len(vs.index))
    #     self.assertEqual(df.voter_id.iloc[13], vs.voter_id.iloc[13])
    #     self.assertEqual(df.sen.iloc[13], vs.sen.iloc[13])
    #
    # def test_replace_multiple_voter_status(self):
    #     df = pd.DataFrame()
    #     df = df.assign(voter_id=[str(x) for x in range(0, 5000)])
    #     df = df.assign(status=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(status_reason=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(date_added=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(date_changed=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(registration_date=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     df = df.assign(last_contact_date=[str(randint(0, 3)) for _ in range(0, 5000)])
    #     start = time.perf_counter()
    #     self.sut.replace_multiple_voter_status(df)
    #     self.assertTrue(0.1 > time.perf_counter() - start)
    #     vs = self.sut.voter_status
    #     self.assertEqual(5000, len(vs.index))
    #     self.assertEqual(df.voter_id.iloc[13], vs.voter_id.iloc[13])
    #     self.assertEqual(df.status.iloc[13], vs.status.iloc[13])
    #     self.assertEqual(df.status_reason.iloc[13], vs.status_reason.iloc[13])
    #     self.assertEqual(df.date_added.iloc[13], vs.date_added.iloc[13])
    #     self.assertEqual(df.date_changed.iloc[13], vs.date_changed.iloc[13])
    #     self.assertEqual(df.registration_date.iloc[13], vs.registration_date.iloc[13])
    #     self.assertEqual(df.last_contact_date.iloc[13], vs.last_contact_date.iloc[13])
