import unittest
from pathlib import Path
import pandas as pd
from data.voterdb import VoterDb
import time
from random import randint
from data.voter_list_ingest import IngestVoterList
from segmentation.utils import categorize_age


class TestVoterDbContent(unittest.TestCase):
    def setUp(self):
        self.root_dir = '~/Documents/data'
        self.sut = VoterDb(self.root_dir)
        self.county_code = f'{randint(1, 150):03d}'
        ivl = IngestVoterList(self.root_dir)
        self.df = ivl.read_csv(self.county_code)

    def test_voter_name(self):
        n = randint(0, len(self.df.index))
        voter_id = self.df.voter_id.iloc[n]
        details = self.sut.get_voter_name_details(voter_id)
        self.assertEqual(self.df.voter_id.iloc[n], details.voter_id.iloc[0])
        self.assertEqual(self.df.last_name.iloc[n], details.last_name.iloc[0])
        self.assertEqual(self.df.first_name.iloc[n], details.first_name.iloc[0])
        self.assertEqual(self.df.middle_maiden_name.iloc[n], details.middle_name.iloc[0])
        self.assertEqual(self.df.name_suffix.iloc[n], details.name_suffix.iloc[0])
        self.assertEqual(self.df.name_title.iloc[n], details.name_title.iloc[0])

    def test_voter_demographics(self):
        n = randint(0, len(self.df.index))
        voter_id = self.df.voter_id.iloc[n]
        details = self.sut.get_voter_demographics_details(voter_id)
        self.assertEqual(self.df.voter_id.iloc[n], details.voter_id.iloc[0])
        self.assertEqual(self.df.race_id.iloc[n], details.race_id.iloc[0])
        self.assertEqual(self.df.gender.iloc[n], details.gender.iloc[0])
        self.assertEqual(int(self.df.year_of_birth.iloc[n]), details.year_of_birth.iloc[0])

    def test_precinct_summary(self):
        pdetails = self.sut.precinct_details
        pdetails = pdetails[pdetails.county_code == self.county_code]
        n = randint(0, len(pdetails.index) - 1)
        id = pdetails.id.iloc[n]
        precinct_id = pdetails.precinct_id.iloc[n]
        ps = self.sut.precinct_summary
        ps = ps[ps.precinct_id == id]
        df_p = self.df[self.df.precinct_id == precinct_id]
        df_p = df_p.assign(gen=categorize_age(df_p.year_of_birth))
        self.assertEqual(ps.total.iloc[0], ps.WH.iloc[0] + ps.BH.iloc[0] + ps.AP.iloc[0] + ps.AI.iloc[0] +
                         ps.OT.iloc[0] + ps.U.iloc[0] + ps.HP.iloc[0])
        self.assertEqual(ps.total.iloc[0], len(df_p.index))
        df_p_bh_f_gx = df_p[(df_p.race_id == 'BH') & (df_p.gender == 'F') & (df_p.gen == 'GX')]
        self.assertEqual(ps.BH_F_GX.iloc[0], len(df_p_bh_f_gx.index))
