import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from hse_map.models import HseMap
from core.models import PartyTallyMapConfig, MapConfig
from datetime import datetime
from voter.models import ListEdition


class HseMapTestCase(unittest.TestCase):
    def setUp(self) -> None:
        edition_date = datetime.strptime('2022-08-05', '%Y-%m-%d')
        edition = ListEdition.objects.get(date=edition_date)
        self.hd51 = HseMap.objects.get(district='051')
        self.hd51.edition = edition

    def test_counties(self):
        counties = self.hd51.counties
        self.assertEqual(1, len(counties))
        self.assertIn('060', counties)

    def test_voters(self):
        voters = self.hd51.voters
        self.assertEqual(38076, len(voters))
        self.assertIn('02080407', [x.voter_id for x in voters])

    def test_demographics(self):
        d = self.hd51.demographics
        self.assertEqual(38076, len(d.index))
        v = d[d.voter_id == '02080407']
        self.assertEqual('WH', v.race_id.iloc[0])
        self.assertEqual(1961, v.year_of_birth.iloc[0])
        self.assertEqual('M', v.gender.iloc[0])

    def test_district_vtd_map(self):
        dmap = self.hd51.district_vtd_map
        self.assertEqual(15, len(dmap.index))
        self.assertIn('RW20', dmap.precinct_id.unique())
        self.assertEqual(1, len(dmap.county_code.unique()))
        self.assertIn('060', dmap.county_code.unique())

    def test_county_vtd_map(self):
        cmap = self.hd51.county_vtd_map
        print(cmap.head())
        self.assertEqual(383, len(cmap.index))
        self.assertIn('RW20', cmap.precinct_id.unique())
        self.assertEqual(1, len(cmap.county_code.unique()))
        self.assertIn('060', cmap.county_code.unique())

    def test_get_election_result_details(self):
        results = self.hd51.get_election_result_details('2022-5-24')
        self.assertEqual(240, len(results.index))
        self.assertIn('RW20', results.precinct_id.unique())
        self.assertEqual(1, len(results.county_code.unique()))
        self.assertIn('060', results.county_code.unique())

    def test_get_party_tally(self):
        config = PartyTallyMapConfig('../resources/map_config/hd51_party_tally.json')
        tally = self.hd51.get_party_tally(config)
        self.assertEqual(15, len(tally.index))
        self.assertIn('RW20', tally.precinct_id.unique())

    def test_get_undervote(self):
        config = PartyTallyMapConfig('../resources/map_config/hd51_party_tally.json')
        uv = self.hd51.get_undervote(config)
        self.assertEqual(15, len(uv.index))
        self.assertIn('RW20', uv.precinct_id.unique())

    def test_get_voter_history_demographics(self):
        config = PartyTallyMapConfig('../resources/map_config/hd51_party_tally.json')
        vhd = self.hd51.get_voter_history_demographics(config)
        self.assertEqual(15, len(vhd.index))
        print(vhd.precinct_id.unique())
        self.assertIn('RW20', vhd.precinct_id.unique())

    def test_generation_summary(self):
        config = MapConfig('../resources/map_config/hd51_demographics.json')
        d = self.hd51.demographics
        print(d.precinct_id.unique())
        gs = self.hd51.generation_summary(d, config)
        self.assertEqual(15, len(gs.index))
        self.assertIn('RW20', gs.precinct_id.unique())
        print(gs)

    def test_get_demographics_choropleth(self):
        """
        Just make sure method runs without error.
        :return: None
        """
        _ = self.hd51.get_demographics_choropleth('../resources/map_config/hd51_demographics.json')

    def test_check_vtd_precinct(self):
        """
         Just make sure method runs without error.
         :return: None
         """
        _ = self.hd51.check_vtd_precinct('2022-5-24')

    def test_get_party_tally_choropleth(self):
        """
         Just make sure method runs without error.
         :return: None
         """
        _ = self.hd51.get_party_tally_choropleth('../resources/map_config/hd51_party_tally.json')

    def test_get_primary_demographics(self):
        config = MapConfig('../resources/map_config/hd51_primary_demographics.json')
        self.hd51.get_primary_demographics(config)