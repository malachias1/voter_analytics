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

    def test_district_precinct_map(self):
        dmap = self.hd51.district_precinct_map
        self.assertEqual(18, len(dmap.index))
        self.assertIn('RW20', dmap.precinct_short_name.unique())
        self.assertEqual(1, len(dmap.county_code.unique()))
        self.assertIn('060', dmap.county_code.unique())

    def test_precinct_map(self):
        cmap = self.hd51.precinct_map
        self.assertEqual(18, len(cmap.index))
        self.assertIn('RW20', cmap.precinct_short_name.unique())
        self.assertEqual(1, len(cmap.county_code.unique()))
        self.assertIn('060', cmap.county_code.unique())

    def test_get_election_result_details(self):
        results = self.hd51.get_election_result_details('2022-5-24')
        self.assertEqual(240, len(results.index))
        self.assertIn('RW20', results.precinct_short_name.unique())
        self.assertEqual(1, len(results.county_code.unique()))
        self.assertIn('060', results.county_code.unique())

    def test_get_party_tally(self):
        config = PartyTallyMapConfig('../resources/fig_config/hd51/summary.json')
        tally = self.hd51.get_party_tally(config)
        self.assertEqual(17, len(tally.index))
        self.assertIn('RW20', tally.precinct_short_name.unique())

    def test_get_undervote(self):
        config = PartyTallyMapConfig('../resources/fig_config/hd51/summary.json')
        uv = self.hd51.get_undervote(config)
        self.assertEqual(17, len(uv.index))
        self.assertIn('RW20', uv.precinct_short_name.unique())

    def test_get_demographics_choropleth(self):
        """
        Just make sure method runs without error.
        :return: None
        """
        _ = self.hd51.get_demographics_choropleth('../resources/fig_config/hd51/demographics.json')

    def test_check_precinct(self):
        """
         Just make sure method runs without error.
         :return: None
         """
        _ = self.hd51.check_precincts('2022-5-24')

    def test_get_party_tally_choropleth(self):
        """
         Just make sure method runs without error.
         :return: None
         """
        _ = self.hd51.get_party_tally_choropleth('../resources/fig_config/hd51/summary.json')

    def test_get_primary_demographics(self):
        config = MapConfig('../resources/fig_config/hd51/primary_demographics.json')
        self.hd51.get_primary_demographics(config)

    def test_get_precinct_choropleth(self):
        """
         Just make sure method runs without error.
         :return: None
         """
        _ = self.hd51.get_precinct_choropleth('../resources/fig_config/hd51/precincts.json')
