import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from precinct.models import Precinct, PrecinctMap
from county.models import County


class PrecinctTestCase(unittest.TestCase):
    def test_get_precinct(self):
        p = Precinct.objects.get(id=14)
        self.assertEqual('2', p.precinct_short_name)
        self.assertEqual('LINCOLN CENTER', p.precinct_name)
        self.assertEqual('LINCOLN', p.county.county_name)

    def test_precinct_map_of(self):
        p = Precinct.objects.get(id=14)
        self.assertEqual(1, p.map_of.count())

    def test_get_maps_for_counties(self):
        c = County.objects.all().first()
        self.assertEqual(9, len(PrecinctMap.objects.get_maps_for(counties=c)))
        self.assertEqual(9, len(PrecinctMap.objects.get_maps_for(counties=[c])))

    def test_get_maps_for_county_codes(self):
        c = County.objects.all().first()
        self.assertEqual(9, len(PrecinctMap.objects.get_maps_for(county_codes=c.county_code)))
        self.assertEqual(9, len(PrecinctMap.objects.get_maps_for(county_codes=[c.county_code])))
