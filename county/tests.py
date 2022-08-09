import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from county.models import County
from shapely.geometry import MultiPolygon


class CountyTestCase(unittest.TestCase):
    def test_get_county_code(self):
        c = County.objects.get(county_code='033')
        self.assertEqual('COBB', c.county_name)
        self.assertEqual('13', c.state_fips)
        self.assertEqual('067', c.county_fips)
        self.assertTrue(c.aland > 10000)
        self.assertTrue(c.awater > 10000)
        self.assertEqual('13067', c.geoid)
        self.assertIsNotNone(c.county_map)

    def test_count(self):
        self.assertEqual(159, County.objects.count())

    def test_geometry(self):
        c = County.objects.get(county_code='033')
        self.assertTrue(isinstance(c.county_map.geometry, MultiPolygon))

