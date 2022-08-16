import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from county.models import County, CountyMap


class CountyTestCase(unittest.TestCase):
    def test_get_county_code(self):
        c = County.objects.get(county_code='033')
        self.assertEqual('COBB', c.county_name)
        self.assertEqual('13', c.state_fips)
        self.assertEqual('067', c.county_fips)
        self.assertTrue(c.aland > 10000)
        self.assertTrue(c.awater > 10000)
        self.assertEqual('13067', c.geoid)

    def test_count(self):
        self.assertEqual(159, County.objects.count())

    def test_map_of(self):
        c = County.objects.get(county_code='033')
        cmap = c.map_of.all().first()
        self.assertIsNotNone(cmap)

    def test_get_map(self):
        cm = CountyMap.objects.get_map('033')
        self.assertEqual('COBB', cm.county_name.iloc[0])
        self.assertEqual('033', cm.county_code.iloc[0])
        self.assertEqual('067', cm.county_fips.iloc[0])

    def test_state_map(self):
        sm = CountyMap.objects.state_map
        self.assertIsNotNone(sm)
        self.assertEqual(159, len(sm.index))

    def test_count_map(self):
        self.assertEqual(159, CountyMap.objects.count())
        self.assertEqual(1, CountyMap.objects.filter(county__county_code='033').count())

    def test_precinct_maps(self):
        cm = CountyMap.objects.get(county__county_code='033')
        self.assertIsNotNone(cm.precinct_maps)
