import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from county_map.models import CountyMap
from data.voterdb import VoterDb


class CngMapTestCase(unittest.TestCase):
    def test_get_map(self):
        m = CountyMap.get_map('033')
        print(m.shape)

    def test_get_us_house_districts(self):
        d = CountyMap.get_us_house_districts('060')
        self.assertIn('005', d)
        self.assertIn('006', d)
        self.assertIn('007', d)
        self.assertIn('013', d)

    def test_get_ga_house_districts(self):
        d = CountyMap.get_ga_house_districts('060')
        print(d)

    def test_get_ga_senate_districts(self):
        d = CountyMap.get_ga_senate_districts('060')
        print(d)
