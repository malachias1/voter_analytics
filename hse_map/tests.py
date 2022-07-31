import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from hse_map.models import HseMap


class HseMapTestCase(unittest.TestCase):
    def test_get_county_choropleth(self):
        maps = HseMap.objects.get_county_choropleth('033')
        print(maps.shape)
