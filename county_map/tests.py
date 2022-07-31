import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from county_map.models import CountyMap


class CountyMapTestCase(unittest.TestCase):
    def test_get_map(self):
        m = CountyMap.objects.get_map('033')
        print(m.shape)

