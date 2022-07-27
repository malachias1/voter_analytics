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

