import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from cng_map.models import CngMap


class CngMapTestCase(unittest.TestCase):
    def test_get_map(self):
        m = CngMap.get_map(7)
        print(m.shape)
