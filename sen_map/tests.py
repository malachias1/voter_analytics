import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from sen_map.models import SenMap


class SenMapTestCase(unittest.TestCase):
    def test_get_map(self):
        m = SenMap.get_map(7)
        print(m.shape)
