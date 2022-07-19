import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from vtd_map.models import VtdMap


class VtdMapTestCase(unittest.TestCase):
    def test_get_vtd_maps(self):
        m = VtdMap.get_vtd_maps('060')
        print(m.shape)

    def test_center(self):
        m = VtdMap.get_map(443)
        c = m.center
