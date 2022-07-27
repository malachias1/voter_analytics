import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from hse_map.models import HseMap


class HseMapTestCase(unittest.TestCase):
    def test_get_maps(self):
        maps = HseMap.objects.get_maps('033', )
        print(maps.shape)
