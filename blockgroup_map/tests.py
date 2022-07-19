import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from blockgroup_map.models import BlockGroupMap


class BlockGroupMapTestCase(unittest.TestCase):
    def test_get_blockgroup_maps(self):
        m = BlockGroupMap.get_blockgroup_maps('060')
        print(m.shape)

    def test_center(self):
        m = BlockGroupMap.get_map('130019501001')
        c = m.center
