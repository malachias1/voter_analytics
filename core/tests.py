import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from core.models import MapConfig


class CoreTestCase(unittest.TestCase):
    def test_map_config(self):
        c = MapConfig('../resources/fig_config/hd51/demographics.json')
        self.assertEqual("https://novemberpathways.com/maps/hd51/logo.png", c.logo_source)
        self.assertEqual('<b>Active Voter Demographic Summary</b><br><i><b>State House District 51</b></i><br><br>'
                         'Created by <a href="https://novemberpathways.com">NovemberPathways.com</a>', c.description)
