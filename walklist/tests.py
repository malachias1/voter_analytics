import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from walklist.models import Walklist
from hse_map.models import HseMap
from datetime import datetime
from voter.models import ListEdition


class WalklistTestCase(unittest.TestCase):
    def setUp(self) -> None:
        edition_date = datetime.strptime('2022-08-05', '%Y-%m-%d')
        edition = ListEdition.objects.get(date=edition_date)
        self.hd51 = HseMap.objects.get(district='051')
        self.hd51.edition = edition

    def test_data(self):
        walklist = Walklist(self.hd51)
        d = walklist.voter_data
        self.assertEqual(1000, len(d.index))

    def test_score(self):
        walklist = Walklist(self.hd51)
        ad = walklist.address_score
        print(ad.head(20))
        self.assertEqual(1000, len(ad.index))
