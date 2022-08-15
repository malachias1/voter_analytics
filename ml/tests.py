import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from ml.models import PrimaryDataSet
from hse_map.models import HseMap
from voter.models import ListEdition
from datetime import datetime


class PrimaryDataSetTestCase(unittest.TestCase):
    def setUp(self) -> None:
        edition_date = datetime.strptime('2022-08-05', '%Y-%m-%d')
        edition = ListEdition.objects.get(date=edition_date)
        self.hd51 = HseMap.objects.get(district='051')
        self.hd51.edition = edition
        election_date = datetime.strptime('2022-05-24', '%Y-%m-%d')
        self.m = PrimaryDataSet(self.hd51, election_date)

    def test_dosomething(self):
        self.m.dosomething()
