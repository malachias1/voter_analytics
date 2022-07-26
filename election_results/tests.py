from django.test import TestCase
import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from election_results.models import ElectionResultDetails


class ElectionResultDetailsTestCase(unittest.TestCase):
    def test_voterdb(self):
        x = ElectionResultDetails()
        print(x.fetchone(f"""
            select count(*) from contest_class
        """))