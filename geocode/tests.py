import unittest
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from county.models import County
from geocode.models import Address


class AddressTestCase(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def test_get_addresses_for_counties(self):
        addresses = Address.objects.get_addresses_for_counties(['060'])
        self.assertEqual(997, len(addresses.index))
        self.assertIn('key', addresses.columns)
        print(addresses.head())

    def test_key(self):
        address = Address.objects.all().first()
        self.assertIsNotNone(address.key)
