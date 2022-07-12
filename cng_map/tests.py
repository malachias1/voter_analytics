from django.test import TestCase
from .models import CngMap


class CngMapTestCase(TestCase):
    def test_maps(self):
        self.assertIsNotNone(CngMap.objects.all())
        self.assertTrue(len(CngMap.objects.all()) > 0)
        print(CngMap.objects.all().values_list())
