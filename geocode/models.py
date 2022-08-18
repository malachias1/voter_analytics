import pandas as pd
from django.db import models

from county.models import County


class AddressManager(models.Manager):
    def get_addresses_for_counties(self, county_codes):
        return pd.DataFrame.from_records(
            [a.as_record for a in self.filter(county__county_code__in=county_codes)]
        )


class Address(models.Model):
    lat = models.FloatField()
    lon = models.FloatField()
    accuracy_score = models.FloatField()
    accuracy_type = models.CharField(max_length=64)
    county = models.ForeignKey(County, on_delete=models.SET_NULL, blank=True, null=True)
    street = models.CharField(max_length=96)
    city = models.CharField(max_length=32)
    state = models.CharField(max_length=2)
    zipcode = models.CharField(max_length=5)
    plus4 = models.CharField(max_length=5, blank=True, null=True)
    building_name = models.CharField(max_length=64, blank=True, null=True)
    record_type_description = models.CharField(max_length=32, blank=True, null=True)

    objects = AddressManager()

    @property
    def as_record(self):
        return {
            'lat': self.lat,
            'lon': self.lon,
            'accuracy_score': self.accuracy_score,
            'accuracy_type': self.accuracy_type,
            'county_code': self.county.county_code,
            'street': self.street,
            'city': self.city,
            'state': self.state,
            'zipcode': self.zipcode,
            'plus4': self.plus4,
            'building_name': self.building_name,
            'record_type_description': self.record_type_description,
            'key': self.key
        }

    @property
    def key(self):
        return f'{self.street}, {self.city} {self.state} {self.zipcode}'

    @classmethod
    def make_key(cls, address):
        return f'{address.house_number} {address.street_name}, {address.city} {address.state} {address.zipcode}'
