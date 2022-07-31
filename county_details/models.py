from django.db import models
import pandas as pd


class CountyDetailsManager(models.Manager):
    @property
    def details(self):
        return pd.DataFrame.from_records([x.as_record for x in self.all()])


class CountyDetails(models.Model):
    county_code = models.CharField(max_length=3)
    county_fips = models.CharField(max_length=3)
    county_name = models.TextField()

    objects = CountyDetailsManager()

    def as_record(self):
        return {'county_code': self.county_code,
                'county_fips': self.county_fips,
                'county_name': self.county_name
                }

    class Meta:
        db_table = 'county_details'
