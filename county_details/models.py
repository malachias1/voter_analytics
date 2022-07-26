from django.db import models
import pandas as pd


class CountyDetailsManager(models.Manager):
    @property
    def details(self):
        return pd.DataFrame.from_records([{'county_code': x.county_code,
                                           'county_fips': x.county_fips,
                                           'county_name': x.county_name}
                                          for x in self.all()])


class CountyDetails(models.Model):
    county_code = models.TextField(primary_key=True)
    county_fips = models.TextField()
    county_name = models.TextField()

    objects = CountyDetailsManager()

    class Meta:
        managed = False
        db_table = 'county_details'
