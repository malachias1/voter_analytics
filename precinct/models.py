from django.db import models
import pandas as pd
import geopandas as gpd
import re
from datetime import date

from core.base_fig import BaseMap
from core.models import BaseMapModel


class PrecinctEditionManager(models.Manager):
    @property
    def default_comment(self):
        return f'Updated on {self.today_str}'

    def update_edition(self, path, comments=None, year=None, create=False):
        comments = comments or self.default_comment
        year = year or int(re.match(r'VTD(\d+)', path.stem).group(1))
        try:
            edition = PrecinctEdition.objects.get(year=year)
            edition.comments += comments
            edition.save()
        except PrecinctEdition.DoesNotExist:
            if create:
                edition_date = date.today()
                edition = PrecinctEdition(year=year,
                                          date=edition_date,
                                          path=str(path),
                                          comments=comments)
                edition.save()
            else:
                edition = None
        return edition


class PrecinctEdition(models.Model):
    year = models.IntegerField()
    date = models.DateField()
    path = models.TextField()
    comments = models.TextField()

    objects = PrecinctEditionManager()


class Precinct(models.Model):
    edition = models.ForeignKey('precinct.PrecinctEdition', on_delete=models.CASCADE)
    county = models.ForeignKey('county.County', on_delete=models.CASCADE, related_name='precincts_of')
    precinct_short_name = models.TextField()
    precinct_name = models.TextField(blank=True, null=True)
    aka = models.TextField(blank=True, null=True)

    @property
    def as_record(self):
        return {'pid': self.id,
                'precinct_short_name': self.precinct_short_name,
                'precinct_name': self.precinct_name,
                'county_code': self.county.county_code,
                'county_fips': self.county.county_fips,
                'county_name': self.county.county_name
                }


class PrecinctMapManager(models.Manager, BaseMap):
    def get_maps_for_counties(self, edition, counties):
        precinct_maps = list(self.filter(precinct__edition=edition, precinct__county__in=counties))
        return self.build_map(precinct_maps)

    def get_maps_for_county_codes(self, edition, county_codes):
        precinct_maps = list(self.filter(precinct__edition=edition, precinct__county__county_code__in=county_codes))
        return self.build_map(precinct_maps)

    def get_maps_for_precincts(self, precincts):
        precinct_maps = list(self.filter(precinct__in=precincts))
        return self.build_map(precinct_maps)

    def build_map(self, precinct_maps):
        records = [pm.as_record for pm in precinct_maps]
        df = pd.DataFrame.from_records(records)
        geometry = self.from_wkb(df.geometry_wkb)
        return gpd.GeoDataFrame(df.drop(columns='geometry_wkb'), geometry=geometry, crs=self.CRS_LAT_LON)


class PrecinctMap(BaseMapModel):
    precinct = models.ForeignKey('precinct.Precinct', on_delete=models.CASCADE, related_name='map_of')
    geometry_wkb = models.TextField()

    objects = PrecinctMapManager()

    @property
    def as_record(self):
        record = self.precinct.as_record
        record['geometry_wkb'] = self.geometry_wkb
        return record


class PrecinctMapMixin:
    @property
    def precinct_map(self):
        return PrecinctMap.objects.get_maps_for_precincts(self.precincts)
