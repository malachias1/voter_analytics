import pandas as pd
from django.db import models
from core.models import BaseMapModel, BaseMap
import geopandas as gpd


class VtdMapManager(models.Manager, BaseMap):
    def get_maps(self, counties):
        """

        :param counties: a county code or list of county codes
        :return: a GeoDataFrame
        """
        if isinstance(counties, str):
            counties = (counties,)
        records = []
        for o in self.filter(county_code__in=counties):
            records.append(o.as_record)
        df = pd.DataFrame.from_records(records)
        return gpd.GeoDataFrame(df, crs=self.CRS_LAT_LON)


class VtdMap(BaseMapModel):
    id = models.IntegerField(primary_key=True)
    area = models.FloatField()
    precinct_id = models.TextField()
    precinct_name = models.TextField()
    county_code = models.TextField()
    county_fips = models.TextField()
    county_name = models.TextField()
    geometry_wkb = models.TextField()
    center_wkb = models.TextField()

    objects = VtdMapManager()

    @property
    def as_record(self):
        record = super().as_record
        record.update({'id': self.id,
                       'area': self.area,
                       'precinct_id': self.precinct_id,
                       'precinct_name': self.precinct_name,
                       'county_code': self.county_code,
                       'county_fips': self.county_fips,
                       'county_name': self.county_name
                       })
        return record

    class Meta:
        managed = False
        db_table = 'vtd_map'


class VtdMapMixin:

    @property
    def county_vtd_map(self):
        vmaps = VtdMap.objects.get_maps(self.counties)
        return vmaps.assign(vtd_id=range(0, len(vmaps.index)))
