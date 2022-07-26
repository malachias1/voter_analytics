import pandas as pd
from django.db import models
from geopandas import GeoSeries
from geopandas import GeoDataFrame


class BaseMap:
    CRS_METERS = 'epsg:3035'
    CRS_LAT_LON = 'epsg:4326'

    @classmethod
    def centroid(cls, gdf):
        return gdf.to_crs(crs=cls.CRS_METERS).centroid.to_crs(crs=cls.CRS_LAT_LON)

    @classmethod
    def from_wkb(cls, geometry_wkb):
        return GeoSeries.from_wkb(geometry_wkb, crs=cls.CRS_METERS).to_crs(crs=cls.CRS_LAT_LON)


class BaseMapModel(models.Model, BaseMap):
    @property
    def geometry(self):
        return self.from_wkb(pd.Series(self.geometry_wkb))

    @property
    def center(self):
        try:
            return self.from_wkb(pd.Series(self.center_wkb))
        except AttributeError as _:
            return self.centroid(GeoDataFrame(geometry=[self.geometry], crs=self.CRS_LAT_LON))

    @classmethod
    def get_object(cls, map_id):
        raise NotImplemented('get_object is not implemented!')

    @classmethod
    def get_map_data(cls, map_id):
        o = cls.get_object(map_id)
        data = {'geometry': o.geometry,
                'center': o.center}
        data.update(o.get_map_data_extensions())
        return data

    def get_map_data_extensions(self):
        return {}

    @classmethod
    def get_map(cls, map_id):
        data = cls.get_map_data(map_id)
        return GeoDataFrame(data, crs=cls.CRS_LAT_LON)

    class Meta:
        abstract = True


class DistrictMapModel(BaseMapModel):
    id = models.IntegerField(primary_key=True)
    area = models.FloatField()
    district = models.TextField()
    population = models.IntegerField()
    ideal_value = models.FloatField()
    geometry_wkb = models.TextField()
    center_wkb = models.TextField()

    def get_map_data_extensions(self):
        return {'id': [self.id],
                'area': [self.area],
                'district': [self.district],
                'population': [self.population],
                'ideal_value': [self.ideal_value]}

    @classmethod
    def get_object(cls, map_id):
        map_id = map_id if isinstance(map_id, str) else f'{map_id:03d}'
        return cls.objects.get(district=map_id)

    class Meta:
        abstract = True
