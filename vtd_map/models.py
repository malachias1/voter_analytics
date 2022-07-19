import pandas as pd
from django.db import models
from core.models import BaseMapModel
from data.voterdb import VoterDb
import geopandas as gpd


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

    @classmethod
    def get_vtd_maps(cls, county_code):
        db = VoterDb()
        results = db.fetchall(f"""
                    select vm.id, vm.area, vm.precinct_id, vm.precinct_name, 
                        vm.geometry_wkb, vm.center_wkb
                    from vtd_map as vm
                    where vm.county_code = '{county_code}'
                """)
        df = pd.DataFrame.from_records(results, columns=['precinct_id', 'area', 'name', 'description',
                                                         'geometry_wkb', 'center_wkb'])
        df = df.assign(geometry=cls.from_wkb(df.geometry_wkb),
                       center=cls.from_wkb(df.center_wkb))
        gdf = gpd.GeoDataFrame(df, crs=cls.CRS_LAT_LON)
        return gdf

    def get_map_data_extensions(self):
        return {'id': [self.id],
                'area': [self.area],
                'precinct_id': [self.precinct_id],
                'precinct_name': [self.precinct_name],
                'county_code': [self.county_code],
                'county_fips': [self.county_fips],
                'county_name': [self.county_name]
                }

    @classmethod
    def get_object(cls, map_id: int):
        return cls.objects.get(id=map_id)

    class Meta:
        managed = False
        db_table = 'vtd_map'
