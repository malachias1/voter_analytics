import pandas as pd
from django.db import models
from core.models import BaseMapModel
from data.voterdb import VoterDb
import geopandas as gpd


class BlockGroupMap(BaseMapModel):
    geoid = models.TextField(primary_key=True)
    state_fips = models.TextField()
    county_fips = models.TextField()
    tract = models.TextField()
    blockgroup = models.TextField()
    geometry_wkb = models.TextField()
    center_wkb = models.TextField()

    @classmethod
    def get_blockgroup_maps(cls, county_code):
        db = VoterDb()
        results = db.fetchall(f"""
                    select bgg.geoid, bgg.geometry_wkb, bgg.center_wkb
                    from blockgroup_map as bgg
                    join county_details as cd
                    on bgg.county_fips = cd.county_fips
                    where cd.county_code = '{county_code}'
                """)
        df = pd.DataFrame.from_records(results, columns=['geoid', 'geometry_wkb', 'center_wkb'])
        df = df.assign(geometry=cls.from_wkb(df.geometry_wkb),
                       center=cls.from_wkb(df.center_wkb))
        gdf = gpd.GeoDataFrame(df, crs=cls.CRS_LAT_LON)
        return gdf

    def get_map_data_extensions(self):
        return {'geoid': [self.geoid],
                'state_fips': [self.state_fips],
                'county_fips': [self.county_fips],
                'tract': [self.tract],
                'blockgroup': [self.blockgroup]
                }

    @classmethod
    def get_object(cls, map_id: str):
        return cls.objects.get(geoid=map_id)

    class Meta:
        managed = False
        db_table = 'blockgroup_map'
