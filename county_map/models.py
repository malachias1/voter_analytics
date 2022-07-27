from core.models import BaseMapModel, BaseMap
from django.db import models
from data.voterdb import VoterDb
from vtd_map.models import VtdMap
from cng_map.models import CngMap
from hse_map.models import HseMap
from sen_map.models import SenMap
import pandas as pd
import geopandas as gpd
import json
import plotly.express as px
import plotly.graph_objects as go
from shapely.geometry import MultiPolygon, Polygon


class CountyMapManager(models.Manager, BaseMap):
    @property
    def state_map(self):
        records = [{'county_code': x.county_code,
                    'county_fips': x.county_fips,
                    'geoid': x.geoid,
                    'county_name': x.county_name,
                    'geometry_wkb': x.geometry_wkb
                    } for x in self.all()]
        df = pd.DataFrame.from_records(records)
        df = df.assign(geometry=self.from_wkb(df.geometry_wkb)).drop(columns=['geometry_wkb'])
        return gpd.GeoDataFrame(df, crs=self.CRS_LAT_LON)


class CountyMap(BaseMapModel):
    county_code = models.TextField(primary_key=True)
    state_fips = models.TextField()
    county_fips = models.TextField()
    geoid = models.TextField()
    county_name = models.TextField()
    aland = models.TextField()
    awater = models.TextField()
    geometry_wkb = models.TextField()
    center_wkb = models.TextField()

    objects = CountyMapManager()

    @property
    def vtds_maps(self):
        return VtdMap.objects.get_maps(self.county_code)

    def get_map_data_extensions(self):
        return {'county_code': [self.county_code],
                'state_fips': [self.state_fips],
                'county_fips': [self.county_fips],
                'geoid': [self.geoid],
                'county_name': [self.county_name],
                'aland': [self.aland],
                'awater': [self.awater]
                }

    @classmethod
    def get_object(cls, map_id):
        map_id = map_id if isinstance(map_id, str) else f'{map_id:03d}'
        return cls.objects.get(county_code=map_id)

    def get_choropleth(self, dmaps, labels=None, hover_data=None):
        hover_data = hover_data or {}
        hover_data.update({'district': True, 'precinct_name': True})
        labels = labels or {}
        labels.update({'precinct_name': 'Precinct Name'})

        vmaps = self.vtds_maps[['precinct_name', 'geometry']]
        vmaps.geometry = vmaps.geometry.apply(self.to_multipolygon)
        dmaps.geometry = dmaps.geometry.apply(self.to_multipolygon)
        base_map = vmaps.overlay(dmaps, how='intersection', keep_geom_type=True)
        # vtds are split across districts and need to have unique names.
        base_map = base_map.assign(vtd_id=base_map.district+base_map.precinct_name)

        gj = json.loads(base_map.to_json())
        center = base_map.to_crs(crs='epsg:3035').centroid.to_crs(crs='epsg:4326').iloc[0]

        fig = px.choropleth_mapbox(
            base_map,
            geojson=gj,
            color='district',
            locations='vtd_id',
            featureidkey="properties.vtd_id",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map", zoom=9.5,
            labels={'precinct_name': 'Precinct Name'},
            hover_data={'district': True, 'precinct_name': True, 'vtd_id': False}
        )

        self.add_watermark(fig)
        self.configure_legend(fig)

        fig.update_layout(margin={"r": 0, "t": 30, "l": 0, "b": 30})
        return fig

    def get_district_map(self, maps):
        vtd_maps = self.vtds_maps
        d_maps = []
        for m in maps:
            d_maps.append(m.overlay(vtd_maps[['precinct_name', 'geometry']], how='intersection', keep_geom_type=True))
        gdf = gpd.GeoDataFrame(pd.concat(d_maps, ignore_index=True), crs='epsg:4326')
        return gdf.sort_values(by='district', ascending=False)

    def to_multipolygon(self, p):
        x = type(p)
        if isinstance(p, MultiPolygon):
            return p
        return MultiPolygon([p])

    @property
    def ga_house_choropleth(self):
        return self.get_choropleth(HseMap.objects.get_maps(self.county_code),
                                   labels={'district': "State House District"})

    def get_ga_senate_choropleth(self):
        return self.get_choropleth(SenMap.objects.get_maps(self.county_code),
                                   labels={'district': "State Senate District"})

    def get_us_house_choropleth(self):
        return self.get_choropleth(CngMap.objects.get_maps(self.county_code),
                                   labels={'district': "US House District"})

    class Meta:
        managed = False
        db_table = 'county_map'
