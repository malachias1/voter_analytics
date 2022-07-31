from core.models import BaseMapModel, BaseMap
from django.db import models
from vtd_map.models import VtdMap
import pandas as pd
import geopandas as gpd
import json
import plotly.express as px


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

    def get_map(self, county_code):
        county_code = county_code if isinstance(county_code, str) else f'{county_code:03d}'
        return self.get(county_code=county_code).as_geodataframe


class CountyMap(BaseMapModel):
    county_code = models.CharField(max_length=3, primary_key=True)
    state_fips = models.CharField(max_length=2)
    county_fips = models.CharField(max_length=3)
    geoid = models.TextField()
    county_name = models.TextField()
    aland = models.TextField()
    awater = models.TextField()
    geometry_wkb = models.TextField()
    center_wkb = models.TextField()

    objects = CountyMapManager()

    @property
    def as_record(self):
        record = super().as_record
        record.update({'county_code': self.county_code,
                       'state_fips': self.state_fips,
                       'county_fips': self.county_fips,
                       'geoid': self.geoid,
                       'county_name': self.county_name,
                       'aland': self.aland,
                       'awater': self.awater
                       })
        return record

    @property
    def vtds_maps(self):
        return VtdMap.objects.get_maps(self.county_code)

    def get_district_choropleth(self, dmaps, labels=None, hover_data=None):
        hover_data = hover_data or {}
        hover_data.update({'district': True, 'precinct_name': True})
        labels = labels or {}
        labels.update({'precinct_name': 'Precinct Name'})

        vmaps = self.vtds_maps[['precinct_name', 'geometry']]
        base_map = vmaps.overlay(dmaps, how='intersection', keep_geom_type=True)
        # vtds are split across districts and need to have unique names.
        # IF THEY DO NOT THEY WILL NOT DISPLAY
        base_map = base_map.assign(vtd_id=base_map.district + base_map.precinct_name)
        base_map = base_map.sort_values(['district'])
        if 'center' in base_map.columns:
            base_map = base_map.drop(columns=['center'])

        gj = json.loads(base_map.to_json())
        center = base_map.to_crs(crs='epsg:3035').centroid.to_crs(crs='epsg:4326').iloc[0]
        category_orders = {'district': sorted(base_map.district.unique())}
        fig = px.choropleth_mapbox(
            base_map,
            geojson=gj,
            color='district',
            locations='vtd_id',
            featureidkey="properties.vtd_id",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map", zoom=9.5,
            labels=labels,
            hover_data=hover_data,
            category_orders=category_orders
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

    class Meta:
        managed = False
        db_table = 'county_map'
