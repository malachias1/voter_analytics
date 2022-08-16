from django.db import models
import geopandas as gpd
import pandas as pd
import json
import plotly.express as px

from core.base_fig import BaseMapModel, BaseMap, BaseMapModelManager
from precinct.models import PrecinctMap


class CountyMapManager(BaseMapModelManager):
    @property
    def state_map(self):
        """
        state counties are in meters.
        from_wkb with wkb_crs returns a lat-lon map
        :return:
        """
        records = [x.as_record for x in self.all()]
        df = pd.DataFrame.from_records(records)
        geometry = self.from_wkb(df.geometry_wkb)
        df = df.drop(columns='geometry_wkb')
        return gpd.GeoDataFrame(df, geometry=geometry, crs=self.CRS_LAT_LON)

    @property
    def wkb_crs(self):
        return self.CRS_METERS

    def get_map(self, county_code):
        return self.get(county__county_code=county_code).as_map


class CountyMap(BaseMapModel):
    county = models.ForeignKey('county.County', on_delete=models.CASCADE, null=True, related_name='map_of')
    geometry_wkb = models.TextField()

    objects = CountyMapManager()

    @property
    def as_map(self):
        records = [self.as_record]
        df = pd.DataFrame.from_records(records)
        geometry = self.from_wkb(df.geometry_wkb)
        df = df.drop(columns='geometry_wkb')
        return gpd.GeoDataFrame(df, geometry=geometry, crs=self.CRS_METERS).to_crs(crs=self.CRS_LAT_LON)

    @property
    def as_record(self):
        record = self.county.as_record
        record['geometry_wkb'] = self.geometry_wkb
        return record

    def get_precinct_maps(self, edition):
        return PrecinctMap.objects.get_maps_for_counties(edition, [self.county])

    def get_district_map(self, edition, dmaps):
        """
        Clip district maps to county boundaries using
        precinct maps.
        :param edition: a precinct edition
        :param dmaps: one or more district maps, idelly covering the county
        :return: 
        """
        pmaps = self.get_precinct_maps(edition)
        pmaps = pmaps[['precinct_short_name', 'geometry']]
        clipped_dmaps = []
        for m in dmaps:
            clipped_dmaps.append(m.overlay(pmaps, how='intersection'))
        return gpd.GeoDataFrame(pd.concat(clipped_dmaps, ignore_index=True), crs=self.CRS_LAT_LON)

    def get_district_choropleth(self, edition, dmaps, labels=None, hover_data=None):
        hover_data = hover_data or {}
        hover_data.update({'district': True, 'precinct_short_name': True})
        labels = labels or {}
        labels.update({'precinct_short_name': 'Precinct Name'})

        pmap = self.get_precinct_maps(edition)
        pmap = pmap[['precinct_short_name', 'geometry']]
        base_map = pmap.overlay(dmaps, how='intersection', keep_geom_type=True)
        # precincts are split across districts and need to have unique names.
        # IF THEY DO NOT THEY WILL NOT DISPLAY
        base_map = base_map.assign(pid=base_map.district + base_map.precinct_short_name)
        base_map = base_map.sort_values(['district'])
        # Points are not JSON serializable; so, drop center
        if 'center' in base_map.columns:
            base_map = base_map.drop(columns=['center'])
        gj = json.loads(base_map.to_json())
        center = self.centroid(base_map).iloc[0]
        # sorting doesn't seem to work
        category_orders = {'district': sorted(base_map.district.unique())}
        fig = px.choropleth_mapbox(
            base_map,
            geojson=gj,
            color='district',
            locations='pid',
            featureidkey="properties.pid",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map", zoom=9.5,
            labels=labels,
            hover_data=hover_data,
            category_orders=category_orders
        )

        self.add_watermark(fig)
        self.configure_legend(fig)

        fig.update_layout(margin={"r": 8, "t": 8, "l": 8, "b": 8})
        return fig


class CountyManager(models.Manager, BaseMap):
    @property
    def as_df(self):
        return pd.DataFrame.from_records(
            [c.as_record for c in self.all()]
        )


class County(models.Model, BaseMap):
    county_code = models.CharField(max_length=3)
    county_fips = models.CharField(max_length=3)
    county_name = models.TextField()
    state_fips = models.CharField(max_length=2)
    geoid = models.CharField(max_length=5)
    aland = models.FloatField()
    awater = models.FloatField()

    objects = CountyManager()

    @property
    def as_record(self):
        return {'county_code': self.county_code,
                'county_fips': self.county_fips,
                'county_name': self.county_name,
                'geoid': self.geoid
                }
