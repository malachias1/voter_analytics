from core.models import BaseMapModel, BaseMap
from county_map.models import CountyMap as OldCountyMap
from django.db import models
import geopandas as gpd
import pandas as pd
from pathlib import Path
from vtd_map.models import VtdMap
import json
import plotly.express as px


class CountyManager(models.Manager):
    @property
    def county_codes_path(self):
        return '../resources/county_codes.csv'

    @property
    def county_code2fips(self):
        df = pd.DataFrame.from_csv(self.county_codes_path,
                                   dtypes={'county_name': str,
                                           'county_code': str,
                                           'county_fips': str,
                                           })
        return {df.loc[i].county_code: {df.loc[i].county_fips, df.loc[i].county_name}
                for i in df.index}

    @property
    def county_fips2code(self):
        df = pd.DataFrame.from_csv(self.county_codes_path,
                                   dtypes={'county_name': str,
                                           'county_code': str,
                                           'county_fips': str,
                                           })
        return {df.loc[i].county_fips: {df.loc[i].county_code, df.loc[i].county_name}
                for i in df.index}

    def get_map(self, county_code):
        county_code = county_code if isinstance(county_code, str) else f'{county_code:03d}'
        return self.get(county_code=county_code).as_map

    def load(self, path):
        p = Path(path).expanduser()
        county_fips2code = self.county_fips
        cmaps = gpd.read_file(p)
        county_maps = []
        for i in cmaps.index:
            row = cmaps.loc[i]
            county_fips = row.COUNTYFP
            county_code = county_fips2code[county_fips]
            geometry_wkb = row.geometry.to_wkb(hex=True)
            county_maps.append(CountyMap(county_code=county_code,
                                         geometry_wkb=geometry_wkb))
        CountyMap.objects.bulk_create(county_maps)

        county_maps = {x.county_code: x for x in CountyMap.objects.all()}
        counties = []
        for i in cmaps.index:
            row = cmaps.loc[i]
            county_fips = row.COUNTYFP
            county_code = county_fips2code[county_fips],
            counties.append(County(
                county_fips=county_fips,
                county_code=county_code,
                state_fips=row.STATEFP,
                geoid=row.GEOID,
                county_name=row.NAME.upper(),
                aland=float(row.ALAND),
                awater=float(row.AWATER),
                county_map=county_maps[county_code]))

        County.objects.bulk_create(counties)

    @classmethod
    def migrate(cls):
        County.objects.all().delete()
        county_maps = {x.county_code: x for x in CountyMap.objects.all()}
        counties = []
        for x in OldCountyMap.objects.all():
            counties.append(County(county_code=x.county_code,
                                   county_fips=x.county_fips,
                                   county_name=x.county_name.upper(),
                                   state_fips=x.state_fips,
                                   geoid=x.geoid,
                                   aland=float(x.aland),
                                   awater=float(x.awater),
                                   county_map=county_maps[x.county_code]))
        County.objects.bulk_create(counties)

    def state_map(self):
        records = [x.as_map_record for x in self.all()]
        df = pd.DataFrame.from_records(records)
        df = df.assign(geometry=self.from_wkb(df.geometry_wkb)).drop(columns=['geometry_wkb'])
        return gpd.GeoDataFrame(df, crs=self.CRS_LAT_LON)


class County(models.Model, BaseMap):
    county_code = models.CharField(max_length=3)
    county_fips = models.CharField(max_length=3)
    county_name = models.TextField()
    state_fips = models.CharField(max_length=2)
    geoid = models.CharField(max_length=5)
    aland = models.FloatField()
    awater = models.FloatField()
    county_map = models.ForeignKey('CountyMap', on_delete=models.SET_NULL, blank=True, null=True)

    objects = CountyManager()

    @property
    def as_map(self):
        records = [self.as_map_record]
        df = pd.DataFrame.from_records(records)
        df = df.assign(geometry=self.from_wkb(df.geometry_wkb)).drop(columns=['geometry_wkb'])
        return gpd.GeoDataFrame(df, crs=self.CRS_LAT_LON)

    @property
    def as_map_record(self):
        return {'county_code': self.county_code,
                'county_fips': self.county_fips,
                'geoid': self.geoid,
                'county_name': self.county_name,
                'geometry_wkb': self.county_map.geometry_wkb
                }

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


class CountyMapManager(models.Manager):
    @classmethod
    def migrate(cls):
        CountyMap.objects.all().delete()
        countyMaps = []
        for x in OldCountyMap.objects.all():
            countyMaps.append(CountyMap(county_code=x.county_code,
                                        geometry_wkb=x.geometry_wkb))
        CountyMap.objects.bulk_create(countyMaps)


class CountyMap(BaseMapModel):
    county_code = models.CharField(max_length=3)
    geometry_wkb = models.TextField()

    objects = CountyMapManager()
