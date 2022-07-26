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
    def ga_house_districts(self):
        return self.get_districts('hse')

    @property
    def ga_senate_districts(self):
        return self.get_districts('sen')

    @property
    def us_house_districts(self):
        return self.get_districts('cng')

    @property
    def ga_house_maps(self):
        for d in self.ga_house_districts:
            yield HseMap.get_map(d)

    @property
    def ga_senate_maps(self):
        for d in self.ga_senate_districts:
            yield SenMap.get_map(d)

    @property
    def us_house_maps(self):
        for d in self.us_house_districts:
            yield CngMap.get_map(d)

    @property
    def vtds_maps(self):
        return VtdMap.get_vtd_maps(self.county_code)

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

    def get_choropleth(self, base_map, variables, labels=None, hover_data=None):
        if hover_data is None:
            hover_data = {}
        hover_data.update({'district': True, 'name': True, 'name_district': False})
        if labels is None:
            labels = {}
        labels.update({'name': 'Precinct Name'})
        base_map = base_map.drop(columns=['center'])
        gj = json.loads(base_map.to_json())
        center = base_map.to_crs(crs='epsg:3035').centroid.to_crs(crs='epsg:4326').iloc[0]
        lc = px.colors.hex_to_rgb(px.colors.sequential.Viridis[0])
        hc = px.colors.hex_to_rgb(px.colors.sequential.Viridis[-1])
        districts = sorted(base_map.district.unique())
        # colors = px.colors.n_colors(lc, hc, len(districts))
        # color_map = {k: webcolors.rgb_to_hex([int(vv) for vv in v]) for k, v in zip(districts, colors)}
        # print(color_map)
        fig = px.choropleth_mapbox(
            base_map,
            geojson=gj,
            color='district',
            locations='name_district',
            featureidkey="properties.name_district",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map", zoom=9.5,
            labels=labels,
            hover_data=hover_data,
            color_discrete_sequence=px.colors.qualitative.Alphabet
        )

        fig.update_layout(
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                traceorder="reversed",
                title_font_family="Times New Roman",
                title_font_color="black",
                font=dict(
                    family="Courier",
                    size=12,
                    color="black"
                ),
                bgcolor="white",
                bordercolor="Black",
                borderwidth=1
            ),
            annotations=[
                go.layout.Annotation(
                    text='Copyright November Pathways, 2022',
                    align='center',
                    showarrow=False,
                    yanchor="middle",
                    xanchor="center",
                    borderwidth=0,
                    font=dict(
                        family="Times New Roman",
                        size=48,
                        color="gray"
                    ),
                    opacity=0.5)
            ]
        )

        fig.update_layout(margin={"r": 0, "t": 30, "l": 0, "b": 30})
        return fig

    def get_districts(self, district_type):
        db = VoterDb()
        results = db.fetchall(f"""
            select distinct vd.{district_type} 
            from voter_{district_type} as vd
            join voter_precinct as vp on vp.voter_id = vd.voter_id
            join precinct_details as pd on vp.precinct_id = pd.id
            where county_code = '{self.county_code}'
        """)
        return [x[0] for x in results]

    def get_district_map(self, maps):
        vtd_maps = self.vtds_maps
        d_maps = []
        for m in maps:
            d_maps.append(m.overlay(vtd_maps[['name', 'geometry']], how='intersection'))
        gdf = gpd.GeoDataFrame(pd.concat(d_maps, ignore_index=True), crs='epsg:4326')
        gdf = gdf.assign(name_district=gdf.district + gdf.name)
        return gdf.sort_values(by='district', ascending=False)

    def get_ga_house_choropleth(self):
        d_map = self.get_district_map(self.ga_house_maps)
        return self.get_choropleth(d_map, None, {'district': "State House District"})

    def get_ga_senate_choropleth(self):
        d_map = self.get_district_map(self.ga_senate_maps)
        return self.get_choropleth(d_map, None, {'district': "State Senate District"})

    def get_us_house_choropleth(self):
        d_map = self.get_district_map(self.us_house_maps)
        return self.get_choropleth(d_map, None, {'district': "US House District"})

    class Meta:
        managed = False
        db_table = 'county_map'
