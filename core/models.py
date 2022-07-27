import pandas as pd
from django.db import models
from geopandas import GeoSeries
from geopandas import GeoDataFrame
from analytics.models import VoterPrecinct
from analytics.models import PrecinctDetails
import json
import plotly.express as px
import plotly.graph_objects as go
import geopandas as gpd


class BaseMap:
    CRS_METERS = 'epsg:3035'
    CRS_LAT_LON = 'epsg:4326'

    @classmethod
    def centroid(cls, gdf):
        return gdf.to_crs(crs=cls.CRS_METERS).centroid.to_crs(crs=cls.CRS_LAT_LON)

    @classmethod
    def from_wkb(cls, geometry_wkb):
        return GeoSeries.from_wkb(geometry_wkb, crs=cls.CRS_METERS).to_crs(crs=cls.CRS_LAT_LON)

    @classmethod
    def add_watermark(cls, fig):
        fig.update_layout(
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
        return fig

    @classmethod
    def configure_legend(cls, fig):
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
            )
        )
        return fig

    @classmethod
    def hide_legend(cls, fig):
        fig.update_layout(showlegend=False)
        return fig


class BaseMapModel(models.Model, BaseMap):
    @property
    def as_record(self):
        record = {'geometry': self.geometry,
                  'center': self.center}
        record.update(self.get_map_data_extensions())
        return record

    @property
    def as_geodataframe(self):
        return GeoDataFrame([self.as_record], crs=self.CRS_LAT_LON)

    @property
    def geometry(self):
        return self.from_wkb(pd.Series(self.geometry_wkb)).iloc[0]

    @property
    def center(self):
        try:
            return self.from_wkb(pd.Series(self.center_wkb)).iloc[0]
        except AttributeError as _:
            return self.centroid(GeoDataFrame(geometry=[self.geometry], crs=self.CRS_LAT_LON)).iloc[0]

    @classmethod
    def get_object(cls, map_id):
        raise NotImplemented('get_object is not implemented!')

    def get_map_data_extensions(self):
        return {}

    @classmethod
    def get_map(cls, map_id):
        o = cls.get_object(map_id)
        return o.as_geodataframe

    class Meta:
        abstract = True


class DistrictMapModelManager(models.Manager, BaseMap):
    def get_maps(self, counties, map_cls):
        """
        These maps are not clipped to county boundaries.
        :param counties:
        :param map_cls:
        :return:
        """
        if isinstance(counties, str):
            counties = (counties,)
        precinct_ids = [p.id for p in PrecinctDetails.objects.filter(county_code__in=counties)]
        voter_ids = [v.voter_id for v in VoterPrecinct.objects.filter(precinct_id__in=precinct_ids)]
        hses = [h.hse for h in map_cls.objects.filter(voter_id__in=voter_ids)]
        records = [o.as_record for o in self.filter(district__in=hses)]
        return gpd.GeoDataFrame(records, crs=self.CRS_LAT_LON)


class DistrictMapModel(BaseMapModel):
    id = models.IntegerField(primary_key=True)
    area = models.FloatField()
    district = models.TextField()
    population = models.IntegerField()
    ideal_value = models.FloatField()
    geometry_wkb = models.TextField()
    center_wkb = models.TextField()

    @property
    def counties(self):
        """
        TODO need to refactor so I can use Django ORM.
        :return:
        """
        precinct_ids = [x.precinct_id for x in VoterPrecinct.objects.filter(voter_id__in=self.voters)]

        return [x.county_code for x in PrecinctDetails.objects.filter(id__in=precinct_ids)]

    @property
    def voters(self):
        raise NotImplemented('voters not implemented!')

    @property
    def vtd_choropleth_map(self):
        dmap = self.as_geodataframe.drop(columns=['area'])
        vmaps = self.county_vtd_map[['vtd_id', 'county_code', 'precinct_name', 'geometry']]
        district_vtd_map = dmap.overlay(vmaps, how='intersection', keep_geom_type=True).drop(columns=['center'])
        area = district_vtd_map.to_crs(self.CRS_METERS).area
        district_vtd_map = district_vtd_map[area > 1000.0]
        gj = json.loads(district_vtd_map.to_json())
        center = self.centroid(district_vtd_map).iloc[0]
        fig = px.choropleth_mapbox(
            district_vtd_map,
            geojson=gj,
            color='vtd_id',
            locations='vtd_id',
            featureidkey="properties.vtd_id",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map", zoom=9.5,
            labels={'precinct_name': 'Precinct Name'},
            hover_data={'precinct_name': True,
                        'vtd_id': False},
            color_discrete_sequence=px.colors.qualitative.Alphabet
        )

        self.add_watermark(fig)
        # if len(district_vtd_map.index) > 25:
        #     self.hide_legend(fig)
        # else:
        #     self.configure_legend(fig)
        return fig

    @property
    def county_vtd_map(self):
        raise NotImplemented('county_vtd_map not implemented!')

    def get_map_data_extensions(self):
        return {'id': self.id,
                'area': self.area,
                'district': self.district,
                'population': self.population,
                'ideal_value': self.ideal_value}

    @classmethod
    def get_object(cls, map_id):
        map_id = map_id if isinstance(map_id, str) else f'{map_id:03d}'
        return cls.objects.get(district=map_id)

    class Meta:
        abstract = True
