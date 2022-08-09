import pandas as pd
from django.db import models
from geopandas import GeoSeries
from geopandas import GeoDataFrame

import json
import plotly.express as px
import plotly.graph_objects as go
from shapely.geometry import MultiPolygon, Polygon
from election_results.models import Detail, OverUnderVote
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from string import Template
from voter_history.models import VoterHistory
from segmentation.precinct_segmentation import PrecinctSegmentation
from segmentation.utils import categorize_age


class MapConfig:
    def __init__(self, path):
        with Path(path).open('r') as f:
            self.config = json.load(f)

    @property
    def color(self):
        return self.config['color']

    @property
    def combine(self):
        return self.config['combine']

    @property
    def date(self):
        return datetime.strptime(self.config['date'], '%Y-%m-%d')

    @property
    def description(self):
        d = self.config['description']
        if isinstance(d, str):
            return d
        return '<br>'.join(d)

    @property
    def drop(self):
        return self.config['drop']

    @property
    def hover_data(self):
        return self.config['hover_data']

    @property
    def labels(self):
        return self.config['labels']

    @property
    def logo_sizex(self):
        return self.config['logo']['sizex']

    @property
    def logo_sizey(self):
        return self.config['logo']['sizey']

    @property
    def logo_source(self):
        return self.config['logo']['source']

    @property
    def logo_x(self):
        return self.config['logo']['x']

    @property
    def logo_xanchor(self):
        return self.config['logo']['xanchor']

    @property
    def logo_y(self):
        return self.config['logo']['y']

    @property
    def logo_yanchor(self):
        return self.config['logo']['yanchor']


class PartyTallyMapConfig(MapConfig):
    @property
    def contest_mappings(self):
        return self.config['contest_mappings']


class BaseMap:
    CRS_METERS = 'epsg:3035'
    CRS_LAT_LON = 'epsg:4326'

    @classmethod
    def centroid(cls, gdf):
        return gdf.to_crs(crs=cls.CRS_METERS).centroid.to_crs(crs=cls.CRS_LAT_LON)

    @classmethod
    def from_wkb(cls, geometry_wkb):
        """
        Convert a well known binaary to a geometry. Then
        convert the geometry to a multipolygon if it
        is a polygon. See docs for to_multipolygon_if
        :param geometry_wkb: a geometry in well known binary form
        :return: a geoseries
        """
        # overlay only works with uniform types
        return GeoSeries.from_wkb(geometry_wkb, crs=cls.CRS_METERS).to_crs(crs=cls.CRS_LAT_LON). \
            apply(cls.to_multipolygon_if)

    @classmethod
    def build_map_description_annotation(cls, text):
        return go.layout.Annotation(
            text=text,
            align='left',
            showarrow=False,
            x=0,
            y=1,
            yanchor="top",
            xanchor="left",
            bgcolor="white",
            bordercolor="Black",
            borderwidth=1,
            borderpad=6,
            font=dict(
                family="Times New Roman",
                size=12,
                color="gray"
            )
        )

    @classmethod
    def add_logo(cls, fig, config):
        fig.add_layout_image(
            dict(
                source=config.logo_source,
                yanchor=config.logo_yanchor,
                xanchor=config.logo_xanchor,
                xref="paper",
                yref="paper",
                x=config.logo_x,
                y=config.logo_y,
                sizex=config.logo_sizex,
                sizey=config.logo_sizey,
                opacity=1.0)
        )

    @classmethod
    def add_watermark(cls, fig, description=None):
        fig.add_layout_image(
            dict(
                source='https://novemberpathways.com/maps/novemberpathways.png',
                yanchor='bottom',
                xanchor='left',
                xref="paper",
                yref="paper",
                x=.02,
                y=.02,
                sizex=.15,
                sizey=.15,
                opacity=1.0)
        )

        if description is not None:
            fig.update_layout(annotations=[cls.build_map_description_annotation(description)])

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

    @classmethod
    def to_multipolygon_if(cls, p):
        """
        According to the docs for overlay, overlay only
        works if all the shape types are the same.
        So, I have this utility method to convert
        polygons to Multipolygons when present.
        There should be no loss of information.
        :param p: a geometry object
        :return: a multipolygon if p is a polygon; otherwise, p
        """
        # if it is a polygon -- ignore everything else
        if isinstance(p, Polygon):
            return MultiPolygon([p])
        return p

    class Meta:
        abstract = True


class BaseMapModelManager(models.Manager, BaseMap):
    def get_map(self, map_id):
        """
        Return a map as a geodataframe.

        :param map_id: a unique identifier for the map that
        can be used to retrieve it from the database
        :return: a map as a geodataframe
        """
        raise NotImplemented('get_map is not implemented!')


class BaseMapModel(models.Model, BaseMap):
    @property
    def as_record(self):
        return {'geometry': self.geometry,
                'center': self.center}

    @property
    def as_geodataframe(self):
        return GeoDataFrame([self.as_record], crs=self.CRS_LAT_LON)

    @property
    def geometry(self):
        """
        Return a geometry versus a well known binary. Note
        that from_wkb only works for a series, which means
        I can use if on a dataframe as well.
        :return:
        """
        return self.from_wkb(pd.Series(self.geometry_wkb)).iloc[0]

    @property
    def center(self):
        """
        Return a center versus a well known binary. Note
        that from_wkb only works for a series, which means
        I can use if on a dataframe as well. If a center
        is not available, then compute one.
        :return: the center of the object in lat,lon.
        """
        try:
            return self.from_wkb(pd.Series(self.center_wkb)).iloc[0]
        except AttributeError as _:
            return self.centroid(GeoDataFrame(geometry=[self.geometry], crs=self.CRS_LAT_LON)).iloc[0]

    class Meta:
        abstract = True


class DistrictMapModelManager(BaseMapModelManager):
    @classmethod
    def get_districts_for_voters(cls, voter_ids):
        raise NotImplemented('get_districts_for_voters is not implemented!')

    def get_map(self, district):
        district = district if isinstance(district, str) else f'{district:03d}'
        return self.get(district=district).as_geodataframe


class DistrictMapModel(BaseMapModel):
    id = models.IntegerField(primary_key=True)
    area = models.FloatField()
    district = models.TextField()
    population = models.IntegerField()
    ideal_value = models.FloatField()
    geometry_wkb = models.TextField()
    center_wkb = models.TextField()

    @property
    def as_record(self):
        """
        Return this object in record form, i.e. suitable for use
        in pd.DataFrame.from_records method. Note this is A
        record. It would need to be put in a list for
        from_records.
        :return:
        """
        record = super().as_record
        record.update({'id': self.id,
                       'area': self.area,
                       'district': self.district,
                       'population': self.population,
                       'ideal_value': self.ideal_value})
        return record

    @property
    def counties(self):
        """
        Get a list of county_codes that this district overlaps.
        Counties are obtained by getting a list of voters
        and then constructing a set of counties in which
        the voters reside. I use a set to ensure that I
        have a unique set of counties.

        :return: a list of counties
        """
        return set([x.county.county_code for x in self.voters])

    @property
    def edition(self):
        try:
            return self.edition_
        except AttributeError:
            return None

    @property
    def demographics(self):
        df = pd.DataFrame.from_records(
            [{
                'voter_id': x.voter_id,
                'county_code': x.county.county_code,
                'precinct_id': x.precinct.precinct_id,
                'race_id': x.race_id,
                'gender': x.gender,
                'year_of_birth': x.year_of_birth
            } for x in self.voters]
        )
        return df.assign(gen=categorize_age(df.year_of_birth))

    @property
    def district_vtd_map(self):
        dmap = self.as_geodataframe.drop(columns=['area'])
        vmaps = self.county_vtd_map[['vtd_id', 'county_code', 'precinct_id', 'area', 'geometry']]
        vmaps = vmaps.rename(columns={'area': 'orig_area'})
        dmap = dmap.overlay(vmaps, how='intersection', keep_geom_type=True).drop(columns=['center'])
        dmap_m = dmap.to_crs(crs=self.CRS_METERS)
        dmap = dmap.assign(darea=dmap_m.area)
        return dmap[dmap.darea > 5000.0]

    @edition.setter
    def edition(self, value):
        self.edition_ = value

    def get_vtd_choropleth(self, config_path):
        config = MapConfig(config_path)
        district_vtd_map = self.district_vtd_map
        gj = json.loads(district_vtd_map.to_json())
        center = self.centroid(district_vtd_map).iloc[0]
        fig = px.choropleth_mapbox(
            district_vtd_map,
            geojson=gj,
            color='precinct_id',
            locations='vtd_id',
            featureidkey="properties.vtd_id",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map", zoom=12,
            labels=config.labels,
            hover_data=config.hover_data
        )

        self.add_watermark(fig)
        self.configure_legend(fig)
        self.add_logo(fig, config)
        fig.update_layout(margin={"r": 30, "t": 30, "l": 30, "b": 30})
        return fig

    @property
    def county_vtd_map(self):
        """
        Return a district map with VTD boundaries. VTDs
        are clipped to the district boundary
        :return: a geodataframe of a district map with VTD boundaries
        """
        raise NotImplemented('county_vtd_map not implemented!')

    def get_election_result_details(self, election_date):
        return Detail.objects.get_results(self.contest_category, self.district, election_date)

    @classmethod
    def combiner(cls, df, config: MapConfig):
        for r in config.drop:
            df = df.drop(r, errors='ignore')
        df = df.assign(precinct_id=df.precinct_id.apply(lambda x: config.combine.get(x, x)))
        df = df.groupby(['precinct_id'], as_index=False).sum()
        return df

    @classmethod
    def collapse_precincts(cls, df, config):
        for r in config.drop:
            df = df.drop(r, errors='ignore')
        df = df.assign(precinct_id=df.precinct_id.apply(lambda x: config.combine.get(x, x)))
        return df

    @classmethod
    def get_party_demographics(cls, df, party):
        df = df[df.party == party]
        df_age = df.assign(age=(2022 - df.year_of_birth))
        df_age = df_age[['precinct_id', 'age']].groupby(['precinct_id'], as_index=False).median()
        df_sum = PrecinctSegmentation.summarize(df, 2022)
        df = df_age.merge(df_sum, on='precinct_id', how='inner')
        columns = list(df.columns)
        columns.remove('precinct_id')
        columns = {x: x + f'_{party.lower()}' for x in columns}
        df = df.rename(columns=columns)
        return df

    def get_primary_demographics(self, config):
        voters = [x.voter_id for x in self.voters]
        vh = VoterHistory.objects.get_for(config.date, voters)[['voter_id', 'party']]
        df_orig = self.demographics
        df_orig = df_orig.merge(vh, on='voter_id', how='inner')
        df_orig = self.collapse_precincts(df_orig, config)
        df_r = self.get_party_demographics(df_orig, 'R')
        df_d = self.get_party_demographics(df_orig, 'D')
        df = df_r.merge(df_d, on='precinct_id', how='inner')
        df_age = df_orig.assign(age=(2022 - df_orig.year_of_birth))
        df_age = df_age[['precinct_id', 'age']].groupby(['precinct_id'], as_index=False).median()
        df = df_age.merge(df, on='precinct_id', how='inner')
        df = df.assign(total_rd=df.total_r-df.total_d)
        return df

    def get_party_tally(self, config: MapConfig):
        election_date = config.date
        df = self.get_election_result_details(election_date)
        df = self.collapse_precincts(df, config)
        df = df.groupby(['party', 'precinct_id'], as_index=False)['votes'].sum()
        df = df.pivot(index='precinct_id', columns='party', values='votes').reset_index()
        df.columns.name = None
        df.index.name = None
        return df

    def get_undervote(self, config):
        election_date = config.date
        contest_mappings = config.contest_mappings
        df = self.get_election_result_details(election_date)
        contests = df.contest.unique()
        df = OverUnderVote.objects.get_for_contests(contests)
        df = df.assign(contest=df.contest.apply(lambda x: contest_mappings.get(x) if x in contest_mappings else x))
        df = df.pivot(index='precinct_id', columns='contest', values='undervotes').reset_index()
        df = self.collapse_precincts(df, config)
        df = df.groupby(['precinct_id'], as_index=False).sum()
        df.index.name = None
        return df

    # gdf = gdf.assign(
    #     weighting=gdf.apply(lambda row: row.darea / row.orig_area if row.darea / row.orig_area < 0.95 else 1), axis=1)
    # for i in gdf.index:
    #     print(f'pname={gdf.precinct_id.loc[i]}, darea={gdf.darea.loc[i]}, '
    #           f'orig={gdf.orig_area.loc[i]}, fraction={gdf.darea.loc[i] / gdf.orig_area.loc[i]}')

    def get_party_tally_choropleth(self, config_path):
        config = PartyTallyMapConfig(config_path)
        results = self.get_party_tally(config)
        undervote = self.get_undervote(config)
        df = results.merge(undervote, on='precinct_id', how='inner')
        df = df.assign(r_ballots=df.R_undervote + df.R, d_ballots=df.D_undervote + df.D)
        df = df.assign(r_affinity=df.r_ballots / (df.r_ballots + df.d_ballots) * 100,
                       d_affinity=df.d_ballots / (df.r_ballots + df.d_ballots) * 100)
        district_vtd_map = self.district_vtd_map
        gdf = district_vtd_map.merge(df, on='precinct_id', how='inner')
        gj = json.loads(gdf.to_json())
        center = self.centroid(gdf).iloc[0]
        fig = px.choropleth_mapbox(
            gdf,
            geojson=gj,
            color='r_affinity',
            locations='vtd_id',
            featureidkey="properties.vtd_id",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map", zoom=12,
            labels=config.labels,
            hover_data=config.hover_data,
            color_continuous_scale='Plasma'
        )

        r_total = df.r_ballots.sum()
        r_candidate = df.R.sum()
        r_undervote = df.R_undervote.sum()
        d_total = df.d_ballots.sum()
        d_candidate = df.D.sum()
        d_undervote = df.D_undervote.sum()

        s = Template(config.description)
        text = s.substitute(r_total=r_total, r_candidate=r_candidate, r_undervote=r_undervote,
                            d_total=d_total, d_candidate=d_candidate, d_undervote=d_undervote)
        self.add_logo(fig, config)
        self.add_watermark(fig, text)
        fig.update_layout(margin={"r": 30, "t": 30, "l": 30, "b": 30})
        return fig

    def get_demographics_choropleth(self, config_path):
        config = MapConfig(config_path)
        df = self.demographics
        df = self.collapse_precincts(df, config)
        df = PrecinctSegmentation.summarize(df, 2022)
        return self.get_demographics_choropleth_for_data(df, config)

    def get_primary_demographics_choropleth(self, config_path):
        config = MapConfig(config_path)
        df = self.get_primary_demographics(config)
        return self.get_demographics_choropleth_for_data(df, config)

    def get_demographics_choropleth_for_data(self, df, config):
        district_vtd_map = self.district_vtd_map
        gdf = district_vtd_map.merge(df, on='precinct_id', how='inner')
        gj = json.loads(gdf.to_json())
        center = self.centroid(gdf).iloc[0]
        fig = px.choropleth_mapbox(
            gdf,
            geojson=gj,
            color=config.color,
            locations='vtd_id',
            featureidkey="properties.vtd_id",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map", zoom=12,
            labels=config.labels,
            hover_data=config.hover_data,
            color_continuous_scale='Plasma'
        )
        self.add_logo(fig, config)
        self.add_watermark(fig, config.description)
        fig.update_layout(margin={"r": 30, "t": 30, "l": 30, "b": 30})
        return fig

    def check_vtd_precinct(self, election_date):
        vtd_names = set(self.district_vtd_map.precinct_id.unique())
        details = self.get_election_result_details(election_date)
        precincts = set(details.precinct_id.unique())
        missing = precincts - vtd_names
        votes = defaultdict(int)
        missing_details = details[details.precinct_id.isin(missing)]
        for i in missing_details.index:
            row = missing_details.loc[i]
            votes[row.precinct_id] += row.votes
        print(votes)
        return votes

    class Meta:
        abstract = True
