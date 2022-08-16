from django.db import models
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from string import Template
import pandas as pd
import geopandas as gpd

import json
import plotly.express as px

from core.base_fig import BaseMapModelManager, BaseMapModel
from election_results.models import Detail, OverUnderVote
from segmentation.utils import categorize_age
from voter_history.models import VoterHistory
from segmentation.precinct_segmentation import PrecinctSegmentation


class MapConfig:
    def __init__(self, path):
        p = Path(path)
        defaults_path = Path(p.parent.parent, 'defaults', p.name)
        # Load defaults first
        try:
            with Path(defaults_path).open('r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            self.config = {}

        # Load overrides
        with Path(p).open('r') as f:
            self.config.update(json.load(f))

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
        description = '<br>'.join(d)
        subtitle = self.config.get('subtitle', None)
        if subtitle:
            description = Template(description)
            description = description.substitute(subtitle=subtitle)
        return description

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
        logo_source = self.config['logo']['source']
        logo_path = self.config.get('logo_path', None)
        if logo_path:
            logo_source = Template(self.config['logo']['source'])
            logo_source = logo_source.substitute(logo_path=logo_path)
        return logo_source

    @logo_source.setter
    def logo_source(self, value):
        self.config['logo']['source'] = value


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
    def as_geodataframe(self):
        return gpd.GeoDataFrame([self.as_record], crs=self.CRS_LAT_LON)

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
        try:
            return self.counties_
        except AttributeError:
            self.counties_ = set([x.county.county_code for x in self.voters])
        return self.counties_

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
                'precinct_short_name': x.precinct.precinct_short_name,
                'race_id': x.race_id,
                'gender': x.gender,
                'year_of_birth': x.year_of_birth
            } for x in self.voters]
        )
        return df.assign(gen=categorize_age(df.year_of_birth))

    @property
    def district_vtd_map(self):
        dmap = self.as_geodataframe.drop(columns=['area'])
        vmaps = self.precinct_map
        vmaps = vmaps[['pid', 'county_code', 'precinct_short_name', 'geometry']]
        dmap = dmap.overlay(vmaps, how='intersection', keep_geom_type=True)
        dmap_m = dmap.to_crs(crs=self.CRS_METERS)
        dmap = dmap.assign(darea=dmap_m.area)
        return dmap[dmap.darea > 5000.0]

    @edition.setter
    def edition(self, value):
        self.edition_ = value

    @property
    def precincts(self):
        try:
            return self.precincts_
        except AttributeError:
            self.precincts_ = set([v.precinct for v in self.voters])
            return self.precincts_

    @property
    def voters(self):
        try:
            return self.voters_
        except AttributeError:
            if self.edition is None:
                raise AttributeError('edition not set!')
            self.voters_ = list(self.get_voters_())
            return self.voters_

    @property
    def wkb_crs(self):
        return self.CRS_METERS

    def get_vtd_choropleth(self, config_path):
        self.reset_figure()
        config = MapConfig(config_path)
        district_vtd_map = self.district_vtd_map
        gj = json.loads(district_vtd_map.to_json())
        center = self.centroid(district_vtd_map).iloc[0]
        fig = px.choropleth_mapbox(
            district_vtd_map,
            geojson=gj,
            color='precinct_short_name',
            locations='pid',
            featureidkey="properties.pid",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map", zoom=12,
            labels=config.labels,
            hover_data=config.hover_data
        )

        self.add_watermark(fig)
        self.configure_legend(fig)
        self.add_logo(fig, config)
        self.add_margin(fig)
        return fig

    def get_county_precinct_map(self):
        """
        Return a district map with VTD boundaries. VTDs
        are clipped to the district boundary
        :return: a geodataframe of a district map with VTD boundaries
        """
        raise NotImplemented('get_county_precinct_map not implemented!')

    def get_election_result_details(self, election_date):
        return Detail.objects.get_results(self.contest_category, self.district, election_date)

    @classmethod
    def combiner(cls, df, config: MapConfig):
        for r in config.drop:
            df = df.drop(r, errors='ignore')
        df = df.assign(precinct_short_name=df.precinct_short_name.apply(lambda x: config.combine.get(x, x)))
        df = df.groupby(['precinct_short_name'], as_index=False).sum()
        return df

    @classmethod
    def collapse_precincts(cls, df, config):
        for r in config.drop:
            df = df.drop(r, errors='ignore')
        df = df.assign(precinct_short_name=df.precinct_short_name.apply(lambda x: config.combine.get(x, x)))
        return df

    @classmethod
    def get_party_demographics(cls, df, party):
        df = df[df.party == party]
        df_age = df.assign(age=(2022 - df.year_of_birth))
        df_age = df_age[['precinct_short_name', 'age']].groupby(['precinct_short_name'], as_index=False).median()
        df_sum = PrecinctSegmentation.summarize(df, 2022)
        df = df_age.merge(df_sum, on='precinct_short_name', how='inner')
        columns = list(df.columns)
        columns.remove('precinct_short_name')
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
        df = df_r.merge(df_d, on='precinct_short_name', how='inner')
        df_age = df_orig.assign(age=(2022 - df_orig.year_of_birth))
        df_age = df_age[['precinct_short_name', 'age']].groupby(['precinct_short_name'], as_index=False).median()
        df = df_age.merge(df, on='precinct_short_name', how='inner')
        df = df.assign(total_rd=df.total_r - df.total_d)
        df = df.assign(total_rd1=df.total_rd)
        return df

    def get_party_tally(self, config: MapConfig):
        election_date = config.date
        df = self.get_election_result_details(election_date)
        df = self.collapse_precincts(df, config)
        df = df.groupby(['party', 'precinct_short_name'], as_index=False)['votes'].sum()
        df = df.pivot(index='precinct_short_name', columns='party', values='votes').reset_index()
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
        df = df.pivot(index='precinct_short_name', columns='contest', values='undervotes').reset_index()
        df = self.collapse_precincts(df, config)
        df = df.groupby(['precinct_short_name'], as_index=False).sum()
        df.index.name = None
        return df

    # gdf = gdf.assign(
    #     weighting=gdf.apply(lambda row: row.darea / row.orig_area if row.darea / row.orig_area < 0.95 else 1), axis=1)
    # for i in gdf.index:
    #     print(f'pname={gdf.precinct_short_name.loc[i]}, darea={gdf.darea.loc[i]}, '
    #           f'orig={gdf.orig_area.loc[i]}, fraction={gdf.darea.loc[i] / gdf.orig_area.loc[i]}')

    def get_party_tally_choropleth(self, config_path):
        self.reset_figure()
        config = PartyTallyMapConfig(config_path)
        results = self.get_party_tally(config)
        undervote = self.get_undervote(config)
        df = results.merge(undervote, on='precinct_short_name', how='inner')
        df = df.assign(r_ballots=df.R_undervote + df.R, d_ballots=df.D_undervote + df.D)
        df = df.assign(r_affinity=df.r_ballots / (df.r_ballots + df.d_ballots) * 100,
                       d_affinity=df.d_ballots / (df.r_ballots + df.d_ballots) * 100)
        df = df.assign(r_affinity1=df.r_affinity)
        district_vtd_map = self.district_vtd_map
        gdf = district_vtd_map.merge(df, on='precinct_short_name', how='inner')
        gj = json.loads(gdf.to_json())
        center = self.centroid(gdf).iloc[0]
        fig = px.choropleth_mapbox(
            gdf,
            geojson=gj,
            color='r_affinity1',
            locations='pid',
            featureidkey="properties.pid",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map",
            zoom=12,
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
        self.add_watermark(fig)
        self.add_legend_text(fig, text)
        self.add_margin(fig)
        return fig

    def get_demographics_choropleth(self, config_path):
        config = MapConfig(config_path)
        df = self.demographics
        df = self.collapse_precincts(df, config)
        df = PrecinctSegmentation.summarize(df, 2022)
        df = df.assign(s_b_gx1=df.s_b_gx)
        return self.get_demographics_choropleth_for_data(df, config)

    def get_primary_demographics_choropleth(self, config_path):
        config = MapConfig(config_path)
        df = self.get_primary_demographics(config)
        return self.get_demographics_choropleth_for_data(df, config)

    def get_demographics_choropleth_for_data(self, df, config):
        self.reset_figure()
        district_vtd_map = self.district_vtd_map
        gdf = district_vtd_map.merge(df, on='precinct_short_name', how='inner')
        gj = json.loads(gdf.to_json())
        center = self.centroid(gdf).iloc[0]
        fig = px.choropleth_mapbox(
            gdf,
            geojson=gj,
            color=config.color,
            locations='pid',
            featureidkey="properties.pid",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map", zoom=12,
            labels=config.labels,
            hover_data=config.hover_data,
            color_continuous_scale='Plasma'
        )
        self.add_logo(fig, config)
        self.add_watermark(fig)
        self.add_legend_text(fig, config.description)
        self.add_margin(fig)
        return fig

    def check_vtd_precinct(self, election_date):
        vtd_names = set(self.district_vtd_map.precinct_short_name.unique())
        print(vtd_names)
        details = self.get_election_result_details(election_date)
        precincts = set(details.precinct_short_name.unique())
        print(precincts)
        missing = precincts - vtd_names
        print(missing)
        votes = defaultdict(int)
        missing_details = details[details.precinct_short_name.isin(missing)]
        for i in missing_details.index:
            row = missing_details.loc[i]
            votes[row.precinct_short_name] += row.votes
        print(votes)
        return votes

    class Meta:
        abstract = True
