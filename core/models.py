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
from shapely.geometry import MultiPolygon, Polygon
from election_results.models import Detail, OverUnderVote
from voter_demographics.models import VoterDemographics
from voter_status.models import VoterStatus
from collections import defaultdict


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
    def add_logo(cls, fig, source, x, y, xanchor, yanchor, sizex, sizey):
        fig.add_layout_image(
            dict(
                source=source,
                yanchor=yanchor,
                xanchor=xanchor,
                xref="paper",
                yref="paper",
                x=x,
                y=y,
                sizex=sizex,
                sizey=sizey,
                opacity=1.0)
        )

    @classmethod
    def add_watermark(cls, fig, description):
        fig.update_layout(
            annotations=[
                cls.build_map_description_annotation(description),
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

        :param map_id: a unique identifier for the map that can be used to retrieve it from the data base
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

    @classmethod
    def get_voters_for_counties(cls, counties):
        if isinstance(counties, str):
            counties = (counties,)
        precinct_ids = [p.id for p in PrecinctDetails.objects.filter(county_code__in=counties)]
        return [v.voter_id for v in VoterPrecinct.objects.filter(precinct_id__in=precinct_ids)]

    def get_maps(self, counties, map_cls):
        """
        These maps are not clipped to county boundaries.
        :param counties:
        :param map_cls:
        :return:
        """
        if isinstance(counties, str):
            counties = (counties,)
        voters = self.get_voters_for_counties(counties)
        districts = self.get_districts_for_voters(voters)
        records = [o.as_record for o in self.filter(district__in=districts)]
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
        Get a list of counties that this district overlaps.
        Counties are obtained by getting a list of voters
        in a district. Then I get a list of precincts.
        Finally, I get a list of counties the precincts
        are in.

        This should probably be in a abstract district
        superclass.
        TODO need to refactor so I can use Django ORM.
        :return: a list of county codes
        """
        precinct_ids = [x.precinct_id for x in VoterPrecinct.objects.filter(voter_id__in=self.voters)]
        precinct_ids = set(precinct_ids)

        return [x.county_code for x in PrecinctDetails.objects.filter(id__in=precinct_ids)]

    @property
    def demographics(self):
        voters = self.voters
        d = pd.DataFrame.from_records([x.as_record for x in VoterDemographics.objects.get_for_voter_ids(voters)])
        s = pd.DataFrame.from_records([x.as_record for x in VoterStatus.objects.get_for_voter_ids(voters)])
        d = d.merge(s[s.status == 'A'][['voter_id']], on='voter_id', how='inner')
        vp = pd.DataFrame.from_records([x.as_record for x in VoterPrecinct.objects.filter(voter_id__in=self.voters)])
        vp = vp.rename(columns={'precinct_id': 'id'})
        precinct_ids = vp.id.unique()
        details = pd.DataFrame.from_records([x.as_record for x in PrecinctDetails.objects.filter(id__in=precinct_ids)])
        vp = vp.merge(details, on='id', how='inner')
        d = d.merge(vp, on='voter_id', how='inner').drop(columns=['id'])
        return d

    @property
    def district_vtd_map(self):
        dmap = self.as_geodataframe.drop(columns=['area'])
        vmaps = self.county_vtd_map[['vtd_id', 'county_code', 'precinct_name', 'area', 'geometry']]
        vmaps = vmaps.rename(columns={'area': 'orig_area'})
        dmap = dmap.overlay(vmaps, how='intersection', keep_geom_type=True).drop(columns=['center'])
        dmap_m = dmap.to_crs(crs=self.CRS_METERS)
        dmap = dmap.assign(darea=dmap_m.area)
        return dmap

    @property
    def voters(self):
        """
        Return a list of voter_ids for the voters in this district
        :return:
        """
        raise NotImplemented('voters not implemented!')

    @property
    def vtd_choropleth_map(self):
        district_vtd_map = self.district_vtd_map
        area = district_vtd_map.to_crs(self.CRS_METERS).area
        district_vtd_map = district_vtd_map[area > 5000.0]
        gj = json.loads(district_vtd_map.to_json())
        center = self.centroid(district_vtd_map).iloc[0]
        fig = px.choropleth_mapbox(
            district_vtd_map,
            geojson=gj,
            color='precinct_name',
            locations='vtd_id',
            featureidkey="properties.vtd_id",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map", zoom=12,
            labels={'precinct_name': 'Precinct Name'},
            hover_data={'precinct_name': True,
                        'vtd_id': False}
        )

        self.add_watermark(fig)
        self.configure_legend(fig)
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
    def combiner(cls, df, drop=None, combine=None):
        for r in drop:
            df = df.drop(r)
        df = df.assign(p=df.index)
        for k, master in combine.items():
            df.at[k, 'p'] = master
        df = df.groupby(['p'], as_index=False).sum().reset_index(drop=True)
        df = df.rename(columns={'p': 'precinct_name'})
        return df

    def get_party_tally(self, election_date, drop=None, combine=None):
        drop = drop or []
        combine = combine or {}
        details = self.get_election_result_details(election_date)
        df = pd.DataFrame.from_records([d.as_record for d in details])
        df = pd.DataFrame(df.groupby(['party', 'precinct_name'], as_index=False)['votes'].sum())
        df = df.pivot(index='precinct_name', columns='party', values='votes')
        df = self.combiner(df, drop, combine)
        df.index.name = None
        return df

    @classmethod
    def affinity_decorator(cls, df):
        df = df.assign(r_affinity=df.R / (df.R + df.D) * 100, d_affinity=df.D / (df.R + df.D) * 100)
        return df

    def get_undervote(self, election_date, drop=None, combine=None, contest_mappings=None):
        drop = drop or []
        combine = combine or {}
        contest_mappings = contest_mappings or {}
        details = self.get_election_result_details(election_date)
        df = pd.DataFrame.from_records([d.as_record for d in details])
        contests = df.contest.unique()
        over_under_vote = [{
            'contest': ouv.contest.name,
            'precinct_name': ouv.precinct_name,
            'votes': ouv.undervotes
        } for ouv in OverUnderVote.objects.filter(contest__in=contests)]

        df = pd.DataFrame.from_records(over_under_vote)
        df = df.assign(contest=df.contest.apply(lambda x: contest_mappings.get(x) if x in contest_mappings else x))
        df = df.pivot(index='precinct_name', columns='contest', values='votes')
        df = self.combiner(df, drop, combine)
        df = df.rename(columns={'votes': 'undervotes'})
        df.index.name = None
        return df

    # gdf = gdf.assign(
    #     weighting=gdf.apply(lambda row: row.darea / row.orig_area if row.darea / row.orig_area < 0.95 else 1), axis=1)
    # for i in gdf.index:
    #     print(f'pname={gdf.precinct_name.loc[i]}, darea={gdf.darea.loc[i]}, '
    #           f'orig={gdf.orig_area.loc[i]}, fraction={gdf.darea.loc[i] / gdf.orig_area.loc[i]}')

    def get_party_tally_choropleth(self, election_date, drop=None, combine=None,
                                   contest_mappings=None, title=None, subtitle=None, logo=None):
        results = self.get_party_tally(election_date, drop=drop, combine=combine)
        undervote = self.get_undervote(election_date, drop=drop, combine=combine, contest_mappings=contest_mappings)
        df = results.merge(undervote, on='precinct_name', how='inner')
        df = df.assign(r_ballots=df.R_undervote + df.R, d_ballots=df.D_undervote + df.D)
        df = df.assign(r_affinity=df.r_ballots / (df.r_ballots + df.d_ballots) * 100,
                       d_affinity=df.d_ballots / (df.r_ballots + df.d_ballots) * 100)
        district_vtd_map = self.district_vtd_map
        gdf = district_vtd_map.merge(df, on='precinct_name', how='inner')
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
            labels={'precinct_name': 'Precinct Name',
                    'r_affinity': 'Republican<br>Party Affinity',
                    'R': 'Republican Votes',
                    'D': 'Democratic Votes',
                    'R_undervote': 'Republican Primary Under Votes',
                    'D_undervote': 'Democratic Primary Under Votes',
                    },
            hover_data={'precinct_name': True,
                        'r_affinity': ':.1f',
                        'R': True,
                        'D': True,
                        'R_undervote': True,
                        'D_undervote': True,
                        'vtd_id': False},
            color_continuous_scale='Plasma'
        )

        r_total = df.r_ballots.sum()
        r_candidate = df.R.sum()
        r_undervote = df.R_undervote.sum()
        d_total = df.d_ballots.sum()
        d_candidate = df.D.sum()
        d_undervote = df.D_undervote.sum()

        text = f"""
<b>{title}</b><br>
<i><b>{subtitle}</b></i><br>
<br>
<i>Republican Party Summary</i><br>
The total Republican ballots cast<br>
was {r_total}. The number of votes<br>
for Republican candidates in this<br>
contest was {r_candidate}. The number of<br>
undervotes in this contest was {r_undervote}.
<br><br>
<i>Democratic Party Summary</i><br>
The total Republican ballots cast<br>
was {d_total}. The number of votes<br>
for Republican candidates in this<br>
contest was {d_candidate}. The number of<br>
undervotes in this contest was {d_undervote}.
"""
        self.add_logo(fig, "https://johnbaileyforga.com/wp-content/uploads/2022/05/john-bailey-FINAL-logo.png",
                      0.98, 0.98, 'right', 'top', 0.1, 0.1)
        self.add_watermark(fig, text)
        fig.update_layout(margin={"r": 30, "t": 30, "l": 30, "b": 30})
        return fig

    def get_le_30_choropleth(self, year, drop=None, combine=None, title=None, subtitle=None):
        df = self.demographics[['precinct_id', 'year_of_birth']]
        df = df.assign(le_30=df.year_of_birth >= year - 30)
        df = df.groupby(['precinct_id', 'le_30'], as_index=False).size()
        df = df.pivot(index='precinct_id', columns='le_30', values='size')

        print(df)

    #     district_vtd_map = self.district_vtd_map
    #     gdf = district_vtd_map.merge(df, on='precinct_name', how='inner')
    #     gj = json.loads(gdf.to_json())
    #     center = self.centroid(gdf).iloc[0]
    #     fig = px.choropleth_mapbox(
    #         gdf,
    #         geojson=gj,
    #         color='r_affinity',
    #         locations='vtd_id',
    #         featureidkey="properties.vtd_id",
    #         center={"lat": center.y, "lon": center.x},
    #         opacity=0.5,
    #         mapbox_style="open-street-map", zoom=12,
    #         labels={'precinct_name': 'Precinct Name',
    #                 'r_affinity': 'Republican<br>Party Affinity',
    #                 'R': 'Republican Votes',
    #                 'D': 'Democratic Votes',
    #                 'R_undervote': 'Republican Primary Under Votes',
    #                 'D_undervote': 'Democratic Primary Under Votes',
    #                 },
    #         hover_data={'precinct_name': True,
    #                     'r_affinity': ':.1f',
    #                     'R': True,
    #                     'D': True,
    #                     'R_undervote': True,
    #                     'D_undervote': True,
    #                     'vtd_id': False},
    #         color_continuous_scale='Plasma'
    #     )
    #
    #     r_total = df.r_ballots.sum()
    #     r_candidate = df.R.sum()
    #     r_undervote = df.R_undervote.sum()
    #     d_total = df.d_ballots.sum()
    #     d_candidate = df.D.sum()
    #     d_undervote = df.D_undervote.sum()
    #
    #     text = f"""
    # <b>{title}</b><br>
    # <i><b>{subtitle}</b></i><br>
    # <br>
    # <i>Republican Party Summary</i><br>
    # The total Republican ballots cast<br>
    # was {r_total}. The number of votes<br>
    # for Republican candidates in this<br>
    # contest was {r_candidate}. The number of<br>
    # undervotes in this contest was {r_undervote}.
    # <br><br>
    # <i>Democratic Party Summary</i><br>
    # The total Republican ballots cast<br>
    # was {d_total}. The number of votes<br>
    # for Republican candidates in this<br>
    # contest was {d_candidate}. The number of<br>
    # undervotes in this contest was {d_undervote}.
    # """
    #
    #     extra_annotations = [
    #         go.layout.Annotation(
    #             text=text,
    #             align='left',
    #             showarrow=False,
    #             x=0,
    #             y=1,
    #             yanchor="top",
    #             xanchor="left",
    #             bgcolor="white",
    #             bordercolor="Black",
    #             borderwidth=1,
    #             borderpad=6,
    #             font=dict(
    #                 family="Times New Roman",
    #                 size=12,
    #                 color="gray"
    #             )
    #         )
    #     ]
    #     self.add_watermark(fig, annotations=extra_annotations)
    #     fig.update_layout(margin={"r": 30, "t": 30, "l": 30, "b": 30})
    #     return fig

    def check_vtd_precinct(self, election_date):
        vtd_names = set(self.district_vtd_map[['precinct_name']].precinct_name.unique())
        details = self.get_election_result_details(election_date)
        precincts = set([x.precinct_name for x in details])
        missing = precincts - vtd_names
        votes = defaultdict(int)
        missing_details = filter(lambda d: d.precinct_name in missing, [d for d in details])
        for m in missing_details:
            votes[m.precinct_name] += m.votes
        print(votes)

    class Meta:
        abstract = True
