from string import Template

import pandas as pd
import geopandas as gpd
import json
import plotly.express as px
from plotly.graph_objs import Figure

from core.base_fig import BaseMap
from geocode.models import Address
from segmentation.voter_segmentation import VoterSegmentation
from voter.models import ResidenceAddress


class VoterFilter:
    def filter(self, voters):
        return voters


class Walklist(BaseMap):
    """
    The Walklist class combines geocoded addresses and voters
    in a map visualization. The objective is to provide
    the viewer with a visual represention of the voter
    location that will be useful to the cavasser as well
    as in canvasing planning. Several possibilities
    emerge in this context. I can present the location
    of scored voters as well as scored addresses. I can
    also cluster addresses for the purpose of assignment
    to canvassing teams.
    """
    def __init__(self, district, voter_filter=VoterFilter()):
        """
        Create a walklist instance for a given district
        and voter filter. The voter filter will reduce
        the district voters to a selected set of voters.
        :param district: a district map model
        :param voter_filter: a voter filter.
        """
        self.district = district
        self.voter_filter = voter_filter

    @property
    def score(self) -> pd.DataFrame:
        """
        Return a score for voters in the district,
        where a score indicates whether a voter
        leans left or right and by how much.
        :return: a data frame
        """
        return self.score_voters(self.district.voters)

    @property
    def voter_details(self) -> pd.DataFrame:
        """
        Return voter details with respect to demogrpahics,
        address and precinct.
        :return: a data frame
        """
        voters = self.voter_filter.filter(self.district.voters)
        precincts = {p: {'precinct_id': p.id,
                         'precinct_short_name': p.precinct_short_name}
                     for p in self.district.precincts}
        address_df = pd.DataFrame.from_records(
            [{'voter': a.voter,
              'address': a
              } for a in ResidenceAddress.objects.filter(voter__in=voters)
             ])
        voter_df = pd.DataFrame.from_records(
            [{
                'voter': v,
                'voter_id': v.voter_id,
                'name': f'{v.first_name} {v.last_name}',
                'year_of_birth': v.year_of_birth,
                'race_id': v.race_id,
                'precinct': v.precinct
            } for v in voters]
        )

        df = voter_df.merge(address_df, on='voter', how='inner')
        df = df.assign(street=df.address.apply(lambda a: a.street),
                       apt_no=df.address.apply(lambda a: a.apt_no),
                       city=df.address.apply(lambda a: a.city),
                       state=df.address.apply(lambda a: a.state),
                       zipcode=df.address.apply(lambda a: a.zipcode),
                       precinct_short_name=df.precinct.apply(lambda p: precincts[p]['precinct_short_name']),
                       precinct_id=df.precinct.apply(lambda p: precincts[p]['precinct_id']),
                       key=df.address.apply(lambda a: Address.make_key(a)),
                       ).drop(columns=['address', 'precinct'])
        return df

    @property
    def voter_data(self) -> pd.DataFrame:
        """
        Return voter data which combines voter details and
        geocoding info.
        TODO should I combine with voter details
        :return: a data frame
        """
        voter_details = self.voter_details
        gc = Address.objects.get_addresses_for_counties(self.district.counties)
        gc = gc[['lat', 'lon', 'accuracy_score', 'accuracy_type', 'plus4',
                 'building_name', 'record_type_description', 'key']]

        voter_details = voter_details.merge(gc, on='key', how='inner')
        return voter_details

    @property
    def address_score(self) -> pd.DataFrame:
        """
        Summarize voter details at the address level.
        An address score does not return voter names,
        but rather returns aggregate details about
        the voters at an address, i.e., median score,
        number of voters at the address, number of
        voters at the address with a primary
        history.
        :return: a data frame
        """
        data = self.voter_data
        score_details = data[['voter_id', 'key']].merge(self.score_voters(data.voter.unique()),
                                                        on='voter_id', how='inner')[['key', 'ra']]
        summary_score = score_details.groupby(['key'], as_index=False)['ra'].median()
        summary_score = summary_score.rename(columns={'ra': 'score'})
        voter_sample_count = score_details.groupby(['key'], as_index=False)['ra'].count()
        voter_sample_count = voter_sample_count.rename(columns={'ra': 'voter_sample_count'})
        voter_count = score_details.groupby(['key'], as_index=False)['ra'].size()
        voter_count = voter_count.rename(columns={'size': 'voter_count'})
        summary_score = summary_score.merge(voter_sample_count, on='key', how='inner')
        summary_score = summary_score.merge(voter_count, on='key', how='inner')
        # TODO do I gain anything by doing this?
        summary_data = data[['lat', 'lon', 'accuracy_score', 'accuracy_type',
                             'building_name', 'record_type_description',
                             'street', 'city', 'state', 'zipcode', 'plus4',
                             'precinct_short_name', 'key']].drop_duplicates(subset='key')
        summary_data = summary_data.merge(summary_score, on='key', how='inner')
        summary_data = summary_data.assign(
            population_count_display_size=summary_data.population_count.apply(lambda c: min(c, 5)))
        return summary_data

    @property
    def voter_score(self) -> pd.DataFrame:
        """
        Return a voter score that combines voter details,
        geocode information, and score.
        :return: a data frame
        """
        data = self.voter_data
        score_details = data.merge(self.score_voters(data.voter.unique()), on='voter_id', how='inner')
        score_details = score_details.rename(columns={'ra': 'score'})
        score_details = score_details.assign(score_str=score_details.score.apply(self.format_score))
        sample_count = score_details[['key', 'score']].groupby(['key'], as_index=False)['score'].count()
        sample_count = sample_count.rename(columns={'score': 'sample_count'})
        score_details = score_details.merge(sample_count, on='key', how='inner')
        score_details = score_details.assign(
            sample_count_display_size=score_details.sample_count.apply(lambda c: min(c, 5)))
        score_details = score_details.assign(hovertext=score_details.apply(self.format_voter_hovertext, axis=1))
        # TODO do I gain anything by doing this?
        score_details = score_details[['lat', 'lon', 'accuracy_score', 'accuracy_type',
                                       'building_name', 'record_type_description',
                                       'name', 'sample_count', 'sample_count_display_size', 'street', 'city',
                                       'state', 'zipcode', 'plus4', 'precinct_short_name',
                                       'hovertext', 'key']]
        return score_details


    def format_voter_hovertext(self, row):
        """
        Provide formatted hover text for the
        given row of voter info.
        :param row:
        :return:
        """
        template_text = '<b>$precinct</b><br>' \
                        '<b><i>$name [Score: $score]</i></b><br>' \
                        '$more_voters' \
                        '$street $apt_no<br>' \
                        '$city $state $zipcode' \
                        '$building_name'
        # TODO Sample count may never be 0
        more_voters = f'{row.sample_count} voter(s) at address<br>' if row.sample_count > 0 else '<br>'
        # Most location do not have a building.
        building_name = f'<br>{row.building_name}' if len(row.building_name) > 0 else ''
        apt_no = row.apt_no if row.apt_no is not None else ''
        s = Template(template_text)
        # row.name returns the index value so use row['name']
        text = s.substitute(precinct=row.precinct_short_name,
                            name=row['name'], score=row.score_str,
                            more_voters=more_voters, street=row.street, apt_no=apt_no,
                            city=row.city, state=row.state, zipcode=row.zipcode,
                            building_name=building_name)
        return text

    def format_score(self, s):
        """
        Return a formatted score. Negative scores
        are surrounded by parenthesis. Positive
        scores have a + prefix. Zero scores are
        unadorned. Raw scores range from 0-1.
        The displayed score ranges from -5 - +5.
        Negative values lean left. Positive values
        lean right.
        :param s: a raw score ranging from 0 - 1
        :return: a scaled score ranging from -5 - +5
        """
        s = (s - 0.5) * 10
        if s < 0:
            return f'({s:.1f})'
        elif s > 0:
            return f'+{s:.1f}'
        return '0'

    def score_voters(self, voters):
        vs = VoterSegmentation(voters)
        history = vs.history_summary()
        return vs.score_voters(history)

    def build_address_score_map(self) -> Figure:
        """
        Return an address score map. This method
        is incomplete. It needs to provide information like
        the voter address map. The plan is to make the
        score be the median of all voters with a primary
        history. Provide a count of the voters with a primary
        history as well as a count of all voters.
        :return: a scatter mapbox figure
        """
        score = self.address_score
        fig = px.scatter_mapbox(score,
                                lat="lat",
                                lon="lon",
                                color="precinct_short_name",
                                size='population_count_display_size',
                                size_max=6,
                                labels={'precinct_short_name': 'Precinct',
                                        'population_count': 'Population Count'})
        self.finish_map(fig)
        return fig

    def build_voter_score_map(self) -> Figure:
        """
        Return a voter score map.  A voter score map is
        a scatter mapbox that maps voters to a residence.
        The hover text includes: the precinct, the voter name,
        an affinity score, the total number of voters at the
        address, and the address. If the address is a building
        with a name, the building name is provided.
        The voter whose info is displayed may be one of several
        at the address. Additional voters are dropped using
        drop_duplicates.
        :return: a scatter mapbox figure
        """
        # Get the list of voters. Drop all but one
        # voter for the address
        score = self.voter_score
        score = score.drop_duplicates(subset='key').reset_index(drop=True)
        # Drop unneeded columns -- not sure if this matters.
        score = score[['lat', 'lon', 'sample_count_display_size',
                       'precinct_short_name', 'hovertext', 'key']]
        # The color of the marker is determined by the precinct.
        # The size of the marker is determined by the
        # sample_count_display_size, which has a ceiling of 5.
        # Apartments can have a multitude of voters; so,
        # the sample size needs to be capped or most
        # residences will have tiny markers.
        #
        # Custom data is different for scatter_mapbox
        # TODO Double check custom data differences
        fig = px.scatter_mapbox(score,
                                lat="lat",
                                lon="lon",
                                color="precinct_short_name",
                                size='sample_count_display_size',
                                size_max=6,
                                labels={'precinct_short_name': 'Precinct',
                                        'sample_count': 'Voter Count'},
                                custom_data=score)
        # Set the hover template -- this seems to be the same.
        fig.update_traces(
            hovertemplate='%{customdata[4]}<extra></extra>'
        )

        self.finish_map(fig)
        return fig

    def finish_map(self, fig):
        fig.update_layout(
            mapbox={
                'zoom': 11.5,
                "style": "open-street-map",
                "layers": [
                    {
                        "source": json.loads(self.district.as_geodataframe.geometry.to_json()),
                        "below": "traces",
                        "type": "line",
                        "color": "purple",
                        "line": {"width": 2},
                        "name": 'district_boundary'
                    },
                    {
                        "source": json.loads(self.district.precinct_map.to_json()),
                        "below": "district_boundary",
                        "type": "line",
                        "color": 'black',
                        "line": {"width": .75},
                    }
                ],
            },
            margin={"l": 0, "r": 0, "t": 0, "b": 0})
        self.add_watermark(fig)
