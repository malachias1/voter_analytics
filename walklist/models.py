import pandas as pd
import geopandas as gpd
import json
import plotly.express as px
from core.base_fig import BaseMap
from geocode.models import Address
from segmentation.voter_segmentation import VoterSegmentation
from voter.models import ResidenceAddress


class Walklist(BaseMap):
    def __init__(self, district):
        self.district = district

    @property
    def score(self) -> pd.DataFrame:
        return self.score_voters(self.district.voters)

    @property
    def voter_details(self):
        voters = self.district.voters
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
        voter_details = self.voter_details
        gc = Address.objects.get_addresses_for_counties(self.district.counties)
        gc = gc[['lat', 'lon', 'accuracy_score', 'accuracy_type', 'plus4',
                 'building_name', 'record_type_description', 'key']]

        voter_details = voter_details.merge(gc, on='key', how='inner')
        return voter_details

    @property
    def address_score(self):
        data = self.voter_data
        score_details = data[['voter_id', 'key']].merge(self.score_voters(data.voter.unique()),
                                                        on='voter_id', how='inner')[['key', 'ra']]
        summary_score = score_details.groupby(['key'], as_index=False)['ra'].median()
        summary_score = summary_score.rename(columns={'ra': 'score'})
        sample_count = score_details.groupby(['key'], as_index=False)['ra'].count()
        sample_count = sample_count.rename(columns={'ra': 'sample_count'})
        population_count = score_details.groupby(['key'], as_index=False)['ra'].size()
        population_count = population_count.rename(columns={'size': 'population_count'})
        summary_score = summary_score.merge(sample_count, on='key', how='inner')
        summary_score = summary_score.merge(population_count, on='key', how='inner')
        summary_data = data[['lat', 'lon', 'accuracy_score', 'accuracy_type',
                             'building_name', 'record_type_description',
                             'street', 'city', 'state', 'zipcode', 'plus4',
                             'precinct_short_name', 'key']].drop_duplicates(subset='key')
        summary_data = summary_data.merge(summary_score, on='key', how='inner')
        summary_data = summary_data.assign(population_count_display_size=summary_data.population_count.apply(lambda c: min(c, 5)))
        return summary_data

    def score_voters(self, voters):
        vs = VoterSegmentation(voters)
        history = vs.history_summary()
        return vs.score_voters(history)

    def build_address_score_map(self):
        score = self.address_score
        fig = px.scatter_mapbox(score,
                                lat="lat",
                                lon="lon",
                                color="precinct_short_name",
                                size='population_count_display_size',
                                size_max=6,
                                labels={'precinct_short_name': 'Precinct',
                                        'population_count': 'Population Count'},
                                hover_data={
                                    "precinct_short_name": True,
                                    'population_count_display_size': False,
                                    'population_count': True
                                })
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
        return fig
