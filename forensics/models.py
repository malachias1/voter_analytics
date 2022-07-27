import pandas as pd
from django.db import models
from election_results.models import ContestCategory, ContestCategoryMap, Detail, OverUnderVote
import re
from collections import defaultdict
from county_details.models import CountyDetails
import plotly.graph_objects as go
from county_map.models import CountyMap
import json
import plotly.express as px


class SplitTicketAnalysis:
    def __init__(self, election_date):
        self.election_date = election_date
        self.rep_choice_p = re.compile(r'[(]REP[)]|[(]DEM[)]')
        self.is_rep_p = re.compile(r'[(]REP[)]')
        self.is_dem_p = re.compile(r'[(]DEM[)]')
        self.is_perdue_p = re.compile(r'PERDUE.*[(]REP[)]')
        self.is_ossoff_p = re.compile(r'OSSOFF.*[(]DEM[)]')

    def count_votes(self, party_selector, category):
        mappings = ContestCategoryMap.objects.filter(detail__election_date=self.election_date,
                                                     contest_category__category=category)
        votes = defaultdict(int)
        for m in mappings:
            detail = m.detail
            if party_selector.search(detail.choice.name):
                votes[detail.county_code] += detail.votes
        return votes

    def count_undervotes(self, category):
        mappings = ContestCategoryMap.objects.filter(detail__election_date=self.election_date,
                                                     contest_category__category=category)
        contests = [x.detail.contest for x in mappings]
        undervote = OverUnderVote.objects.filter(election_date=self.election_date,
                                                 contest__in=contests)
        undervote_count = defaultdict(int)
        for x in undervote:
            undervote_count[x.county_code] += x.undervotes

        return undervote_count

    def get_state_biden_splits(self):
        us_senate_votes = self.count_votes(self.is_ossoff_p, 'us_senate')
        us_house_votes = self.count_votes(self.is_dem_p, 'us_house')
        ga_house_votes = self.count_votes(self.is_dem_p, 'ga_house')
        ga_senate_votes = self.count_votes(self.is_dem_p, 'ga_senate')
        trump_votes = self.count_votes(self.is_rep_p, 'president')
        binden_votes = self.count_votes(self.is_dem_p, 'president')
        president_undervote = self.count_undervotes('president')

        splits = [{'county_code': x,
                   'us_senate_vote': us_senate_votes[x],
                   'us_house_vote': us_house_votes[x],
                   'ga_house_vote': ga_house_votes[x],
                   'ga_senate_vote': ga_senate_votes[x],
                   'trump_vote': trump_votes[x],
                   'biden_vote': binden_votes[x],
                   'president_undervote': president_undervote[x]
                   } for x in us_house_votes.keys()]
        splits = pd.DataFrame.from_records(splits)
        county_details = CountyDetails.objects.details
        max_votes = splits[['us_senate_vote', 'us_house_vote', 'ga_house_vote', 'ga_senate_vote']].max(axis=1)
        splits = splits.merge(county_details, on=['county_code'], how='inner').drop(
            columns=['county_fips'])
        splits = splits.assign(max_votes=max_votes,
                               max_diff=splits.biden_vote - max_votes,
                               margin=splits.biden_vote * 100 / (splits.trump_vote + splits.biden_vote))
        splits = splits.sort_values(by=['max_diff'], ascending=False).reset_index(drop=True)
        splits = splits.assign(rank=range(1, len(splits.index) + 1))
        return splits

    def get_state_trump_splits(self):
        us_senate_votes = self.count_votes(self.is_perdue_p, 'us_senate')
        us_house_votes = self.count_votes(self.is_rep_p, 'us_house')
        ga_house_votes = self.count_votes(self.is_rep_p, 'ga_house')
        ga_senate_votes = self.count_votes(self.is_rep_p, 'ga_senate')
        trump_votes = self.count_votes(self.is_rep_p, 'president')
        binden_votes = self.count_votes(self.is_dem_p, 'president')
        president_undervote = self.count_undervotes('president')

        splits = [{'county_code': x,
                   'us_senate_vote': us_senate_votes[x],
                   'us_house_vote': us_house_votes[x],
                   'ga_house_vote': ga_house_votes[x],
                   'ga_senate_vote': ga_senate_votes[x],
                   'trump_vote': trump_votes[x],
                   'biden_vote': binden_votes[x],
                   'president_undervote': president_undervote[x]
                   } for x in us_house_votes.keys()]
        splits = pd.DataFrame.from_records(splits)
        county_details = CountyDetails.objects.details
        max_votes = splits[['us_senate_vote', 'us_house_vote', 'ga_house_vote', 'ga_senate_vote']].max(axis=1)
        splits = splits.merge(county_details, on=['county_code'], how='inner').drop(
            columns=['county_fips'])
        splits = splits.assign(max_votes=max_votes,
                               max_diff=splits.trump_vote - max_votes,
                               margin=splits.trump_vote * 100 / (splits.trump_vote + splits.biden_vote))
        splits = splits.sort_values(by=['max_diff'], ascending=False).reset_index(drop=True)
        splits = splits.assign(rank=range(1, len(splits.index) + 1))
        return splits

    def get_state_trump_splits_map(self, splits):
        state_map = CountyMap.objects.state_map
        hover_data = {
            'county_name': True,
            'max_diff': True,
            'president_undervote': True,
            'trump_vote': True,
            'biden_vote': True,
            'margin': ':.1f',
            'us_senate_vote': True,
            'us_house_vote': True,
            'ga_house_vote': True,
            'ga_senate_vote': True,
            'max_votes': True,
            'county_code': False,
        }
        labels = {
            'county_name': 'County Name',
            'max_diff': 'Trump Performance vs. Estimated Max (votes)',
            'president_undervote': 'President Under Vote Count',
            'trump_vote': 'Former President Trump Tally',
            'biden_vote': 'President Biden Tally',
            'margin': 'Trump Vote %',
            'us_senate_vote': 'Aggregate US Senate (Perdue) Vote Tally',
            'us_house_vote': 'Aggregate US House Vote Tally',
            'ga_senate_vote': 'Aggregate State Senate Vote Tally',
            'ga_house_vote': 'Aggregate State House Vote Tally',
            'max_votes': 'Observed Max Republican Vote'
        }

        state_map = state_map.merge(splits, on=['county_code', 'county_name'], how='inner')
        gj = json.loads(state_map.to_json())
        center = state_map.to_crs(crs='epsg:3035').centroid.to_crs(crs='epsg:4326').iloc[0]

        fig = px.choropleth_mapbox(
            state_map,
            geojson=gj,
            color='margin',
            locations='county_code',
            featureidkey="properties.county_code",
            center={"lat": center.y, "lon": center.x},
            opacity=0.5,
            mapbox_style="open-street-map",
            zoom=6.5,
            labels=labels,
            hover_data=hover_data,
            color_discrete_sequence=px.colors.sequential.Plasma
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

    def get_state_trump_splits_map2(self, splits):
        state_map = CountyMap.objects.state_map
        hover_data = {
            'county_name': True,
            'rank': True,
            'max_diff': True,
            'president_undervote': True,
            'trump_vote': True,
            'biden_vote': True,
            'margin_str': True,
            'us_house_vote': True,
            'ga_house_vote': True,
            'ga_senate_vote': True,
            'max_votes': True,
            'county_code': False,
            'margin': False
        }
        labels = {
            'county_name': 'County Name',
            'rank': 'Trump Performance Rank',
            'max_diff': 'Trump Under/Over Performance (votes)',
            'president_undervote': 'Under Vote Count',
            'trump_vote': 'Former President Trump Tally',
            'biden_vote': 'President Biden Tally',
            'margin_str': 'Trump Vote Percentage',
            'us_house_vote': 'Aggregate US House Vote Tally',
            'ga_house_vote': 'Aggregate State House Vote Tally',
            'ga_senate_vote': 'Aggregate State Senate Vote Tally',
            'max_votes': 'Observed Max Republican Vote'
        }
        hovertemplate = """
        <b>%{county_name} County</b><br>
        <b>Trump Under/Over Performance (votes)</b>: %{max_diff}<br>
        <i>Aggregate US House Vote Tally</i>: %{us_house_vote}<br>
        <i>Aggregate State House Vote Tally</i>: %{ga_house_vote}<br>
        <i>Aggregate State Senate Vote Tally</i>: %{ga_senate_vote}<br>
        The observed max Republican vote is the 
        maximum of the above three tallies.<br>
        <i>Observed Max</i>: %{max_votes}<br>
        <i>Under Vote Count</i>: %{president_undervote}<br>
        <i>Trump Tally</i>: %{trump_vote}<br>
        <i>Biden Tally</i>: %{biden_vote}<br>
        <i>Trump Margin of Victory/Defeat</i>: %{margin}<br>
        """

        splits = splits.assign(margin=splits.trump_vote / (splits.trump_vote + splits.biden_vote))
        splits = splits.assign(margin_str=splits.margin.apply(lambda x: f'{x * 100:.1f}%'))
        # splits = splits.sort_values(by=['max_diff'], ascending=False).reset_index(drop=True)
        # splits = splits.assign(rank=range(1, len(splits.index) + 1))

        state_map = state_map.merge(splits, on='county_code', how='inner')
        gj = json.loads(state_map.to_json())
        center = state_map.to_crs(crs='epsg:3035').centroid.to_crs(crs='epsg:4326').iloc[0]

        data = go.Choroplethmapbox(
            geojson=gj,
            # featureidkey="properties.county_code",
            customdata=splits,
            hovertemplate=hovertemplate,
            # color_discrete_sequence=px.colors.sequential.Plasma,
            autocolorscale=True,
            colorscale="Viridis", zmin=0, zmax=12,
            marker_opacity=0.5, marker_line_width=0,
            z=splits.margin
        )

        fig = go.Figure(data=data)

        fig.update_layout(mapbox_style="open-street-map",
                          mapbox_zoom=6.5,
                          mapbox_center={"lat": center.y, "lon": center.x})

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
