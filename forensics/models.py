import pandas as pd
import re
import json
from collections import defaultdict
import plotly.express as px
from county.models import County, CountyMap
from election_results.models import ContestCategoryMap, OverUnderVote
from core.models import MapConfig
from core.base_fig import BaseMap


class SplitTicketMapConfig(MapConfig):
    @property
    def contest_patterns(self):
        return self.config['contest_patterns']

    @property
    def undervote_category(self):
        return self.config['undervote_category']

    @property
    def undervote_column_name(self):
        return self.config['undervote_column_name']


class SplitTicketAnalysis(BaseMap):
    def __init__(self, config_path):
        self.config = SplitTicketMapConfig(config_path)

    @property
    def election_date(self):
        return self.config.date

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

    def get_state_splits(self):
        splits = {}
        standards = []
        for category, pattern, colname, is_standard in self.config.contest_patterns:
            if is_standard:
                standards.append(colname)
            splits[colname] = self.count_votes(re.compile(pattern), category)
        splits[self.config.undervote_column_name] = self.count_undervotes(self.config.undervote_category)

        county_codes = splits[list(splits.keys())[0]]
        records = []
        for cc in county_codes:
            record = {'county_code': cc}
            for key in splits.keys():
                record[key] = splits[key][cc]
            records.append(record)
        df = pd.DataFrame.from_records(records)
        county_details = County.objects.as_df
        max_votes = df[standards].max(axis=1)
        df = df.merge(county_details, on=['county_code'], how='inner').drop(columns=['county_fips'])
        df = df.assign(max_votes=max_votes,
                       max_diff=df.candidate1 - max_votes,
                       margin=df.candidate1 * 100 / (df.candidate1 + df.candidate2))
        df = df.sort_values(by=['max_diff'], ascending=False).reset_index(drop=True)
        df = df.assign(rank=range(1, len(df.index) + 1))
        return df

    def get_split_choroplath(self):
        splits = self.get_state_splits()
        state_map = CountyMap.objects.state_map
        state_map = state_map.merge(splits, on=['county_code', 'county_name'], how='inner')
        gj = json.loads(state_map.to_json())
        center = self.centroid(state_map).iloc[0]

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
            labels=self.config.labels,
            hover_data=self.config.hover_data,
            color_discrete_sequence=px.colors.sequential.Plasma
        )

        self.add_watermark(fig)
        self.add_legend_text(fig, self.config.description)
        fig.update_layout(margin={"r": 0, "t": 30, "l": 0, "b": 30})
        return fig
