import pandas as pd

from core.models import MapConfig
import plotly.express as px
from string import Template


class PrimaryTrendsConfig(MapConfig):
    @property
    def you_are_here(self):
        return '<br>'.join(self.config['you_are_here'])

    @property
    def older_cluster(self):
        return self.config.get('older_cluster', None)

    @property
    def younger_cluster(self):
        return self.config.get('younger_cluster', None)


class PrimaryTrendsChart:
    def __init__(self, district, config_path):
        self.district = district
        self.config = PrimaryTrendsConfig(config_path)

    @property
    def chart(self):
        df = self.district.get_primary_demographics(self.config)
        df = df.assign(pwh=(df.WH_r + df.WH_d) / (df.total_r + df.total_d) * 100)
        df = df.assign(a=df.total_r / (df.total_r + df.total_d) * 100)
        df = df.assign(total=df.total_r + df.total_d)
        df = self.build_hover_text(df)
        d_row = self.district_row(df)
        df = pd.concat([df, pd.DataFrame.from_records([d_row])])
        df = df.assign(a1=df.a)[['precinct_short_name', 'total', 'a', 'pwh', 'age', 'a1', 'hover_text']]

        fig = px.scatter(df,
                         x="pwh",
                         y="age",
                         text='precinct_short_name',
                         color="a1",
                         size='total',
                         # width=1280,
                         # height=720,
                         labels={
                             'precinct_short_name': 'Precinct',
                             'total': 'Total Primary Participants',
                             'a': 'Republican Candidate % of Votes',
                             'pwh': "% Identifying as White",
                             'age': 'Median Age',
                             'a1': 'Republican<br>Candidate<br>% of Votes'
                         }
                         )

        fig.update_traces(
            textposition='top center',
            customdata=df.to_records(index=False),
            hovertemplate='%{customdata[6]}'
        )

        fig.update_layout(
            paper_bgcolor="black",
            plot_bgcolor="black",
            font=dict(
                family="Times New Roman",
                size=10,
                color="lightgray"
            ))

        self.add_older_cluster_annotation(fig)
        self.add_younger_cluster_annotation(fig)
        self.add_legend_text(fig)
        self.add_watermark(fig)
        self.add_logo(fig, self.config)
        fig.update_layout(margin={"r": 30, "t": 100, "l": 30, "b": 60})
        return fig

    def district_row(self, df):
        dg = self.district.demographics
        dg_median_age = (2022 - dg.year_of_birth).median()
        dg_pwh = len(dg[dg.race_id == 'WH']) / len(dg) * 100
        total = (df.total_r.sum() + df.total_d.sum())
        a = df.total_r.sum() / total * 100
        return {
            'precinct_short_name': 'District Overall',
            # Reduce the size, so the rest of the dots aren't tiny
            'total': total/len(df.index)*2,
            'a': a,
            'pwh': dg_pwh,
            'age': dg_median_age,
            'hover_text': f'<b>District Overall</b><br><br>'
                          f'District Population Count = {len(dg.index)}<br>'
                          f'% Identifying as White = {dg_pwh:.1f}<br>'
                          f'Median Age = {dg_median_age:.0f}'
        }

    def build_hover_text(self, df):
        text = [
            '<b>Precinct $name</b><br>',
            'Total Primary Participants = $total',
            'Republican Candidate % of Votes = $a',
            '% Identifying as White = $pwh',
            'Median Age = $age'
        ]
        s = Template('<br>'.join(text))
        hover_text = []
        for i in df.index:
            ht = s.substitute(name=df.loc[i].precinct_short_name,
                              total=df.loc[i].total,
                              a=f'{df.loc[i].a:.1f}',
                              pwh=f'{df.loc[i].pwh:.1f}',
                              age=f'{df.loc[i].age:.0f}')
            hover_text.append(ht)
        return df.assign(hover_text=pd.Series(hover_text))

    def add_cluster_annotation(self, fig, config):
        fig.add_annotation(x=config['x'],
                           y=config['y'],
                           xanchor=config['xanchor'],
                           yanchor=config['yanchor'],
                           align='left',
                           text='<br>'.join(config['text']),
                           showarrow=False,
                           font=dict(
                               family="Times New Roman",
                               size=12,
                               color="lightgray"
                           ),
                           arrowhead=1
                           )
        fig.add_shape(type="rect",
                      x0=config['x0'],
                      y0=config['y0'],
                      x1=config['x1'],
                      y1=config['y1'],
                      line=dict(color=config['color']),
                      )

    def add_older_cluster_annotation(self, fig):
        config = self.config.older_cluster
        if config:
            self.add_cluster_annotation(fig, config)

    def add_younger_cluster_annotation(self, fig):
        config = self.config.younger_cluster
        if config:
            self.add_cluster_annotation(fig, config)

    def add_legend_text(self, fig):
        fig.add_annotation(
            text=self.config.description,
            align='left',
            showarrow=False,
            x=0.01,
            y=0.98,
            xref='paper',
            yref='paper',
            yanchor="top",
            xanchor="left",
            bgcolor="white",
            bordercolor="Black",
            borderwidth=1,
            borderpad=6,
            font=dict(
                family="Times New Roman",
                size=11,
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
    def add_watermark(cls, fig):
        fig.add_layout_image(
            dict(
                source='https://novemberpathways.com/maps/novemberpathways.png',
                yanchor='bottom',
                xanchor='left',
                xref="paper",
                yref="paper",
                x=.02,
                y=0,
                sizex=.1,
                sizey=.1,
                opacity=1.0)
        )
        return fig
