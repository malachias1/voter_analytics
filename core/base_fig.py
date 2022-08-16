from django.db import models
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
import plotly.graph_objects as go


class BaseFig:
    @property
    def annotations(self):
        try:
            return self.annotations_
        except AttributeError:
            self.annotations_ = []
            return self.annotations_

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
        return fig

    def add_annotation(self, fig, annotation):
        self.annotations.append(annotation)
        fig.update_layout(annotations=self.annotations)
        return fig

    def add_legend_text(self, fig, text):
        self.add_annotation(fig,
                            go.layout.Annotation(
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
                            ))
        return fig

    @classmethod
    def add_margin(cls, fig):
        fig.update_layout(margin={"r": 8, "t": 8, "l": 8, "b": 8})
        return fig

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
                y=.02,
                sizex=.15,
                sizey=.15,
                opacity=1.0)
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

    def reset_figure(self):
        try:
            self.annotations_ = []
        except AttributeError:
            pass


class BaseMap(BaseFig):
    CRS_METERS = 'epsg:3035'
    CRS_LAT_LON = 'epsg:4326'

    @property
    def wkb_crs(self):
        return self.CRS_LAT_LON

    def centroid(self, gdf):
        """
        Return the center of the given GeoDataframe
        :param gdf: a GeoDataframe in lat-lon
        :return:
        """
        return gdf.to_crs(crs=self.CRS_METERS).centroid.to_crs(crs=self.CRS_LAT_LON)

    def from_wkb(self, geometry_wkb):
        """
        Convert a well known binary to a geometry.
        :param geometry_wkb: a geometry in well known binary form
        :return: a geoseries
        """
        gdf = gpd.GeoSeries.from_wkb(geometry_wkb, crs=self.wkb_crs)
        if self.wkb_crs == self.CRS_METERS:
            gdf = gdf.to_crs(self.CRS_LAT_LON)
        return gdf

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
        return {'geometry': self.geometry}

    @property
    def as_geodataframe(self):
        return gpd.GeoDataFrame([self.as_record], crs=self.CRS_LAT_LON)

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
        Return a center of the geometry
        :return: the center of the object in lat,lon.
        """
        return self.centroid(self.as_geodataframe).iloc[0]

    class Meta:
        abstract = True
