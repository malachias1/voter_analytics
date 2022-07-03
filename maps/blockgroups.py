from maps.map_base import MapBase
from geopandas import GeoDataFrame
import pandas as pd
import json
from maps.county_maps import CountyMaps
from maps.district_maps import CngDistrictMap, HseDistrictMap, SenDistrictMap
import plotly.express as px


class BlockGroupMaps(MapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.county_details = self.db.county_details

    def get_blockgroup_boundaries(self, county_codes):
        """
        Get the boundaries within the given counties.
        I need to translate county codes to fips codes.
        Note, the number of parameters SQLite can handle
        is 999, which is far less than the number of counties
        I will ever encounter. I construct the select
        in this manner because there is no means of constructing
        a single parameter to satisfy the in.
        :param county_codes: an iterable of county codes
        :return: a GeoDataFrame of blockgroup boundaries with county code
        """
        county_details = self.to_fips(county_codes)
        parms = ','.join([f"'{x}'" for x in county_details.county_fips])
        b = pd.read_sql_query(f"""
            select * from block_group_geometry where county in ({parms})
        """, self.db.con)
        data = {'GEOID': b.GEOID,
                'state': b.state,
                'county_fips': b.county,
                'tract': b.tract,
                'block_group': b.block_group,
                'geometry': self.from_json(b.geometry)}
        gdf = GeoDataFrame(data, crs=self.crs_lat_lon)
        gdf = gdf.merge(county_details, on='county_fips', how='inner')
        return gdf[['GEOID', 'state', 'county_fips', 'county_code', 'tract', 'block_group', 'geometry']]

    def to_fips(self, county_codes):
        return self.county_details[self.county_details.county_code.isin(county_codes)][['county_code', 'county_fips']]

    def from_json(self, geometry_json):
        """
        Blockgroup boundaries are saved as geojson.
        :param geometry_json: pd.Series of string
        :return: a GeoSeries of boundaries in lat-long
        """
        g = geometry_json.apply(lambda gj: json.loads(gj))
        return GeoDataFrame.from_features(g, crs=self.crs_lat_lon).geometry

    @classmethod
    def to_params(cls, county_details):
        return ','.join([f"'{x}'" for x in county_details.county_fips])

    @classmethod
    def get_figure_(cls, title, feature, feature_label, the_map, mi):
        the_map_center = the_map.center.iloc[0]
        geojson = json.loads(mi[['GEOID', 'geometry']].to_json())
        data = mi[['GEOID', feature]]

        fig = px.choropleth_mapbox(data, geojson=geojson, color=feature,
                                   locations="GEOID", featureidkey="properties.GEOID",
                                   center={"lat": the_map_center.y, "lon": the_map_center.x},
                                   opacity=0.5,
                                   mapbox_style="open-street-map", zoom=9.5,
                                   labels={feature: f'{feature_label} ',
                                           'GEOID': 'Blockgroup '
                                           },
                                   title=title,
                                   hover_data={feature: ':-4.0f'
                                               })
        fig.update_layout(margin={"r": 10, "t": 40, "l": 10, "b": 10})
        return fig


class MedianIncome(BlockGroupMaps):
    FEATURE = 'median_household_income'
    FEATURE_LABEL = 'Income'

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    def get_median_income(self, county_codes):
        county_details = self.to_fips(county_codes)
        params = self.to_params(county_details)
        mi = pd.read_sql_query(f"""
            select * from median_house_hold_income where county in ({params})
        """, self.db.con)
        mi = mi[['GEOID', 'median_household_income', 'median_household_income_moe']]
        mi = self.get_blockgroup_boundaries(county_codes).merge(mi, on='GEOID', how='inner')
        mi = mi[['GEOID', 'state', 'county_fips', 'county_code', 'tract', 'block_group', 'median_household_income',
                 'median_household_income_moe', 'geometry']]
        return mi

    def get_county_map(self, title, county_code):
        the_map = CountyMaps.get_county_map(county_code)
        mi = self.get_median_income([county_code])
        return self.get_figure_(title, self.FEATURE, self.FEATURE_LABEL, the_map, mi)

    def get_district_map_(self, dmap, title, district):
        the_map = dmap.get_district_map(district)
        counties = dmap.get_precincts(district).county_code.unique()
        mi = self.get_median_income(counties)
        mi = mi.overlay(the_map, how='intersection')
        return self.get_figure_(title, self.FEATURE, self.FEATURE_LABEL, the_map, mi)

    def get_cng_map(self, title, district):
        return self.get_district_map_(CngDistrictMap(self.root_dir_), title, district)

    def get_hse_map(self, title, district):
        return self.get_district_map_(HseDistrictMap(self.root_dir_), title, district)

    def get_sen_map(self, title, district):
        return self.get_district_map_(SenDistrictMap(self.root_dir_), title, district)


class BaseChildPopulation(BlockGroupMaps):
    FEATURE = None
    FEATURE_LABEL = None

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    def get_population(self, county_codes):
        raise NotImplemented('get_population is not implemented.')

    def get_county_map(self, title, county_code):
        the_map = CountyMaps.get_county_map(county_code)
        mi = self.get_population([county_code])
        return self.get_figure_(title, self.FEATURE, self.FEATURE_LABEL, the_map, mi)

    def get_district_map_(self, dmap, title, district):
        the_map = dmap.get_district_map(district)
        counties = dmap.get_precincts(district).county_code.unique()
        mi = self.get_population(counties)
        mi = mi.overlay(the_map, how='intersection')
        return self.get_figure_(title, self.FEATURE, self.FEATURE_LABEL, the_map, mi)

    def get_cng_map(self, title, district):
        return self.get_district_map_(CngDistrictMap(self.root_dir_), title, district)

    def get_hse_map(self, title, district):
        return self.get_district_map_(HseDistrictMap(self.root_dir_), title, district)

    def get_sen_map(self, title, district):
        return self.get_district_map_(SenDistrictMap(self.root_dir_), title, district)


class Under5Population(BaseChildPopulation):
    FEATURE = 'pop'
    FEATURE_LABEL = 'Under 5 Years'

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    def get_population(self, county_codes):
        county_details = self.to_fips(county_codes)
        params = self.to_params(county_details)
        f = pd.read_sql_query(f"""
            select GEOID, male_under_5, female_under_5 from children where county in ({params})
        """, self.db.con)
        f = f[['GEOID', 'male_under_5', 'female_under_5']]
        f = f.assign(pop=f.male_under_5 + f.female_under_5).drop(columns=['male_under_5', 'female_under_5'])
        f = self.get_blockgroup_boundaries(county_codes).merge(f, on='GEOID', how='inner')
        f = f[['GEOID', 'pop', 'geometry']]
        return f


class Under15Population(BaseChildPopulation):
    FEATURE = 'pop'
    FEATURE_LABEL = 'Under 15 Years'

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    def get_population(self, county_codes):
        county_details = self.to_fips(county_codes)
        params = self.to_params(county_details)
        f = pd.read_sql_query(f"""
            select GEOID, male_under_5, female_under_5, male_5_to_9, female_5_to_9, male_10_to_14, 
            female_10_to_14 from children where county in ({params})
        """, self.db.con)
        f = f[['GEOID', 'male_under_5', 'female_under_5',
               'male_5_to_9', 'female_5_to_9',
               'male_10_to_14', 'female_10_to_14']]
        f = f.assign(pop=f.male_under_5 + f.female_under_5 +
                         f.male_5_to_9 + f.female_5_to_9 +
                         f.male_10_to_14 + f.female_10_to_14).drop(columns=['male_under_5', 'female_under_5',
                                                                            'male_5_to_9', 'female_5_to_9',
                                                                            'male_10_to_14', 'female_10_to_14'
                                                                            ])
        f = self.get_blockgroup_boundaries(county_codes).merge(f, on='GEOID', how='inner')
        f = f[['GEOID', 'pop', 'geometry']]
        return f


class Under18Population(BaseChildPopulation):
    FEATURE = 'pop'
    FEATURE_LABEL = 'Under 18 Years'

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    def get_population(self, county_codes):
        county_details = self.to_fips(county_codes)
        params = self.to_params(county_details)
        f = pd.read_sql_query(f"""
            select GEOID, male_under_5, female_under_5, 
            male_5_to_9, female_5_to_9, 
            male_10_to_14, female_10_to_14, 
            male_15_to_17, female_15_to_17 
            from children where county in ({params})
        """, self.db.con)
        f = f[['GEOID', 'male_under_5', 'female_under_5',
               'male_5_to_9', 'female_5_to_9',
               'male_10_to_14', 'female_10_to_14',
               'male_15_to_17', 'female_15_to_17']]
        f = f.assign(pop=f.male_under_5 + f.female_under_5 +
                         f.male_5_to_9 + f.female_5_to_9 +
                         f.male_10_to_14 + f.female_10_to_14 +
                         f.male_15_to_17 + f.female_15_to_17).drop(columns=['male_under_5', 'female_under_5',
                                                                            'male_5_to_9', 'female_5_to_9',
                                                                            'male_10_to_14', 'female_10_to_14',
                                                                            'male_15_to_17', 'female_15_to_17'
                                                                            ])
        f = self.get_blockgroup_boundaries(county_codes).merge(f, on='GEOID', how='inner')
        f = f[['GEOID', 'pop', 'geometry']]
        return f
