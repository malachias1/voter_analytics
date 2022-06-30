from maps.map_base import MapBase
from geopandas import GeoDataFrame
import pandas as pd
import json


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


class MedianIncome(BlockGroupMaps):
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
