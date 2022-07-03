from maps.map_base import MapBase
from geopandas import GeoDataFrame
import pandas as pd


class DistrictMapBase(MapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.voters_ = None
        self.voter_precincts_ = None
        self.precincts_ = None

    @property
    def maps_(self):
        raise NotImplemented('maps_ is not implemented')

    @property
    def maps(self):
        return self.to_geodataframe_(self.maps_)

    def to_geodataframe_(self, m):
        data = {'id': m.id, 'area': m.area,
                'district': m.district,
                'population': m.population,
                'ideal_value': m.ideal_value,
                'geometry': self.from_wkb(m.geometry_wkb),
                'center': self.from_wkb(m.center_wkb)}
        return GeoDataFrame(data, crs=self.crs_lat_lon)

    def get_map(self, district):
        district = f'{int(district):03d}'
        m = self.maps
        return m[m.district == district]

    def get_voter_history(self, district, election_date):
        q = self.get_voter_query(district)
        return pd.read_sql_query(f"""
             select * from voter_history where voter_id in (
                 {q}
             ) and date='{election_date}'
         """, self.db.con)

    def get_voter_precincts(self, district):
        q = self.get_voter_query(district)
        return pd.read_sql_query(f"""
            select distinct(precinct_id) from voter_precinct where voter_id in (
                {q}
            )
        """, self.db.con)

    def get_voter_query(self, district):
        raise NotImplemented('get_voter_query is not implemented.')

    def get_voters(self, district):
        q = self.get_voter_query(district)
        return pd.read_sql_query(q, self.db.con)

    def get_precincts(self, district):
        q = self.get_voter_query(f'{int(district):03d}')
        return pd.read_sql_query(f"""
            select * from precinct_details where id in (
                select precinct_id from voter_precinct where voter_id in (
                    {q}
                )
            )
        """, self.db.con)

    def get_vtd_maps(self, district):
        q = self.get_voter_query(district)
        df = pd.read_sql_query(f"""
            select * from vtd_map where (county_code, precinct_id) in (
                select county_code, precinct_id from precinct_details where id in (
                    select precinct_id from voter_precinct where voter_id in (
                        {q}
                    )
                )
            )
        """, self.db.con)
        data = {'id': df.id,
                'area': df.area,
                'precinct_id': df.precinct_id,
                'precinct_name': df.precinct_name,
                'county_code': df.county_code,
                'county_fips': df.county_fips,
                'county_name': df.county_name,
                'geometry': self.from_wkb(df.geometry_wkb)}
        gdf = GeoDataFrame(data, crs=self.crs_lat_lon)
        gdf = gdf.assign(center=self.centroid(gdf.geometry))

        return gdf.overlay(self.get_map('007')[['geometry']],
                           how='intersection', keep_geom_type=True)


class CngDistrictMap(DistrictMapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.cng_maps_ = self.db.cng_maps

    @property
    def maps_(self):
        return self.cng_maps_

    @classmethod
    def get_district_map(cls, district, root_dir='~/Documents/data'):
        return CngDistrictMap(root_dir).get_map(district)

    def get_voter_query(self, district):
        return f"""
            select voter_id from voter_cng where cng='{district}'
            """


class HseDistrictMap(DistrictMapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.hse_maps_ = self.db.hse_maps

    @property
    def maps_(self):
        return self.hse_maps_

    @classmethod
    def get_district_map(cls, district, root_dir='~/Documents/data'):
        return HseDistrictMap(root_dir).get_map(district)

    def get_voter_query(self, district):
        return f"""
            select voter_id from voter_hse where hse='{int(district):03d}'
            """


class SenDistrictMap(DistrictMapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.sen_maps_ = self.db.sen_maps

    @property
    def maps_(self):
        return self.sen_maps_

    @classmethod
    def get_district_map(cls, district, root_dir='~/Documents/data'):
        return SenDistrictMap(root_dir).get_map(district)

    def get_voter_query(self, district):
        return f"""
            select voter_id from voter_sen where sen='{int(district):03d}'
            """

