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
        m = self.maps_
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
        q = self.get_voter_query(district)
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
                'district': df.district,
                'county_sosid': df.county_sosid,
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

    def get_voter_query(self, district):
        return f"""
            select voter_id from voter_cng where cng='{district}'
            """


class HseDistrictMap(DistrictMapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.hse_maps_ = self.db.cng_maps

    @property
    def maps_(self):
        return self.hse_maps_


class SenDistrictMap(DistrictMapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.sen_maps_ = self.db.cng_maps

    @property
    def maps_(self):
        return self.sen_maps_
