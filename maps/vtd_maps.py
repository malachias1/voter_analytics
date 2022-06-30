from maps.map_base import MapBase
from geopandas import GeoDataFrame


class VtdMaps(MapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    @property
    def maps_(self):
        return self.db.vtd_maps

    @property
    def maps(self):
        return self.to_geodataframe(self.maps_)

    def to_geodataframe(self, df):
        data = {'id': df.id,
                'area': df.area,
                'district': df.district,
                'county_sosid': df.county_sosid,
                'precinct_id': df.precinct_id,
                'precinct_name': df.precinct_name,
                'county_code': df.county_code,
                'county_fips': df.county_fips,
                'county_name': df.county_name,
                'geometry': self.from_wkb(df.geometry_wkb),
                'center': self.from_wkb(df.center_wkb)
                }
        return GeoDataFrame(data, self.crs_lat_lon)

    def get_map(self, county_code):
        return self.to_geodataframe(self.db.get_vtd_map(county_code))
