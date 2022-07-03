from geopandas import GeoDataFrame
from maps.map_base import MapBase


class CountyMaps(MapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    @property
    def maps_(self):
        return self.db.county_maps

    @property
    def maps(self):
        return self.to_geodataframe_(self.maps_)

    def to_geodataframe_(self, df):
        m = self.maps_
        data = {'state_fips':  m.state_fips,
                'county_fips':  m.county_fips,
                'county_code':  m.county_code,
                'geoid':  m.geoid,
                'county_name':  m.county_name,
                'aland':  m.aland,
                'awater':  m.awater,
                'geometry': self.from_wkb(df.geometry_wkb),
                'center': self.from_wkb(df.center_wkb)
                }
        return GeoDataFrame(data, crs=self.crs_lat_lon)

    def get_map(self, county_code):
        return self.to_geodataframe_(self.db.get_county_map(county_code))

    @staticmethod
    def get_county_map(county_code, root_dir='~/Documents/data'):
        return CountyMaps(root_dir).get_map(county_code)
