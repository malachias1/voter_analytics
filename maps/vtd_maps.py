from data.pathes import Pathes
from data.voterdb import VoterDb
from geopandas import GeoSeries, GeoDataFrame


class VtdMaps(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    @property
    def maps_(self):
        return self.db.vtd_maps

    @property
    def maps(self):
        return self.to_geodataframe(self.maps_)

    @classmethod
    def to_geodataframe(cls, df):
        data = {'id': df.id,
                'area': df.area,
                'district': df.district,
                'county_sosid': df.county_sosid,
                'precinct_id': df.precinct_id,
                'precinct_name': df.precinct_name,
                'county_code': df.county_code,
                'county_fips': df.county_fips,
                'county_name': df.county_name,
                'geometry': GeoSeries.from_wkb(df.geometry_wkb, crs='epsg:3035').to_crs(crs='epsg:4326'),
                'center': GeoSeries.from_wkb(df.center_wkb, crs='epsg:3035').to_crs(crs='epsg:4326')}
        return GeoDataFrame(data, crs='epsg:4326')

    def get_map(self, county_code):
        return self.to_geodataframe(self.db.get_vtd_map(county_code))
