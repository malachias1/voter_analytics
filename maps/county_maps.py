from data.pathes import Pathes
from data.voterdb import VoterDb
from geopandas import GeoSeries, GeoDataFrame


class CountyMaps(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    @property
    def maps_(self):
        return self.db.county_maps

    @property
    def maps(self):
        m = self.maps_
        data = {'state_fips':  m.state_fips,
                'county_fips':  m.county_fips,
                'county_code':  m.county_code,
                'geoid':  m.geoid,
                'county_name':  m.county_name,
                'aland':  m.aland,
                'awater':  m.awater,
                'geometry': GeoSeries.from_wkb(m.geometry_wkb, crs='epsg:3035').to_crs(crs='epsg:4326'),
                'center': GeoSeries.from_wkb(m.center_wkb, crs='epsg:3035').to_crs(crs='epsg:4326')}
        return GeoDataFrame(data, crs='epsg:4326')

    def get_map(self, county_code):
        m = self.maps
        return m[m.county_code == county_code].reset_index()
