from data.pathes import Pathes
from data.voterdb import VoterDb
from geopandas import GeoSeries


class MapBase(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)
        self.crs_meters = 'epsg:3035'
        self.crs_lat_lon = 'epsg:4326'

    def from_wkb(self, geometry_wkb):
        return GeoSeries.from_wkb(geometry_wkb, crs=self.crs_meters).to_crs(crs=self.crs_lat_lon)

    def centroid(self, geometry):
        return geometry.to_crs(crs=self.crs_meters).centroid.to_crs(crs=self.crs_lat_lon)
