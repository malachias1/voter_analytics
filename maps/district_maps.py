from data.pathes import Pathes
from data.voterdb import VoterDb
from geopandas import GeoSeries, GeoDataFrame


class MapBase(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

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
                'geometry': GeoSeries.from_wkb(m.geometry_wkb, crs='epsg:3035').to_crs(crs='epsg:4326'),
                'center': GeoSeries.from_wkb(m.center_wkb, crs='epsg:3035').to_crs(crs='epsg:4326')}
        return GeoDataFrame(data, crs='epsg:4326')

    def get_map(self, district):
        district = f'{int(district):03d}'
        m = self.maps
        return m[m.district == district]


class CngMap(MapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    @property
    def maps_(self):
        return self.db.cng_maps


class HseMap(MapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    @property
    def maps_(self):
        return self.db.hse_maps


class SenMap(MapBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    @property
    def maps_(self):
        return self.db.sen_maps

