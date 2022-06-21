import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb
from pathlib import Path
import geopandas as gpd


class IngestMapsBase(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    @property
    def filename(self):
        raise NotImplemented('filename as not been implemented')

    @property
    def tablename(self):
        raise NotImplemented('tablename as not been implemented')

    def read_maps(self):
        p = Path(self.maps_dir, self.filename, 'districts.shp').expanduser()
        dmaps = gpd.read_file(p).to_crs(epsg=3035)
        dmaps = dmaps.assign(center=dmaps.centroid)
        dmaps = dmaps[['ID', 'AREA', 'DISTRICT', 'POPULATION', 'IDEAL_VALU', 'geometry', 'center']].reset_index()
        dmaps = dmaps.rename(columns={'ID': 'id', 'AREA': 'area', 'DISTRICT': 'district', 'POPULATION': 'population',
                                      'IDEAL_VALU': 'ideal_value'})
        data = {'id': dmaps.id, 'area': dmaps.area,
                'district': dmaps.district,
                'population': dmaps.population,
                'ideal_value': dmaps.ideal_value,
                'geometry_json': dmaps.geometry.to_json(),
                'center_json': dmaps.center.to_json()}
        return pd.DataFrame(data=data)

    def ingest(self):
        dmaps = self.read_maps()
        con = self.db.con
        dmaps.to_sql(self.tablename, con, if_exists='replace', index=False)


class IngestUSHouseMaps(IngestMapsBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    @property
    def filename(self):
        return 'cng-prop1-2021-shape'

    @property
    def tablename(self):
        return 'cng_map'


class IngestGAHouseMaps(IngestMapsBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    @property
    def filename(self):
        return 'hse-prop1-2021-shape'

    @property
    def tablename(self):
        return 'hse_map'


class IngestGASenateMaps(IngestMapsBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    @property
    def filename(self):
        return 'sen-prop1-2021-shape'

    @property
    def tablename(self):
        return 'hse_map'
