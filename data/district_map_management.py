from psycopg2.extras import execute_values
import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb
from pathlib import Path
import geopandas as gpd


class DistrictMapManagementBase(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb()

    @property
    def con(self):
        return self.db.con

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
                'geometry_wkb': dmaps.geometry.to_wkb(hex=True),
                'center_wkb': dmaps.center.to_wkb(hex=True)}
        return pd.DataFrame(data=data)

    # -------------------------------------------------------------------------
    # Ingest and Update Methods
    # -------------------------------------------------------------------------

    def ingest(self):
        self.truncate()
        records = self.read_maps().to_records(index=False)
        with self.con:
            with self.con.cursor() as cur:
                execute_values(cur, f"""
                        insert into cng_map (id, area, district, population,
                                         ideal_value, geometry_wkb, center_wkb)
                            values %s
                """, records)
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(f"""
                    drop index if exists {self.tablename}_idx;
                    CREATE INDEX IF NOT EXISTS {self.tablename}_idx ON {self.tablename} (district);
                """)

    def truncate(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(f"""
                    truncate {self.tablename};
                """)


class USHouseMapManagement(DistrictMapManagementBase):

    @property
    def filename(self):
        return 'cng-prop1-2021-shape'

    @property
    def tablename(self):
        return 'cng_map'


class GAHouseMapManagement(DistrictMapManagementBase):

    @property
    def filename(self):
        return 'hse-prop1-2021-shape'

    @property
    def tablename(self):
        return 'hse_map'


class GASenateMapManagement(DistrictMapManagementBase):

    @property
    def filename(self):
        return 'sen-prop1-2021-shape'

    @property
    def tablename(self):
        return 'sen_map'

