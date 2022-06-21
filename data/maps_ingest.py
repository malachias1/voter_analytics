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
                'geometry_wkb': dmaps.geometry.to_wkb(hex=True),
                'center_wkb': dmaps.center.to_wkb(hex=True)}
        return pd.DataFrame(data=data)

    def ingest(self):
        dmaps = self.read_maps()
        con = self.db.con
        dmaps.to_sql(self.tablename, con, if_exists='replace', index=False)
        stmt = f"""
        drop index if exists {self.tablename}_idx;
        CREATE INDEX IF NOT EXISTS {self.tablename}_idx ON {self.tablename} (district);
        """
        self.db.run_script(stmt)


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
        return 'sen_map'


class IngestVTDMaps(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    @property
    def filename(self):
        return 'VTD2020-Shapefile'

    @property
    def tablename(self):
        return 'vtd_map'

    def read_maps(self):
        p = Path(self.maps_dir, 'vtd', self.filename, f'{self.filename}.shp').expanduser()
        vmaps = gpd.read_file(p).to_crs(epsg=3035)
        vmaps = vmaps.assign(center=vmaps.centroid)
        vmaps = vmaps[['ID', 'AREA', 'DISTRICT', 'CTYSOSID', 'PRECINCT_I', 'PRECINCT_N', 'CNTY', 'FIPS2',
                       'CTYNAME', 'geometry', 'center']].reset_index()
        vmaps = vmaps.rename(columns={'ID': 'id', 'AREA': 'area', 'DISTRICT': 'district', 'CTYSOSID': 'county_sosid',
                                      'PRECINCT_I': 'precinct_id', 'PRECINCT_N': 'precinct_name',
                                      'CNTY': 'county_code', 'FIPS2': 'county_fips',
                                      'CTYNAME': 'county_name'})
        vmaps = vmaps.dropna()
        data = {'id': vmaps.id, 'area': vmaps.area,
                'district': vmaps.district,
                'county_sosid': vmaps.county_sosid,
                'precinct_id': vmaps.precinct_id,
                'precinct_name': vmaps.precinct_name,
                'county_code': vmaps.county_code,
                'county_fips': vmaps.county_fips,
                'county_name': vmaps.county_name,
                'geometry_wkb': vmaps.geometry.to_wkb(hex=True),
                'center_wkb': vmaps.center.to_wkb(hex=True)}
        return pd.DataFrame(data=data)

    def ingest(self):
        dmaps = self.read_maps()
        con = self.db.con
        dmaps.to_sql(self.tablename, con, if_exists='replace', index=False)
        stmt = f"""
        drop index if exists {self.tablename}_idx;
        CREATE INDEX IF NOT EXISTS {self.tablename}_idx ON {self.tablename} (county_code);
        """
        self.db.run_script(stmt)
        county_details = dmaps[['county_code', 'county_fips', 'county_name']].drop_duplicates()
        county_details.to_sql('county_details', con, if_exists='replace', index=False)


class IngestCountyMaps(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    @property
    def filename(self):
        return 'cb_2018_ga_county_500k'

    @property
    def tablename(self):
        return 'county_map'

    def read_maps(self):
        p = Path(self.maps_dir, self.filename, f'{self.filename}.shp').expanduser()
        cmaps = gpd.read_file(p).to_crs(epsg=3035)
        cmaps = cmaps.assign(center=cmaps.centroid)
        cmaps = cmaps[['STATEFP', 'COUNTYFP', 'GEOID', 'NAME', 'ALAND',
                       'AWATER', 'geometry', 'center']].reset_index()
        cmaps = cmaps.rename(columns={'STATEFP': 'state_fips', 'COUNTYFP': 'county_fips',
                                      'GEOID': 'geoid', 'NAME': 'county_name', 'ALAND': 'aland', 'AWATER': 'awater'})
        data = {'state_fips': cmaps.state_fips,
                'county_fips': cmaps.county_fips,
                'geoid': cmaps.geoid,
                'county_name': cmaps.county_name,
                'aland': cmaps.aland,
                'awater': cmaps.awater,
                'geometry_wkb': cmaps.geometry.to_wkb(hex=True),
                'center_wkb': cmaps.center.to_wkb(hex=True)}
        return pd.DataFrame(data=data)

    def ingest(self):
        dmaps = self.read_maps()
        cd = self.db.county_details[['county_code', 'county_fips']]
        dmaps = dmaps.merge(cd, on='county_fips', how='inner')[['state_fips', 'county_fips', 'county_code',
                                                                'geoid', 'county_name', 'aland',
                                                                'awater', 'geometry_wkb', 'center_wkb']]
        con = self.db.con
        dmaps.to_sql(self.tablename, con, if_exists='replace', index=False)
        stmt = f"""
        drop index if exists {self.tablename}_idx;
        CREATE INDEX IF NOT EXISTS {self.tablename}_idx ON {self.tablename} (county_code);
        """
        self.db.run_script(stmt)
