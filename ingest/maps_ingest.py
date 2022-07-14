import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb
from pathlib import Path
import geopandas as gpd


class IngestMapsBase(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb()

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


class IngestVTDMapsBase(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    @property
    def dirname(self):
        raise NotImplemented('dirname method is not implemented.')

    @property
    def filename(self):
        raise NotImplemented('filename method is not implemented.')

    @property
    def tablename(self):
        return 'vtd_map'

    @classmethod
    def add_county_code(cls, df):
        raise NotImplemented('add_county_code method is not implemented.')

    @classmethod
    def add_county_fips(cls, df):
        raise NotImplemented('add_county_fips method is not implemented.')

    @classmethod
    def add_county_name(cls, df):
        raise NotImplemented('add_county_name method is not implemented.')

    def rename_columns(self, df):
        raise NotImplemented('rename_columns method is not implemented.')

    @classmethod
    def select_columns(self, df):
        return df[['area', 'precinct_id', 'precinct_name',
                   'county_code', 'county_fips', 'county_name',
                   'center', 'geometry']]

    def read_maps(self):
        p = Path(self.maps_dir, 'vtd', self.dirname, f'{self.filename}.shp').expanduser()
        vmaps = gpd.read_file(p).to_crs(epsg=3035)
        vmaps = vmaps.assign(center=vmaps.centroid)
        vmaps = self.rename_columns(vmaps)
        vmaps = self.add_county_code(vmaps)
        vmaps = self.add_county_fips(vmaps)
        vmaps = self.add_county_name(vmaps)
        vmaps = self.select_columns(vmaps)
        vmaps = vmaps[vmaps.geometry.notna()]
        data = {'area': vmaps.area,
                'precinct_id': vmaps.precinct_id,
                'precinct_name': vmaps.precinct_name,
                'county_code': vmaps.county_code,
                'county_fips': vmaps.county_fips,
                'county_name': vmaps.county_name,
                'geometry_wkb': vmaps.geometry.to_wkb(hex=True),
                'center_wkb': vmaps.center.to_wkb(hex=True)}
        return pd.DataFrame(data=data)

    def purge_county_vtds(self, c):
        if self.db.vtd_maps_exists:
            script = f"""
                delete from vtd_map where county_code = '{c}';
            """
            self.db.run_script(script)

    def ingest(self):
        vmaps = self.read_maps()
        con = self.db.con
        next_id = self.db.next_vtd_id
        for c in vmaps.county_code.unique():
            print(f'county_code {c}')
            self.purge_county_vtds(c)
            df = vmaps[vmaps.county_code == c]
            df.insert(0, 'id', range(next_id, next_id + len(df.index)))
            next_id += len(df.index)
            df.to_sql(self.tablename, con, if_exists='append', index=False)
        stmt = f"""
        drop index if exists {self.tablename}_idx;
        CREATE INDEX IF NOT EXISTS {self.tablename}_idx ON {self.tablename} (county_code, precinct_id);
        """
        self.db.run_script(stmt)
        county_details = vmaps[['county_code', 'county_fips', 'county_name']].drop_duplicates()
        county_details.to_sql('county_details', con, if_exists='replace', index=False)


class IngestStateVTDMaps(IngestVTDMapsBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    @property
    def dirname(self):
        return 'VTD2020-Shapefile'

    @property
    def filename(self):
        return 'VTD2020-Shapefile'

    @classmethod
    def add_county_code(cls, df):
        return df

    @classmethod
    def add_county_fips(cls, df):
        return df

    @classmethod
    def add_county_name(cls, df):
        return df

    @classmethod
    def rename_columns(cls, df):
        return df.rename(columns={'ID': 'id', 'AREA': 'area',
                                  'PRECINCT_I': 'precinct_id', 'PRECINCT_N': 'precinct_name',
                                  'CNTY': 'county_code', 'FIPS2': 'county_fips',
                                  'CTYNAME': 'county_name'})


class IngestFultonVTDMaps(IngestVTDMapsBase):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    @property
    def dirname(self):
        return 'fulton_county_2022'

    @property
    def filename(self):
        return 'Voting_Precincts'

    @classmethod
    def add_county_code(cls, df):
        return df.assign(county_code='060')

    @classmethod
    def add_county_fips(cls, df):
        return df.assign(county_fips='121')

    @classmethod
    def add_county_name(cls, df):
        return df.assign(county_name='FULTON')

    @classmethod
    def rename_columns(cls, df):
        return df.rename(columns={'Shape__Are': 'area', 'VoterDist': 'precinct_id', 'PrecinctN': 'precinct_name'})


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
