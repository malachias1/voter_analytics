from pathlib import Path
import pandas as pd
import geopandas as gpd

from county.models import County, CountyMap


class CountyGeographyLoader:
    @property
    def county_codes_path(self):
        return '../resources/county_codes.csv'

    @property
    def county_code2fips(self):
        df = pd.DataFrame.from_csv(self.county_codes_path,
                                   dtypes={'county_name': str,
                                           'county_code': str,
                                           'county_fips': str,
                                           })
        return {df.loc[i].county_code: {df.loc[i].county_fips, df.loc[i].county_name}
                for i in df.index}

    @property
    def county_fips2code(self):
        df = pd.DataFrame.from_csv(self.county_codes_path,
                                   dtypes={'county_name': str,
                                           'county_code': str,
                                           'county_fips': str,
                                           })
        return {df.loc[i].county_fips: {df.loc[i].county_code, df.loc[i].county_name}
                for i in df.index}

    def load(self, path):
        p = Path(path).expanduser()
        county_fips2code = self.county_fips2code
        cmaps = gpd.read_file(p)
        counties = []
        for i in cmaps.index:
            row = cmaps.loc[i]
            county_fips = row.COUNTYFP
            county_code = county_fips2code[county_fips],
            counties.append(County(
                county_fips=county_fips,
                county_code=county_code,
                state_fips=row.STATEFP,
                geoid=row.GEOID,
                county_name=row.NAME.upper(),
                aland=float(row.ALAND),
                awater=float(row.AWATER)))
        County.objects.bulk_create(counties)

        county_maps = []
        for i in cmaps.index:
            row = cmaps.loc[i]
            county_fips = row.COUNTYFP
            county_code = county_fips2code[county_fips]
            geometry_wkb = row.geometry.to_wkb(hex=True)
            county = County.objects.get(county_code=county_code)
            county_maps.append(CountyMap(county=county,
                                         geometry_wkb=geometry_wkb))
        CountyMap.objects.bulk_create(county_maps)
