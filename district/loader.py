import pandas as pd
from pathlib import Path
import geopandas as gpd

from core.base_fig import BaseMap


class DistrictMapLoaderBase(BaseMap):
    def load(self, path):
        p = Path(path, 'districts.shp').expanduser()
        # Other code expects geometry to remain in meters
        dmaps = gpd.read_file(p).to_crs(self.CRS_METERS)
        dmaps = dmaps[['DISTRICT', 'AREA', 'POPULATION', 'IDEAL_VALU', 'geometry']]
        dmaps = dmaps.rename(columns={'AREA': 'area',
                                      'DISTRICT': 'district',
                                      'POPULATION': 'population',
                                      'IDEAL_VALU': 'ideal_value'})
        data = {'area': dmaps.area,
                'district': dmaps.district,
                'population': dmaps.population,
                'ideal_value': dmaps.ideal_value,
                'geometry_wkb': dmaps.geometry.to_wkb(hex=True),
                'center_wkb': dmaps.center.to_wkb(hex=True)}

        return pd.DataFrame(data=data)

