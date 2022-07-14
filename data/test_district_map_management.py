import unittest
import django
import os

import pandas as pd

from data.district_map_management import USHouseMapManagement
import geopandas as gpd
from pathlib import Path

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from cng_map.models import CngMap

class TestDistrictMapManagement(unittest.TestCase):
    def test_read_maps(self):
        root_dir = '~/Documents/data'
        mam = USHouseMapManagement(root_dir)
        p = Path(mam.maps_dir, mam.filename, 'districts.shp').expanduser()
        dmaps = mam.read_maps()
        gpd.GeoSeries.from_wkb(dmaps.geometry_wkb, crs='epsg:3035')
        results = mam.db.fetchall('select * from cng_map')
        x = pd.DataFrame.from_records(results, columns=[
            'id', 'area', 'district', 'population', 'ideal_value',
            'geometry_wkb', 'center_wkb'
        ])
        gpd.GeoSeries.from_wkb(x.geometry_wkb)
        print(x.geometry_wkb.shape)
        print(len(x.geometry_wkb.iloc[0]))
        print(x.district.iloc[0])
        print(len(CngMap.objects.all().first().geometry_wkb))
        print(CngMap.objects.all().first().district)
        k = str(CngMap.objects.all().first().geometry_wkb)
        print(x.geometry_wkb.iloc[0] == CngMap.objects.all().first().geometry_wkb)
        print(type(x.geometry_wkb.values[0]))
        gpd.GeoSeries.from_wkb(x[x.district=='007'].geometry_wkb)

if __name__ == '__main__':
    unittest.main()
