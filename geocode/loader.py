import shutil
from pathlib import Path

import pandas as pd

from county.models import County
from geocode.models import Address


class AddressLoader:
    def __init__(self, root_dir):
        self.root_dir = Path(root_dir)

    def load(self):
        dtypes = {
            'County': str,
            'street': str,
            'city': str,
            'state': str,
            'zipcode': str,
            'Latitude': float,
            'Longitude': float,
            'Accuracy Score': float,
            'Accuracy Type': str,
            'ZIP+4 Range Start': str,
            'Building or Firm Name': str,
            'Record Type Description': str
        }

        rename_spec = {
            'County': 'county_name',
            'Latitude': 'lat',
            'Longitude': 'lon',
            'Accuracy Score': 'accuracy_score',
            'Accuracy Type': 'accuracy_type',
            'ZIP+4 Range Start': 'plus4',
            'Building or Firm Name': 'building_name',
            'Record Type Description': 'record_type_description'
        }

        keep_columns = [
            'county_name',
            'street',
            'city',
            'state',
            'zipcode',
            'plus4',
            'building_name',
            'lat',
            'lon',
            'accuracy_score',
            'accuracy_type',
            'record_type_description'
        ]

        for f in Path(self.root_dir).expanduser().iterdir():
            if f.suffix == '.csv':
                print(f'Loading {f.name} ...', end='')
                df = pd.read_csv(f, dtype=dtypes, engine='python',
                                 keep_default_na=False)
                df = df.rename(columns=rename_spec)
                df = df[keep_columns]
                df = df.assign(county_name=df.county_name.apply(lambda x: x.replace(' county', '').upper()),
                               building_name=df.building_name.str.upper())
                counties = {}
                addresses = []
                for i in df.index:
                    row = df.loc[i]
                    county_name = row.county_name.replace(' COUNTY', '').upper()
                    county = counties.get(county_name, None)
                    if county is None:
                        county = County.objects.get(county_name=county_name)
                        counties[county_name] = county
                    addresses.append(Address(lat=row.lat,
                                             lon=row.lon,
                                             accuracy_score=row.accuracy_score,
                                             accuracy_type=row.accuracy_type,
                                             county=county,
                                             street=row.street,
                                             city=row.city,
                                             state=row.state,
                                             zipcode=row.zipcode,
                                             plus4=row.plus4,
                                             building_name=row.building_name,
                                             record_type_description=row.record_type_description))
                Address.objects.bulk_create(addresses)
                move_to = Path(f.parent, 'processed', f.name)
                shutil.move(f, move_to)
                print(f'done!')
