from pathlib import Path
import geopandas as gpd
from datetime import date

from county.models import County
from precinct.models import Precinct, PrecinctMap, PrecinctEdition


class PrecinctMapLoader:
    @property
    def today_str(self):
        return date.today().strftime("%Y-%m-%d")

    def load_fulton_county(self, path, year=None):
        p = Path(path)
        comments = f'Loading Fulton County, {self.today_str}'
        edition = PrecinctEdition.objects.update_edition(p, comments=comments, year=year, create=True)
        shapes = gpd.read_file(p)
        shapes = shapes[shapes.geometry.notnull() & shapes.VoterDist.notnull()]
        self.load_fulton_county_maps(shapes, edition)
        self.load_fulton_county_details(shapes, edition)

    @classmethod
    def load_fulton_county_maps(cls, shapes, edition):
        county_code = '060'
        county = County.objects.get(county_code=county_code)
        PrecinctMap.objects.filter(edition=edition, county=county).delete()
        shapes = shapes.assign(geometry_wkb=shapes.geometry.to_wkb(hex=True))
        count = 1
        precinct_maps = []
        for i in shapes.index:
            row = shapes.loc[i]
            precinct_id = row.VoterDist.upper()
            print(f'{count:04d}: {county_code} {precinct_id}')
            count += 1
            precinct_maps.append(PrecinctMap(edition=edition,
                                             county=county,
                                             precinct_id=precinct_id,
                                             geometry_wkb=row.geometry_wkb))
        PrecinctMap.objects.bulk_create(precinct_maps, batch_size=100)

    @classmethod
    def load_fulton_county_details(cls, shapes, edition):
        county_code = '060'
        county = County.objects.get(county_code=county_code)
        Precinct.objects.filter(edition=edition, county=county).delete()
        precincts = []
        count = 1
        for i in shapes.index:
            row = shapes.loc[i]
            precinct_id = row.VoterDist.upper()
            precinct_name = precinct_id
            print(f'{count:04d}: {county_code} {precinct_id}')
            count += 1
            precinct_map = PrecinctMap.objects.get(edition=edition,
                                                   county=county,
                                                   precinct_id=precinct_id)

            precincts.append(Precinct(edition=edition,
                                      county=county,
                                      precinct_id=precinct_id,
                                      precinct_name=precinct_name,
                                      precinct_map=precinct_map))

        Precinct.objects.bulk_create(precincts)

    def load_state(self, path, comments, year=None):
        p = Path(path)
        edition = PrecinctEdition.objects.update_edition(p, comments=comments, year=year, create=True)
        shapes = gpd.read_file(p)
        shapes = shapes[shapes.geometry.notnull() & shapes.PRECINCT_I.notnull()]
        self.load_state_maps(shapes, edition)
        self.load_state_details(shapes, edition)

    @classmethod
    def load_state_maps(cls, shapes, edition):
        PrecinctMap.objects.filter(edition=edition).delete()
        counties = {x.county_code: x for x in County.objects.all()}
        precinct_maps = []
        count = 1
        shapes = shapes.assign(geometry_wkb=shapes.geometry.to_wkb(hex=True))
        for i in shapes.index:
            row = shapes.loc[i]
            county_code = row.CNTY
            precinct_id = row.PRECINCT_I.upper()
            print(f'{count:04d}: {county_code} {precinct_id}')
            count += 1
            county = counties[county_code]
            precinct_maps.append(PrecinctMap(edition=edition,
                                             county=county,
                                             precinct_id=precinct_id,
                                             geometry_wkb=row.geometry_wkb))
        PrecinctMap.objects.bulk_create(precinct_maps, batch_size=100)

    @classmethod
    def load_state_details(cls, shapes, edition):
        Precinct.objects.filter(edition=edition).delete()
        counties = {x.county_code: x for x in County.objects.all()}
        precincts = []
        count = 1
        for i in shapes.index:
            row = shapes.loc[i]
            county_code = row.CNTY
            precinct_id = row.PRECINCT_I.upper()
            print(f'{count:04d}: {county_code} {precinct_id}')
            count += 1
            county = counties[county_code]
            precinct_map = PrecinctMap.objects.get(county=county,
                                                   precinct_id=precinct_id)

            precincts.append(Precinct(edition=edition,
                                      county=county,
                                      precinct_id=precinct_id,
                                      precinct_name=row.PRECINCT_N,
                                      precinct_map=precinct_map))

        Precinct.objects.bulk_create(precincts)
