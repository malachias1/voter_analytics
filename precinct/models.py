from django.db import models
from core.models import BaseMapModel
from county.models import County
import geopandas as gpd
from pathlib import Path
import re
from datetime import date


class PrecinctEditionManager(models.Manager):
    @property
    def default_comment(self):
        return f'Updated on {self.today_str}'

    @property
    def today_str(self):
        return date.today().strftime("%Y-%m-%d")

    def update_edition(self, path, comments=None, year=None, create=False):
        comments = comments or self.default_comment
        year = year or int(re.match(r'VTD(\d+)', path.stem).group(1))
        try:
            edition = PrecinctEdition.objects.get(year=year)
            edition.comments += comments
            edition.save()
        except PrecinctEdition.DoesNotExist:
            if create:
                edition_date = date.today()
                edition = PrecinctEdition(year=year,
                                          date=edition_date,
                                          path=str(path),
                                          comments=comments)
                edition.save()
            else:
                edition = None
        return edition

    def load_fulton_county(self, path, year=None):
        p = Path(path)
        comments = f'Loading Fulton County, {self.today_str}'
        edition = self.update_edition(p, comments=comments, year=year, create=True)
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
        edition = self.update_edition(p, comments=comments, year=year, create=True)
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


class PrecinctEdition(models.Model):
    year = models.IntegerField()
    date = models.DateField()
    path = models.TextField()
    comments = models.TextField()

    objects = PrecinctEditionManager()


class Precinct(models.Model):
    edition = models.ForeignKey('PrecinctEdition', on_delete=models.CASCADE)
    county = models.ForeignKey(County, on_delete=models.CASCADE)
    precinct_id = models.TextField()
    precinct_name = models.TextField(blank=True, null=True)
    aka = models.TextField(blank=True, null=True)
    precinct_map = models.ForeignKey('PrecinctMap', on_delete=models.SET_NULL, blank=True, null=True)


class PrecinctMap(BaseMapModel):
    edition = models.ForeignKey('PrecinctEdition', on_delete=models.CASCADE)
    county = models.ForeignKey(County, on_delete=models.CASCADE)
    precinct_id = models.TextField()
    geometry_wkb = models.TextField()
