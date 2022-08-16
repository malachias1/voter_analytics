from core.models import DistrictMapModel, DistrictMapModelManager
from voter.models import Voter
from precinct.models import PrecinctMapMixin
import geopandas as gpd


class HseMapManager(DistrictMapModelManager):
    @classmethod
    def get_districts_in_county(cls, county):
        return [x.cng for x in Voter.objects.filter(county=county).distinct('cng')]

    def get_county_choropleth(self, precinct_edition, county):
        county_map = county.map_of.first()
        districts = self.get_districts_in_county(county)
        gdf = gpd.GeoDataFrame([self.get(district=d).as_record for d in districts], crs=self.CRS_LAT_LON)
        return county_map.get_district_choropleth(gdf, precinct_edition, labels={'district': "State House District"})


class HseMap(PrecinctMapMixin, DistrictMapModel):
    objects = HseMapManager()

    @property
    def contest_category(self):
        return 'ga_house'

    def get_voters_(self):
        return Voter.objects.filter(edition=self.edition, hse=self.district, status='A')

    class Meta:
        managed = False
        db_table = 'hse_map'
