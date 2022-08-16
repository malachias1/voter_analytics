from core.models import DistrictMapModel, DistrictMapModelManager
from precinct.models import PrecinctMapMixin
from voter.models import Voter
import geopandas as gpd


class SenMapManager(DistrictMapModelManager):
    @classmethod
    def get_districts_in_county(cls, county):
        return [x.cng for x in Voter.objects.filter(county=county).distinct('cng')]

    def get_county_choropleth(self, precinct_edition, county):
        county_map = county.map_of.first()
        districts = self.get_districts_in_county(county)
        gdf = gpd.GeoDataFrame([self.get(district=d).as_record for d in districts], crs=self.CRS_LAT_LON)
        return county_map.get_district_choropleth(gdf, precinct_edition, labels={'district': "State Senate District"})


class SenMap(PrecinctMapMixin, DistrictMapModel):
    objects = SenMapManager()

    @property
    def contest_category(self):
        return 'ga_senate'

    def get_voters_(self):
        return Voter.objects.filter(edition=self.edition, sen=self.district, status='A')

    class Meta:
        managed = False
        db_table = 'sen_map'
