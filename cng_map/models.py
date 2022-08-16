from core.models import DistrictMapModel, DistrictMapModelManager
from precinct.models import PrecinctMapMixin
from voter.models import Voter
import geopandas as gpd


class CngMapManager(DistrictMapModelManager):
    @classmethod
    def get_districts_in_county(cls, county):
        return [x.cng for x in Voter.objects.filter(county=county).distinct('cng')]

    def get_county_choropleth(self, precinct_edition, county):
        county_map = county.map_of.first()
        districts = self.get_districts_in_county(county)
        gdf = gpd.GeoDataFrame([self.get(district=d).as_record for d in districts], crs=self.CRS_LAT_LON)
        return county_map.get_district_choropleth(gdf, precinct_edition, labels={'district': "US House District"})


class CngMap(PrecinctMapMixin, DistrictMapModel):
    objects = CngMapManager()

    @property
    def contest_category(self):
        return 'us_house'

    def get_voters_(self):
        return Voter.objects.filter(edition=self.edition, cng=self.district, status='A')

    class Meta:
        managed = False
        db_table = 'cng_map'
