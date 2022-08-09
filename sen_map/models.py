from core.models import DistrictMapModel, DistrictMapModelManager
from vtd_map.models import VtdMapMixin
from voter.models import Voter
from county.models import County
import geopandas as gpd


class SenMapManager(DistrictMapModelManager):
    def get_county_choropleth(self, county_code):
        county = County.objects.get(county_code=county_code)
        districts = [x.cng for x in Voter.objects.filter(county=county).distinct('cng')]
        gdf = gpd.GeoDataFrame([self.get(district=d).as_record for d in districts],
                               crs=self.CRS_LAT_LON)
        return county.get_district_choropleth(gdf, labels={'district': "State Senate District"})


class SenMap(VtdMapMixin, DistrictMapModel):
    objects = SenMapManager()

    @property
    def voters(self):
        if self.edition is None:
            raise AttributeError('edition not set!')
        return list(Voter.objects.filter(edition=self.edition, sen=self.district, status='A'))

    class Meta:
        managed = False
        db_table = 'sen_map'
