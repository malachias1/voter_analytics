from core.models import DistrictMapModel, DistrictMapModelManager
from vtd_map.models import VtdMapMixin
from analytics.models import VoterCng
from county_map.models import CountyMap


class CngMapManager(DistrictMapModelManager):
    @classmethod
    def get_districts_for_voters(cls, voter_ids):
        return [o.cng for o in VoterCng.objects.filter(voter_id__in=voter_ids)]

    def get_maps(self, counties, **kwargs):
        return super().get_maps(counties, map_cls=VoterCng)

    def get_county_choropleth(self, county_code):
        cm = CountyMap.objects.get(county_code=county_code)
        return cm.get_district_choropleth(self.get_maps(county_code),
                                          labels={'district': "US House District"})


class CngMap(VtdMapMixin, DistrictMapModel):
    objects = CngMapManager()

    @property
    def voters(self):
        return [v.voter_id for v in VoterCng.objects.filter(cng=self.district)]

    class Meta:
        managed = False
        db_table = 'cng_map'
