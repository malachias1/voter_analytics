from core.models import DistrictMapModel, DistrictMapModelManager
from vtd_map.models import VtdMapMixin
from analytics.models import VoterHse
from county_map.models import CountyMap


class HseMapManager(DistrictMapModelManager):
    @classmethod
    def get_districts_for_voters(cls, voter_ids):
        return [o.hse for o in VoterHse.objects.filter(voter_id__in=voter_ids)]

    def get_maps(self, counties, **kwargs):
        return super().get_maps(counties, map_cls=VoterHse)

    def get_county_choropleth(self, county_code):
        cm = CountyMap.objects.get(county_code=county_code)
        return cm.get_district_choropleth(self.get_maps(county_code),
                                          labels={'district': "State House District"})


class HseMap(VtdMapMixin, DistrictMapModel):
    objects = HseMapManager()

    @property
    def contest_category(self):
        return 'ga_house'

    @property
    def voters(self):
        return [v.voter_id for v in VoterHse.objects.filter(hse=self.district)]

    class Meta:
        managed = False
        db_table = 'hse_map'
