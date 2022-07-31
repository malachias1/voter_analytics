from core.models import DistrictMapModel, DistrictMapModelManager
from vtd_map.models import VtdMapMixin
from analytics.models import VoterSen
from county_map.models import CountyMap


class SenMapManager(DistrictMapModelManager):
    @classmethod
    def get_districts_for_voters(cls, voter_ids):
        return [o.sen for o in VoterSen.objects.filter(voter_id__in=voter_ids)]

    def get_maps(self, counties, **kwargs):
        return super().get_maps(counties, map_cls=VoterSen)

    def get_county_choropleth(self, county_code):
        cm = CountyMap.objects.get(county_code=county_code)
        return cm.get_district_choropleth(self.get_maps(county_code),
                                          labels={'district': "State Senate District"})


class SenMap(VtdMapMixin, DistrictMapModel):
    objects = SenMapManager()

    @property
    def voters(self):
        return [v.voter_id for v in VoterSen.objects.filter(sen=self.district)]

    class Meta:
        managed = False
        db_table = 'sen_map'
