from core.models import DistrictMapModel, DistrictMapModelManager
from vtd_map.models import VtdMapMixin
from analytics.models import VoterHse


class HseMapManager(DistrictMapModelManager):
    def get_maps(self, counties, **kwargs):
        return super().get_maps(counties, map_cls=VoterHse)


class HseMap(VtdMapMixin, DistrictMapModel):
    objects = HseMapManager()

    @property
    def voters(self):
        return [v.voter_id for v in VoterHse.objects.filter(hse=self.district)]

    class Meta:
        managed = False
        db_table = 'hse_map'
