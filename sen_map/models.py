from core.models import DistrictMapModel, DistrictMapModelManager
from analytics.models import VoterSen


class SenMapManager(DistrictMapModelManager):
    def get_maps(self, counties, **kwargs):
        return super().get_maps(counties, map_cls=VoterSen)


class SenMap(DistrictMapModel):
    objects = SenMapManager()

    @property
    def voters(self):
        return [v.voter_id for v in VoterSen.objects.filter(sen=self.district)]

    class Meta:
        managed = False
        db_table = 'sen_map'
