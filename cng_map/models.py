from core.models import DistrictMapModel, DistrictMapModelManager
from analytics.models import VoterCng


class CngMapManager(DistrictMapModelManager):
    def get_maps(self, counties, **kwargs):
        return super().get_maps(counties, map_cls=VoterCng)


class CngMap(DistrictMapModel):
    objects = CngMapManager()

    @property
    def voters(self):
        return [v.voter_id for v in VoterCng.objects.filter(sen=self.district)]

    class Meta:
        managed = False
        db_table = 'cng_map'
