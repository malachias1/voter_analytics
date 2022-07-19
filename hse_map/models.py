from core.models import DistrictMapModel


class HseMap(DistrictMapModel):
    pass

    class Meta:
        managed = False
        db_table = 'hse_map'


