from django.db import models
from core.models import DistrictMapModel


class SenMap(DistrictMapModel):
    pass

    class Meta:
        managed = False
        db_table = 'sen_map'

