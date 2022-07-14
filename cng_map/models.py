from django.db import models
from core.models import DistrictMapModel


class CngMap(DistrictMapModel):
    pass

    class Meta:
        managed = False
        db_table = 'cng_map'
