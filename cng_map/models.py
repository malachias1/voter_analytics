from django.db import models


class CngMap(models.Model):
    id = models.IntegerField(primary_key=True)
    area = models.FloatField()
    district = models.TextField()
    population = models.IntegerField()
    ideal_value = models.FloatField()
    geometry_wkb = models.TextField()
    center_wkb = models.TextField()

    class Meta:
        managed = False
        db_table = 'cng_map'

    @property
    def maps(self):
        return CngMap.objects.all()
