from django.db import models
from core.base_fig import BaseMapModel, BaseMapModelManager
from county.models import County


class BlockGroupMapManager(BaseMapModelManager):
    def get_maps(self, counties):
        if isinstance(counties, str):
            counties = (counties,)
        county_fips = [x.county_fips for x in County.objects.filter(county_code__in=counties)]
        self.filter(county_fips__in=county_fips)


class BlockGroupMap(BaseMapModel):
    geoid = models.CharField(max_length=12)
    state_fips = models.CharField(max_length=2)
    county_fips = models.CharField(max_length=3)
    tract = models.CharField(max_length=6)
    blockgroup = models.CharField(max_length=1)
    geometry_wkb = models.TextField()
    center_wkb = models.TextField()

    @property
    def as_record(self):
        record = super().as_record()
        record.update({
            'geoid': self.geoid,
            'state_fips': self.state_fips,
            'county_fips': self.county_fips,
            'tract': self.tract,
            'blockgroup': self.blockgroup
        })
        return record

    class Meta:
        indexes = [
            models.Index(fields=['county_fips'])
        ]
        db_table = 'blockgroup_map'


class BlockGroupMapMixin:
    @property
    def county_blockgroup_map(self):
        bmaps = BlockGroupMap.objects.get_maps(self.counties)
        return bmaps.assign(bg_id=range(0, len(bmaps.index)))
