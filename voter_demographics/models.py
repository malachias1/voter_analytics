from django.db import models


class VoterDemographicsManager(models.Manager):
    def get_for_voter_ids(self, voter_ids):
        return self.filter(voter_id__in=voter_ids)


class VoterDemographics(models.Model):
    voter_id = models.TextField(primary_key=True)
    race_id = models.TextField()
    gender = models.CharField(max_length=1)
    year_of_birth = models.IntegerField()

    objects = VoterDemographicsManager()

    @property
    def as_record(self):
        return {
            'voter_id': self.voter_id,
            'race_id': self.race_id,
            'gender': self.gender,
            'year_of_birth': self.year_of_birth
        }

    class Meta:
        managed = False
        db_table = 'voter_demographics'
