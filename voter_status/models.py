from django.db import models


class VoterStatusManager(models.Manager):
    def get_for_voter_ids(self, voter_ids):
        return self.filter(voter_id__in=voter_ids)


class VoterStatus(models.Model):
    voter_id = models.TextField(primary_key=True)
    status = models.TextField()
    status_reason = models.TextField(blank=True, null=True)
    date_added = models.TextField()
    date_changed = models.TextField()
    registration_date = models.TextField()
    last_contact_date = models.TextField()

    objects = VoterStatusManager()

    @property
    def as_record(self):
        return {
            'voter_id': self.voter_id,
            'status': self.status,
            'status_reason': self.status_reason,
            'date_added': self.date_added,
            'date_changed': self.date_changed,
            'registration_date': self.registration_date,
            'last_contact_date': self.last_contact_date
        }

    class Meta:
        managed = False
        db_table = 'voter_status'

