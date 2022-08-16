from django.db import models
from county.models import County
from precinct.models import Precinct
from datetime import datetime


class ListEditionManager(models.Manager):
    pass


class ListEdition(models.Model):
    date = models.DateField()
    path = models.TextField()
    comments = models.TextField()

    objects = ListEditionManager()

    @property
    def general_election_year(self):
        if self.date >= datetime.strptime('2022-5-24', '%Y-%m-%d').date():
            return 2022
        return 2020

    def voters_for_county(self, county_code):
        return Voter.objects.filter(edition=self, county__county_code=county_code)


class MailingAddress(models.Model):
    voter = models.ForeignKey('voter.Voter', on_delete=models.CASCADE, related_name="mailing_address_of")
    # PO Boxes don't have house numbers
    house_number = models.CharField(max_length=32, blank=True, null=True)
    street_name = models.CharField(max_length=128)
    apt_no = models.CharField(max_length=32, blank=True, null=True)
    city = models.CharField(max_length=64)
    state = models.CharField(max_length=2, blank=True, null=True)
    zipcode = models.CharField(max_length=5, blank=True, null=True)
    plus4 = models.CharField(max_length=4, blank=True, null=True)
    address_line2 = models.CharField(max_length=64, blank=True, null=True)
    address_line3 = models.CharField(max_length=64, blank=True, null=True)
    country = models.CharField(max_length=32, blank=True, null=True)


class OtherJurisdictions(models.Model):
    voter = models.ForeignKey('voter.Voter', on_delete=models.CASCADE)
    county = models.ForeignKey(County, on_delete=models.CASCADE)
    land_district = models.CharField(max_length=5, blank=True, null=True)
    land_lot = models.CharField(max_length=5, blank=True, null=True)
    city_precinct_id = models.CharField(max_length=5, blank=True, null=True)
    judicial_district = models.CharField(max_length=4, blank=True, null=True)
    commission_district = models.CharField(max_length=3, blank=True, null=True)
    school_district = models.CharField(max_length=3, blank=True, null=True)
    county_districta_name = models.CharField(max_length=5, blank=True, null=True)
    county_districta_value = models.CharField(max_length=3, blank=True, null=True)
    county_districtb_name = models.CharField(max_length=5, blank=True, null=True)
    county_districtb_value = models.CharField(max_length=2, blank=True, null=True)
    municipal_name = models.CharField(max_length=32, blank=True, null=True)
    municipal_code = models.CharField(max_length=3, blank=True, null=True)
    ward_city_council_name = models.CharField(max_length=5, blank=True, null=True)
    ward_city_council_code = models.CharField(max_length=3, blank=True, null=True)
    city_school_district_name = models.CharField(max_length=5, blank=True, null=True)
    city_school_district_value = models.CharField(max_length=3, blank=True, null=True)
    district_combo = models.CharField(max_length=3, blank=True, null=True)


class ResidenceAddress(models.Model):
    # I need an address for every voter.
    # I need to also be able to capture
    # the fact that a voter has a bad address.
    voter = models.ForeignKey('voter.Voter', on_delete=models.CASCADE, related_name="residence_address_of")
    house_number = models.CharField(max_length=32, blank=True, null=True)
    street_name = models.CharField(max_length=64, blank=True, null=True)
    apt_no = models.CharField(max_length=32, blank=True, null=True)
    city = models.CharField(max_length=32, blank=True, null=True)
    state = models.CharField(max_length=2)
    zipcode = models.CharField(max_length=5, blank=True, null=True)
    plus4 = models.CharField(max_length=4, blank=True, null=True)


class VoterManager(models.Manager):
    def active(self):
        return self.filter(status='A')


class Voter(models.Model):
    edition = models.ForeignKey('voter.ListEdition', on_delete=models.SET_NULL, blank=True, null=True)
    voter_id = models.CharField(max_length=8)
    race_id = models.CharField(max_length=2)
    gender = models.CharField(max_length=1)
    year_of_birth = models.IntegerField()
    status = models.CharField(max_length=1)
    status_reason = models.CharField(max_length=16, blank=True, null=True)
    date_added = models.DateField(blank=True, null=True)
    date_changed = models.DateField(blank=True, null=True)
    registration_date = models.DateField(blank=True, null=True)
    last_contact_date = models.DateField(blank=True, null=True)
    last_name = models.CharField(max_length=64)
    first_name = models.CharField(max_length=32)
    middle_name = models.CharField(max_length=32, blank=True, null=True)
    name_suffix = models.CharField(max_length=3, blank=True, null=True)
    precinct = models.ForeignKey(Precinct, on_delete=models.SET_NULL, blank=True, null=True)
    county = models.ForeignKey(County, on_delete=models.SET_NULL, blank=True, null=True)
    cng = models.CharField(max_length=3)
    hse = models.CharField(max_length=3)
    sen = models.CharField(max_length=3)
    # For recovery of edition
    edition_year = models.IntegerField()
    edition_date = models.DateField()
    date_loaded = models.DateField()
    # in case precinct_id is not found.
    precinct_id_text = models.CharField(max_length=16)

    objects = VoterManager()

    class Meta:
        indexes = [
            models.Index(fields=['voter_id']),
            models.Index(fields=['status']),
            models.Index(fields=['cng']),
            models.Index(fields=['hse']),
            models.Index(fields=['sen']),
            models.Index(fields=['county']),
            models.Index(fields=['edition'])
        ]
