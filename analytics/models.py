# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class AddressVoter(models.Model):
    voter_id = models.TextField(primary_key=True)
    address_id = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'address_voter'


class BlockGroupGeometry(models.Model):
    geoid = models.TextField(db_column='GEOID', primary_key=True)  # Field name made lowercase.
    state = models.TextField()
    county = models.TextField()
    tract = models.TextField()
    block_group = models.TextField()
    geometry = models.TextField()

    class Meta:
        managed = False
        db_table = 'block_group_geometry'


class Children(models.Model):
    geoid = models.TextField(db_column='GEOID', primary_key=True)  # Field name made lowercase.
    state = models.TextField()
    county = models.TextField()
    tract = models.TextField()
    block_group = models.TextField()
    total = models.FloatField()
    male_under_5 = models.FloatField(blank=True, null=True)
    male_5_to_9 = models.FloatField(blank=True, null=True)
    male_10_to_14 = models.FloatField(blank=True, null=True)
    male_15_to_17 = models.FloatField(blank=True, null=True)
    female_under_5 = models.FloatField(blank=True, null=True)
    female_5_to_9 = models.FloatField(blank=True, null=True)
    female_10_to_14 = models.FloatField(blank=True, null=True)
    female_15_to_17 = models.FloatField(blank=True, null=True)
    total_moe = models.FloatField(blank=True, null=True)
    male_under_5_moe = models.FloatField(blank=True, null=True)
    male_5_to_9_moe = models.FloatField(blank=True, null=True)
    male_10_to_14_moe = models.FloatField(blank=True, null=True)
    male_15_to_17_moe = models.FloatField(blank=True, null=True)
    female_under_5_moe = models.FloatField(blank=True, null=True)
    female_5_to_9_moe = models.FloatField(blank=True, null=True)
    female_10_to_14_moe = models.FloatField(blank=True, null=True)
    female_15_to_17_moe = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'children'


# class EducationalAttainment(models.Model):
#     geoid = models.TextField(db_column='GEOID', primary_key=True)  # Field name made lowercase.
#     state = models.TextField()
#     county = models.TextField()
#     tract = models.TextField()
#     block_group = models.TextField()
    # total = models.FloatField(db_column='educational_attainment_total', blank=True, null=True)
    # regular_high_school_diploma = models.FloatField(db_column='educational_attainment_regular_high_school_diploma', blank=True, null=True)
    # ged_or_alternative_credential = models.FloatField(db_column='educational_attainment_ged_or_alternative_credential', blank=True, null=True)
    # some_college_less_than_1_year = models.FloatField(db_column='educational_attainment_some_college_less_than_1_year', blank=True, null=True)
    # some_college_1_or_more_years_no_degree = models.FloatField(db_column='educational_attainment_some_college_1_or_more_years_no_degree', blank=True, null=True)
    # associates_degree = models.FloatField(db_column='educational_attainment_associates_degree', blank=True, null=True)
    # bachelors_degree = models.FloatField(db_column='educational_attainment_bachelors_degree', blank=True, null=True)
    # masters_degree = models.FloatField(db_column='educational_attainment_masters_degree', blank=True, null=True)
    # professional_school_degree = models.FloatField(db_column='educational_attainment_professional_school_degree', blank=True, null=True)
    # doctorate_degree = models.FloatField(db_column='educational_attainment_doctorate_degree', blank=True, null=True)
    # total_moe = models.FloatField(db_column='educational_attainment_total_moe', blank=True, null=True)
    # regular_high_school_diploma_moe = models.FloatField(db_column='educational_attainment_regular_high_school_diploma_moe', blank=True, null=True)
    # ged_or_alternative_credential_moe = models.FloatField(db_column='educational_attainment_ged_or_alternative_credential_moe', blank=True, null=True)
    # some_college_less_than_1_year_moe = models.FloatField(db_column='educational_attainment_some_college_less_than_1_year_moe', blank=True, null=True)
    # some_college_1_or_more_years_no_degree_moe = models.FloatField(db_column='educational_attainment_some_college_1_or_more_years_no_degree_moe', blank=True, null=True)
    # associates_degree_moe = models.FloatField(db_column='educational_attainment_associates_degree_moe', blank=True, null=True)
    # bachelors_degree_moe = models.FloatField(db_column='educational_attainment_bachelors_degree_moe', blank=True, null=True)
    # masters_degree_moe = models.FloatField(db_column='educational_attainment_masters_degree_moe', blank=True, null=True)
    # professional_school_degree_moe = models.FloatField(db_column='educational_attainment_professional_school_degree_moe', blank=True, null=True)
    # doctorate_degree_moe = models.FloatField(db_column='educational_attainment_doctorate_degree_moe', blank=True, null=True)

class EducationalAttainment(models.Model):
        geoid = models.TextField(db_column='GEOID', primary_key=True)  # Field name made lowercase.
        state = models.TextField()
        county = models.TextField()
        tract = models.TextField()
        block_group = models.TextField()
        total = models.FloatField(  db_column='total', blank=True, null=True)
        regular_high_school_diploma = models.FloatField(  db_column='regular_high_school_diploma',
                                                        blank=True, null=True)
        ged_or_alternative_credential = models.FloatField(
              db_column='ged_or_alternative_credential', blank=True, null=True)
        some_college_less_than_1_year = models.FloatField(
              db_column='some_college_less_than_1_year', blank=True, null=True)
        some_college_1_or_more_years_no_degree = models.FloatField(
              db_column='some_college_1_or_more_years_no_degree', blank=True, null=True)
        associates_degree = models.FloatField(  db_column='associates_degree', blank=True,
                                              null=True)
        bachelors_degree = models.FloatField(  db_column='bachelors_degree', blank=True, null=True)
        masters_degree = models.FloatField(  db_column='masters_degree', blank=True, null=True)
        professional_school_degree = models.FloatField(  db_column='professional_school_degree',
                                                       blank=True, null=True)
        doctorate_degree = models.FloatField(  db_column='doctorate_degree', blank=True, null=True)
        total_moe = models.FloatField(  db_column='total_moe', blank=True, null=True)
        regular_high_school_diploma_moe = models.FloatField(
              db_column='regular_high_school_diploma_moe', blank=True, null=True)
        ged_or_alternative_credential_moe = models.FloatField(
              db_column='ged_or_alternative_credential_moe', blank=True, null=True)
        some_college_less_than_1_year_moe = models.FloatField(
              db_column='some_college_less_than_1_year_moe', blank=True, null=True)
        some_college_1_or_more_years_no_degree_moe = models.FloatField(
              db_column='some_college_1_or_more_years_no_degree_moe', blank=True, null=True)
        associates_degree_moe = models.FloatField(  db_column='associates_degree_moe', blank=True,
                                                  null=True)
        bachelors_degree_moe = models.FloatField(  db_column='bachelors_degree_moe', blank=True,
                                                 null=True)
        masters_degree_moe = models.FloatField(  db_column='masters_degree_moe', blank=True,
                                               null=True)
        professional_school_degree_moe = models.FloatField(
              db_column='professional_school_degree_moe', blank=True, null=True)
        doctorate_degree_moe = models.FloatField(  db_column='doctorate_degree_moe', blank=True,
                                                 null=True)

        class Meta:
            managed = False
            db_table = 'educational_attainment'


class MailingAddress(models.Model):
    address_id = models.IntegerField(primary_key=True)
    house_number = models.TextField()
    street_name = models.TextField()
    apt_no = models.TextField(blank=True, null=True)
    city = models.TextField()
    state = models.TextField()
    zipcode = models.TextField()
    plus4 = models.TextField(blank=True, null=True)
    address_line2 = models.TextField(blank=True, null=True)
    address_line3 = models.TextField(blank=True, null=True)
    country = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'mailing_address'


class MailingAddressVoter(models.Model):
    voter_id = models.TextField(primary_key=True)
    address_id = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'mailing_address_voter'


class MedianHouseHoldIncome(models.Model):
    geoid = models.TextField(db_column='GEOID', primary_key=True)  # Field name made lowercase.
    state = models.TextField()
    county = models.TextField()
    tract = models.TextField()
    block_group = models.TextField()
    median_household_income = models.FloatField(blank=True, null=True)
    median_household_income_moe = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'median_house_hold_income'


class PrecinctDetails(models.Model):
    id = models.IntegerField(primary_key=True)
    county_code = models.TextField()
    precinct_id = models.TextField()
    precinct_name = models.TextField(blank=True, null=True)

    @property
    def as_record(self):
        return {
            'id': self.id,
            'county_code': self.county_code,
            'precinct_id': self.precinct_id,
            'precinct_name': self.precinct_name,
        }

    class Meta:
        managed = False
        db_table = 'precinct_details'


class PrecinctSummary(models.Model):
    precinct_id = models.IntegerField(primary_key=True)
    total = models.IntegerField()
    ap = models.IntegerField(db_column='AP')  # Field name made lowercase.
    ai = models.IntegerField(db_column='AI')  # Field name made lowercase.
    hp = models.IntegerField(db_column='HP')  # Field name made lowercase.
    bh = models.IntegerField(db_column='BH')  # Field name made lowercase.
    ot = models.IntegerField(db_column='OT')  # Field name made lowercase.
    u = models.IntegerField(db_column='U')  # Field name made lowercase.
    wh = models.IntegerField(db_column='WH')  # Field name made lowercase.
    s = models.IntegerField(db_column='S')  # Field name made lowercase.
    b = models.IntegerField(db_column='B')  # Field name made lowercase.
    gx = models.IntegerField(db_column='GX')  # Field name made lowercase.
    m = models.IntegerField(db_column='M')  # Field name made lowercase.
    gz = models.IntegerField(db_column='GZ')  # Field name made lowercase.
    wh_f_s = models.IntegerField(db_column='WH_F_S')  # Field name made lowercase.
    wh_f_b = models.IntegerField(db_column='WH_F_B')  # Field name made lowercase.
    wh_f_gx = models.IntegerField(db_column='WH_F_GX')  # Field name made lowercase.
    wh_f_m = models.IntegerField(db_column='WH_F_M')  # Field name made lowercase.
    wh_f_gz = models.IntegerField(db_column='WH_F_GZ')  # Field name made lowercase.
    wh_m_s = models.IntegerField(db_column='WH_M_S')  # Field name made lowercase.
    wh_m_b = models.IntegerField(db_column='WH_M_B')  # Field name made lowercase.
    wh_m_gx = models.IntegerField(db_column='WH_M_GX')  # Field name made lowercase.
    wh_m_m = models.IntegerField(db_column='WH_M_M')  # Field name made lowercase.
    wh_m_gz = models.IntegerField(db_column='WH_M_GZ')  # Field name made lowercase.
    bh_f_s = models.IntegerField(db_column='BH_F_S')  # Field name made lowercase.
    bh_f_b = models.IntegerField(db_column='BH_F_B')  # Field name made lowercase.
    bh_f_gx = models.IntegerField(db_column='BH_F_GX')  # Field name made lowercase.
    bh_f_m = models.IntegerField(db_column='BH_F_M')  # Field name made lowercase.
    bh_f_gz = models.IntegerField(db_column='BH_F_GZ')  # Field name made lowercase.
    bh_m_s = models.IntegerField(db_column='BH_M_S')  # Field name made lowercase.
    bh_m_b = models.IntegerField(db_column='BH_M_B')  # Field name made lowercase.
    bh_m_gx = models.IntegerField(db_column='BH_M_GX')  # Field name made lowercase.
    bh_m_m = models.IntegerField(db_column='BH_M_M')  # Field name made lowercase.
    bh_m_gz = models.IntegerField(db_column='BH_M_GZ')  # Field name made lowercase.
    u_f_s = models.IntegerField(db_column='U_F_S')  # Field name made lowercase.
    u_f_b = models.IntegerField(db_column='U_F_B')  # Field name made lowercase.
    u_f_gx = models.IntegerField(db_column='U_F_GX')  # Field name made lowercase.
    u_f_m = models.IntegerField(db_column='U_F_M')  # Field name made lowercase.
    u_f_gz = models.IntegerField(db_column='U_F_GZ')  # Field name made lowercase.
    u_m_s = models.IntegerField(db_column='U_M_S')  # Field name made lowercase.
    u_m_b = models.IntegerField(db_column='U_M_B')  # Field name made lowercase.
    u_m_gx = models.IntegerField(db_column='U_M_GX')  # Field name made lowercase.
    u_m_m = models.IntegerField(db_column='U_M_M')  # Field name made lowercase.
    u_m_gz = models.IntegerField(db_column='U_M_GZ')  # Field name made lowercase.
    ot_f_s = models.IntegerField(db_column='OT_F_S')  # Field name made lowercase.
    ot_f_b = models.IntegerField(db_column='OT_F_B')  # Field name made lowercase.
    ot_f_gx = models.IntegerField(db_column='OT_F_GX')  # Field name made lowercase.
    ot_f_m = models.IntegerField(db_column='OT_F_M')  # Field name made lowercase.
    ot_f_gz = models.IntegerField(db_column='OT_F_GZ')  # Field name made lowercase.
    ot_m_s = models.IntegerField(db_column='OT_M_S')  # Field name made lowercase.
    ot_m_b = models.IntegerField(db_column='OT_M_B')  # Field name made lowercase.
    ot_m_gx = models.IntegerField(db_column='OT_M_GX')  # Field name made lowercase.
    ot_m_m = models.IntegerField(db_column='OT_M_M')  # Field name made lowercase.
    ot_m_gz = models.IntegerField(db_column='OT_M_GZ')  # Field name made lowercase.
    hp_f_s = models.IntegerField(db_column='HP_F_S')  # Field name made lowercase.
    hp_f_b = models.IntegerField(db_column='HP_F_B')  # Field name made lowercase.
    hp_f_gx = models.IntegerField(db_column='HP_F_GX')  # Field name made lowercase.
    hp_f_m = models.IntegerField(db_column='HP_F_M')  # Field name made lowercase.
    hp_f_gz = models.IntegerField(db_column='HP_F_GZ')  # Field name made lowercase.
    hp_m_s = models.IntegerField(db_column='HP_M_S')  # Field name made lowercase.
    hp_m_b = models.IntegerField(db_column='HP_M_B')  # Field name made lowercase.
    hp_m_gx = models.IntegerField(db_column='HP_M_GX')  # Field name made lowercase.
    hp_m_m = models.IntegerField(db_column='HP_M_M')  # Field name made lowercase.
    hp_m_gz = models.IntegerField(db_column='HP_M_GZ')  # Field name made lowercase.
    ai_f_s = models.IntegerField(db_column='AI_F_S')  # Field name made lowercase.
    ai_f_b = models.IntegerField(db_column='AI_F_B')  # Field name made lowercase.
    ai_f_gx = models.IntegerField(db_column='AI_F_GX')  # Field name made lowercase.
    ai_f_m = models.IntegerField(db_column='AI_F_M')  # Field name made lowercase.
    ai_f_gz = models.IntegerField(db_column='AI_F_GZ')  # Field name made lowercase.
    ai_m_s = models.IntegerField(db_column='AI_M_S')  # Field name made lowercase.
    ai_m_b = models.IntegerField(db_column='AI_M_B')  # Field name made lowercase.
    ai_m_gx = models.IntegerField(db_column='AI_M_GX')  # Field name made lowercase.
    ai_m_m = models.IntegerField(db_column='AI_M_M')  # Field name made lowercase.
    ai_m_gz = models.IntegerField(db_column='AI_M_GZ')  # Field name made lowercase.
    ap_f_s = models.IntegerField(db_column='AP_F_S')  # Field name made lowercase.
    ap_f_b = models.IntegerField(db_column='AP_F_B')  # Field name made lowercase.
    ap_f_gx = models.IntegerField(db_column='AP_F_GX')  # Field name made lowercase.
    ap_f_m = models.IntegerField(db_column='AP_F_M')  # Field name made lowercase.
    ap_f_gz = models.IntegerField(db_column='AP_F_GZ')  # Field name made lowercase.
    ap_m_s = models.IntegerField(db_column='AP_M_S')  # Field name made lowercase.
    ap_m_b = models.IntegerField(db_column='AP_M_B')  # Field name made lowercase.
    ap_m_gx = models.IntegerField(db_column='AP_M_GX')  # Field name made lowercase.
    ap_m_m = models.IntegerField(db_column='AP_M_M')  # Field name made lowercase.
    ap_m_gz = models.IntegerField(db_column='AP_M_GZ')  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'precinct_summary'


class ResidenceAddress(models.Model):
    address_id = models.IntegerField(primary_key=True)
    county_code = models.TextField()
    house_number = models.TextField()
    street_name = models.TextField()
    apt_no = models.TextField(blank=True, null=True)
    city = models.TextField()
    state = models.TextField()
    zipcode = models.TextField()
    plus4 = models.TextField(blank=True, null=True)
    lat = models.FloatField(blank=True, null=True)
    lon = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'residence_address'


class VoterCng(models.Model):
    voter_id = models.TextField(primary_key=True)
    cng = models.TextField()

    class Meta:
        managed = False
        db_table = 'voter_cng'


class VoterHistorySummary(models.Model):
    voter_id = models.TextField(primary_key=True)
    county_code = models.TextField()
    number_2014_05_20 = models.TextField(db_column='2014-05-20', blank=True, null=True)  # Field renamed to remove unsuitable characters. Field renamed because it wasn't a valid Python identifier.
    number_2014_11_04 = models.TextField(db_column='2014-11-04', blank=True, null=True)  # Field renamed to remove unsuitable characters. Field renamed because it wasn't a valid Python identifier.
    number_2016_05_24 = models.TextField(db_column='2016-05-24', blank=True, null=True)  # Field renamed to remove unsuitable characters. Field renamed because it wasn't a valid Python identifier.
    number_2016_11_08 = models.TextField(db_column='2016-11-08', blank=True, null=True)  # Field renamed to remove unsuitable characters. Field renamed because it wasn't a valid Python identifier.
    number_2018_05_22 = models.TextField(db_column='2018-05-22', blank=True, null=True)  # Field renamed to remove unsuitable characters. Field renamed because it wasn't a valid Python identifier.
    number_2018_11_06 = models.TextField(db_column='2018-11-06', blank=True, null=True)  # Field renamed to remove unsuitable characters. Field renamed because it wasn't a valid Python identifier.
    number_2020_06_09 = models.TextField(db_column='2020-06-09', blank=True, null=True)  # Field renamed to remove unsuitable characters. Field renamed because it wasn't a valid Python identifier.
    number_2020_11_03 = models.TextField(db_column='2020-11-03', blank=True, null=True)  # Field renamed to remove unsuitable characters. Field renamed because it wasn't a valid Python identifier.
    number_2022_05_24 = models.TextField(db_column='2022-05-24', blank=True, null=True)  # Field renamed to remove unsuitable characters. Field renamed because it wasn't a valid Python identifier.

    class Meta:
        managed = False
        db_table = 'voter_history_summary'


class VoterHse(models.Model):
    voter_id = models.TextField(primary_key=True)
    hse = models.TextField()

    class Meta:
        managed = False
        db_table = 'voter_hse'


class VoterName(models.Model):
    voter_id = models.TextField(primary_key=True)
    last_name = models.TextField()
    first_name = models.TextField()
    middle_name = models.TextField(blank=True, null=True)
    name_suffix = models.TextField(blank=True, null=True)
    name_title = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'voter_name'


class VoterPrecinct(models.Model):
    voter_id = models.TextField(primary_key=True)
    precinct_id = models.IntegerField()

    @property
    def as_record(self):
        return {
            'voter_id': self.voter_id,
            'precinct_id': self.precinct_id
        }

    class Meta:
        managed = False
        db_table = 'voter_precinct'


class VoterScore(models.Model):
    voter_id = models.TextField(primary_key=True)
    county_code = models.TextField()
    max_ballots_cast = models.IntegerField()
    ballots_cast = models.IntegerField()
    gn_max = models.IntegerField()
    pn_max = models.IntegerField()
    gn = models.IntegerField()
    rn = models.IntegerField()
    dn = models.IntegerField()
    gr = models.FloatField(blank=True, null=True)
    pr = models.FloatField(blank=True, null=True)
    ra = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'voter_score'


class VoterSearch(models.Model):
    voter_id = models.TextField(primary_key=True)
    address_id = models.IntegerField()
    last_name = models.TextField()
    first_name = models.TextField()
    middle_name = models.TextField(blank=True, null=True)
    house_number = models.TextField()
    zipcode = models.TextField()

    class Meta:
        managed = False
        db_table = 'voter_search'


class VoterSen(models.Model):
    voter_id = models.TextField(primary_key=True)
    sen = models.TextField()

    class Meta:
        managed = False
        db_table = 'voter_sen'


class WorkTravelTime(models.Model):
    geoid = models.TextField(db_column='GEOID', primary_key=True)  # Field name made lowercase.
    state = models.TextField()
    county = models.TextField()
    tract = models.TextField()
    block_group = models.TextField()
    work_travel_time_total = models.FloatField()
    work_travel_time_less_than_10_minutes = models.FloatField(blank=True, null=True)
    work_travel_time_10_to_14_minutes = models.FloatField(blank=True, null=True)
    work_travel_time_15_to_19_minutes = models.FloatField(blank=True, null=True)
    work_travel_time_20_to_24_minutes = models.FloatField(blank=True, null=True)
    work_travel_time_25_to_29_minutes = models.FloatField(blank=True, null=True)
    work_travel_time_30_to_34_minutes = models.FloatField(blank=True, null=True)
    work_travel_time_35_to_44_minutes = models.FloatField(blank=True, null=True)
    work_travel_time_45_to_59_minutes = models.FloatField(blank=True, null=True)
    work_travel_time_60_or_more_minutes = models.FloatField(blank=True, null=True)
    work_travel_time_total_moe = models.FloatField(blank=True, null=True)
    work_travel_time_less_than_10_minutes_moe = models.FloatField(blank=True, null=True)
    work_travel_time_10_to_14_minutes_moe = models.FloatField(blank=True, null=True)
    work_travel_time_15_to_19_minutes_moe = models.FloatField(blank=True, null=True)
    work_travel_time_20_to_24_minutes_moe = models.FloatField(blank=True, null=True)
    work_travel_time_25_to_29_minutes_moe = models.FloatField(blank=True, null=True)
    work_travel_time_30_to_34_minutes_moe = models.FloatField(blank=True, null=True)
    work_travel_time_35_to_44_minutes_moe = models.FloatField(blank=True, null=True)
    work_travel_time_45_to_59_minutes_moe = models.FloatField(blank=True, null=True)
    work_travel_time_60_or_more_minutes_moe = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'work_travel_time'
