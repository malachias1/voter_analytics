# Generated by Django 4.0.6 on 2022-07-04 16:49

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='AddressVoter',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('address_id', models.IntegerField()),
            ],
            options={
                'db_table': 'address_voter',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='BlockGroupGeometry',
            fields=[
                ('geoid', models.TextField(db_column='GEOID', primary_key=True, serialize=False)),
                ('state', models.TextField()),
                ('county', models.TextField()),
                ('tract', models.TextField()),
                ('block_group', models.TextField()),
                ('geometry', models.TextField()),
            ],
            options={
                'db_table': 'block_group_geometry',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='Children',
            fields=[
                ('geoid', models.TextField(db_column='GEOID', primary_key=True, serialize=False)),
                ('state', models.TextField()),
                ('county', models.TextField()),
                ('tract', models.TextField()),
                ('block_group', models.TextField()),
                ('total', models.FloatField()),
                ('male_under_5', models.FloatField(blank=True, null=True)),
                ('male_5_to_9', models.FloatField(blank=True, null=True)),
                ('male_10_to_14', models.FloatField(blank=True, null=True)),
                ('male_15_to_17', models.FloatField(blank=True, null=True)),
                ('female_under_5', models.FloatField(blank=True, null=True)),
                ('female_5_to_9', models.FloatField(blank=True, null=True)),
                ('female_10_to_14', models.FloatField(blank=True, null=True)),
                ('female_15_to_17', models.FloatField(blank=True, null=True)),
                ('total_moe', models.FloatField(blank=True, null=True)),
                ('male_under_5_moe', models.FloatField(blank=True, null=True)),
                ('male_5_to_9_moe', models.FloatField(blank=True, null=True)),
                ('male_10_to_14_moe', models.FloatField(blank=True, null=True)),
                ('male_15_to_17_moe', models.FloatField(blank=True, null=True)),
                ('female_under_5_moe', models.FloatField(blank=True, null=True)),
                ('female_5_to_9_moe', models.FloatField(blank=True, null=True)),
                ('female_10_to_14_moe', models.FloatField(blank=True, null=True)),
                ('female_15_to_17_moe', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'children',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='CngMap',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('area', models.FloatField()),
                ('district', models.TextField()),
                ('population', models.IntegerField()),
                ('ideal_value', models.FloatField()),
                ('geometry_wkb', models.TextField()),
                ('center_wkb', models.TextField()),
            ],
            options={
                'db_table': 'cng_map',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ContestClass',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('election_date', models.TextField()),
                ('contest', models.TextField()),
                ('category', models.TextField()),
                ('canonical_name', models.TextField()),
                ('type', models.TextField()),
                ('subcategory', models.TextField(blank=True, null=True)),
                ('party', models.TextField(blank=True, null=True)),
                ('is_question', models.BooleanField()),
                ('ambiguous', models.BooleanField()),
            ],
            options={
                'db_table': 'contest_class',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='CountyDetails',
            fields=[
                ('county_code', models.TextField(primary_key=True, serialize=False)),
                ('county_fips', models.TextField()),
                ('county_name', models.TextField()),
            ],
            options={
                'db_table': 'county_details',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='CountyMap',
            fields=[
                ('state_fips', models.TextField()),
                ('county_fips', models.TextField()),
                ('county_code', models.TextField(primary_key=True, serialize=False)),
                ('geoid', models.TextField()),
                ('county_name', models.TextField()),
                ('aland', models.TextField()),
                ('awater', models.TextField()),
                ('geometry_wkb', models.TextField()),
                ('center_wkb', models.TextField()),
            ],
            options={
                'db_table': 'county_map',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='EducationalAttainment',
            fields=[
                ('geoid', models.TextField(db_column='GEOID', primary_key=True, serialize=False)),
                ('state', models.TextField()),
                ('county', models.TextField()),
                ('tract', models.TextField()),
                ('block_group', models.TextField()),
                ('total', models.FloatField(blank=True, null=True)),
                ('regular_high_school_diploma', models.FloatField(blank=True, null=True)),
                ('ged_or_alternative_credential', models.FloatField(blank=True, null=True)),
                ('some_college_less_than_1_year', models.FloatField(blank=True, null=True)),
                ('some_college_1_or_more_years_no_degree', models.FloatField(blank=True, null=True)),
                ('associates_degree', models.FloatField(blank=True, null=True)),
                ('bachelors_degree', models.FloatField(blank=True, null=True)),
                ('masters_degree', models.FloatField(blank=True, null=True)),
                ('professional_school_degree', models.FloatField(blank=True, null=True)),
                ('doctorate_degree', models.FloatField(blank=True, null=True)),
                ('total_moe', models.FloatField(blank=True, null=True)),
                ('regular_high_school_diploma_moe', models.FloatField(blank=True, null=True)),
                ('ged_or_alternative_credential_moe', models.FloatField(blank=True, null=True)),
                ('some_college_less_than_1_year_moe', models.FloatField(blank=True, null=True)),
                ('some_college_1_or_more_years_no_degree_moe', models.FloatField(blank=True, null=True)),
                ('associates_degree_moe', models.FloatField(blank=True, null=True)),
                ('bachelors_degree_moe', models.FloatField(blank=True, null=True)),
                ('masters_degree_moe', models.FloatField(blank=True, null=True)),
                ('professional_school_degree_moe', models.FloatField(blank=True, null=True)),
                ('doctorate_degree_moe', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'educational_attainment',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ElectionResultDetails',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('election_date', models.TextField()),
                ('county_code', models.TextField()),
                ('contest', models.TextField()),
                ('choice', models.TextField()),
                ('party', models.TextField(blank=True, null=True)),
                ('is_question', models.BooleanField()),
                ('precinct_name', models.TextField()),
                ('vote_type', models.TextField()),
                ('votes', models.IntegerField()),
            ],
            options={
                'db_table': 'election_result_details',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ElectionResults',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('election_date', models.TextField()),
                ('county_code', models.TextField()),
                ('contest', models.TextField()),
                ('choice', models.TextField()),
                ('party', models.TextField(blank=True, null=True)),
                ('is_question', models.BooleanField()),
                ('precinct_name', models.TextField()),
                ('votes', models.IntegerField()),
            ],
            options={
                'db_table': 'election_results',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ElectionResultsOverUnder',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('election_date', models.TextField()),
                ('contest', models.TextField()),
                ('county_code', models.TextField()),
                ('precinct_name', models.TextField()),
                ('overvotes', models.IntegerField()),
                ('undervotes', models.IntegerField()),
            ],
            options={
                'db_table': 'election_results_over_under',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='HseMap',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('area', models.FloatField()),
                ('district', models.TextField()),
                ('population', models.IntegerField()),
                ('ideal_value', models.FloatField()),
                ('geometry_wkb', models.TextField()),
                ('center_wkb', models.TextField()),
            ],
            options={
                'db_table': 'hse_map',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='MailingAddress',
            fields=[
                ('address_id', models.IntegerField(primary_key=True, serialize=False)),
                ('house_number', models.TextField()),
                ('street_name', models.TextField()),
                ('apt_no', models.TextField(blank=True, null=True)),
                ('city', models.TextField()),
                ('state', models.TextField()),
                ('zipcode', models.TextField()),
                ('plus4', models.TextField(blank=True, null=True)),
                ('address_line2', models.TextField(blank=True, null=True)),
                ('address_line3', models.TextField(blank=True, null=True)),
                ('country', models.TextField(blank=True, null=True)),
            ],
            options={
                'db_table': 'mailing_address',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='MailingAddressVoter',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('address_id', models.IntegerField()),
            ],
            options={
                'db_table': 'mailing_address_voter',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='MedianHouseHoldIncome',
            fields=[
                ('geoid', models.TextField(db_column='GEOID', primary_key=True, serialize=False)),
                ('state', models.TextField()),
                ('county', models.TextField()),
                ('tract', models.TextField()),
                ('block_group', models.TextField()),
                ('median_household_income', models.FloatField(blank=True, null=True)),
                ('median_household_income_moe', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'median_house_hold_income',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='PrecinctDetails',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('county_code', models.TextField()),
                ('precinct_id', models.TextField()),
                ('precinct_name', models.TextField(blank=True, null=True)),
            ],
            options={
                'db_table': 'precinct_details',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='PrecinctSummary',
            fields=[
                ('precinct_id', models.IntegerField(primary_key=True, serialize=False)),
                ('total', models.IntegerField()),
                ('ap', models.IntegerField(db_column='AP')),
                ('ai', models.IntegerField(db_column='AI')),
                ('hp', models.IntegerField(db_column='HP')),
                ('bh', models.IntegerField(db_column='BH')),
                ('ot', models.IntegerField(db_column='OT')),
                ('u', models.IntegerField(db_column='U')),
                ('wh', models.IntegerField(db_column='WH')),
                ('s', models.IntegerField(db_column='S')),
                ('b', models.IntegerField(db_column='B')),
                ('gx', models.IntegerField(db_column='GX')),
                ('m', models.IntegerField(db_column='M')),
                ('gz', models.IntegerField(db_column='GZ')),
                ('wh_f_s', models.IntegerField(db_column='WH_F_S')),
                ('wh_f_b', models.IntegerField(db_column='WH_F_B')),
                ('wh_f_gx', models.IntegerField(db_column='WH_F_GX')),
                ('wh_f_m', models.IntegerField(db_column='WH_F_M')),
                ('wh_f_gz', models.IntegerField(db_column='WH_F_GZ')),
                ('wh_m_s', models.IntegerField(db_column='WH_M_S')),
                ('wh_m_b', models.IntegerField(db_column='WH_M_B')),
                ('wh_m_gx', models.IntegerField(db_column='WH_M_GX')),
                ('wh_m_m', models.IntegerField(db_column='WH_M_M')),
                ('wh_m_gz', models.IntegerField(db_column='WH_M_GZ')),
                ('bh_f_s', models.IntegerField(db_column='BH_F_S')),
                ('bh_f_b', models.IntegerField(db_column='BH_F_B')),
                ('bh_f_gx', models.IntegerField(db_column='BH_F_GX')),
                ('bh_f_m', models.IntegerField(db_column='BH_F_M')),
                ('bh_f_gz', models.IntegerField(db_column='BH_F_GZ')),
                ('bh_m_s', models.IntegerField(db_column='BH_M_S')),
                ('bh_m_b', models.IntegerField(db_column='BH_M_B')),
                ('bh_m_gx', models.IntegerField(db_column='BH_M_GX')),
                ('bh_m_m', models.IntegerField(db_column='BH_M_M')),
                ('bh_m_gz', models.IntegerField(db_column='BH_M_GZ')),
                ('u_f_s', models.IntegerField(db_column='U_F_S')),
                ('u_f_b', models.IntegerField(db_column='U_F_B')),
                ('u_f_gx', models.IntegerField(db_column='U_F_GX')),
                ('u_f_m', models.IntegerField(db_column='U_F_M')),
                ('u_f_gz', models.IntegerField(db_column='U_F_GZ')),
                ('u_m_s', models.IntegerField(db_column='U_M_S')),
                ('u_m_b', models.IntegerField(db_column='U_M_B')),
                ('u_m_gx', models.IntegerField(db_column='U_M_GX')),
                ('u_m_m', models.IntegerField(db_column='U_M_M')),
                ('u_m_gz', models.IntegerField(db_column='U_M_GZ')),
                ('ot_f_s', models.IntegerField(db_column='OT_F_S')),
                ('ot_f_b', models.IntegerField(db_column='OT_F_B')),
                ('ot_f_gx', models.IntegerField(db_column='OT_F_GX')),
                ('ot_f_m', models.IntegerField(db_column='OT_F_M')),
                ('ot_f_gz', models.IntegerField(db_column='OT_F_GZ')),
                ('ot_m_s', models.IntegerField(db_column='OT_M_S')),
                ('ot_m_b', models.IntegerField(db_column='OT_M_B')),
                ('ot_m_gx', models.IntegerField(db_column='OT_M_GX')),
                ('ot_m_m', models.IntegerField(db_column='OT_M_M')),
                ('ot_m_gz', models.IntegerField(db_column='OT_M_GZ')),
                ('hp_f_s', models.IntegerField(db_column='HP_F_S')),
                ('hp_f_b', models.IntegerField(db_column='HP_F_B')),
                ('hp_f_gx', models.IntegerField(db_column='HP_F_GX')),
                ('hp_f_m', models.IntegerField(db_column='HP_F_M')),
                ('hp_f_gz', models.IntegerField(db_column='HP_F_GZ')),
                ('hp_m_s', models.IntegerField(db_column='HP_M_S')),
                ('hp_m_b', models.IntegerField(db_column='HP_M_B')),
                ('hp_m_gx', models.IntegerField(db_column='HP_M_GX')),
                ('hp_m_m', models.IntegerField(db_column='HP_M_M')),
                ('hp_m_gz', models.IntegerField(db_column='HP_M_GZ')),
                ('ai_f_s', models.IntegerField(db_column='AI_F_S')),
                ('ai_f_b', models.IntegerField(db_column='AI_F_B')),
                ('ai_f_gx', models.IntegerField(db_column='AI_F_GX')),
                ('ai_f_m', models.IntegerField(db_column='AI_F_M')),
                ('ai_f_gz', models.IntegerField(db_column='AI_F_GZ')),
                ('ai_m_s', models.IntegerField(db_column='AI_M_S')),
                ('ai_m_b', models.IntegerField(db_column='AI_M_B')),
                ('ai_m_gx', models.IntegerField(db_column='AI_M_GX')),
                ('ai_m_m', models.IntegerField(db_column='AI_M_M')),
                ('ai_m_gz', models.IntegerField(db_column='AI_M_GZ')),
                ('ap_f_s', models.IntegerField(db_column='AP_F_S')),
                ('ap_f_b', models.IntegerField(db_column='AP_F_B')),
                ('ap_f_gx', models.IntegerField(db_column='AP_F_GX')),
                ('ap_f_m', models.IntegerField(db_column='AP_F_M')),
                ('ap_f_gz', models.IntegerField(db_column='AP_F_GZ')),
                ('ap_m_s', models.IntegerField(db_column='AP_M_S')),
                ('ap_m_b', models.IntegerField(db_column='AP_M_B')),
                ('ap_m_gx', models.IntegerField(db_column='AP_M_GX')),
                ('ap_m_m', models.IntegerField(db_column='AP_M_M')),
                ('ap_m_gz', models.IntegerField(db_column='AP_M_GZ')),
            ],
            options={
                'db_table': 'precinct_summary',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ResidenceAddress',
            fields=[
                ('address_id', models.IntegerField(primary_key=True, serialize=False)),
                ('county_code', models.TextField()),
                ('house_number', models.TextField()),
                ('street_name', models.TextField()),
                ('apt_no', models.TextField(blank=True, null=True)),
                ('city', models.TextField()),
                ('state', models.TextField()),
                ('zipcode', models.TextField()),
                ('plus4', models.TextField(blank=True, null=True)),
                ('lat', models.FloatField(blank=True, null=True)),
                ('lon', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'residence_address',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SenMap',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('area', models.FloatField()),
                ('district', models.TextField()),
                ('population', models.IntegerField()),
                ('ideal_value', models.FloatField()),
                ('geometry_wkb', models.TextField()),
                ('center_wkb', models.TextField()),
            ],
            options={
                'db_table': 'sen_map',
            },
        ),
        migrations.CreateModel(
            name='VoterCng',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('cng', models.TextField()),
            ],
            options={
                'db_table': 'voter_cng',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='VoterDemographics',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('race_id', models.TextField()),
                ('gender', models.TextField()),
                ('year_of_birth', models.IntegerField()),
            ],
            options={
                'db_table': 'voter_demographics',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='VoterHistory',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('date', models.TextField()),
                ('type', models.TextField()),
                ('party', models.TextField(blank=True, null=True)),
                ('county_id', models.TextField()),
                ('absentee', models.IntegerField()),
                ('provisional', models.IntegerField()),
                ('supplemental', models.IntegerField()),
            ],
            options={
                'db_table': 'voter_history',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='VoterHistorySummary',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('county_code', models.TextField()),
                ('number_2014_05_20', models.TextField(blank=True, db_column='2014-05-20', null=True)),
                ('number_2014_11_04', models.TextField(blank=True, db_column='2014-11-04', null=True)),
                ('number_2016_05_24', models.TextField(blank=True, db_column='2016-05-24', null=True)),
                ('number_2016_11_08', models.TextField(blank=True, db_column='2016-11-08', null=True)),
                ('number_2018_05_22', models.TextField(blank=True, db_column='2018-05-22', null=True)),
                ('number_2018_11_06', models.TextField(blank=True, db_column='2018-11-06', null=True)),
                ('number_2020_06_09', models.TextField(blank=True, db_column='2020-06-09', null=True)),
                ('number_2020_11_03', models.TextField(blank=True, db_column='2020-11-03', null=True)),
                ('number_2022_05_24', models.TextField(blank=True, db_column='2022-05-24', null=True)),
            ],
            options={
                'db_table': 'voter_history_summary',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='VoterHse',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('hse', models.TextField()),
            ],
            options={
                'db_table': 'voter_hse',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='VoterName',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('last_name', models.TextField()),
                ('first_name', models.TextField()),
                ('middle_name', models.TextField(blank=True, null=True)),
                ('name_suffix', models.TextField(blank=True, null=True)),
                ('name_title', models.TextField(blank=True, null=True)),
            ],
            options={
                'db_table': 'voter_name',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='VoterPrecinct',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('precinct_id', models.IntegerField()),
            ],
            options={
                'db_table': 'voter_precinct',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='VoterScore',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('county_code', models.TextField()),
                ('max_ballots_cast', models.IntegerField()),
                ('ballots_cast', models.IntegerField()),
                ('gn_max', models.IntegerField()),
                ('pn_max', models.IntegerField()),
                ('gn', models.IntegerField()),
                ('rn', models.IntegerField()),
                ('dn', models.IntegerField()),
                ('gr', models.FloatField(blank=True, null=True)),
                ('pr', models.FloatField(blank=True, null=True)),
                ('ra', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'voter_score',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='VoterSearch',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('address_id', models.IntegerField()),
                ('last_name', models.TextField()),
                ('first_name', models.TextField()),
                ('middle_name', models.TextField(blank=True, null=True)),
                ('house_number', models.TextField()),
                ('zipcode', models.TextField()),
            ],
            options={
                'db_table': 'voter_search',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='VoterSen',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('sen', models.TextField()),
            ],
            options={
                'db_table': 'voter_sen',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='VoterStatus',
            fields=[
                ('voter_id', models.TextField(primary_key=True, serialize=False)),
                ('status', models.TextField()),
                ('status_reason', models.TextField(blank=True, null=True)),
                ('date_added', models.TextField()),
                ('date_changed', models.TextField()),
                ('registration_date', models.TextField()),
                ('last_contact_date', models.TextField()),
            ],
            options={
                'db_table': 'voter_status',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='VtdMap',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('area', models.FloatField()),
                ('precinct_id', models.TextField()),
                ('precinct_name', models.TextField()),
                ('county_code', models.TextField()),
                ('county_fips', models.TextField()),
                ('county_name', models.TextField()),
                ('geometry_wkb', models.TextField()),
                ('center_wkb', models.TextField()),
            ],
            options={
                'db_table': 'vtd_map',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='WorkTravelTime',
            fields=[
                ('geoid', models.TextField(db_column='GEOID', primary_key=True, serialize=False)),
                ('state', models.TextField()),
                ('county', models.TextField()),
                ('tract', models.TextField()),
                ('block_group', models.TextField()),
                ('work_travel_time_total', models.FloatField()),
                ('work_travel_time_less_than_10_minutes', models.FloatField(blank=True, null=True)),
                ('work_travel_time_10_to_14_minutes', models.FloatField(blank=True, null=True)),
                ('work_travel_time_15_to_19_minutes', models.FloatField(blank=True, null=True)),
                ('work_travel_time_20_to_24_minutes', models.FloatField(blank=True, null=True)),
                ('work_travel_time_25_to_29_minutes', models.FloatField(blank=True, null=True)),
                ('work_travel_time_30_to_34_minutes', models.FloatField(blank=True, null=True)),
                ('work_travel_time_35_to_44_minutes', models.FloatField(blank=True, null=True)),
                ('work_travel_time_45_to_59_minutes', models.FloatField(blank=True, null=True)),
                ('work_travel_time_60_or_more_minutes', models.FloatField(blank=True, null=True)),
                ('work_travel_time_total_moe', models.FloatField(blank=True, null=True)),
                ('work_travel_time_less_than_10_minutes_moe', models.FloatField(blank=True, null=True)),
                ('work_travel_time_10_to_14_minutes_moe', models.FloatField(blank=True, null=True)),
                ('work_travel_time_15_to_19_minutes_moe', models.FloatField(blank=True, null=True)),
                ('work_travel_time_20_to_24_minutes_moe', models.FloatField(blank=True, null=True)),
                ('work_travel_time_25_to_29_minutes_moe', models.FloatField(blank=True, null=True)),
                ('work_travel_time_30_to_34_minutes_moe', models.FloatField(blank=True, null=True)),
                ('work_travel_time_35_to_44_minutes_moe', models.FloatField(blank=True, null=True)),
                ('work_travel_time_45_to_59_minutes_moe', models.FloatField(blank=True, null=True)),
                ('work_travel_time_60_or_more_minutes_moe', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'work_travel_time',
                'managed': False,
            },
        ),
    ]
