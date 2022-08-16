# Generated by Django 4.0.6 on 2022-08-06 23:01

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('county', '0004_alter_county_geoid'),
        ('precinct', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='ListEdition',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date', models.DateField()),
                ('path', models.TextField()),
                ('comments', models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name='Voter',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('voter_id', models.CharField(max_length=8)),
                ('race_id', models.CharField(max_length=2)),
                ('gender', models.CharField(max_length=1)),
                ('year_of_birth', models.IntegerField()),
                ('status', models.CharField(max_length=1)),
                ('status_reason', models.CharField(blank=True, max_length=16, null=True)),
                ('date_added', models.DateField()),
                ('date_changed', models.DateField()),
                ('registration_date', models.DateField()),
                ('last_contact_date', models.DateField()),
                ('last_name', models.TextField()),
                ('first_name', models.TextField()),
                ('middle_name', models.TextField(blank=True, null=True)),
                ('name_suffix', models.TextField(blank=True, null=True)),
                ('name_title', models.TextField(blank=True, null=True)),
                ('cng', models.CharField(max_length=3)),
                ('hse', models.CharField(max_length=3)),
                ('sen', models.CharField(max_length=3)),
                ('edition_year', models.IntegerField()),
                ('date_loaded', models.DateField()),
                ('county', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='county.county')),
                ('edition', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='voter.listedition')),
                ('precinct', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='precinct.precinct')),
            ],
        ),
        migrations.CreateModel(
            name='ResidenceAddress',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('house_number', models.CharField(max_length=16)),
                ('street_name', models.TextField()),
                ('apt_no', models.TextField(blank=True, null=True)),
                ('city', models.TextField()),
                ('state', models.CharField(max_length=2)),
                ('zipcode', models.CharField(max_length=5)),
                ('plus4', models.CharField(blank=True, max_length=4, null=True)),
                ('voter', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='voter.voter')),
            ],
        ),
        migrations.CreateModel(
            name='MailingAddress',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('house_number', models.TextField()),
                ('street_name', models.TextField()),
                ('apt_no', models.TextField(blank=True, null=True)),
                ('city', models.TextField()),
                ('state', models.CharField(max_length=2)),
                ('zipcode', models.CharField(max_length=5)),
                ('plus4', models.CharField(blank=True, max_length=4, null=True)),
                ('address_line2', models.TextField(blank=True, null=True)),
                ('address_line3', models.TextField(blank=True, null=True)),
                ('country', models.TextField(blank=True, null=True)),
                ('voter', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='voter.voter')),
            ],
        ),
        migrations.AddIndex(
            model_name='voter',
            index=models.Index(fields=['voter_id'], name='voter_voter_voter_i_c26ca0_idx'),
        ),
        migrations.AddIndex(
            model_name='voter',
            index=models.Index(fields=['status'], name='voter_voter_status_7aba78_idx'),
        ),
        migrations.AddIndex(
            model_name='voter',
            index=models.Index(fields=['cng'], name='voter_voter_cng_708cf1_idx'),
        ),
        migrations.AddIndex(
            model_name='voter',
            index=models.Index(fields=['hse'], name='voter_voter_hse_38ea5a_idx'),
        ),
        migrations.AddIndex(
            model_name='voter',
            index=models.Index(fields=['sen'], name='voter_voter_sen_a9447f_idx'),
        ),
        migrations.AddIndex(
            model_name='voter',
            index=models.Index(fields=['county'], name='voter_voter_county__060c42_idx'),
        ),
        migrations.AddIndex(
            model_name='voter',
            index=models.Index(fields=['edition'], name='voter_voter_edition_960071_idx'),
        ),
    ]