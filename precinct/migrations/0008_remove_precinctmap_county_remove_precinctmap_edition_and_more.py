# Generated by Django 4.1 on 2022-08-14 20:03

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('precinct', '0007_rename_precinct_id_precinct_precinct_short_name'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='precinctmap',
            name='county',
        ),
        migrations.RemoveField(
            model_name='precinctmap',
            name='edition',
        ),
        migrations.RemoveField(
            model_name='precinctmap',
            name='precinct_short_name',
        ),
    ]
