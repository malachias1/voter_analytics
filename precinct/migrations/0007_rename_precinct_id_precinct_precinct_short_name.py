# Generated by Django 4.1 on 2022-08-14 20:03

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('precinct', '0006_alter_precinctmap_precinct'),
    ]

    operations = [
        migrations.RenameField(
            model_name='precinct',
            old_name='precinct_id',
            new_name='precinct_short_name',
        ),
    ]
