# Generated by Django 4.1 on 2022-08-14 12:44

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('county', '0005_remove_county_county_map_countymap_county'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='countymap',
            name='county_code',
        ),
    ]