# Generated by Django 4.0.6 on 2022-08-06 16:18

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('county', '0003_countymap_county_county_map'),
    ]

    operations = [
        migrations.AlterField(
            model_name='county',
            name='geoid',
            field=models.CharField(max_length=5),
        ),
    ]