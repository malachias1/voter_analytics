# Generated by Django 4.1 on 2022-08-14 12:39

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('county', '0004_alter_county_geoid'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='county',
            name='county_map',
        ),
        migrations.AddField(
            model_name='countymap',
            name='county',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='map_of', to='county.county'),
        ),
    ]