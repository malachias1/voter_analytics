# Generated by Django 4.0.6 on 2022-08-08 02:11

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('voter', '0004_alter_otherjurisdictions_county_districtb_value'),
    ]

    operations = [
        migrations.AlterField(
            model_name='otherjurisdictions',
            name='county_districtb_value',
            field=models.CharField(blank=True, max_length=2, null=True),
        ),
    ]
