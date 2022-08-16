# Generated by Django 4.1 on 2022-08-14 19:38

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('precinct', '0004_precinctmap_precinct'),
    ]

    operations = [
        migrations.AlterField(
            model_name='precinctmap',
            name='precinct',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='map_of', to='precinct.precinct'),
        ),
    ]
