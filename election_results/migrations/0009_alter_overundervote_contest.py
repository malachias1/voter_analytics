# Generated by Django 4.0.6 on 2022-07-24 19:25

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('election_results', '0008_rename_details_detail_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='overundervote',
            name='contest',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='election_results.contest'),
        ),
    ]