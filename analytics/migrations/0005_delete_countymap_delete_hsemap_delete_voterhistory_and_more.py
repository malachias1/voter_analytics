# Generated by Django 4.0.6 on 2022-07-21 16:48

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0004_delete_senmap'),
    ]

    operations = [
        migrations.DeleteModel(
            name='CountyMap',
        ),
        migrations.DeleteModel(
            name='HseMap',
        ),
        migrations.DeleteModel(
            name='VoterHistory',
        ),
        migrations.DeleteModel(
            name='VtdMap',
        ),
    ]
