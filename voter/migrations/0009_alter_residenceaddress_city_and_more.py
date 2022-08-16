# Generated by Django 4.0.6 on 2022-08-08 15:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('voter', '0008_alter_mailingaddress_state_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='residenceaddress',
            name='city',
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
        migrations.AlterField(
            model_name='residenceaddress',
            name='street_name',
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AlterField(
            model_name='residenceaddress',
            name='zipcode',
            field=models.CharField(blank=True, max_length=5, null=True),
        ),
    ]