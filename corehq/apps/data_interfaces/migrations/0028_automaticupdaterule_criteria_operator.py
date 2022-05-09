# Generated by Django 3.2.13 on 2022-04-27 14:11

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_interfaces', '0027_locationfilterdefinition_include_child_locations'),
    ]

    operations = [
        migrations.AddField(
            model_name='automaticupdaterule',
            name='criteria_operator',
            field=models.CharField(choices=[
                ('ALL', 'ALL of the criteria are met'),
                ('ANY', 'ANY of the criteria are met')
            ], default='ALL', max_length=3),
        ),
    ]
