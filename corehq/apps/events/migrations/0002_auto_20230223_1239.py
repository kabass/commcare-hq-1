# Generated by Django 3.2.16 on 2023-02-23 12:39

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('events', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Attendee',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('domain', models.CharField(max_length=255)),
                ('case_id', models.CharField(max_length=126)),
            ],
            options={
                'db_table': 'commcare_attendee',
            },
        ),
        migrations.AlterField(
            model_name='event',
            name='end_date',
            field=models.DateField(),
        ),
        migrations.AlterField(
            model_name='event',
            name='start_date',
            field=models.DateField(),
        ),
        migrations.AddIndex(
            model_name='attendee',
            index=models.Index(fields=['domain'], name='commcare_at_domain_c749ab_idx'),
        ),
    ]