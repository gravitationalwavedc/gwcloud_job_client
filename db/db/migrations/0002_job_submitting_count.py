# Generated by Django 3.0.6 on 2020-12-10 15:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('db', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='submitting_count',
            field=models.IntegerField(default=0),
        ),
    ]
