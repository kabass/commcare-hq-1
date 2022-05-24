from django.db import migrations
from corehq.messaging.scheduling.scheduling_partitioned.models import CaseTimedScheduleInstance
from corehq.sql_db.util import get_db_aliases_for_partitioned_query


def update_custom_recipient_ids(*args, **kwargs):
    for db in get_db_aliases_for_partitioned_query():
        CaseTimedScheduleInstance.objects.using(db).filter(recipient_id="CASE_OWNER_LOCATION_PARENT").update(
            recipient_id='MOBILE_WORKER_CASE_OWNER_LOCATION_PARENT'
        )


class Migration(migrations.Migration):

    dependencies = [
        ('scheduling_partitioned', '0008_track_attempts'),
    ]

    operations = [migrations.RunPython(update_custom_recipient_ids)]
