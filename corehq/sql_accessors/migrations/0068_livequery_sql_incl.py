from django.db import migrations

from corehq.sql_db.operations import RawSQLMigration

migrator = RawSQLMigration(('corehq', 'sql_accessors', 'sql_templates'), {})


class Migration(migrations.Migration):

    dependencies = [
        ('sql_accessors', '0067_livequery_sql_include_deleted_indices'),
    ]

    operations = [
        migrator.get_migration(
            forward_template='get_related_indices_incl.sql',
            reverse_template='drop_get_related_indices_incl.sql',
        ),
    ]
