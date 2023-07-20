from django.utils.functional import classproperty

from dimagi.utils.parsing import json_format_datetime

from corehq.apps.cleanup.management.commands.populate_sql_model_from_couch_model import PopulateSQLCommand
from corehq.util.couch_helpers import paginate_view

from ...models import Repeater


class Command(PopulateSQLCommand):

    @classmethod
    def couch_db_slug(cls):
        return "receiverwrapper"

    @classmethod
    def couch_doc_type(cls):
        return 'RepeatRecord'

    @classmethod
    def sql_class(cls):
        from ...models import SQLRepeatRecord
        return SQLRepeatRecord

    @classmethod
    def commit_adding_migration(cls):
        return "TODO: add once the PR adding this file is merged"

    @classmethod
    def diff_couch_and_sql(cls, couch, sql):
        """
        Compare each attribute of the given couch document and sql
        object. Return a list of human-readable strings describing their
        differences, or None if the two are equivalent. The list may
        contain `None` or empty strings.
        """
        fields = ["domain", "payload_id"]
        diffs = [cls.diff_attr(name, couch, sql) for name in fields]
        diffs.append(cls.diff_value(
            "repeater_id",
            couch["repeater_id"],
            cls.get_couch_repeater_id(sql.repeater_id),
        ))
        diffs.append(cls.diff_value(
            "state",
            get_state(couch),
            sql.state,
        ))
        diffs.append(cls.diff_value(
            "registered_at",
            couch["registered_on"],
            json_format_datetime(sql.registered_at),
        ))
        return diffs

    def get_ids_to_ignore(self, docs):
        """Ignore records that reference repeaters created since the migration started

        All such repeat records would have been migrated on save. Also
        ignore records associated with deleted repeaters.

        NOTE: there is a race condition between this repeaters existence
        check and saving new records. A repeater could be deleted
        between when this function is called and when the new records
        are saved, which would cause the migration to fail with
        IntegrityError on "repeater_id" column value. Since that is a
        rare condition, it is not handled. It should be sufficient to
        rerun the migration to recover from that error.
        """
        existing_ids = set(Repeater.objects.filter(
            repeater_id__in=list({d["repeater_id"] for d in docs})
        ).values_list("repeater_id", flat=True))
        return {d["_id"] for d in docs if d["repeater_id"] not in existing_ids}

    @classmethod
    def get_couch_repeater_id(cls, sql_repeater_id):
        try:
            return cls.repeater_id_map[sql_repeater_id]
        except KeyError:
            repeater_id = Repeater.objects.filter(id=sql_repeater_id).values_list("repeater_id", flat=True)[0]
            cls.repeater_id_map[sql_repeater_id] = repeater_id
            return repeater_id

    @classproperty
    def repeater_id_map(cls):
        try:
            data = cls._repeater_id_map
        except AttributeError:
            data = cls._repeater_id_map = dict(Repeater.objects.values_list("id", "repeater_id"))
        return data

    def _get_couch_doc_count_for_type(self):
        from ...models import RepeatRecord
        result = RepeatRecord.get_db().view(
            'repeaters/repeat_records',
            include_docs=False,
            reduce=True,
            descending=True,
        ).one()
        if not result:
            return 0
        # repeaters/repeat_records's map emits twice per doc, so its count is doubled
        # repeaters/repeat_records_by_payload_id has no reduce, so cannot be used
        assert result['value'] % 2 == 0, result['value']
        return int(result['value'] / 2)

    def _get_all_couch_docs_for_model(self, chunk_size):
        from ...models import RepeatRecord
        # repeaters/repeat_records_by_payload_id's map emits once per document
        for result in paginate_view(
            RepeatRecord.get_db(),
            'repeaters/repeat_records_by_payload_id',
            chunk_size=chunk_size,
            include_docs=True,
            reduce=False,
        ):
            yield result['doc']


def get_state(doc):
    from ... import models
    state = models.RECORD_PENDING_STATE
    if doc['succeeded'] and doc['cancelled']:
        state = models.RECORD_EMPTY_STATE
    elif doc['succeeded']:
        state = models.RECORD_SUCCESS_STATE
    elif doc['cancelled']:
        state = models.RECORD_CANCELLED_STATE
    elif doc['failure_reason']:
        state = models.RECORD_FAILURE_STATE
    return state
