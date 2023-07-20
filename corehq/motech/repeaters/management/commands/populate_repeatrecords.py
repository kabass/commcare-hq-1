from django.utils.functional import classproperty

from dimagi.utils.parsing import json_format_datetime

from corehq.apps.cleanup.management.commands.populate_sql_model_from_couch_model import PopulateSQLCommand
from corehq.util.couch_helpers import paginate_view

from ...models import Repeater, RepeatRecordAttempt


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

        # TODO avoid importing RepeatRecordAttempt in this file?
        def transform_couch_attempts(items):
            for attempt in items:
                obj = RepeatRecordAttempt(attempt)
                yield {f: getattr(obj, f) for f in attempt_fields}

        attempt_fields = ["state", "message", "created_at"]
        diffs.extend(x for x in cls.diff_lists(
            "attempts",
            list(transform_couch_attempts(couch["attempts"])),
            sql.attempts,
            attempt_fields,
        ) if x)
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
    def get_couch_repeater_id(cls, sql_id):
        try:
            return cls.repeater_id_map.get_couch_id(sql_id)
        except KeyError:
            couch_id = Repeater.objects.filter(id=sql_id).values_list("repeater_id", flat=True)[0]
            cls.repeater_id_map.add_pair(sql_id, couch_id)
            return couch_id

    @classmethod
    def get_sql_repeater_id(cls, couch_id):
        try:
            return cls.repeater_id_map.get_sql_id(couch_id)
        except KeyError:
            sql_id = Repeater.objects.filter(repeater_id=couch_id).values_list("id", flat=True)[0]
            cls.repeater_id_map.add_pair(sql_id, couch_id)
            return sql_id

    @classproperty
    def repeater_id_map(cls):
        try:
            data = cls._repeater_id_map
        except AttributeError:
            data = cls._repeater_id_map = TwoWayDict(Repeater.objects.values_list("id", "repeater_id"))
        return data

    def handle(self, *args, **kw):
        def _optimized_sync_to_sql(self_, sql_object, save=True):
            # Avoid repeater lookup
            sql_object.repeater_id = self.get_sql_repeater_id(self_.repeater_id)
            return super(RepeatRecord, self_)._migration_sync_to_sql(sql_object, save=save)

        from ...models import RepeatRecord
        original_migration_sync_to_sql = RepeatRecord._migration_sync_to_sql
        RepeatRecord._migration_sync_to_sql = _optimized_sync_to_sql
        try:
            return super().handle(*args, **kw)
        finally:
            RepeatRecord._migration_sync_to_sql = original_migration_sync_to_sql

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

    #def _get_couch_doc_count_for_domains(self, domains):
    #def _iter_couch_docs_for_domains(self, domains, chunk_size):


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


class TwoWayDict:
    def __init__(self, sql_and_couch_id_pairs):
        self.couch_by_sql = dict(sql_and_couch_id_pairs)
        self.sql_by_couch = {v: k for k, v in self.couch_by_sql.items()}
        assert len(self.couch_by_sql) == len(self.sql_by_couch), \
            f"{len(self.couch_by_sql)} != {len(self.sql_by_couch)}"

    def get_couch_id(self, sql_id):
        return self.couch_by_sql[sql_id]

    def get_sql_id(self, couch_id):
        return self.sql_by_couch[couch_id]

    def add_pair(self, sql_id, couch_id):
        self.couch_by_sql[sql_id] = couch_id
        self.sql_by_couch[couch_id] = sql_id
        assert len(self.couch_by_sql) == len(self.sql_by_couch), \
            f"{len(self.couch_by_sql)} != {len(self.sql_by_couch)}"
