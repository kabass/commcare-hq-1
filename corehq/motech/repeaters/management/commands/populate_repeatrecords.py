from django.utils.functional import classproperty

from dimagi.utils.parsing import json_format_datetime

from corehq.apps.cleanup.management.commands.populate_sql_model_from_couch_model import PopulateSQLCommand


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
            cls.repeater_id_map[sql.repeater_id],
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

    @classproperty
    def repeater_id_map(cls):
        from ...models import Repeater
        try:
            data = cls._repeater_id_map
        except AttributeError:
            data = cls._repeater_id_map = dict(Repeater.objects.values_list("id", "repeater_id"))
        return data


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
