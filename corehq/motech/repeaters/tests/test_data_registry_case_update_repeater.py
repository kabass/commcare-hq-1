import uuid
from unittest.mock import patch, Mock
from xml.etree import cElementTree as ElementTree

from testil import eq

from casexml.apps.case.mock import CaseBlock
from casexml.apps.case.xml import V2_NAMESPACE
from corehq.apps.registry.helper import DataRegistryHelper
from corehq.apps.users.models import CouchUser
from corehq.form_processor.models import CommCareCaseSQL, CaseTransaction
from corehq.motech.repeaters.models import DataRegistryCaseUpdateRepeater, Repeater
from corehq.motech.repeaters.repeater_generators import DataRegistryCaseUpdatePayloadGenerator, SYSTEM_FORM_XMLNS


def test_data_registry_case_update_payload_generator_empty_update():
    _test_data_registry_case_update_payload_generator(PropertyBuilder().include_props([]).props, {})


def test_data_registry_case_update_payload_generator_include_list():
    builder = PropertyBuilder().properties(new_prop="new_prop_val").include_props(["new_prop"])
    _test_data_registry_case_update_payload_generator(builder.props, {"new_prop": "new_prop_val"})


def test_data_registry_case_update_payload_generator_exclude_list():
    builder = (
        PropertyBuilder()
        .properties(new_prop="new_prop_val", target_something="1", other_prop="other_prop_val")
        .exclude_props(["other_prop"])
    )
    _test_data_registry_case_update_payload_generator(builder.props, {
        "new_prop": "new_prop_val",
        "target_something": "1"
    })


def _test_data_registry_case_update_payload_generator(intent_properties, expected_updates):
    domain = "source_domain"
    repeater = DataRegistryCaseUpdateRepeater(domain=domain)
    generator = DataRegistryCaseUpdatePayloadGenerator(repeater)
    generator.submission_user_id = Mock(return_value='user1')
    generator.submission_username = Mock(return_value='user1')
    intent_case = CommCareCaseSQL(
        domain=domain,
        type="registry_case_update",
        case_json=intent_properties,
        case_id=uuid.uuid4().hex,
        user_id="local_user1"
    )
    intent_case.track_create(CaseTransaction(form_id="form123", type=CaseTransaction.TYPE_FORM))
    target_case = CommCareCaseSQL(
        case_id="1",
        type="patient",
        case_json={
            "existing_prop": uuid.uuid4().hex,
        }
    )
    with patch.object(DataRegistryHelper, "get_case", return_value=target_case), \
         patch.object(CouchUser, "get_by_user_id", return_value=Mock(username="local_user")):
        repeat_record = Mock(repeater=Repeater())
        form = UpdateForm(generator.get_payload(repeat_record, intent_case))
        form.assert_form_props({
            "source_domain": domain,
            "source_form_id": "form123",
            "source_case_id": intent_case.case_id,
            "source_username": "local_user",
        })
        form.assert_case_updates(expected_updates)


class UpdateForm:
    def __init__(self, form):
        self.formxml = ElementTree.fromstring(form)
        self.case = CaseBlock.from_xml(self.formxml.find("{%s}case" % V2_NAMESPACE))

    def _get_form_value(self, name):
        return self.formxml.find(f"{{{SYSTEM_FORM_XMLNS}}}{name}").text

    def assert_case_updates(self, expected_updates):
        eq(self.case.update, expected_updates)

    def assert_form_props(self, expected):
        actual = {
            key: self._get_form_value(key)
            for key in expected
        }
        eq(actual, expected)


class PropertyBuilder:
    def __init__(self, registry="registry1", create_case=False, override_properties=True):
        self.props: dict = {
            "target_data_registry": registry,
            "target_create_case": int(create_case),
            "target_property_override": int(override_properties),
        }
        self.target_case()

    def target_case(self, domain="target_domain", case_id="1", case_type="patient"):
        self.props.update({
            "target_case_id": case_id,
            "target_domain": domain,
            "target_case_type": case_type,
        })
        return self

    def set_owner(self, new_owner):
        self.props["target_case_owner_id"] = new_owner
        return self

    def create_index(self, index_case_id="case2", index_type="child"):
        self.props.update({
            "target_index_case_id": index_case_id,
            "target_index_type": index_type,
        })
        return self

    def include_props(self, include):
        self.props["target_property_includelist"] = " ".join(include)
        return self

    def exclude_props(self, exclude):
        self.props["target_property_excludelist"] = " ".join(exclude)
        return self

    def properties(self, **kwargs):
        self.props.update(kwargs)
        return self
