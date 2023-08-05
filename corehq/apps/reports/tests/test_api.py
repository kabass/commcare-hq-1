from contextlib import contextmanager

from django.test import TestCase

from casexml.apps.case.mock import CaseFactory, CaseIndex, CaseStructure

from corehq.apps.export.const import DEID_ID_TRANSFORM
from corehq.apps.users.util import SYSTEM_USER_ID
from corehq.form_processor.models import CommCareCase

from ..filters.api import _copy_cases

DOMAIN = 'test-domain'


class TestCaseCopy(TestCase):

    def test_no_cases_to_copy(self):
        case_id_pairs, errors = _copy_cases(
            domain=DOMAIN,
            case_ids=[],
            to_owner='new owner'
        )
        self.assertEqual(case_id_pairs, [])
        self.assertEqual(errors, [])

    def test_copy_case_to_blank_owner(self):
        case_id_pairs, errors = _copy_cases(
            domain=DOMAIN,
            case_ids=[],
            to_owner=''
        )
        self.assertEqual(case_id_pairs, [])
        self.assertEqual(errors, ['Must copy cases to valid new owner'])

    def test_copy_case_to_same_owner(self):
        with get_mother_case(owner_id='owner_id') as case:
            case_id_pairs, errors = _copy_cases(
                domain=DOMAIN,
                case_ids=[case.case_id],
                to_owner=case.owner_id
            )
            self.assertEqual(case_id_pairs, [])
            self.assertEqual(errors, [
                'Original case owner owner_id cannot copy case '
                f'{case.case_id} to themselves.'
            ])

    def test_copy_case_to_new_owner(self):
        properties = {
            'family_name': 'Nature',
        }

        with get_mother_case('owner_id', update=properties) as case:
            case_id_pairs, errors = _copy_cases(
                domain=DOMAIN,
                case_ids=[case.case_id],
                to_owner='new_owner_id'
            )

            self.assertEqual(len(case_id_pairs), 1)
            orig_case_id, new_case_id = case_id_pairs[0]
            new_case = CommCareCase.objects.get_case(new_case_id, DOMAIN)
            self.assertEqual(new_case.owner_id, 'new_owner_id')
            self.assertEqual(new_case.case_json['family_name'], 'Nature')
            self.assertEqual(new_case.opened_by, SYSTEM_USER_ID)

    def test_copy_case_with_sensitive_properties(self):
        properties = {
            'age': '34',
        }

        with get_mother_case('owner_id', update=properties) as case:
            case_id_pairs, errors = _copy_cases(
                domain=DOMAIN,
                case_ids=[case.case_id],
                to_owner='new_owner_id',
                censor_data={
                    'age': DEID_ID_TRANSFORM,
                    'case_name': DEID_ID_TRANSFORM,
                }
            )

            self.assertEqual(len(case_id_pairs), 1)
            orig_case_id, new_case_id = case_id_pairs[0]
            new_case = CommCareCase.objects.get_case(new_case_id, DOMAIN)
            self.assertTrue(new_case.name != case.name)
            self.assertTrue(new_case.case_json['age'] != case.case_json['age'])
            self.assertEqual(new_case.owner_id, 'new_owner_id')

    def test_indices_copied(self):
        with get_child_case() as case:
            parent_case_id = case.get_indices()[0].referenced_id
            case_id_pairs, errors = _copy_cases(
                domain=DOMAIN,
                case_ids=[case.case_id, parent_case_id],
                to_owner='new_owner_id',
            )

            self.assertEqual(len(case_id_pairs), 2)
            new_case_ids = [pair[1] for pair in case_id_pairs]
            new_cases = CommCareCase.objects.get_cases(new_case_ids, DOMAIN)
            mother_case = next(c for c in new_cases if c.type == 'mother')
            child_case = next(c for c in new_cases if c.type == 'child')

            self.assertEqual(mother_case.owner_id, 'new_owner_id')
            self.assertEqual(child_case.owner_id, 'new_owner_id')

            self.assertEqual(mother_case.get_indices(), [])
            self.assertEqual(len(child_case.get_indices()), 1)
            self.assertEqual(
                child_case.get_indices()[0].referenced_id,
                mother_case.case_id,
            )

    def test_indices_not_copied_if_not_in_case_list(self):
        with get_child_case() as case:
            case_id_pairs, errors = _copy_cases(
                domain=DOMAIN,
                case_ids=[case.case_id],
                to_owner='new_owner_id',
            )
        self.assertEqual(len(case_id_pairs), 1)
        orig_case_id, new_case_id = case_id_pairs[0]
        new_case = CommCareCase.objects.get_case(new_case_id, DOMAIN)
        self.assertEqual(new_case.owner_id, 'new_owner_id')
        self.assertEqual(new_case.get_indices(), [])


@contextmanager
def get_mother_case(*args, **kwargs):
    factory = CaseFactory(DOMAIN)
    mother = factory.create_case(
        case_type='mother',
        case_name='Haumea',
        **kwargs,
    )
    try:
        yield mother
    finally:
        factory.close_case(mother.case_id)


@contextmanager
def get_child_case():
    factory = CaseFactory(DOMAIN)
    with get_mother_case() as mother:
        struct = CaseStructure(
            attrs={
                'case_type': 'child',
                'case_name': 'Namaka',
                'create': True,
            },
            indices=[CaseIndex(
                relationship='child',
                identifier='mother',
                related_structure=CaseStructure(case_id=mother.case_id),
                related_type='mother',
            )],
        )
        child, __ = factory.create_or_update_cases([struct])
        try:
            yield child
        finally:
            factory.close_case(child.case_id)
