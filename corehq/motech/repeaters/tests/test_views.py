from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from corehq.apps.domain.shortcuts import create_domain
from corehq.apps.reports.dispatcher import DomainReportDispatcher
from corehq.apps.users.models import WebUser
from corehq.motech.models import ConnectionSettings

from ..models import FormRepeater, RepeaterStub, are_repeat_records_migrated
from ..views import SQLRepeatRecordReport

DOMAIN = 'gaidhlig'


class TestReportQueries(TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.domain = create_domain(DOMAIN)
        cls.user = WebUser.create(DOMAIN, 'username', 'password',
                                  created_by=None, created_via=None)

    def setUp(self):
        are_repeat_records_migrated.clear(DOMAIN)
        now = timezone.now()
        self.connection_settings = ConnectionSettings.objects.create(
            domain=DOMAIN,
            name='Example API',
            url="https://www.example.com/api/"
        )
        self.repeater = FormRepeater(
            domain=DOMAIN,
            connection_settings_id=self.connection_settings.id,
        )
        self.repeater.save()
        self.repeater_stub = RepeaterStub.objects.get(
            domain=DOMAIN,
            repeater_id=self.repeater.get_id,
        )
        for payload_id in [
            'aon', 'dha', 'trì', 'ceithir', 'coig', 'sia', 'seachd',
            'ochd', 'naoi', 'deich',
        ]:
            self.repeater_stub.repeat_records.create(
                domain=self.repeater_stub.domain,
                payload_id=payload_id,
                registered_at=now,
            )

    def tearDown(self):
        self.repeater.delete()
        self.connection_settings.delete()

    @classmethod
    def tearDownClass(cls):
        cls.user.delete(deleted_by=None)
        cls.domain.delete()
        super().tearDownClass()

    def test_report_num_queries(self):
        self.client.login(username='username', password='password')

        with self.assertNumQueries(18):
            # Queries we don't care about:
            # 1. SELECT ... FROM "auth_user" ...
            # 2. SELECT ... FROM "users_domainpermissionsmirror" ...
            # 3. SELECT ... FROM "users_domainpermissionsmirror" ...
            # 4. SELECT ... FROM "accounting_defaultproductplan" ...
            # 5. SELECT ... FROM "django_prbac_role" ...
            # 6. SELECT ... FROM "auth_user" ...
            # 7. UPDATE "auth_user" ...
            # 8. SELECT  ... FROM "accounting_defaultproductplan" ...
            # 9. SELECT ...FROM "auditcare_viewname" ...
            # 10. SAVEPOINT "s140263734662976_x3"
            # 11. INSERT INTO "auditcare_viewname" ...
            # 12. RELEASE SAVEPOINT "s140263734662976_x3"
            # ...
            # 18. INSERT INTO "auditcare_navigationeventaudit" ...
            #
            # Queries we do care about:
            # 13. SELECT (1) AS "a" FROM "repeaters_repeatrecord" ...
            # 14. SELECT ... FROM "repeaters_repeatrecord" ...
            # 15. SELECT ... FROM "repeaters_repeatrecordattempt" ...
            # 16. SELECT ... FROM "motech_connectionsettings" ...
            # 17. EXPLAIN SELECT ... FROM "repeaters_repeatrecord" ...
            report_json_url = reverse(DomainReportDispatcher.name(), args=[
                DOMAIN, 'json/', SQLRepeatRecordReport.slug])
            self.client.get(report_json_url)
