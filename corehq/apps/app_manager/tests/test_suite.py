from django.test import SimpleTestCase

from corehq.apps.app_manager.exceptions import SuiteValidationError
from corehq.apps.app_manager.models import (
    Application,
    CaseSearch,
    CaseSearchAgainLabel,
    CaseSearchLabel,
    CaseSearchProperty,
    GraphConfiguration,
    GraphSeries,
    ReportAppConfig,
    ReportModule,
)
from corehq.apps.app_manager.tests.app_factory import AppFactory
from corehq.apps.app_manager.tests.util import (
    SuiteMixin,
    TestXmlMixin,
    patch_get_xform_resource_overrides,
)
from corehq.apps.hqmedia.models import HQMediaMapItem
from corehq.apps.userreports.models import ReportConfiguration


@patch_get_xform_resource_overrides()
class SuiteTest(SimpleTestCase, TestXmlMixin, SuiteMixin):
    file_path = ('data', 'suite')

    def test_normal_suite(self, *args):
        self._test_generic_suite('app', 'normal-suite')

    def test_tiered_select(self, *args):
        self._test_generic_suite('tiered-select', 'tiered-select')

    def test_3_tiered_select(self, *args):
        self._test_generic_suite('tiered-select-3', 'tiered-select-3')

    def test_case_search_action(self):
        app = Application.wrap(self.get_json('suite-advanced'))
        app.modules[0].search_config = CaseSearch(
            search_label=CaseSearchLabel(
                label={'en': 'Get them'},
                media_image={'en': "jr://file/commcare/image/1.png"},
                media_audio={'en': "jr://file/commcare/image/2.mp3"}
            ),
            search_again_label=CaseSearchAgainLabel(
                label={'en': 'Get them'},
                media_audio={'en': "jr://file/commcare/image/2.mp3"}
            ),
            properties=[CaseSearchProperty(name='name', label={'en': 'Name'})],
        )
        # wrap to have assign_references called
        app = Application.wrap(app.to_json())

        # test for legacy action node for older versions
        self.assertXmlPartialEqual(
            self.get_xml('case-search-with-action'),
            app.create_suite(),
            "./detail[@id='m0_case_short']/action"
        )
        self.assertXmlPartialEqual(
            self.get_xml('case-search-again-with-action'),
            app.create_suite(),
            "./detail[@id='m0_search_short']/action"
        )

        # test for localized action node for apps with CC version > 2.21
        app.build_spec.version = '2.21.0'
        self.assertXmlPartialEqual(
            self.get_xml('case-search-with-localized-action'),
            app.create_suite(),
            "./detail[@id='m0_case_short']/action"
        )
        self.assertXmlPartialEqual(
            self.get_xml('case-search-again-with-localized-action'),
            app.create_suite(),
            "./detail[@id='m0_search_short']/action"
        )

    def test_callcenter_suite(self, *args):
        self._test_generic_suite('call-center')

    def test_attached_picture(self, *args):
        self._test_generic_suite_partial('app_attached_image', "./detail", 'suite-attached-image')

    def test_owner_name(self, *args):
        self._test_generic_suite('owner-name')

    def test_printing(self, *args):
        self._test_generic_suite('app_print_detail', 'suite-print-detail')

    def test_fixture_to_case_selection(self, *args):
        factory = AppFactory(build_version='2.9.0')

        module, form = factory.new_basic_module('my_module', 'cases')
        module.fixture_select.active = True
        module.fixture_select.fixture_type = 'days'
        module.fixture_select.display_column = 'my_display_column'
        module.fixture_select.variable_column = 'my_variable_column'
        module.fixture_select.xpath = 'date(scheduled_date) <= date(today() + $fixture_value)'

        factory.form_requires_case(form)

        self.assertXmlEqual(self.get_xml('fixture-to-case-selection'), factory.app.create_suite())

    def test_fixture_to_case_selection_with_form_filtering(self, *args):
        factory = AppFactory(build_version='2.9.0')

        module, form = factory.new_basic_module('my_module', 'cases')
        module.fixture_select.active = True
        module.fixture_select.fixture_type = 'days'
        module.fixture_select.display_column = 'my_display_column'
        module.fixture_select.variable_column = 'my_variable_column'
        module.fixture_select.xpath = 'date(scheduled_date) <= date(today() + $fixture_value)'

        factory.form_requires_case(form)

        form.form_filter = "$fixture_value <= today()"

        self.assertXmlEqual(self.get_xml('fixture-to-case-selection-with-form-filtering'), factory.app.create_suite())

    def test_fixture_to_case_selection_localization(self, *args):
        factory = AppFactory(build_version='2.9.0')

        module, form = factory.new_basic_module('my_module', 'cases')
        module.fixture_select.active = True
        module.fixture_select.fixture_type = 'days'
        module.fixture_select.display_column = 'my_display_column'
        module.fixture_select.localize = True
        module.fixture_select.variable_column = 'my_variable_column'
        module.fixture_select.xpath = 'date(scheduled_date) <= date(today() + $fixture_value)'

        factory.form_requires_case(form)

        self.assertXmlEqual(self.get_xml('fixture-to-case-selection-localization'), factory.app.create_suite())

    def test_fixture_to_case_selection_parent_child(self, *args):
        factory = AppFactory(build_version='2.9.0')

        m0, m0f0 = factory.new_basic_module('parent', 'parent')
        m0.fixture_select.active = True
        m0.fixture_select.fixture_type = 'province'
        m0.fixture_select.display_column = 'display_name'
        m0.fixture_select.variable_column = 'var_name'
        m0.fixture_select.xpath = 'province = $fixture_value'

        factory.form_requires_case(m0f0)

        m1, m1f0 = factory.new_basic_module('child', 'child')
        m1.fixture_select.active = True
        m1.fixture_select.fixture_type = 'city'
        m1.fixture_select.display_column = 'display_name'
        m1.fixture_select.variable_column = 'var_name'
        m1.fixture_select.xpath = 'city = $fixture_value'

        factory.form_requires_case(m1f0, parent_case_type='parent')

        self.assertXmlEqual(self.get_xml('fixture-to-case-selection-parent-child'), factory.app.create_suite())

    def test_case_detail_tabs(self, *args):
        self._test_generic_suite("app_case_detail_tabs", 'suite-case-detail-tabs')

    def test_case_detail_tabs_with_nodesets(self, *args):
        with flag_enabled('DISPLAY_CONDITION_ON_TABS'):
            self._test_generic_suite("app_case_detail_tabs_with_nodesets", 'suite-case-detail-tabs-with-nodesets')

    def test_case_detail_tabs_with_nodesets_for_sorting_search_only_field(self, *args):
        app_json = self.get_json("app_case_detail_tabs_with_nodesets")
        app = Application.wrap(app_json)

        # update app to add in 2 new columns both with the field 'gender'

        # 1. add a column to the 2nd tab that is marked as 'search only'.
        #    This should get sorting applied to it
        tab_spans = app.modules[0].case_details.long.get_tab_spans()

        # 2. add a second column to the last tab which already has a 'gender' field
        #    This should result in the 'gender' field being displayed as well
        #    as being used for sorting
        sorted_gender_col = DetailColumn.from_json(
            app_json["modules"][0]["case_details"]["long"]["columns"][-1]
        )
        app.modules[0].case_details.long.columns.insert(tab_spans[1][1] - 1, sorted_gender_col)
        plain_gender_col = DetailColumn.from_json(
            app_json["modules"][0]["case_details"]["long"]["columns"][-1]
        )
        plain_gender_col.format = "plain"
        index = len(app.modules[0].case_details.long.columns) - 1
        app.modules[0].case_details.long.columns.insert(index, plain_gender_col)
        self.assertXmlPartialEqual(
            self.get_xml("suite-case-detail-tabs-with-nodesets-for-sorting-search-only"),
            app.create_suite(),
            './detail[@id="m0_case_long"]')

    def test_case_detail_instance_adding(self, *args):
        # Tests that post-processing adds instances used in calculations
        # by any of the details (short, long, inline, persistent)
        self._test_generic_suite('app_case_detail_instances', 'suite-case-detail-instances')

    def test_case_tile_suite(self, *args):
        self._test_generic_suite("app_case_tiles", "suite-case-tiles")

    def test_case_detail_conditional_enum(self, *args):
        app = Application.new_app('domain', 'Untitled Application')

        module = app.add_module(Module.new_module('Unititled Module', None))
        module.case_type = 'patient'

        module.case_details.short.columns = [
            DetailColumn(
                header={'en': 'Gender'},
                model='case',
                field='gender',
                format='conditional-enum',
                enum=[
                    MappingItem(key="gender = 'male' and age <= 21", value={'en': 'Boy'}),
                    MappingItem(key="gender = 'female' and age <= 21", value={'en': 'Girl'}),
                    MappingItem(key="gender = 'male' and age > 21", value={'en': 'Man'}),
                    MappingItem(key="gender = 'female' and age > 21", value={'en': 'Woman'}),
                ],
            ),
        ]

        key1_varname = hashlib.md5("gender = 'male' and age <= 21".encode('utf-8')).hexdigest()[:8]
        key2_varname = hashlib.md5("gender = 'female' and age <= 21".encode('utf-8')).hexdigest()[:8]
        key3_varname = hashlib.md5("gender = 'male' and age > 21".encode('utf-8')).hexdigest()[:8]
        key4_varname = hashlib.md5("gender = 'female' and age > 21".encode('utf-8')).hexdigest()[:8]

        icon_mapping_spec = """
        <partial>
          <template>
            <text>
              <xpath function="join(' ', if(gender = 'male' and age &lt;= 21, $h{key1_varname}, if(gender = 'female' and age &lt;= 21, $h{key2_varname}, if(gender = 'male' and age &gt; 21, $h{key3_varname}, if(gender = 'female' and age &gt; 21, $h{key4_varname})">
                <variable name="h{key4_varname}">
                  <locale id="m0.case_short.case_gender_1.enum.h{key4_varname}"/>
                </variable>
                <variable name="h{key2_varname}">
                  <locale id="m0.case_short.case_gender_1.enum.h{key2_varname}"/>
                </variable>
                <variable name="h{key3_varname}">
                  <locale id="m0.case_short.case_gender_1.enum.h{key3_varname}"/>
                </variable>
                <variable name="h{key1_varname}">
                  <locale id="m0.case_short.case_gender_1.enum.h{key1_varname}"/>
                </variable>
              </xpath>
            </text>
          </template>
        </partial>
        """.format(
            key1_varname=key1_varname,
            key2_varname=key2_varname,
            key3_varname=key3_varname,
            key4_varname=key4_varname,
        )
        # check correct suite is generated
        self.assertXmlPartialEqual(
            icon_mapping_spec,
            app.create_suite(),
            './detail[@id="m0_case_short"]/field/template'
        )
        # check app strings mapped correctly
        app_strings = commcare_translations.loads(app.create_app_strings('en'))
        self.assertEqual(
            app_strings['m0.case_short.case_gender_1.enum.h{key1_varname}'.format(key1_varname=key1_varname, )],
            'Boy'
        )
        self.assertEqual(
            app_strings['m0.case_short.case_gender_1.enum.h{key2_varname}'.format(key2_varname=key2_varname, )],
            'Girl'
        )
        self.assertEqual(
            app_strings['m0.case_short.case_gender_1.enum.h{key3_varname}'.format(key3_varname=key3_varname, )],
            'Man'
        )
        self.assertEqual(
            app_strings['m0.case_short.case_gender_1.enum.h{key4_varname}'.format(key4_varname=key4_varname, )],
            'Woman'
        )

    def test_case_detail_calculated_conditional_enum(self, *args):
        app = Application.new_app('domain', 'Untitled Application')

        module = app.add_module(Module.new_module('Unititled Module', None))
        module.case_type = 'patient'

        module.case_details.short.columns = [
            DetailColumn(
                header={'en': 'Gender'},
                model='case',
                field="if(gender = 'male', 'boy', 'girl')",
                format='enum',
                enum=[
                    MappingItem(key="boy", value={'en': 'Boy'}),
                    MappingItem(key="girl", value={'en': 'Girl'}),
                ],
            ),
        ]

        icon_mapping_spec = """
        <partial>
          <template>
            <text>
              <xpath function="join(' ', if(selected(if(gender = 'male', 'boy', 'girl'), 'boy'), $kboy, ''), if(selected(if(gender = 'male', 'boy', 'girl'), 'girl'), $kgirl, ''))">
                <variable name="kboy">
                  <locale id="m0.case_short.case_if(gender  'male', 'boy', 'girl')_1.enum.kboy"/>
                </variable>
                <variable name="kgirl">
                  <locale id="m0.case_short.case_if(gender  'male', 'boy', 'girl')_1.enum.kgirl"/>
                </variable>
              </xpath>
            </text>
          </template>
        </partial>
        """
        # check correct suite is generated
        self.assertXmlPartialEqual(
            icon_mapping_spec,
            app.create_suite(),
            './detail[@id="m0_case_short"]/field/template'
        )
        # check app strings mapped correctly
        app_strings = commcare_translations.loads(app.create_app_strings('en'))
        self.assertEqual(
            app_strings["m0.case_short.case_if(gender  'male', 'boy', 'girl')_1.enum.kboy"],
            'Boy'
        )
        self.assertEqual(
            app_strings["m0.case_short.case_if(gender  'male', 'boy', 'girl')_1.enum.kgirl"],
            'Girl'
        )

    def test_case_detail_icon_mapping(self, *args):
        app = Application.new_app('domain', 'Untitled Application')

        module = app.add_module(Module.new_module('Untitled Module', None))
        module.case_type = 'patient'

        module.case_details.short.columns = [
            DetailColumn(
                header={'en': 'Age range'},
                model='case',
                field='age',
                format='enum-image',
                enum=[
                    MappingItem(key='10', value={'en': 'jr://icons/10-year-old.png'}),
                    MappingItem(key='age > 50', value={'en': 'jr://icons/old-icon.png'}),
                    MappingItem(key='15%', value={'en': 'jr://icons/percent-icon.png'}),
                ],
            ),
        ]

        key1_varname = '10'
        key2_varname = hashlib.md5('age > 50'.encode('utf-8')).hexdigest()[:8]
        key3_varname = hashlib.md5('15%'.encode('utf-8')).hexdigest()[:8]

        icon_mapping_spec = """
            <partial>
              <template form="image" width="13%">
                <text>
                  <xpath function="join(' ', if(age = '10', $k{key1_varname}, if(age &gt; 50, $h{key2_varname}, if(age = '15%', $h{key3_varname})">
                    <variable name="h{key2_varname}">
                      <locale id="m0.case_short.case_age_1.enum.h{key2_varname}"/>
                    </variable>
                    <variable name="h{key3_varname}">
                      <locale id="m0.case_short.case_age_1.enum.h{key3_varname}"/>
                    </variable>
                    <variable name="k{key1_varname}">
                      <locale id="m0.case_short.case_age_1.enum.k{key1_varname}"/>
                    </variable>
                  </xpath>
                </text>
              </template>
            </partial>
        """.format(
            key1_varname=key1_varname,
            key2_varname=key2_varname,
            key3_varname=key3_varname,
        )
        # check correct suite is generated
        self.assertXmlPartialEqual(
            icon_mapping_spec,
            app.create_suite(),
            './detail/field/template[@form="image"]'
        )
        # check icons map correctly
        app_strings = commcare_translations.loads(app.create_app_strings('en'))
        self.assertEqual(
            app_strings['m0.case_short.case_age_1.enum.h{key3_varname}'.format(key3_varname=key3_varname,)],
            'jr://icons/percent-icon.png'
        )
        self.assertEqual(
            app_strings['m0.case_short.case_age_1.enum.h{key2_varname}'.format(key2_varname=key2_varname,)],
            'jr://icons/old-icon.png'
        )
        self.assertEqual(
            app_strings['m0.case_short.case_age_1.enum.k{key1_varname}'.format(key1_varname=key1_varname,)],
            'jr://icons/10-year-old.png'
        )

    def test_case_tile_pull_down(self, *args):
        app = Application.new_app('domain', 'Untitled Application')

        module = app.add_module(Module.new_module('Untitled Module', None))
        module.case_type = 'patient'
        module.case_details.short.use_case_tiles = True
        module.case_details.short.persist_tile_on_forms = True
        module.case_details.short.pull_down_tile = True
        self._add_columns_for_case_details(module)

        form = app.new_form(0, "Untitled Form", None)
        form.xmlns = 'http://id_m0-f0'
        form.requires = 'case'

        self.assertXmlPartialEqual(
            self.get_xml('case_tile_pulldown_session'),
            app.create_suite(),
            "./entry/session"
        )

    def test_inline_case_detail_from_another_module(self, *args):
        factory = AppFactory()
        module0, form0 = factory.new_advanced_module("m0", "person")
        factory.form_requires_case(form0, "person")
        module0.case_details.short.use_case_tiles = True
        self._add_columns_for_case_details(module0)

        module1, form1 = factory.new_advanced_module("m1", "person")
        factory.form_requires_case(form1, "person")

        # not configured to use other module's persistent case tile so
        # has no detail-inline and detail-persistent attr
        self.ensure_module_session_datum_xml(factory, '', '')

        # configured to use other module's persistent case tile
        module1.case_details.short.persistent_case_tile_from_module = module0.unique_id
        self.ensure_module_session_datum_xml(factory, '', 'detail-persistent="m0_case_short"')

        # configured to use other module's persistent case tile that has custom xml
        module0.case_details.short.use_case_tiles = False
        module0.case_details.short.custom_xml = '<detail id="m0_case_short"></detail>'
        self.ensure_module_session_datum_xml(factory, '', 'detail-persistent="m0_case_short"')
        module0.case_details.short.custom_xml = ''
        module0.case_details.short.use_case_tiles = True

        # configured to use pull down tile from the other module
        module1.case_details.short.pull_down_tile = True
        self.ensure_module_session_datum_xml(factory, 'detail-inline="m0_case_long"',
                                             'detail-persistent="m0_case_short"')

        # set to use persistent case tile of its own as well but it would still
        # persists case tiles and detail inline from another module
        module1.case_details.short.use_case_tiles = True
        module1.case_details.short.persist_tile_on_forms = True
        self._add_columns_for_case_details(module1)
        self.ensure_module_session_datum_xml(factory, 'detail-inline="m0_case_long"',
                                             'detail-persistent="m0_case_short"')

        # set to use case tile from a module that does not support case tiles anymore
        # and has own persistent case tile as well
        # So now detail inline from its own details
        module0.case_details.short.use_case_tiles = False
        self.ensure_module_session_datum_xml(factory, 'detail-inline="m1_case_long"',
                                             'detail-persistent="m1_case_short"')

        # set to use case tile from a module that does not support case tiles anymore
        # and does not have its own persistent case tile as well
        module1.case_details.short.use_case_tiles = False
        self.ensure_module_session_datum_xml(factory, '', '')

    def test_persistent_case_tiles_from_another_module(self, *args):
        factory = AppFactory()
        module0, form0 = factory.new_advanced_module("m0", "person")
        factory.form_requires_case(form0, "person")
        module0.case_details.short.use_case_tiles = True
        self._add_columns_for_case_details(module0)

        module1, form1 = factory.new_advanced_module("m1", "person")
        factory.form_requires_case(form1, "person")

        # not configured to use other module's persistent case tile so
        # has no detail-persistent attr
        self.ensure_module_session_datum_xml(factory, '', '')

        # configured to use other module's persistent case tile
        module1.case_details.short.persistent_case_tile_from_module = module0.unique_id
        self.ensure_module_session_datum_xml(factory, '', 'detail-persistent="m0_case_short"')

        # configured to use other module's persistent case tile that has custom xml
        module0.case_details.short.use_case_tiles = False
        module0.case_details.short.custom_xml = '<detail id="m0_case_short"></detail>'
        self.ensure_module_session_datum_xml(factory, '', 'detail-persistent="m0_case_short"')
        module0.case_details.short.custom_xml = ''
        module0.case_details.short.use_case_tiles = True

        # set to use persistent case tile of its own as well but it would still
        # persists case tiles from another module
        module1.case_details.short.use_case_tiles = True
        module1.case_details.short.persist_tile_on_forms = True
        self._add_columns_for_case_details(module1)
        self.ensure_module_session_datum_xml(factory, '', 'detail-persistent="m0_case_short"')

        # set to use case tile from a module that does not support case tiles anymore
        # and has own persistent case tile as well
        module0.case_details.short.use_case_tiles = False
        self.ensure_module_session_datum_xml(factory, '', 'detail-persistent="m1_case_short"')

        # set to use case tile from a module that does not support case tiles anymore
        # and does not have its own persistent case tile as well
        module1.case_details.short.use_case_tiles = False
        self.ensure_module_session_datum_xml(factory, '', '')

    def test_persistent_case_tiles_in_advanced_forms(self, *args):
        """
        Test that the detail-persistent attributes is set correctly when persistent
        case tiles are used on advanced forms.
        """
        factory = AppFactory()
        module, form = factory.new_advanced_module("my_module", "person")
        factory.form_requires_case(form, "person")
        module.case_details.short.custom_xml = '<detail id="m0_case_short"></detail>'
        module.case_details.short.persist_tile_on_forms = True
        suite = factory.app.create_suite()

        # The relevant check is really that detail-persistent="m0_case_short"
        # but assertXmlPartialEqual xpath implementation doesn't currently
        # support selecting attributes
        self.assertXmlPartialEqual(
            """
            <partial>
                <datum
                    detail-confirm="m0_case_long"
                    detail-persistent="m0_case_short"
                    detail-select="m0_case_short"
                    id="case_id_load_person_0"
                    nodeset="instance('casedb')/casedb/case[@case_type='person'][@status='open']"
                    value="./@case_id"
                />
            </partial>
            """,
            suite,
            "entry/session/datum"
        )

    def test_persistent_case_tiles_in_advanced_module_case_lists(self, *args):
        """
        Test that the detail-persistent attributes is set correctly when persistent
        case tiles are used on advanced module case lists
        """
        factory = AppFactory()
        module, form = factory.new_advanced_module("my_module", "person")
        factory.form_requires_case(form, "person")
        module.case_list.show = True
        module.case_details.short.custom_xml = '<detail id="m0_case_short"></detail>'
        module.case_details.short.persist_tile_on_forms = True
        suite = factory.app.create_suite()

        # The relevant check is really that detail-persistent="m0_case_short"
        # but assertXmlPartialEqual xpath implementation doesn't currently
        # support selecting attributes
        self.assertXmlPartialEqual(
            """
            <partial>
                <datum
                    detail-confirm="m0_case_long"
                    detail-persistent="m0_case_short"
                    detail-select="m0_case_short"
                    id="case_id_case_person"
                    nodeset="instance('casedb')/casedb/case[@case_type='person'][@status='open']"
                    value="./@case_id"
                />
            </partial>
            """,
            suite,
            'entry/command[@id="m0-case-list"]/../session/datum',
        )

    def test_persistent_case_name_in_forms(self, *args):
        """
        Test that the detail-persistent attributes are set correctly when the
        module is configured to persist the case name at the top of the form.
        Also confirm that the appropriate <detail> element is added to the suite.
        """
        factory = AppFactory()
        module, form = factory.new_basic_module("my_module", "person")
        factory.form_requires_case(form, "person")
        module.case_details.short.persist_case_context = True
        suite = factory.app.create_suite()

        self.assertXmlPartialEqual(
            """
            <partial>
                <detail id="m0_persistent_case_context">
                    <title>
                        <text/>
                    </title>
                    <field>
                        <style font-size="large" horz-align="center">
                            <grid grid-height="1" grid-width="12" grid-x="0" grid-y="0"/>
                        </style>
                        <header>
                            <text/>
                        </header>
                        <template>
                            <text>
                                <xpath function="case_name"/>
                            </text>
                        </template>
                    </field>
                </detail>
            </partial>
            """,
            suite,
            "detail[@id='m0_persistent_case_context']"
        )

        # The attribute we care about here is detail-persistent="m0_persistent_case_context"
        self.assertXmlPartialEqual(
            """
            <partial>
                <datum
                    detail-confirm="m0_case_long"
                    detail-persistent="m0_persistent_case_context"
                    detail-select="m0_case_short"
                    id="case_id"
                    nodeset="instance('casedb')/casedb/case[@case_type='person'][@status='open']"
                    value="./@case_id"
                />
            </partial>
            """,
            suite,
            "entry/session/datum"
        )

    def test_persistent_case_name_when_tiles_enabled(self, *args):
        """
        Confirm that the persistent case name context is not added when case tiles
        are configured to persist in forms
        """
        factory = AppFactory()
        module, form = factory.new_advanced_module("my_module", "person")
        factory.form_requires_case(form, "person")
        module.case_details.short.custom_xml = '<detail id="m0_case_short"></detail>'
        module.case_details.short.use_case_tiles = True
        module.case_details.short.persist_tile_on_forms = True
        module.case_details.short.persist_case_context = True
        suite = factory.app.create_suite()

        self.assertXmlDoesNotHaveXpath(suite, "detail[@id='m0_persistent_case_context']")
        self.assertXmlPartialEqual(
            """
            <partial>
                <datum
                    detail-confirm="m0_case_long"
                    detail-persistent="m0_case_short"
                    detail-select="m0_case_short"
                    id="case_id_load_person_0"
                    nodeset="instance('casedb')/casedb/case[@case_type='person'][@status='open']"
                    value="./@case_id"
                />
            </partial>
            """,
            suite,
            "entry/session/datum"
        )

    def test_custom_xml_with_wrong_module_index(self, *args):
        factory = AppFactory()
        module, form = factory.new_advanced_module("my_module", "person")
        # This should be 'm0_case_short'
        module.case_details.short.custom_xml = '<detail id="m1_case_short"></detail>'
        with self.assertRaises(SuiteValidationError):
            factory.app.create_suite()

    def test_subcase_repeat_mixed(self, *args):
        app = Application.new_app(None, "Untitled Application")
        module_0 = app.add_module(Module.new_module('parent', None))
        module_0.unique_id = 'm0'
        module_0.case_type = 'parent'
        form = app.new_form(0, "Form", None)

        form.actions.open_case = OpenCaseAction(name_update=ConditionalCaseUpdate(question_path="/data/question1"))
        form.actions.open_case.condition.type = 'always'

        child_case_type = 'child'
        form.actions.subcases.append(OpenSubCaseAction(
            case_type=child_case_type,
            name_update=ConditionalCaseUpdate(question_path="/data/question1"),
            condition=FormActionCondition(type='always')
        ))
        # subcase in the middle that has a repeat context
        form.actions.subcases.append(OpenSubCaseAction(
            case_type=child_case_type,
            name_update=ConditionalCaseUpdate(question_path="/data/repeat/question1"),
            repeat_context='/data/repeat',
            condition=FormActionCondition(type='always')
        ))
        form.actions.subcases.append(OpenSubCaseAction(
            case_type=child_case_type,
            name_update=ConditionalCaseUpdate(question_path="/data/question1"),
            condition=FormActionCondition(type='always')
        ))

        expected = """
        <partial>
            <session>
              <datum id="case_id_new_parent_0" function="uuid()"/>
              <datum id="case_id_new_child_1" function="uuid()"/>
              <datum id="case_id_new_child_3" function="uuid()"/>
            </session>
        </partial>
        """
        self.assertXmlPartialEqual(expected,
                                   app.create_suite(),
                                   './entry[1]/session')

    def test_report_module(self, *args):
        from corehq.apps.userreports.tests.utils import get_sample_report_config

        app = Application.new_app('domain', "Untitled Application")

        report = get_sample_report_config()
        report._id = 'd3ff18cd83adf4550b35db8d391f6008'
        report_app_config = ReportAppConfig(
            report_id=report._id,
            header={'en': 'CommBugz'},
            uuid='ip1bjs8xtaejnhfrbzj2r6v1fi6hia4i',
            xpath_description='"report description"',
            use_xpath_description=True,
            complete_graph_configs={
                chart.chart_id: GraphConfiguration(
                    graph_type="bar",
                    series=[GraphSeries() for c in chart.y_axis_columns],
                )
                for chart in report.charts
            },
        )
        report_app_config._report = report
        report_module = app.add_module(ReportModule.new_module('Reports', None))
        report_module.unique_id = 'report_module'
        report_module.report_configs = [report_app_config]
        report_module._loaded = True
        self.assertXmlPartialEqual(
            self.get_xml('reports_module_menu'),
            app.create_suite(),
            "./menu",
        )

        app.multimedia_map = {
            "jr://file/commcare/image/module0_en.png": HQMediaMapItem(
                multimedia_id='bb4472b4b3c702f81c0b208357eb22f8',
                media_type='CommCareImage',
                unique_id='fe06454697634053cdb75fd9705ac7e6',
            ),
        }
        report_module.media_image = {
            'en': 'jr://file/commcare/image/module0_en.png',
        }
        report_module.get_details.reset_cache(report_module)
        actual_suite = app.create_suite()
        self.assertXmlPartialEqual(
            self.get_xml('reports_module_menu_multimedia'),
            actual_suite,
            "./menu",
        )

        self.assertXmlPartialEqual(
            self.get_xml('reports_module_select_detail'),
            actual_suite,
            "./detail[@id='reports.ip1bjs8xtaejnhfrbzj2r6v1fi6hia4i.select']",
        )
        self.assertXmlPartialEqual(
            self.get_xml('reports_module_summary_detail_use_xpath_description'),
            actual_suite,
            "./detail[@id='reports.ip1bjs8xtaejnhfrbzj2r6v1fi6hia4i.summary']",
        )
        self.assertXmlPartialEqual(
            self.get_xml('reports_module_data_detail'),
            actual_suite,
            "./detail/detail[@id='reports.ip1bjs8xtaejnhfrbzj2r6v1fi6hia4i.data']",
        )

        report_app_config.show_data_table = False
        report_module.get_details.reset_cache(report_module)
        self.assertXmlPartialEqual(
            self.get_xml('reports_module_summary_detail_hide_data_table'),
            app.create_suite(),
            "./detail[@id='reports.ip1bjs8xtaejnhfrbzj2r6v1fi6hia4i.summary']",
        )

        report_app_config.show_data_table = True
        report_module.get_details.reset_cache(report_module)
        self.assertXmlPartialEqual(
            self.get_xml('reports_module_data_entry'),
            app.create_suite(),
            "./entry",
        )
        self.assertIn(
            'reports.ip1bjs8xtaejnhfrbzj2r6v1fi6hia4i=CommBugz',
            app.create_app_strings('default'),
        )

        report_app_config.use_xpath_description = False
        report_module.get_details.reset_cache(report_module)
        self.assertXmlPartialEqual(
            self.get_xml('reports_module_summary_detail_use_localized_description'),
            app.create_suite(),
            "./detail[@id='reports.ip1bjs8xtaejnhfrbzj2r6v1fi6hia4i.summary']",
        )

        # Tuple mapping translation formats to the expected output of each
        translation_formats = [
            ({
                'एक': {
                    'en': 'one',
                    'es': 'uno',
                },
                '2': {
                    'en': 'two',
                    'es': 'dos\'',
                    'hin': 'दो',
                },
            }, 'reports_module_data_detail-translated'),
            ({
                'एक': 'one',
                '2': 'two',
            }, 'reports_module_data_detail-translated-simple'),
            ({
                'एक': {
                    'en': 'one',
                    'es': 'uno',
                },
                '2': 'two',
            }, 'reports_module_data_detail-translated-mixed'),
        ]
        for translation_format, expected_output in translation_formats:
            report_app_config._report.columns[0]['transform'] = {
                'type': 'translation',
                'translations': translation_format,
            }
            report_app_config._report = ReportConfiguration.wrap(report_app_config._report._doc)
            report_module.get_details.reset_cache(report_module)
            self.assertXmlPartialEqual(
                self.get_xml(expected_output),
                app.create_suite(),
                "./detail/detail[@id='reports.ip1bjs8xtaejnhfrbzj2r6v1fi6hia4i.data']",
            )

    def test_circular_parent_case_ref(self, *args):
        factory = AppFactory()
        m0, m0f0 = factory.new_basic_module('m0', 'case1')
        m1, m1f0 = factory.new_basic_module('m1', 'case2')
        factory.form_requires_case(m0f0, 'case1', parent_case_type='case2')
        factory.form_requires_case(m1f0, 'case2', parent_case_type='case1')

        with self.assertRaises(SuiteValidationError):
            factory.app.create_suite()

    def test_custom_variables(self, *args):
        factory = AppFactory()
        module, form = factory.new_basic_module('m0', 'case1')
        factory.form_requires_case(form, 'case')
        short_custom_variables = "<variable function='true()' /><foo function='bar'/>"
        long_custom_variables = (
            '<bar function="true()" />'
            '<baz function="instance(\'locations\')/locations/location[0]"/>'
        )
        module.case_details.short.custom_variables = short_custom_variables
        module.case_details.long.custom_variables = long_custom_variables
        suite = factory.app.create_suite()
        self.assertXmlPartialEqual(
            """
            <partial>
                <variables>
                    {short_variables}
                </variables>
                <variables>
                    {long_variables}
                </variables>
            </partial>
            """.format(short_variables=short_custom_variables, long_variables=long_custom_variables),
            suite,
            "detail/variables"
        )
        self.assertXmlPartialEqual(
            """
            <partial>
                <instance id="casedb" src="jr://instance/casedb"/>
                <instance id="locations" src="jr://fixture/locations"/>
            </partial>
            """.format(short_variables=short_custom_variables, long_variables=long_custom_variables),
            suite,
            "entry[1]/instance"
        )
