# Generated by Django 3.2.16 on 2022-11-23 23:33

from django.db import migrations
from django.db.models import Q

from corehq.apps.users.permissions import EXPORT_PERMISSIONS
from corehq.apps.users.models_role import Permission, UserRole
from corehq.toggles import DATA_DICTIONARY, DATA_FILE_DOWNLOAD
from corehq.util.django_migrations import skip_on_fresh_install


def build_role_can_edit_commcare_data_q_object():
    edit_data_permission, created = Permission.objects.get_or_create(value='edit_data')
    return Q(rolepermission__permission_fk_id=edit_data_permission.id)


def build_role_can_export_data_q_object():
    view_reports_permission, created = Permission.objects.get_or_create(value='view_reports')
    can_view_commcare_reports = Q(rolepermission__permission_fk_id=view_reports_permission.id)

    can_view_commcare_export_reports = Q()
    for export_permission in EXPORT_PERMISSIONS:
        can_view_commcare_export_reports.add(Q(rolepermission__allowed_items__contains=[export_permission]), Q.OR)

    return can_view_commcare_reports | can_view_commcare_export_reports


def build_role_can_download_data_files_q_object():
    data_file_download_domains = DATA_FILE_DOWNLOAD.get_enabled_domains()
    view_file_dropzone_permission, created = Permission.objects.get_or_create(value='view_file_dropzone')
    edit_file_dropzone_permission, created = Permission.objects.get_or_create(value='edit_file_dropzone')

    data_file_download_feat_flag_on = Q(domain__in=data_file_download_domains)
    can_view_file_dropzone = Q(rolepermission__permission_fk_id=view_file_dropzone_permission.id)
    can_edit_file_dropzone = Q(rolepermission__permission_fk_id=edit_file_dropzone_permission.id)

    return (data_file_download_feat_flag_on & (can_view_file_dropzone | can_edit_file_dropzone))


def role_can_view_data_tab():
    can_edit_commcare_data = build_role_can_edit_commcare_data_q_object()
    can_export_data = build_role_can_export_data_q_object()
    can_download_data_files = build_role_can_download_data_files_q_object()

    return (can_edit_commcare_data | can_export_data | can_download_data_files)


@skip_on_fresh_install
def add_data_dict_permissions(apps, schema_editor):
    view_data_dict_permission, created = Permission.objects.get_or_create(value='view_data_dict')
    edit_data_dict_permission, created = Permission.objects.get_or_create(value='edit_data_dict')

    data_dict_domains = DATA_DICTIONARY.get_enabled_domains()
    for domain in data_dict_domains:
        user_roles = (UserRole.objects
            .filter(domain=domain)
            .filter(role_can_view_data_tab())
            .distinct()
        )
        for role in user_roles:
            role.rolepermission_set.get_or_create(permission_fk=view_data_dict_permission, defaults={"allow_all": True})
            role.rolepermission_set.get_or_create(permission_fk=edit_data_dict_permission, defaults={"allow_all": True})


class Migration(migrations.Migration):

    dependencies = [
        ('users', '0047_rename_sqlpermission_permission'),
    ]

    operations = [
        migrations.RunPython(add_data_dict_permissions, migrations.RunPython.noop)
    ]
