from django.urls import path, re_path

from . import views

urlpatterns = [
    path("run_header_extract/", views.run_header_extract, name="run_header_extract"),
    path("run_deid/", views.run_deid, name="run_deid"),
    path("run_text_deid/", views.run_text_deid, name="run_text_deid"),
    path("run_query/", views.run_query, name="run_query"),
    path("run_export/", views.run_export, name="run_export"),
    path("run_imagedeidexport/", views.run_imagedeidexport, name="run_imagedeidexport"),
    path(
        "run_singleclickicore/",
        views.run_singleclickicore,
        name="run_single_click_icore",
    ),
    path("get_log_content/", views.get_log_content, name="get_log_content"),
    path("api/task_status/<int:project_id>/", views.task_status, name="task_status"),
    path(
        "test_pacs_connection/", views.test_pacs_connection, name="test_pacs_connection"
    ),
    path("save_settings/", views.save_settings, name="save_settings"),
    path("load_settings/", views.load_settings, name="load_settings"),
    path(
        "get_spreadsheet_columns/",
        views.get_spreadsheet_columns,
        name="get_spreadsheet_columns",
    ),
    path("validate_sas_url/", views.validate_sas_url_endpoint, name="validate_sas_url"),
    path("load_admin_settings/", views.load_admin_settings, name="load_admin_settings"),
    path("save_admin_settings/", views.save_admin_settings, name="save_admin_settings"),
    path(
        "get_protocol_settings/<str:protocol_id>/",
        views.get_protocol_settings,
        name="get_protocol_settings",
    ),
    path(
        "verify_admin_password/",
        views.verify_admin_password,
        name="verify_admin_password",
    ),
    path("delete_task/<int:task_id>/", views.delete_task, name="delete_task"),
    path("cancel_task/<int:task_id>/", views.cancel_task, name="cancel_task"),
    path("reset_deid_settings/", views.reset_deid_settings, name="reset_deid_settings"),
    path("api/tasks/", views.api_tasks, name="api_tasks"),
    path("api/constants/", views.api_constants, name="api_constants"),
    path("api/protocols/", views.api_protocols, name="api_protocols"),
    # SPA routes: pages owned by react-router. Listed explicitly (rather than
    # a blanket catch-all) so that slash-less requests to Django-owned URLs
    # still get APPEND_SLASH redirects during the template->React migration.
    # Add each page's path here when its Django URL entry is deleted.
    re_path(
        r"^(?:|tasks/?|task_list/?|task_progress/?|headerextract/?|imageexport/?|textdeid/?|profile/?|imagequery/?|imagedeid/?|imagedeidexport/?|singleclickicore/?|settings/[a-z_]+/?)$",
        views.spa_index,
        name="spa_index",
    ),
]
