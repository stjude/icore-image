from django.urls import path
from . import views

urlpatterns = [
    path('headerquery/', views.HeaderQueryView.as_view(), name='header_query'),
    path('imagedeid/', views.ImageDeIdentificationView.as_view(), name='image_deid'),
    path('imagequery/', views.ImageQueryView.as_view(), name='image_query'),
    path('tasks/', views.TaskListView.as_view(), name='task_list'),
    path('run_header_query/', views.run_header_query, name='run_header_query'),
    path('run_deid/', views.run_deid, name='run_deid'),
    path('run_query/', views.run_query, name='run_query'),
    path('task_progress/', views.TaskProgressView.as_view(), name='task_progress'),
    path('get_log_content/', views.get_log_content, name='get_log_content'),
    path('settings/general/', views.GeneralSettingsView.as_view(), name='general_settings'),
    path('settings/dicom_header_qr/', views.DicomHeaderQRSettingsView.as_view(), name='dicom_header_qr_settings'),
    path('settings/local_header_extraction/', views.LocalHeaderExtractionSettingsView.as_view(), name='local_header_extraction_settings'),
    path('settings/image_qr/', views.ImageQRSettingsView.as_view(), name='image_qr_settings'),
    path('settings/image_deid/', views.ImageDeIdentificationSettingsView.as_view(), name='image_deid_settings'),
    path('settings/report_deid/', views.ReportDeIdentificationSettingsView.as_view(), name='report_deid_settings'),
    path('save_settings/', views.save_settings, name='save_settings'),
    path('load_settings/', views.load_settings, name='load_settings'),
]
