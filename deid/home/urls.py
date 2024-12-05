from django.urls import path
from . import views

urlpatterns = [
    path('imagedeid/', views.ImageDeIdentificationView.as_view(), name='image_deid'),
    path('tasks/', views.TaskListView.as_view(), name='task_list'),
    path('run_deid/', views.run_deid, name='run_deid'),
    path('task_progress/', views.TaskProgressView.as_view(), name='task_progress'),
    path('get_log_content/', views.get_log_content, name='get_log_content'),
    path('settings/', views.SettingsView.as_view(), name='settings'),
    path('save_settings/', views.save_settings, name='save_settings'),
    path('load_settings/', views.load_settings, name='load_settings'),
]
