from django.urls import path
from . import views

urlpatterns = [
    path('imagedeid/', views.ImageDeIdentificationView.as_view(), name='image_deid'),
    path('tasks/', views.TaskListView.as_view(), name='task_list'),
    path('run_deid/', views.run_deid, name='run_deid'),
    path('settings/', views.SettingsView.as_view(), name='settings'),
    path('get_settings/', views.get_settings, name='get_settings'),
    path('save_settings/', views.save_settings, name='save_settings'),
]
