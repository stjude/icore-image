from django.urls import path
from . import views

urlpatterns = [
    path('imagedeid/', views.ImageDeIdentificationView.as_view(), name='image_deid'),
    path('tasks/', views.TaskListView.as_view(), name='task_list'),
    path('run_deid/', views.run_deid, name='run_deid'),
]
