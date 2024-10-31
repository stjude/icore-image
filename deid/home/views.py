from django.views.generic import TemplateView, ListView
from django.views.generic.edit import CreateView, UpdateView, DeleteView
from django.urls import reverse_lazy
from django.http import JsonResponse
import json
from django.views.decorators.csrf import csrf_exempt
from .models import Project, DeidentificationTask

class ImageDeIdentificationView(CreateView):
    model = Project
    fields = ['input_folder', 'output_folder']
    template_name = 'image_deid.html'
    success_url = reverse_lazy('task_list')

class TaskListView(ListView):
    model = DeidentificationTask
    template_name = 'task_list.html'
    context_object_name = 'tasks'
    ordering = ['-created_at']

@csrf_exempt
def run_deid(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        
        task = DeidentificationTask.objects.create(
            status=DeidentificationTask.TaskStatus.PENDING,
            parameters=data
        )
        return JsonResponse({'status': 'success'})
    return JsonResponse({'status': 'error'}, status=400)

