from django.views.generic import TemplateView, ListView
from django.views.generic.edit import CreateView, UpdateView, DeleteView
from django.urls import reverse_lazy
from django.http import JsonResponse
import json
from django.views.decorators.csrf import csrf_exempt
from .models import Project

class ImageDeIdentificationView(CreateView):
    model = Project
    fields = ['name', 'image_source', 'input_folder', 'output_folder', 'ctp_dicom_filter']
    template_name = 'image_deid.html'
    success_url = reverse_lazy('task_list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['dicom_fields'] = get_dicom_fields()
        context['modalities'] = [
            'MR',
            'CT',
            'US',
            'DX',  # Digital Radiography
            'MG',  # Mammography
            'PT',  # PET
            'NM',  # Nuclear Medicine
            'XA',  # X-Ray Angiography
            'RF',  # Radio Fluoroscopy
            'CR'   # Computed Radiography
        ]
        return context

class TaskListView(ListView):
    model = Project
    template_name = 'task_list.html'
    context_object_name = 'tasks'
    ordering = ['-created_at']

@csrf_exempt
def run_deid(request):
    print('Running deid')
    try:
        if request.method == 'POST':
            data = json.loads(request.body)
        
        project = Project.objects.create(
            name=data['study_name'],
            image_source=data['image_source'],
            input_folder=data['input_folder'],
            output_folder=data['output_folder'],
            status=Project.TaskStatus.PENDING,
            parameters={
                'column_header': data['column_header'],
                'input_file': data['input_file'],
                'general_filters': data['general_filters'],
                'modality_filters': data['modality_filters']
            }
        )
        
        return JsonResponse({
            'status': 'success',
            'project_id': project.id
        })
    except Exception as e:
        print(f'Error: {e}')
        return JsonResponse({'status': 'error', 'message': str(e)}, status=400)

def get_dicom_fields():
    dicom_fields = [
        ('PatientName', 'Patient Name'),
        ('PatientID', 'Patient ID'),
        ('StudyDate', 'Study Date'),
        ('StudyTime', 'Study Time'),
        ('Modality', 'Modality'),
        ('StudyDescription', 'Study Description'),
        ('SeriesDescription', 'Series Description'),
        ('AccessionNumber', 'Accession Number'),
        ('InstitutionName', 'Institution Name'),
    ]
    return dicom_fields