from django.views.generic import TemplateView, ListView
from django.views.generic.edit import CreateView, UpdateView, DeleteView
from django.urls import reverse_lazy
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponseNotFound, HttpResponse
from django.views.decorators.http import require_http_methods
import json
import os
from django.views.decorators.csrf import csrf_exempt
from .models import Project, Settings

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
            'DX',
            'MG',
            'PT',
            'NM',
            'XA',
            'RF',
            'CR'
        ]
        return context

class TaskListView(ListView):
    model = Project
    template_name = 'task_list.html'
    context_object_name = 'tasks'
    ordering = ['-created_at']

class SettingsView(TemplateView):
    template_name = 'settings.html'
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['dicom_fields'] = get_dicom_fields()
        context['modalities'] = [
            'MR',
            'CT',
            'US',
            'DX',
            'MG',
            'PT',
            'NM',
            'XA',
            'RF',
            'CR'
        ]
        return context
    
class TaskProgressView(TemplateView):
    template_name = 'task_progress.html'

def get_log_content(request):
    try:
        output_folder = request.GET.get('output_folder')
        if not output_folder:
            return HttpResponseBadRequest("No output folder specified")

        # Construct path to log file
        log_file_path = os.path.join(output_folder, "appdata", 'log.txt')
        
        # Check if file exists
        if not os.path.exists(log_file_path):
            return HttpResponseNotFound("Loading. Please wait...")

        # Read the log file content
        with open(log_file_path, 'r') as f:
            content = f.read()
            
        return HttpResponse(content, content_type='text/plain')

    except Exception as e:
        return HttpResponse(str(e), status=500)

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
                'input_file': data['input_file'],
                'acc_col': data['acc_col'],
                'mrn_col': data['mrn_col'],
                'date_col': data['date_col'],
                'general_filters': data['general_filters'],
                'modality_filters': data['modality_filters'],
                'tags_to_keep': data['tags_to_keep'],
                'tags_to_dateshift': data['tags_to_dateshift'],
                'tags_to_randomize': data['tags_to_randomize'],
                'date_shift_days': data['date_shift_days'],
            }
        )
        
        return JsonResponse({
            'status': 'success',
            'project_id': project.id
        })
    except Exception as e:
        print(f'Error: {e}')
        return JsonResponse({'status': 'error', 'message': str(e)}, status=400)
    
@require_http_methods(['GET'])
def get_settings(request):
    settings = Settings.objects.first()
    if not settings:
        return JsonResponse({
            'default_image_source': 'LOCAL',
            'default_tags_to_keep': '',
            'default_tags_to_dateshift': '',
            'default_tags_to_randomize': '',
            'default_date_shift_days': 30,
            'id_generation_method': 'UNIQUE',
            'general_filters': [],
            'modality_filters': {}
        })
    return JsonResponse({
        'default_image_source': settings.default_image_source,
        'default_tags_to_keep': settings.default_tags_to_keep,
        'default_tags_to_dateshift': settings.default_tags_to_dateshift,
        'default_tags_to_randomize': settings.default_tags_to_randomize,
        'default_date_shift_days': settings.default_date_shift_days,
        'id_generation_method': settings.id_generation_method,
        'general_filters': settings.general_filters,
        'modality_filters': settings.modality_filters
    })

@require_http_methods(["POST"])
def save_settings(request):
    data = json.loads(request.body)
    settings = Settings.objects.first()  # or filter by user if implementing per-user settings
    if not settings:
        settings = Settings()
    
    settings.default_image_source = data.get('default_image_source', 'LOCAL')
    settings.default_tags_to_keep = data.get('default_tags_to_keep', '')
    settings.default_tags_to_dateshift = data.get('default_tags_to_dateshift', '')
    settings.default_tags_to_randomize = data.get('default_tags_to_randomize', '')
    settings.default_date_shift_days = data.get('default_date_shift_days')
    settings.id_generation_method = data.get('id_generation_method', 'UNIQUE')
    settings.general_filters = data.get('general_filters', [])
    settings.modality_filters = data.get('modality_filters', {})
    
    settings.save()
    return JsonResponse({'status': 'success'})

def get_dicom_fields():
    dicom_fields = [
        ('BurnedInAnnotation', "Burned In Annotation"),
        ('SOPClasssUID', 'SOP Class UID'),
        ('Manufacturer', 'Manufacturer'),
        ('ImageType', 'Image Type'),
        ('InstanceNumber', 'Instance Number'),
        ('Rows', 'Rows'),
        ('Columns', 'Columns'),
        ('PixelSpacing', 'Pixel Spacing'),
        ('SliceThickness', 'Slice Thickness'),
        ('NumberOfFrames', 'Number of Frames'),
        ('ReferencedPresentationStateSequence', 'Referenced Presentation State Sequence'),
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