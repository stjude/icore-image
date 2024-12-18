from django.views.generic import TemplateView, ListView
from django.views.generic.edit import CreateView, UpdateView, DeleteView
from django.urls import reverse_lazy
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponseNotFound, HttpResponse
from django.views.decorators.http import require_http_methods
import json
import os
from django.views.decorators.csrf import csrf_exempt
from .models import Project

# Create a mixin for common context data
class CommonContextMixin:
    def get_common_context(self):
        return {
            'dicom_fields': get_dicom_fields(),
            'modalities': [
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
        }
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(self.get_common_context())
        return context

class ImageDeIdentificationView(CommonContextMixin, CreateView):
    model = Project
    fields = ['name', 'image_source', 'input_folder', 'output_folder', 'ctp_dicom_filter']
    template_name = 'image_deid.html'
    success_url = reverse_lazy('task_list')

class ImageQueryView(CommonContextMixin, CreateView):
    model = Project
    fields = ['name', 'image_source', 'output_folder', 'ctp_dicom_filter']
    template_name = 'image_query.html'
    success_url = reverse_lazy('task_list')

class HeaderQueryView(CommonContextMixin, CreateView):
    model = Project
    fields = ['name', 'image_source', 'output_folder', 'ctp_dicom_filter']
    template_name = 'header_query.html'
    success_url = reverse_lazy('task_list')

class TaskListView(ListView):
    model = Project
    template_name = 'task_list.html'
    context_object_name = 'tasks'
    ordering = ['-created_at']

class GeneralSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/general.html'

class DicomHeaderQRSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/header_query.html'

class LocalHeaderExtractionSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/header_extraction.html'

class ImageQRSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/image_query.html'

class ImageDeIdentificationSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/image_deid.html'

class ReportDeIdentificationSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/image_deid.html'

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
def run_header_query(request):
    print('Running header query')
    try:
        if request.method == 'POST':
            data = json.loads(request.body)
        project = Project.objects.create(
            name=data['study_name'],
            task_type=Project.TaskType.HEADER_QUERY,
            output_folder=data['output_folder'],
            pacs_ip=data['pacs_ip'],
            pacs_port=data['pacs_port'],
            pacs_aet=data['pacs_aet'],
            status=Project.TaskStatus.PENDING,
            parameters={
                'input_file': data['input_file'],
                'acc_col': data['acc_col'],
                'mrn_col': data['mrn_col'],
                'date_col': data['date_col'],
                'general_filters': data['general_filters'],
                'modality_filters': data['modality_filters'],
            }
        )
        return JsonResponse({
            'status': 'success',
            'project_id': project.id
        })
    except Exception as e:
        print(f'Error: {e}')
        return JsonResponse({'status': 'error', 'message': str(e)}, status=400)

@csrf_exempt
def run_deid(request):
    print('Running deid')
    try:
        if request.method == 'POST':
            data = json.loads(request.body)
        
        project = Project.objects.create(
            name=data['study_name'],
            task_type=Project.TaskType.IMAGE_DEID,
            image_source=data['image_source'],
            input_folder=data['input_folder'],
            output_folder=data['output_folder'],
            pacs_ip=data['pacs_ip'],
            pacs_port=data['pacs_port'],
            pacs_aet=data['pacs_aet'],
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

@csrf_exempt
def run_query(request):
    print('Running query')
    try:
        if request.method == 'POST':
            data = json.loads(request.body)

        project = Project.objects.create(
            name=data['study_name'],
            task_type=Project.TaskType.IMAGE_QUERY,
            output_folder=data['output_folder'],
            pacs_ip=data['pacs_ip'],
            pacs_port=data['pacs_port'],
            pacs_aet=data['pacs_aet'],
            status=Project.TaskStatus.PENDING,
            parameters={
                'input_file': data['input_file'],
                'acc_col': data['acc_col'],
                'mrn_col': data['mrn_col'],
                'date_col': data['date_col'],
                'general_filters': data['general_filters'],
                'modality_filters': data['modality_filters'],
            }
        )

        return JsonResponse({
            'status': 'success',
            'project_id': project.id
        })
    except Exception as e:
        print(f'Error: {e}')
        return JsonResponse({'status': 'error', 'message': str(e)}, status=400)

@require_http_methods(["POST"])
def save_settings(request):
    try:
        new_settings = json.loads(request.body)
        settings_path = os.path.join(os.path.expanduser('~'), '.aiminer', 'settings.json')
        
        try:
            with open(settings_path, 'r') as f:
                existing_settings = json.load(f)
        except FileNotFoundError:
            existing_settings = {}
            
        # Only update fields that are in the new settings
        existing_settings.update(new_settings)
        
        with open(settings_path, 'w') as f:
            json.dump(existing_settings, f, indent=4)
            
        return JsonResponse({'status': 'success'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=400)

@require_http_methods(["GET"])
def load_settings(request):
    try:
        settings_path = os.path.join(os.path.expanduser('~'), '.aiminer', 'settings.json')
        with open(settings_path, 'r') as f:
            settings = json.load(f)
        return JsonResponse(settings)
    except FileNotFoundError:
        return JsonResponse({})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=400)

def get_dicom_fields():
    dicom_fields = [
        ('BurnedInAnnotation', "Burned In Annotation"),
        ('SOPClassUID', 'SOP Class UID'),
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
        ('StudyDescription', 'Study Description'),
        ('SeriesDescription', 'Series Description'),
        ('AccessionNumber', 'Accession Number'),
        ('InstitutionName', 'Institution Name'),
        ('PatientBirthDate', 'Patient Birth Date'),
        ('PatientSex', 'Patient Sex'),
        ('PatientAge', 'Patient Age'),
        ('PatientAddress', 'Patient Address'),
        ('StudyInstanceUID', 'Study Instance UID'),
        ('SeriesInstanceUID', 'Series Instance UID'),
        ('FrameOfReferenceUID', 'Frame Of Reference UID'),
        ('PerformingPhysicianName', 'Performing Physician Name'),
        ('ReferringPhysicianName', 'Referring Physician Name'),
        ('StationName', 'Station Name'),
        ('DeviceSerialNumber', 'Device Serial Number'),
        ('SoftwareVersions', 'Software Versions'),
        ('ProtocolName', 'Protocol Name'),
        ('BodyPartExamined', 'Body Part Examined'),
        ('ReconstructionDiameter', 'Reconstruction Diameter'),
        ('ImagePositionPatient', 'Image Position (Patient)'),
        ('ImageOrientationPatient', 'Image Orientation (Patient)'),
        ('AcquisitionDate', 'Acquisition Date'),
        ('AcquisitionTime', 'Acquisition Time'),
        ('AcquisitionNumber', 'Acquisition Number'),
        ('ContentDate', 'Content Date'),
        ('ContentTime', 'Content Time'),
        ('MediaStorageSOPInstanceUID', 'Media Storage SOP Instance UID'),
        ('SpecificCharacterSet', 'Specific Character Set'),
        ('ManufacturerModelName', 'Manufacturer Model Name'),
        ('ExposureTime', 'Exposure Time'),
        ('ExposureDoseSequence', 'Exposure Dose Sequence'),
        ('ClinicalTrialSubjectID', 'Clinical Trial Subject ID'),
        ('ClinicalTrialProtocolName', 'Clinical Trial Protocol Name'),
        ('DeidentificationMethod', 'Deidentification Method'),
        ('StudyID', 'Study ID'),
        ('SeriesNumber', 'Series Number'),
        ('PatientWeight', 'Patient Weight'),
        ('PatientSize', 'Patient Size'),
        ('OperatorName', 'Operator Name'),
        ('SliceLocation', 'Slice Location'),
        ('NumberOfSlices', 'Number of Slices'),
        ('GantryDetectorTilt', 'Gantry Detector Tilt'),
        ('KVP', 'Kilovoltage Peak'),
        ('ReconstructionAlgorithm', 'Reconstruction Algorithm'),
        ('ScanOptions', 'Scan Options'),
        ('AcquisitionMatrix', 'Acquisition Matrix'),
        ('FieldOfViewDimensions', 'Field Of View Dimensions'),
        ('SpacingBetweenSlices', 'Spacing Between Slices'),
        ('PhotometricInterpretation', 'Photometric Interpretation'),
        ('TransferSyntaxUID', 'Transfer Syntax UID'),
        ('HighBit', 'High Bit'),
        ('BitsAllocated', 'Bits Allocated'),
        ('BitsStored', 'Bits Stored'),
        ('WindowCenter', 'Window Center'),
        ('WindowWidth', 'Window Width'),
        ('PixelAspectRatio', 'Pixel Aspect Ratio'),
        ('SmallestImagePixelValue', 'Smallest Image Pixel Value'),
        ('LargestImagePixelValue', 'Largest Image Pixel Value'),
        ('ContrastBolusAgent', 'Contrast Bolus Agent'),
        ('ContrastBolusVolume', 'Contrast Bolus Volume'),
        ('FilterType', 'Filter Type'),
        ('PatientPosition', 'Patient Position'),
        ('ImageLaterality', 'Image Laterality'),
        ('StudyComments', 'Study Comments'),
        ('ImageComments', 'Image Comments'),
        ('RadiationSetting', 'Radiation Setting'),
        ('AcquisitionProtocolDescription', 'Acquisition Protocol Description'),
        ('ProcedureStepDescription', 'Procedure Step Description'),
        ('PixelRepresentation', 'Pixel Representation'),
        ('FrameIncrementPointer', 'Frame Increment Pointer'),
        ('MultiPlanarReconstruction', 'Multi Planar Reconstruction'),
        ('FrameTimeVector', 'Frame Time Vector'),
        ('LossyImageCompression', 'Lossy Image Compression'),
        ('CompressionRatio', 'Compression Ratio'),
        ('SliceProgressionDirection', 'Slice Progression Direction'),
        ('ExposureIndex', 'Exposure Index'),
        ('RelativeXRayExposure', 'Relative X-Ray Exposure'),
        ('CollimatorShape', 'Collimator Shape'),
        ('FocalSpot', 'Focal Spot'),
        ('PixelData', 'Pixel Data'),
        ('StudyStatus', 'Study Status'),
        ('SeriesStatus', 'Series Status'),
        ('DetectorType', 'Detector Type'),
        ('IrradiationEventUID', 'Irradiation Event UID'),
        ('Modality', 'Modality'),
    ]
    return dicom_fields