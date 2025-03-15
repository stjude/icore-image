import json
import os
import shutil
import time
import traceback
from datetime import datetime
from typing import Optional

import bcrypt
import pandas as pd
import pytz
from django.db import OperationalError
from django.http import (
    HttpResponse,
    HttpResponseBadRequest,
    HttpResponseNotFound,
    JsonResponse,
)
from django.shortcuts import get_object_or_404
from django.urls import reverse_lazy
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.views.generic import ListView, TemplateView
from django.views.generic.edit import CreateView
from home.constants import RCLONE_CONFIG_PATH, SETTINGS_DIR_PATH, SETTINGS_FILE_PATH
from home.license_management import LICENSE_MANAGER, LicenseValidationError
from home.models import Project


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
        if hasattr(self, "module"):
            context['invalid_license'] = self.check_license()

        return context

    def check_license(self) -> str:
        if self.module in LICENSE_MANAGER.paid_modules:
            try:
                LICENSE_MANAGER.module_license_is_valid(self.module)
            except LicenseValidationError as e:
                return str(e)
        return ""

class ImageDeIdentificationView(CommonContextMixin, CreateView):
    model = Project
    module = Project.TaskType.IMAGE_DEID
    fields = ['name', 'image_source', 'input_folder', 'output_folder', 'ctp_dicom_filter']
    template_name = 'image_deid.html'
    success_url = reverse_lazy('task_list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['protocols'] = get_unique_protocols()
        print(context['protocols'])
        return context

class ImageQueryView(CommonContextMixin, CreateView):
    model = Project
    module = Project.TaskType.IMAGE_QUERY
    fields = ['name', 'image_source', 'output_folder', 'ctp_dicom_filter']
    template_name = 'image_query.html'
    success_url = reverse_lazy('task_list')

class HeaderQueryView(CommonContextMixin, CreateView):
    model = Project
    module = Project.TaskType.HEADER_QUERY
    fields = ['name', 'image_source', 'output_folder', 'ctp_dicom_filter']
    template_name = 'header_query.html'
    success_url = reverse_lazy('task_list')

class TextDeIdentificationView(CommonContextMixin, CreateView):
    model = Project
    module = Project.TaskType.TEXT_DEID
    fields = ['name', 'output_folder']
    template_name = 'text_deid.html'
    success_url = reverse_lazy('task_list')

class ImageExportView(CommonContextMixin, CreateView):
    model = Project
    fields = ['name', 'input_folder']
    template_name = 'image_export.html'
    success_url = reverse_lazy('task_list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['storage_locations'] = load_rclone_config()
        return context

class TextExtractView(CommonContextMixin, CreateView):
    model = Project
    module = Project.TaskType.TEXT_EXTRACT
    fields = ['name', 'output_folder']
    template_name = 'text_extract.html'
    success_url = reverse_lazy('task_list')

class TaskListView(ListView):
    model = Project
    template_name = 'task_list.html'
    context_object_name = 'tasks'
    ordering = ['-created_at']

class GeneralSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/general.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        american_timezones = [tz for tz in pytz.all_timezones if tz.startswith("US/")]
        context['timezones'] = american_timezones
        return context

class DicomHeaderQRSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/header_query.html'

class LocalHeaderExtractionSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/header_extraction.html'

class ImageQRSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/image_query.html'

class ImageDeIdentificationSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/image_deid.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['protocols'] = get_unique_protocols()
        print(context['protocols'])
        return context

class ReportDeIdentificationSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/text_deid.html'

class AdminSettingsView(CommonContextMixin, TemplateView):
    template_name = 'settings/admin_settings.html'

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

def settings_as_dict(raise_exception: Optional[bool] = False) -> dict:
    """
    Get the current settings from the settings.json file.

    Args:
        raise_exception: If True, raise an exception if the file is not found.
            If False, return an empty dictionary.
    """
    try:
        # Load existing settings
        with open(SETTINGS_FILE_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        if not raise_exception:
            return {}
        raise

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
            application_aet=data['application_aet'],
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
    max_attempts = 3
    attempt = 0
    
    while attempt < max_attempts:
        try:
            if request.method == 'POST':
                data = json.loads(request.body)
            
            scheduled_time = None
            if 'scheduled_time' in data:
                settings = settings_as_dict()
                timezone = settings.get('timezone', 'UTC')
                timezone = pytz.timezone(timezone)
                local_dt = datetime.fromisoformat(data['scheduled_time'].replace('Z', ''))
                scheduled_time = timezone.localize(local_dt)
                scheduled_time = scheduled_time.astimezone(pytz.UTC)
            
            project = Project.objects.create(
                name=data['study_name'],
                task_type=Project.TaskType.IMAGE_DEID,
                image_source=data['image_source'],
                input_folder=data['input_folder'],
                output_folder=data['output_folder'],
                pacs_ip=data['pacs_ip'],
                pacs_port=data['pacs_port'],
                pacs_aet=data['pacs_aet'],
                application_aet=data['application_aet'],
                status=Project.TaskStatus.PENDING,
                scheduled_time=scheduled_time,
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
                    'lookup_file': data['lookup_file'],
                    'use_lookup_table': data['use_lookup'],
                }
            )
            
            return JsonResponse({
                'status': 'success',
                'project_id': project.id
            })
            
        except OperationalError as e:
            if "database is locked" in str(e):
                attempt += 1
                if attempt == max_attempts:
                    print(f'Failed after {max_attempts} attempts: {e}')
                    return JsonResponse({
                        'status': 'error',
                        'message': f'Database is locked. Please try again. ({str(e)})'
                    }, status=503)  # 503 Service Unavailable
                print(f'Database locked, attempt {attempt} of {max_attempts}. Waiting...')
                time.sleep(0.5 * attempt)  # Increasing backoff
            else:
                raise
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
            application_aet=data['application_aet'],
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
def run_text_deid(request):
    try:
        if request.method == 'POST':
            data = json.loads(request.body)

        project = Project.objects.create(
            name=data['study_name'],
            output_folder=data['output_folder'],
            task_type=Project.TaskType.TEXT_DEID,
            status=Project.TaskStatus.PENDING,
            parameters={
                'input_file': data['input_file'],
                'text_to_keep': data['text_to_keep'],
                'text_to_remove': data['text_to_remove'],
                'date_shift_days': data['date_shift_days']
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
def run_export(request):
    print('Running export')
    try:
        if request.method == 'POST':
            data = json.loads(request.body)
        project = Project.objects.create(
            name=data['study_name'],
            task_type=Project.TaskType.IMAGE_EXPORT,
            input_folder=data['input_folder'],
            output_folder=data['output_folder'],
            status=Project.TaskStatus.PENDING,
            parameters={
                'storage_location': data['storage_location'],
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
def run_text_extract(request):
    try:
        if request.method == 'POST':
            data = json.loads(request.body)

        project = Project.objects.create(
            name=data['study_name'],
            input_folder=data['input_folder'],
            status=Project.TaskStatus.PENDING,
            output_folder=data['output_folder'],
            task_type=Project.TaskType.TEXT_EXTRACT,
            parameters={},
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
        if request.FILES:
            new_settings = json.loads(request.POST.get('data'))
            lookup_file = request.FILES['lookup_file']
            file_path = os.path.join(SETTINGS_DIR_PATH, 'lookup_table.xlsx')
            with open(file_path, 'wb+') as destination:
                for chunk in lookup_file.chunks():
                    destination.write(chunk)
            new_settings['lookup_file'] = file_path
        else:
            new_settings = json.loads(request.body.decode('utf-8'))
        
        existing_settings = settings_as_dict()
        existing_settings.update(new_settings)
        
        if 'timezone' in new_settings:
            request.session['django_timezone'] = new_settings['timezone']
            
        with open(SETTINGS_FILE_PATH, 'w') as f:
            json.dump(existing_settings, f, indent=4)
            
        return JsonResponse({'status': 'success'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=400)

@require_http_methods(["GET"])
def load_settings(request):
    try:
        settings = settings_as_dict(raise_exception=True)
        if settings.get('lookup_file'):
            settings['lookup_file'] = os.path.abspath(settings['lookup_file'])
        return JsonResponse(settings)
    except FileNotFoundError:
        return JsonResponse({})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=400)

def load_rclone_config():
    with open(RCLONE_CONFIG_PATH, 'r') as f:
        rclone_config = f.read()
    rclone_config_names = []
    for line in rclone_config.splitlines():
        if line.startswith('[') and line.endswith(']'):
            rclone_config_names.append(line[1:-1])
    return rclone_config_names

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

def test_pacs_connection(request):
    data = json.loads(request.body)
    pacs_ip = data.get('pacs_ip')
    pacs_port = data.get('pacs_port')
    pacs_aet = data.get('pacs_aet')
    application_aet = data.get('application_aet')

    if application_aet is None or application_aet == "":
        return JsonResponse({'status': 'error', 'error': 'Application AET is required'})

    # Run C-ECHO command using pynetdicom
    from pynetdicom import AE, sop_class

    ae = AE(ae_title=application_aet)
    ae.add_requested_context(sop_class.Verification)
    pacs_port = int(pacs_port)
    try:
        assoc = ae.associate(pacs_ip, pacs_port, ae_title=pacs_aet)
        if assoc.is_established:
            status = assoc.send_c_echo()
            assoc.release()
            print(status)
            if status.Status == 0:
                return JsonResponse({'status': 'success'})
            else:
                return JsonResponse({'status': 'error', 'error': 'C-ECHO failed'})
        else:
            return JsonResponse({'status': 'error', 'error': 'Association rejected'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'error': str(e)})

@require_http_methods(["GET"])
def load_admin_settings(request):
    try:
        settings = settings_as_dict()

        protocol_path = os.path.join(SETTINGS_DIR_PATH, 'protocol.xlsx')
        if os.path.exists(protocol_path):
            settings['protocol_file'] = os.path.abspath(protocol_path)
        
        if settings.get('date_shift_range'):
            settings['date_shift_range'] = int(settings['date_shift_range'])
        
        if os.path.exists(RCLONE_CONFIG_PATH):
            with open(RCLONE_CONFIG_PATH, 'r') as f:
                settings['rclone_config'] = f.read()
        
        return JsonResponse(settings)
    except Exception as e:
        print(f"Error loading admin settings: {str(e)}")
        return JsonResponse({'error': str(e)}, status=500)

@require_http_methods(["POST"])
def save_admin_settings(request):
    os.makedirs(SETTINGS_DIR_PATH, exist_ok=True)
    existing_settings = settings_as_dict()

    with open(RCLONE_CONFIG_PATH, 'w') as f:
        f.write(request.POST.get('rclone_config'))
    
    # Handle protocol file upload
    if request.FILES.get('protocol_file'):
        protocol_file = request.FILES['protocol_file']
        file_path = os.path.join(SETTINGS_DIR_PATH, 'protocol.xlsx')
        with open(file_path, 'wb+') as destination:
            for chunk in protocol_file.chunks():
                destination.write(chunk)
        
        existing_settings['protocol_file'] = os.path.abspath(file_path)

    # Handle license file upload
    if request.FILES.get('license_file'):
        license_file = request.FILES['license_file']

        with license_file.open('rb') as f:
            license_dict = json.load(f)

        try:
            LICENSE_MANAGER.add_license(
                LICENSE_MANAGER.validate_license(license_dict)
            )
        except Exception as e:
            traceback.print_exc()
            error_message = str(e) if isinstance(
                e, LicenseValidationError
                ) else "License could not be validated."
            return JsonResponse({'error': error_message}, status=400)
    
    # Handle other form data
    if request.POST.get('default_date_shift_days'):
        existing_settings['date_shift_range'] = request.POST['default_date_shift_days']
    
    # Save updated settings
    with open(SETTINGS_FILE_PATH, 'w') as f:
        json.dump(existing_settings, f, indent=4)
    
    return JsonResponse({'status': 'success'})

def get_protocol_settings(request, protocol_id):
    try:
        protocol_path = os.path.join(SETTINGS_DIR_PATH, 'protocol.xlsx')
        
        if not os.path.exists(protocol_path):
            return JsonResponse({'error': 'Protocol file not found'}, status=404)
            
        # Read Excel file
        df = pd.read_excel(protocol_path)
        
        # Convert protocol_id to integer if needed
        try:
            protocol_id = int(protocol_id)
        except ValueError:
            pass  # Keep as string if conversion fails
        
        # Find the row for this protocol
        matching_rows = df[df['Protocol ID'] == protocol_id]
        matching_rows = matching_rows.sort_values('Version', ascending=False)
        print(matching_rows)
        if matching_rows.empty:
                return JsonResponse({'error': f'Protocol ID {protocol_id} not found'}, status=404)
        
        protocol_row = matching_rows.iloc[0]
        
        filters = load_filters_from_protocol(protocol_row)
        # Convert numpy types to Python native types
        protocol_settings = {
            'tags_to_keep': protocol_row.get('Deid Whitelist', ''),
            'tags_to_dateshift': protocol_row.get('Deid Date Shift List', ''),
            'tags_to_randomize': protocol_row.get('Deid Randomize List', ''),
            # 'date_shift_days': protocol_row.get('Deid Date Shift', ''),
            'is_restricted': bool(protocol_row.get('Restricted')),
            'filters': filters
        }
        
        return JsonResponse({'protocol_settings': protocol_settings})
    except Exception as e:
        print(f"Error getting protocol settings: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return JsonResponse({'error': str(e)}, status=500)

def get_unique_protocols():
    try:
        protocol_path = os.path.join(SETTINGS_DIR_PATH, 'protocol.xlsx')
        
        if not os.path.exists(protocol_path):
            print(f"Protocol file not found at {protocol_path}")
            return []
            
        # Read Excel file
        df = pd.read_excel(protocol_path)
        
        # Get unique protocol IDs, assuming the column is named 'Protocol_ID'
        # Adjust the column name if it's different in your Excel file
        unique_protocols = df['Protocol ID'].unique().tolist()
        
        return unique_protocols
    except Exception as e:
        print(f"Error reading protocol file: {str(e)}")
        return []

def load_filters_from_protocol(protocol_row):
    # Handle general filters
    general_filters = []
    general_filter_str = protocol_row.get('General Filters', '')
    if general_filter_str:
        for line in general_filter_str.splitlines():
            if line.strip():
                tag, action, value = [x.strip() for x in line.split(',')]
                general_filters.append({
                    'tag': tag,
                    'action': generate_action_string(action), 
                    'value': value
                })
                print(generate_action_string(action))

    # Handle modality-specific filters
    modality_filters = {}
    for modality in ['MR', 'CT', 'US', 'DX', 'MG', 'PT', 'NM', 'XA', 'RF', 'CR']:
        modality_filter_str = protocol_row.get(f'{modality} Filters', '')
        if modality_filter_str:
            filters = []
            for line in modality_filter_str.splitlines():
                if line.strip():
                    tag, action, value = [x.strip() for x in line.split(',')]
                    filters.append({
                        'tag': tag,
                        'action': generate_action_string(action),
                        'value': value
                    })
            if filters:
                modality_filters[modality] = filters

    return {
        'general_filters': general_filters,
        'modality_filters': modality_filters
    }

def generate_action_string(action):
    if action == 'DoesNotContain':
        action = 'not_containsIgnoreCase'
    elif action == 'Contains':
        action = 'containsIgnoreCase'
    elif action == 'StartsWith':
        action = 'startsWithIgnoreCase'
    elif action == 'DoesNotEndWith':
        action = 'not_endsWithIgnoreCase'
    elif action == 'EndsWith':
        action = 'endsWithIgnoreCase'
    elif action == 'DoesNotEqual':
        action = 'not_equalsIgnoreCase'
    elif action == 'Equals':
        action = 'equalsIgnoreCase'
    return action

def get_password_file_path():
    os.makedirs(SETTINGS_DIR_PATH, exist_ok=True)
    return os.path.join(SETTINGS_DIR_PATH, 'admin_password.txt')

def set_admin_password(password):
    """Hash and store the admin password"""
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    with open(get_password_file_path(), 'wb') as f:
        f.write(hashed)

def check_admin_password(password):
    """Check if the provided password matches the stored hash"""
    try:
        with open(get_password_file_path(), 'rb') as f:
            stored_hash = f.read()
        return bcrypt.checkpw(password.encode('utf-8'), stored_hash)
    except FileNotFoundError:
        return False

@require_http_methods(["POST"])
def verify_admin_password(request):
    try:
        data = json.loads(request.body)
        password = data.get('password', '')
        is_valid = check_admin_password(password)
        return JsonResponse({'valid': is_valid})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

def initialize_admin_password():
    """Initialize admin password file by copying from source"""
    try:
        # Get source and destination paths
        default_password = 'password'
        dest_path = get_password_file_path()

        # Only copy if destination doesn't exist
        if not os.path.exists(dest_path):
            # Create settings directory if needed
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            # Copy the password file
            with open(dest_path, 'wb') as dst:
                dst.write(bcrypt.hashpw(default_password.encode('utf-8'), bcrypt.gensalt()))
    except Exception as e:
        print(f"Error initializing admin password: {str(e)}")

initialize_admin_password()

@require_http_methods(["POST"])
def delete_task(request, task_id):
    try:
        task = get_object_or_404(Project, id=task_id)
        
        # Optionally, delete associated files
        if os.path.exists(task.output_folder):
            shutil.rmtree(task.output_folder)
        task.delete()
            
        return JsonResponse({'status': 'success'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=400)

# Add this middleware function to process timezone from session
def timezone_middleware(get_response):
    def middleware(request):
        tzname = request.session.get('django_timezone')
        if tzname:
            timezone.activate(pytz.timezone(tzname))
        else:
            timezone.deactivate()
        return get_response(request)
    return middleware
