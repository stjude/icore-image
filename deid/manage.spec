# -*- mode: python ; coding: utf-8 -*-

import os
from PyInstaller.utils.hooks import collect_all, collect_data_files, collect_submodules

block_cipher = None

# Get the current working directory (where the spec file is executed)
project_path = os.getcwd()
target_arch = os.environ.get('PYINSTALLER_TARGET_ARCH', None)

# Collect all dependencies for the project
django_datas = collect_data_files('django')

# Add project-specific data files (static files, templates, database, etc.)
resources_path = os.path.dirname(project_path)  # Go up one level to repo root
project_datas = [
    (os.path.join(project_path, 'config'), 'config'),  # Project configuration directory
    (os.path.join(project_path, 'home'), 'home'),  # Application directory
    (os.path.join(project_path, 'static'), 'static'),  # Static files
    (os.path.join(resources_path, 'resources', 'dictionary.xml'), 'resources'),  # Dictionary files
    (os.path.join(resources_path, 'resources', 'pydicom_ctp_tag_dictionary.xml'), 'resources'),  # CTP tag mapping
]

# External tools used by the in-process celery worker (ctp.py, dcmtk.py,
# pipeline/stages/export.py resolve these under _internal/ when frozen).
for tool_dir in ('jre8', 'ctp', 'dcmtk', 'rclone'):
    tool_path = os.path.join(resources_path, tool_dir)
    if not os.path.exists(tool_path):
        raise Exception("Missing " + tool_path)
    project_datas.append((tool_path, tool_dir))

binaries = []
deid_rs_path = os.path.join(resources_path, 'dicom-deid-rs', 'target', 'release', 'dicom-deid-rs')
if not os.path.exists(deid_rs_path):
    raise Exception("Missing " + deid_rs_path)
binaries.append((deid_rs_path, '.'))

# Include GDAL libraries explicitly
gdal_datas = collect_data_files('django.contrib.gis')

# Collect openpyxl and pandas data files
openpyxl_datas = collect_data_files('openpyxl')
pandas_datas = collect_data_files('pandas')
pynetdicom_datas = collect_data_files('pynetdicom')

# Processing-chain data files (pipeline package: presidio text deid via spacy)
processing_datas = []
for pkg in (
    'pydicom',
    'spacy',
    'en_core_web_sm',
    'presidio_analyzer',
    'presidio_anonymizer',
    'thinc',
):
    processing_datas.extend(collect_data_files(pkg))

import presidio_analyzer  # noqa: E402
recognizer_registry_path = os.path.join(
    os.path.dirname(presidio_analyzer.__file__), 'recognizer_registry'
)
if not os.path.exists(recognizer_registry_path):
    raise Exception("Missing " + recognizer_registry_path)
processing_datas.append(
    (recognizer_registry_path, 'presidio_analyzer/recognizer_registry')
)

a = Analysis(
    [os.path.join(project_path, 'manage.py')],
    # Repo root: lets the analysis resolve the worker's processing imports
    # (pipeline/, utils.py, ctp.py, dcmtk.py) reached from tasks.py.
    pathex=[resources_path],
    binaries=binaries,
    datas=django_datas + project_datas + gdal_datas + openpyxl_datas + pandas_datas
    + pynetdicom_datas + processing_datas,
    hiddenimports=[
        'django.core.management.commands.runserver',  # Include runserver
        'django.core.management.commands.migrate',  # Include migrate
        'openpyxl',  # Explicitly include openpyxl
        'pandas',    # Explicitly include pandas
        'pynetdicom',  # Explicitly include pynetdicom
        # Celery worker entry points, reached only via dynamic imports
        # (CELERY_IMPORTS, autodiscover_tasks); importing 'tasks' pulls in the
        # whole pipeline/ processing chain via its static imports.
        'tasks',
        'config.celery',
        'home.tasks',
        'sqlalchemy.dialects.sqlite',  # kombu sqla broker / db results backend
        # Presidio / spacy stack used by the text deid pipeline stage
        'presidio_analyzer',
        'presidio_analyzer.nlp_engine',
        'presidio_analyzer.entity_recognizer',
        'presidio_analyzer.pattern_recognizer',
        'presidio_anonymizer',
        'presidio_anonymizer.entities',
        'en_core_web_sm',
        'spacy',
        'spacy.cli',
        'spacy.lang.en',
        'spacy.util',
        'spacy.errors',
        'spacy.compat',
        'thinc',
        'thinc.api',
        'thinc.backends',
        'thinc.config',
        'thinc.types',
        'thinc.compat',
        'cymem',
        'murmurhash',
        'preshed',
        'blis',
        'srsly',
        'catalogue',
        'wasabi',
        'typer',
        'pydantic',
        'pydantic_core',
    ]
    + collect_all('django')[1]  # Include all Django modules
    # Celery loads worker components and broker transports by name at runtime
    + collect_submodules('celery')
    + collect_submodules('kombu'),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(
    a.pure, a.zipped_data,
    cipher=block_cipher
)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='manage',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    target_arch=target_arch,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='manage'
)
