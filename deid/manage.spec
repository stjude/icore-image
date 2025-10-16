# -*- mode: python ; coding: utf-8 -*-

import os
from PyInstaller.utils.hooks import collect_all, collect_data_files

block_cipher = None

# Get the current working directory (where the spec file is executed)
project_path = os.getcwd()

# Collect all dependencies for the project
django_datas = collect_data_files('django')

# Add project-specific data files (static files, templates, database, etc.)
project_datas = [
    (os.path.join(project_path, 'config'), 'config'),  # Project configuration directory
    (os.path.join(project_path, 'home'), 'home'),  # Application directory
    (os.path.join(project_path, 'templates'), 'templates'),  # Templates
    (os.path.join(project_path, 'static'), 'static'),  # Static files
    # (os.path.join(project_path, 'db.sqlite3'), '.'),  # SQLite database
    (os.path.join(project_path, 'dictionary.xml'), '.'),  # Other data files
]

# Include GDAL libraries explicitly
gdal_datas = collect_data_files('django.contrib.gis')

# Collect openpyxl and pandas data files
openpyxl_datas = collect_data_files('openpyxl')
pandas_datas = collect_data_files('pandas')
# psycopg2_datas = collect_data_files('psycopg2')
pynetdicom_datas = collect_data_files('pynetdicom')

a = Analysis(
    [os.path.join(project_path, 'manage.py')],
    pathex=[],
    binaries=[],
    datas=django_datas + project_datas + gdal_datas + openpyxl_datas + pandas_datas,
    hiddenimports=[
        'django.core.management.commands.runserver',  # Include runserver
        'django.core.management.commands.migrate',  # Include migrate
        'openpyxl',  # Explicitly include openpyxl
        'pandas',    # Explicitly include pandas
        # 'psycopg2',  # Explicitly include psycopg2
        'pynetdicom',  # Explicitly include pynetdicom
    ] + collect_all('django')[1],  # Include all Django modules
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
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None
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