# -*- mode: python ; coding: utf-8 -*-

import os
import sys
from PyInstaller.utils.hooks import collect_all, collect_data_files

block_cipher = None

project_path = os.getcwd()

datas = []

jre_path = os.path.join(project_path, 'jre8')
if os.path.exists(jre_path):
    datas.append((jre_path, 'jre8'))

ctp_path = os.path.join(project_path, 'ctp')
if os.path.exists(ctp_path):
    datas.append((ctp_path, 'ctp'))

scrubber_path = os.path.join(project_path, 'scrubber.19.0403.lnx')
if os.path.exists(scrubber_path):
    datas.append((scrubber_path, '.'))

try:
    lark_datas = collect_data_files('lark')
    datas.extend(lark_datas)
except:
    pass

try:
    openpyxl_datas = collect_data_files('openpyxl')
    datas.extend(openpyxl_datas)
except:
    pass

try:
    pandas_datas = collect_data_files('pandas')
    datas.extend(pandas_datas)
except:
    pass

try:
    pydicom_datas = collect_data_files('pydicom')
    datas.extend(pydicom_datas)
except:
    pass

try:
    ruamel_datas = collect_data_files('ruamel')
    datas.extend(ruamel_datas)
except:
    pass

a = Analysis(
    [os.path.join(project_path, 'icore_processor.py')],
    pathex=[project_path],
    binaries=[],
    datas=datas,
    hiddenimports=[
        'lark',
        'openpyxl',
        'pandas',
        'yaml',
        'pydicom',
        'requests',
        'ruamel.yaml',
        'ruamel.yaml.comments',
        'ruamel.yaml.scalarstring',
        'xml.etree.ElementTree',
        'tempfile',
        'threading',
        'subprocess',
        'signal',
        'contextlib',
        'datetime',
        'string',
        'logging',
        're',
        'shutil',
        'time',
        'os',
        'sys'
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['torch', 'tensorflow', 'matplotlib', 'scipy', 'IPython', 'jupyter', 'notebook', 'sympy', 'numba', 'llvmlite'],
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
    name='icore_processor',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    bundle_identifier='com.icore.processor',
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='icore_processor',
)
