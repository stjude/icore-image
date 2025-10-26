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

dcmtk_path = os.path.join(project_path, 'dcmtk')
if os.path.exists(dcmtk_path):
    datas.append((dcmtk_path, 'dcmtk'))


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

try:
    spacy_datas = collect_data_files('spacy')
    datas.extend(spacy_datas)
except:
    pass

try:
    en_core_web_sm_datas = collect_data_files('en_core_web_sm')
    datas.extend(en_core_web_sm_datas)
except:
    pass

try:
    presidio_analyzer_datas = collect_data_files('presidio_analyzer')
    datas.extend(presidio_analyzer_datas)
    import presidio_analyzer
    presidio_analyzer_path = os.path.dirname(presidio_analyzer.__file__)
    recognizer_registry_path = os.path.join(presidio_analyzer_path, 'recognizer_registry')
    if os.path.exists(recognizer_registry_path):
        datas.append((recognizer_registry_path, 'presidio_analyzer/recognizer_registry'))
except:
    pass

try:
    presidio_anonymizer_datas = collect_data_files('presidio_anonymizer')
    datas.extend(presidio_anonymizer_datas)
except:
    pass

try:
    thinc_datas = collect_data_files('thinc')
    datas.extend(thinc_datas)
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
        'sys',
        'warnings',
        'presidio_analyzer',
        'presidio_analyzer.nlp_engine',
        'presidio_analyzer.entity_recognizer',
        'presidio_analyzer.pattern_recognizer',
        'presidio_anonymizer',
        'presidio_anonymizer.entities',
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
        'pydantic_core'
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['tensorflow', 'matplotlib', 'scipy', 'IPython', 'jupyter', 'notebook', 'sympy', 'numba', 'llvmlite', 'torch', 'torch._C', 'torch.functional', 'torch.nn', 'cupy', 'jax'],
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
    codesign_identity=os.environ.get('CODESIGN_IDENTITY'),
    entitlements_file='entitlements-icore-processor.plist',
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
