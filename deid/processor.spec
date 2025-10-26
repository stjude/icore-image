import os
import sys
from PyInstaller.utils.hooks import collect_dynamic_libs

block_cipher = None

project_path = os.getcwd()

# Collect Python runtime libraries
binaries = []
if sys.platform == 'darwin':
    # Add Python runtime for macOS
    import sysconfig
    lib_dir = sysconfig.get_config_var('LIBDIR')
    lib_name = f"libpython{sys.version_info.major}.{sys.version_info.minor}.dylib"
    lib_path = os.path.join(lib_dir, lib_name)
    if os.path.exists(lib_path):
        binaries.append((lib_path, '.'))

a = Analysis(
    [os.path.join(project_path, 'processor.py')],
    pathex=[],
    binaries=binaries,
    datas=[],
    hiddenimports=['yaml'],
    hookspath=[],
    hooksconfig={},
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='processor',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
)
