import os
from shutil import which

HOME_DIR = os.path.expanduser('~')
DOCKER = which('docker') or '/usr/local/bin/docker'
APP_DIR = os.path.dirname(os.path.abspath(__file__))
RSA_PUBLIC_KEY_PATH = os.path.join(APP_DIR, 'icore_rsa.pub')
PKG_DIR = os.path.abspath(os.path.join(HOME_DIR, '.aiminer'))
CONFIG_PATH = os.path.abspath(os.path.join(PKG_DIR, 'config.yml'))
TMP_INPUT_PATH = os.path.abspath(os.path.join(PKG_DIR, 'temp_input'))
LICENSES_PATH = os.path.abspath(os.path.join(PKG_DIR, 'licenses.json'))
SETTINGS_PATH = os.path.abspath(os.path.join(PKG_DIR, 'settings.json'))
