import os
from shutil import which

HOME_DIR = os.path.expanduser('~')
DOCKER = which('docker') or '/usr/local/bin/docker'
APP_DIR = os.path.dirname(os.path.abspath(__file__))
RSA_PUBLIC_KEY_PATH = os.path.join(APP_DIR, 'icore_rsa.pub')
SETTINGS_DIR_PATH = os.path.abspath(os.path.join(HOME_DIR, '.aiminer'))
CONFIG_PATH = os.path.abspath(os.path.join(SETTINGS_DIR_PATH, 'config.yml'))
TMP_INPUT_PATH = os.path.abspath(os.path.join(SETTINGS_DIR_PATH, 'temp_input'))
LICENSES_PATH = os.path.abspath(os.path.join(SETTINGS_DIR_PATH, 'licenses.json'))
SETTINGS_FILE_PATH = os.path.abspath(os.path.join(SETTINGS_DIR_PATH, 'settings.json'))
