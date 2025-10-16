#!/bin/bash

rm -rf dist
pyinstaller --clean -y icore_processor.spec

cd deid
pyinstaller --clean -y manage.spec
pyinstaller --clean -y processor.spec
pyinstaller --clean -y initialize_admin_password.spec

cd ../electron
rm -rf assets/dist
cp ../deid/home/settings.json assets
cp -r ../deid/dist assets
cp -r ../dist/icore_processor assets/dist

npm run build_signed