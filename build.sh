#!/bin/bash

# Default values
application_image="deid/static/logo.png"
application_name="iCore"

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --application_image) application_image="$2"; shift ;;
        --application_name) application_name="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

echo "Using application image: $application_image"
echo "Using application name: $application_name"

python convert_to_icns.py --png_path $application_image --output_dir electron/assets
# Update the application name throughout the codebase
echo "Updating application name to: $application_name"
python3 update_app_name.py "$application_name"


find . -name "icore_processor.tar" -type f -delete

cd electron
npm run build

# Get the app name from package.json to determine the correct directory
APP_NAME=$(node -e "console.log(require('./package.json').name)")
cd ${APP_NAME}-darwin-$(node -p "process.arch")
../create-dmg.sh

# Copy the DMG with the new name
DMG_NAME=$(ls *.dmg)
cp "$DMG_NAME" "../../${APP_NAME}-$(node -p "process.arch").dmg"
