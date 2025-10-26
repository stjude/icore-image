#!/bin/bash

set -e  # Exit on any error

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Installing deid Node.js dependencies..."
cd deid
npm install
cd ..

echo "Installing electron Node.js dependencies..."
cd electron
npm install
cd ..

echo "Checking for JRE8..."
if [ ! -d "jre8" ]; then
    echo "JRE8 not found, downloading..."
    curl -s "https://api.adoptium.net/v3/assets/feature_releases/8/ga?os=mac&architecture=x64&image_type=jre&jvm_impl=hotspot" \
    | jq -r '.[] | .binaries[] | select(.image_type=="jre") | .package.link' \
    | head -n1 \
    | xargs curl -L \
    | tar -xj
    mv jdk8*-jre jre8
    echo "JRE8 downloaded and set up"
else
    echo "JRE8 already exists"
fi

echo "Checking for DCMTK..."
if [ ! -d "dcmtk" ]; then
    echo "DCMTK not found, downloading..."
    curl -L https://dicom.offis.de/download/dcmtk/dcmtk369/bin/dcmtk-3.6.9-macosx-x86_64.tar.bz2 | tar -xj
    mv dcmtk-3.6.9-macosx-x86_64 dcmtk
    cd dcmtk/bin
    find . -type f ! -name 'findscu' ! -name 'movescu' -delete
    cd ../..
    echo "DCMTK downloaded and set up"
else
    echo "DCMTK already exists"
fi

echo "Building with PyInstaller..."
rm -rf dist
pyinstaller --clean -y icore_processor.spec

echo "Building deid components..."
cd deid
pyinstaller --clean -y manage.spec
pyinstaller --clean -y processor.spec
pyinstaller --clean -y initialize_admin_password.spec
cd ..

echo "Preparing electron assets..."
cd electron
rm -rf assets/dist
cp ../deid/home/settings.json assets
ditto ../deid/dist assets/dist
ditto ../dist/icore_processor assets/dist/icore_processor

echo "Checking notarization environment variables..."
required_vars=("APPLE_ID" "APPLE_APP_SPECIFIC_PASSWORD" "APPLE_TEAM_ID")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo "Error: Missing required environment variables for notarization:"
    for var in "${missing_vars[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "Please set these variables before running the build script:"
    echo "  export APPLE_ID=\"your-apple-id@example.com\""
    echo "  export APPLE_APP_SPECIFIC_PASSWORD=\"your-app-specific-password\""
    echo "  export APPLE_TEAM_ID=\"your-team-id\""
    echo ""
    echo "Or create a .env file with these variables and source it before running the script."
    echo "Note: Code signing certificates will be auto-discovered from your keychain."
    exit 1
fi

echo "All notarization environment variables are set"
echo "Building and signing DMG..."
npm run build_signed