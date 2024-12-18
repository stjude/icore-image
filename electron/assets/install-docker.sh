#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Check if Docker is already installed
if [ -d "/Applications/Docker.app" ]; then
    echo "Docker is already installed."
    
    # Check if Docker is running
    if /usr/local/bin/docker system info > /dev/null 2>&1; then
        echo "Docker is already running."
        /usr/local/bin/docker --version
        exit 0
    else
        echo "Starting Docker Desktop..."
        open -g -a Docker
        
        echo "Waiting for Docker to initialize..."
        while ! /usr/local/bin/docker system info > /dev/null 2>&1; do
            sleep 1
        done
        
        echo "Docker Desktop is now running!"
        /usr/local/bin/docker --version
        exit 0
    fi
fi

echo "Starting Docker installation.."

# Define variables
ARCH=$(uname -m)
if [ "$ARCH" == "arm64" ]; then
    echo "Apple Silicon detected."
    DOCKER_DOWNLOAD_URL="https://desktop.docker.com/mac/stable/arm64/Docker.dmg"
elif [ "$ARCH" == "x86_64" ]; then
    echo "Intel architecture detected."
    DOCKER_DOWNLOAD_URL="https://desktop.docker.com/mac/stable/amd64/Docker.dmg"
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi
DOCKER_DMG="/tmp/Docker.dmg"
DOCKER_VOLUME="/Volumes/Docker"

# Download Docker Desktop
echo "Downloading Docker Desktop..."
curl -L -o "$DOCKER_DMG" "$DOCKER_DOWNLOAD_URL"

# Mount the DMG
echo "Mounting Docker DMG..."
hdiutil attach "$DOCKER_DMG" -quiet -nobrowse
if [ ! -d "$DOCKER_VOLUME" ]; then
    echo "Failed to mount Docker DMG. Exiting."
    exit 1
fi

# Copy Docker to Applications
echo "Installing Docker Desktop..."
cp -R "$DOCKER_VOLUME/Docker.app" /Applications

# Unmount the DMG
echo "Unmounting Docker DMG..."
hdiutil detach "$DOCKER_VOLUME" -quiet

# Cleanup
echo "Cleaning up..."
rm -f "$DOCKER_DMG"

# Start Docker Desktop
echo "Starting Docker Desktop..."
open -g -a Docker

# Wait for Docker to start
echo "Waiting for Docker to initialize..."
while ! /usr/local/bin/docker system info > /dev/null 2>&1; do
    sleep 1
done

echo "Docker Desktop installed and running successfully!"
/usr/local/bin/docker --version
