#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Signing icore_processor Bundle for CI ===${NC}"

# Variables
DIST_DIR="dist/icore_processor"
ENTITLEMENTS="entitlements-icore-processor.plist"
BUNDLE_ID="com.icore.processor"

# Check if dist directory exists
if [ ! -d "$DIST_DIR" ]; then
    echo -e "${RED}Error: Distribution directory not found: $DIST_DIR${NC}"
    echo "Please build the application first using: pyinstaller icore_processor.spec"
    exit 1
fi

# Check if entitlements file exists
if [ ! -f "$ENTITLEMENTS" ]; then
    echo -e "${RED}Error: Entitlements file not found: $ENTITLEMENTS${NC}"
    exit 1
fi

# Auto-detect signing identity from keychain
echo -e "${YELLOW}Detecting signing identity from keychain...${NC}"
SIGNING_IDENTITY=$(security find-identity -v -p codesigning | grep "Developer ID Application" | head -n1 | sed 's/.*"\(.*\)".*/\1/')

if [ -z "$SIGNING_IDENTITY" ]; then
    echo -e "${RED}Error: No 'Developer ID Application' identity found in keychain${NC}"
    echo "Available identities:"
    security find-identity -v -p codesigning
    exit 1
fi

echo -e "${YELLOW}Using signing identity: $SIGNING_IDENTITY${NC}"
echo -e "${YELLOW}Bundle ID: $BUNDLE_ID${NC}"
echo ""

# Step 1: Find and sign all Mach-O binaries (executables, .dylib, .so, etc.)
echo -e "${GREEN}Step 1: Finding all Mach-O binaries to sign...${NC}"

# Find all files and check if they're Mach-O binaries
find "$DIST_DIR/_internal" -type f | while read file; do
    # Check if it's a Mach-O binary (executable, dylib, bundle, etc.)
    if file "$file" | grep -q "Mach-O"; then
        echo "  Signing: $file"
        codesign --force --sign "$SIGNING_IDENTITY" \
            --options runtime \
            --entitlements "$ENTITLEMENTS" \
            --timestamp \
            "$file" 2>&1 || {
                echo -e "${RED}  Failed to sign: $file${NC}"
                exit 1
            }
    fi
done

echo -e "${GREEN}  All internal binaries signed${NC}"

# Step 1.5: Sign all other files to prevent electron-builder from trying to sign them
echo -e "${GREEN}Step 1.5: Signing all other files to prevent re-signing...${NC}"

# Find all files that are NOT Mach-O binaries and sign them without timestamp
find "$DIST_DIR/_internal" -type f | while read file; do
    # Skip if it's already a Mach-O binary (we signed those above)
    if ! file "$file" | grep -q "Mach-O"; then
        echo "  Signing (no timestamp): $file"
        codesign --force --sign "$SIGNING_IDENTITY" \
            "$file" 2>&1 || {
                # If signing fails, it's probably not a signable file - that's OK
                echo "  Skipping (not signable): $file"
            }
    fi
done

echo -e "${GREEN}  All other files signed${NC}"

# Step 2: Sign the main executable
echo -e "${GREEN}Step 2: Signing main executable...${NC}"
MAIN_EXEC="$DIST_DIR/icore_processor"
if [ ! -f "$MAIN_EXEC" ]; then
    echo -e "${RED}Error: Main executable not found: $MAIN_EXEC${NC}"
    exit 1
fi

codesign --force --sign "$SIGNING_IDENTITY" \
    --options runtime \
    --entitlements "$ENTITLEMENTS" \
    --timestamp \
    --identifier "$BUNDLE_ID" \
    "$MAIN_EXEC"

echo -e "${GREEN}  Main executable signed successfully${NC}"

# Verify signing
echo -e "${GREEN}Step 3: Verifying code signature...${NC}"
codesign --verify --deep --strict --verbose=2 "$MAIN_EXEC"
echo -e "${GREEN}  Signature verification passed${NC}"

echo ""
echo -e "${GREEN}=== Code Signing Complete ===${NC}"
echo -e "${GREEN}icore_processor bundle is now signed and ready for electron-builder${NC}"
