#!/bin/bash
# Package SyncBox as macOS .app bundle
set -e

BINARY="dist/client/client-darwin-arm64/syncbox-client"
APP_NAME="SyncBox.app"
APP_DIR="dist/client/client-darwin-arm64/${APP_NAME}"

if [ ! -f "$BINARY" ]; then
    echo "Binary not found: $BINARY"
    exit 1
fi

echo "Creating .app bundle..."
rm -rf "$APP_DIR"
mkdir -p "$APP_DIR/Contents/MacOS"
mkdir -p "$APP_DIR/Contents/Resources"

# Copy binary
cp "$BINARY" "$APP_DIR/Contents/MacOS/syncbox-client"
chmod +x "$APP_DIR/Contents/MacOS/syncbox-client"

# Copy Info.plist
cp deploy/darwin/Info.plist "$APP_DIR/Contents/Info.plist"

# Copy icon (convert PNG to icns if possible, otherwise just copy)
ICON_SRC="cmd/client/resources/icon.png"
if [ -f "$ICON_SRC" ]; then
    cp "$ICON_SRC" "$APP_DIR/Contents/Resources/AppIcon.png"
    # Try to create icns using sips + iconutil
    if command -v sips &> /dev/null && command -v iconutil &> /dev/null; then
        ICONSET_DIR="/tmp/SyncBox.iconset"
        rm -rf "$ICONSET_DIR"
        mkdir -p "$ICONSET_DIR"
        for size in 16 32 64 128 256 512; do
            sips -z $size $size "$ICON_SRC" --out "$ICONSET_DIR/icon_${size}x${size}.png" > /dev/null 2>&1
            size2x=$((size * 2))
            if [ $size2x -le 1024 ]; then
                sips -z $size2x $size2x "$ICON_SRC" --out "$ICONSET_DIR/icon_${size}x${size}@2x.png" > /dev/null 2>&1
            fi
        done
        # 1024x1024
        sips -z 1024 1024 "$ICON_SRC" --out "$ICONSET_DIR/icon_512x512@2x.png" > /dev/null 2>&1
        iconutil -c icns "$ICONSET_DIR" -o "$APP_DIR/Contents/Resources/AppIcon.icns" 2>/dev/null || true
        rm -rf "$ICONSET_DIR"
    fi
fi

# Ad-hoc code sign
if command -v codesign &> /dev/null; then
    echo "Code signing..."
    codesign --force --deep --sign - "$APP_DIR" 2>/dev/null || true
fi

# Remove the raw binary from zip to avoid confusion
# Keep only the .app in the final artifact
echo "Removing raw binary..."
rm -f "$BINARY"

# Verify
if [ -d "$APP_DIR" ]; then
    echo "Created: $APP_DIR"
    ls -la "$APP_DIR/Contents/MacOS/"
    ls -la "$APP_DIR/Contents/Resources/"
else
    echo "ERROR: .app bundle creation failed"
    exit 1
fi
