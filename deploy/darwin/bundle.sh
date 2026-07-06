#!/bin/bash
# Package SyncBox as macOS .app bundle
BINARY="dist/client/client-darwin-arm64/syncbox-client"
APP_NAME="SyncBox.app"
APP_DIR="dist/client/client-darwin-arm64/${APP_NAME}"

if [ ! -f "$BINARY" ]; then
    echo "Binary not found: $BINARY"
    exit 1
fi

echo "=== Creating .app bundle ==="
rm -rf "$APP_DIR"
mkdir -p "$APP_DIR/Contents/MacOS"
mkdir -p "$APP_DIR/Contents/Resources"

cp "$BINARY" "$APP_DIR/Contents/MacOS/syncbox-client"
chmod +x "$APP_DIR/Contents/MacOS/syncbox-client"
cp deploy/darwin/Info.plist "$APP_DIR/Contents/Info.plist"

ICON_SRC="cmd/client/resources/icon.png"
if [ -f "$ICON_SRC" ]; then
    cp "$ICON_SRC" "$APP_DIR/Contents/Resources/AppIcon.png"
    if command -v sips &> /dev/null && command -v iconutil &> /dev/null; then
        ICONSET_DIR="/tmp/SyncBox.iconset"
        rm -rf "$ICONSET_DIR"
        mkdir -p "$ICONSET_DIR"
        sips -z 16 16 "$ICON_SRC" --out "$ICONSET_DIR/icon_16x16.png" > /dev/null 2>&1
        sips -z 32 32 "$ICON_SRC" --out "$ICONSET_DIR/icon_16x16@2x.png" > /dev/null 2>&1
        sips -z 32 32 "$ICON_SRC" --out "$ICONSET_DIR/icon_32x32.png" > /dev/null 2>&1
        sips -z 64 64 "$ICON_SRC" --out "$ICONSET_DIR/icon_32x32@2x.png" > /dev/null 2>&1
        sips -z 128 128 "$ICON_SRC" --out "$ICONSET_DIR/icon_128x128.png" > /dev/null 2>&1
        sips -z 256 256 "$ICON_SRC" --out "$ICONSET_DIR/icon_128x128@2x.png" > /dev/null 2>&1
        sips -z 256 256 "$ICON_SRC" --out "$ICONSET_DIR/icon_256x256.png" > /dev/null 2>&1
        sips -z 512 512 "$ICON_SRC" --out "$ICONSET_DIR/icon_256x256@2x.png" > /dev/null 2>&1
        sips -z 512 512 "$ICON_SRC" --out "$ICONSET_DIR/icon_512x512.png" > /dev/null 2>&1
        sips -z 1024 1024 "$ICON_SRC" --out "$ICONSET_DIR/icon_512x512@2x.png" > /dev/null 2>&1
        iconutil -c icns "$ICONSET_DIR" -o "$APP_DIR/Contents/Resources/AppIcon.icns" 2>/dev/null || true
        rm -rf "$ICONSET_DIR"
    fi
fi

cat > /tmp/syncbox.entitlements << 'ENTEOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>com.apple.security.cs.allow-unsigned-executable-memory</key>
    <true/>
    <key>com.apple.security.cs.disable-library-validation</key>
    <true/>
    <key>com.apple.security.network.client</key>
    <true/>
</dict>
</plist>
ENTEOF

echo "=== Code signing ==="
codesign --force --options runtime --entitlements /tmp/syncbox.entitlements --sign - "$APP_DIR/Contents/MacOS/syncbox-client" || true
codesign --force --sign - "$APP_DIR" || true
codesign --verify --deep "$APP_DIR" 2>&1 || echo "(verify: non-critical)"

rm -f "$BINARY"

echo "=== Bundle contents ==="
ls -la "$APP_DIR/Contents/MacOS/"
echo "Done: $APP_DIR"
