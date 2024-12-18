cd AIMINER-darwin-x64
rm -rf AIMINER.dmg
create-dmg \
  --volname "AIMINER Installer" \
  --window-pos 100 100 \
  --window-size 300 430 \
  --icon-size 75 \
  --icon "AIMINER.app" 150 90 \
  --background ../background.png \
  --hide-extension "AIMINER.app" \
  --app-drop-link 150 240 \
  "AIMINER.dmg" \
  "AIMINER.app"