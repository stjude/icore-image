rm -rf iCore.dmg
create-dmg \
  --volname "iCore Installer" \
  --window-pos 100 100 \
  --window-size 300 430 \
  --icon-size 75 \
  --icon "iCore.app" 150 90 \
  --background ../background.png \
  --hide-extension "iCore.app" \
  --app-drop-link 150 240 \
  "iCore.dmg" \
  "iCore.app"