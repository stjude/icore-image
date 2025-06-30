#!/bin/bash

find . -name "icore_processor.tar" -type f -delete

cd electron
npm run build
cd iCore-darwin-$(node -p "process.arch")
../create-dmg.sh
cp iCore.dmg ../../icore-$(node -p "process.arch").dmg
