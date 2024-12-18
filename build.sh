#!/bin/bash

cd electron
npm run build
cd AIMINER-darwin-$(node -p "process.arch")
../create-dmg.sh
cp AIMINER.dmg ../../aiminer-$(node -p "process.arch").dmg
