#!/usr/bin/env bash
# Render catalog/src/*.html to PNG at exact size with headless Chrome (needs network
# for the web fonts). Usage: bash catalog/render.sh
set -euo pipefail
cd "$(dirname "$0")"
CHROME=${CHROME:-"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"}
command -v google-chrome >/dev/null 2>&1 && [ ! -x "$CHROME" ] && CHROME=google-chrome
shot() { "$CHROME" --headless=new --disable-gpu --hide-scrollbars --force-device-scale-factor=1 \
  --virtual-time-budget=4000 --window-size="$2" --screenshot="$PWD/$3" "file://$PWD/src/$1" >/dev/null 2>&1; }
shot hero.html 1600,900 hero-1600x900.png
shot logo-light.html 800,192 civops-logo-light.png
shot logo-dark.html 800,192 civops-logo-dark.png
echo "rendered: hero-1600x900.png civops-logo-light.png civops-logo-dark.png"
