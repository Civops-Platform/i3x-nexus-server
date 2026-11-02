#!/usr/bin/env bash
# Run the media gateway test on any Linux Docker host: MediaMTX (pinned image) as the
# gateway, a second MediaMTX plus ffmpeg as the "camera", and the test in a pinned
# node:22 container, all on host networking bound to 127.0.0.1. Everything it
# starts, it stops.
set -euo pipefail
cd "$(dirname "$0")/.."
# Under the repo, not /tmp: some Docker daemons (snap, PrivateTmp) see a different /tmp.
REC="$PWD/.e2e-recordings"
cleanup() {
  docker rm -f nexus-gw-mediamtx nexus-gw-camera-srv nexus-gw-camera >/dev/null 2>&1 || true
  # Recordings belong to the gateway's uid: remove them from inside a container.
  [ -d "$REC" ] && docker run --rm --entrypoint "" -v "$REC:/r" node:22.11.0 sh -c 'rm -rf /r/* /r/.[!.]*' >/dev/null 2>&1 || true
  rmdir "$REC" 2>/dev/null || true
}
trap cleanup EXIT
cleanup
mkdir -p "$REC" && chmod 777 "$REC" # the gateway's user may map to another uid (rootless or userns-remapped Docker)
docker run -d --name nexus-gw-mediamtx --network host -m 512m --user "$(id -u):$(id -g)" \
  -v "$PWD/config/test/mediamtx.yml:/mediamtx.yml:ro" -v "$REC:/recordings" bluenviron/mediamtx:1.9.3 >/dev/null
docker run -d --name nexus-gw-camera-srv --network host -m 256m -v "$PWD/config/test/camera.yml:/mediamtx.yml:ro" bluenviron/mediamtx:1.9.3 >/dev/null
sleep 1
docker run -d --name nexus-gw-camera --network host -m 256m linuxserver/ffmpeg:7.1.1 \
  -loglevel error -re -f lavfi -i testsrc2=size=640x360:rate=15 -c:v libx264 -preset ultrafast -tune zerolatency -g 15 \
  -f rtsp rtsp://127.0.0.1:8654/cam >/dev/null
sleep 3
docker run --rm --network host -v "$PWD:/app" -v nexus-gw-node-modules:/app/node_modules -w /app -e NEXUS_TEST_GATEWAY=1 node:22.11.0 \
  bash -c "npm ci --silent --no-audit --no-fund >/dev/null 2>&1; node --import tsx --test tests/gateway.test.ts"
