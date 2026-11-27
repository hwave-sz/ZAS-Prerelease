#!/bin/bash
# Phase D 一键执行（终端只需一条短命令）
set -u
cd /tmp || exit 1

URL=https://codeload.github.com/hwave-sz/ZAS-Prerelease/zip/refs/heads/relay-gw7
if [ ! -s rz.zip ]; then
  echo "[1/5] downloading..."
  curl -sL -m 180 -o rz.zip "$URL"
fi
echo "[1/5] zip size: $(stat -c%s rz.zip 2>/dev/null)"

rm -rf ZAS-Prerelease-relay-gw7
unzip -oq rz.zip
D=/tmp/ZAS-Prerelease-relay-gw7

echo "[2/5] unpack binary"
gunzip -c "$D/gatewayd_linux_amd64.gz" > /tmp/gw7
chmod +x /tmp/gw7
ls -l /tmp/gw7
cp -f "$D/phaseD.py" /tmp/phaseD.py

echo "[3/5] base config probe"
ls -l /mnt/workspace/gw3/gw_c.yaml /mnt/workspace/gw_c.yaml 2>/dev/null

pkill -f 'gw7 -config' 2>/dev/null
sleep 1

echo "[4/5] arms"
python3 /tmp/phaseD.py direct 24 90 A24
python3 /tmp/phaseD.py B 24 90 B24
python3 /tmp/phaseD.py C 24 90 C24

echo "[5/5] report"
python3 /tmp/phaseD.py report
echo "=== DONE ==="
