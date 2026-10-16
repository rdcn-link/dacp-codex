#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p examples/certs
cd examples/certs
# Reuse the demo CA so adding node certificates does not invalidate existing clients.
if [[ ! -f ca.key || ! -f ca.crt ]]; then
  openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 365 -subj /CN=dacp-demo-ca
fi
for name in server client node-a node-b; do
  if [[ -f "$name.key" && -f "$name.crt" ]]; then continue; fi
  openssl req -newkey rsa:2048 -nodes -keyout "$name.key" -out "$name.csr" -subj "/CN=$name"
  cat > "$name.ext" <<EOF
subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth,clientAuth
EOF
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -out "$name.crt" -days 365 -extfile "$name.ext"
done
chmod 600 *.key
