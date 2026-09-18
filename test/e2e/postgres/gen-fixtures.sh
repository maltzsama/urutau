#!/bin/sh
# Generates the TLS and SSH fixtures the e2e stack mounts. Idempotent: an
# existing fixture is left alone. Everything here is throwaway test material
# (self-signed cert, ephemeral keys) and is git-ignored.
set -eu

dir="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$dir/tls" "$dir/ssh"

# TLS: a self-signed server certificate whose SAN matches the host-side
# address the test dials (127.0.0.1:5434) and localhost.
if [ ! -f "$dir/tls/server.crt" ]; then
	openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
		-keyout "$dir/tls/server.key" -out "$dir/tls/server.crt" \
		-subj "/CN=localhost" \
		-addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1
	chmod 600 "$dir/tls/server.key"
	echo "generated TLS fixtures"
fi

# SSH: the bastion's host key, the client's key pair, and a known_hosts file
# matching the host-mapped bastion port (2222) so the source verifies it.
if [ ! -f "$dir/ssh/host_ed25519" ]; then
	ssh-keygen -t ed25519 -N "" -f "$dir/ssh/host_ed25519" -C bastion >/dev/null
	ssh-keygen -t ed25519 -N "" -f "$dir/ssh/client_ed25519" -C client >/dev/null
	hostkey="$(cut -d' ' -f1,2 <"$dir/ssh/host_ed25519.pub")"
	printf '[127.0.0.1]:2222 %s\n' "$hostkey" >"$dir/ssh/known_hosts"
	echo "generated SSH fixtures"
fi
