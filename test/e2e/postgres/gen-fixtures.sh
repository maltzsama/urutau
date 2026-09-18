#!/bin/sh
# Generates the TLS and SSH fixtures the e2e stack mounts. Idempotent: the
# whole set is regenerated when ANY required file is missing, so an
# interrupted run (or a manually deleted sibling) does not leave a partial
# set that silently skips regeneration. Everything here is throwaway test
# material (self-signed cert, ephemeral keys) and is git-ignored.
set -eu

dir="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$dir/tls" "$dir/ssh"

all_present() {
	for f in "$@"; do
		[ -f "$f" ] || return 1
	done
	return 0
}

# TLS: a self-signed server certificate whose SAN matches the host-side
# address the test dials (127.0.0.1:5434) and localhost.
if ! all_present "$dir/tls/server.crt" "$dir/tls/server.key"; then
	rm -f "$dir/tls/server.crt" "$dir/tls/server.key"
	openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
		-keyout "$dir/tls/server.key" -out "$dir/tls/server.crt" \
		-subj "/CN=localhost" \
		-addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1
	chmod 600 "$dir/tls/server.key"
	echo "generated TLS fixtures"
fi

# SSH: the bastion's host key, the client's key pair, and a known_hosts file
# matching the host-mapped bastion port (2222) so the source verifies it.
if ! all_present \
	"$dir/ssh/host_ed25519" "$dir/ssh/host_ed25519.pub" \
	"$dir/ssh/client_ed25519" "$dir/ssh/client_ed25519.pub" \
	"$dir/ssh/known_hosts"; then
	rm -f "$dir/ssh/host_ed25519" "$dir/ssh/host_ed25519.pub" \
		"$dir/ssh/client_ed25519" "$dir/ssh/client_ed25519.pub" "$dir/ssh/known_hosts"
	ssh-keygen -t ed25519 -N "" -f "$dir/ssh/host_ed25519" -C bastion >/dev/null
	ssh-keygen -t ed25519 -N "" -f "$dir/ssh/client_ed25519" -C client >/dev/null
	hostkey="$(cut -d' ' -f1,2 <"$dir/ssh/host_ed25519.pub")"
	printf '[127.0.0.1]:2222 %s\n' "$hostkey" >"$dir/ssh/known_hosts"
	echo "generated SSH fixtures"
fi
