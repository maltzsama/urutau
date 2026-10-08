package core

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// ParseUUIDBytes parses a hyphenated or compact UUID string into its 16 raw
// bytes. It is the single copy the transport and the sinks share (issue #610).
func ParseUUIDBytes(s string) ([]byte, error) {
	compact := strings.ReplaceAll(strings.ToLower(s), "-", "")
	raw, err := hex.DecodeString(compact)
	if err != nil || len(raw) != 16 {
		return nil, fmt.Errorf("core: %q is not a valid uuid", s)
	}
	return raw, nil
}

// UUIDText renders 16 raw bytes as the canonical hyphenated UUID text. It is
// the single copy the sinks share (issue #610).
func UUIDText(b []byte) (string, error) {
	if len(b) != 16 {
		return "", fmt.Errorf("core: uuid must be 16 bytes, got %d", len(b))
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
