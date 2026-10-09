package grpctls

import (
	"crypto/tls"
	"os"
	"sync"
	"time"
)

// certReloader loads a certificate/key pair from disk and re-reads it when the
// certificate file changes, so a cert-manager rotation is picked up on the
// next handshake without a process restart (issue #604). Handshakes are
// infrequent and the mtime check is cheap, so it caches the parsed pair only
// while the file is unchanged.
type certReloader struct {
	certFile string
	keyFile  string

	mu   sync.Mutex
	cert *tls.Certificate
	mod  time.Time
}

// get returns the current certificate, re-reading the files when the
// certificate's mtime moved.
func (r *certReloader) get() (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, err := os.Stat(r.certFile)
	if err != nil {
		return nil, err
	}
	if r.cert != nil && info.ModTime().Equal(r.mod) {
		return r.cert, nil
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return nil, err
	}
	r.cert = &cert
	r.mod = info.ModTime()
	return r.cert, nil
}
