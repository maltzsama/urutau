package grpctls

// CD-T5: the control plane verifies BOTH sides. These assertions pin the
// security-relevant properties of the tls.Config we build — the server
// REQUIRES and verifies a client certificate, and the client presents one
// and verifies the server against the CA. (A cert-less client is rejected by
// the TLS stack precisely because ClientAuth is RequireAndVerifyClientCert.)

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func genCert(t *testing.T, dir, name string, parent *x509.Certificate, parentKey *ecdsa.PrivateKey, isCA, server bool) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	if isCA {
		tmpl.IsCA = true
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	signerCert, signerKey := tmpl, key
	if parent != nil {
		signerCert, signerKey = parent, parentKey
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signerCert, &key.PublicKey, signerKey)
	if err != nil {
		t.Fatal(err)
	}
	certFile = filepath.Join(dir, name+".crt")
	keyFile = filepath.Join(dir, name+".key")
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func testMaterial(t *testing.T) (serverCfg, clientCfg Config) {
	t.Helper()
	dir := t.TempDir()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "urutau-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCertFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caCertFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)

	sc, sk := genCert(t, dir, "server", caCert, caKey, false, true)
	cc, ck := genCert(t, dir, "client", caCert, caKey, false, false)
	return Config{CertFile: sc, KeyFile: sk, ClientCAFile: caCertFile},
		Config{CertFile: cc, KeyFile: ck, ClientCAFile: caCertFile}
}

func TestServerTLSRequiresAndVerifiesClientCert(t *testing.T) {
	serverCfg, _ := testMaterial(t)
	cfg, err := serverCfg.ServerTLS()
	if err != nil {
		t.Fatalf("ServerTLS: %v", err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("ClientAuth = %v, want RequireAndVerifyClientCert (a cert-less client must be rejected)", cfg.ClientAuth)
	}
	if cfg.ClientCAs == nil {
		t.Fatal("ClientCAs must be set to verify worker client certs")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatal("server must present exactly one certificate")
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("MinVersion = %x, want TLS 1.3", cfg.MinVersion)
	}
}

func TestClientTLSPresentsCertAndVerifiesServer(t *testing.T) {
	_, clientCfg := testMaterial(t)
	cfg, err := clientCfg.ClientTLS()
	if err != nil {
		t.Fatalf("ClientTLS: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatal("client must present a certificate")
	}
	if cfg.RootCAs == nil {
		t.Fatal("client must verify the server against the CA")
	}
	if cfg.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify must be false")
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("MinVersion = %x, want TLS 1.3", cfg.MinVersion)
	}
}

func TestPartialConfigIsRejected(t *testing.T) {
	// All three fields are required to enable mTLS; a partial config must
	// error rather than silently run with weaker verification.
	if _, err := (Config{CertFile: "x"}).ServerTLS(); err == nil {
		t.Fatal("partial TLS config must error")
	}
	if _, err := (Config{KeyFile: "x"}).ClientTLS(); err == nil {
		t.Fatal("partial TLS config must error")
	}
	if (Config{}).Enabled() {
		t.Fatal("empty config must report disabled")
	}
	if !(Config{CertFile: "x", KeyFile: "y", ClientCAFile: "z"}).Enabled() {
		t.Fatal("fully-set config must report enabled")
	}
}
