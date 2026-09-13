// Package tls holds the load balancer's pluggable TLS-certificate seam
// (ARCHITECTURE.md §2.5: "load balancer terminates TLS"; the package path
// and interface name are fixed by docs/modularity-and-extensibility.md's
// interface registry — "TLS/certificate provisioning | CertProvider |
// services/loadbalancer/internal/tls"). SelfSigned is Phase 5's only
// implementation (phase-5-networking-ingress.md Task 5) — a real
// ACME/Let's Encrypt/Cloudflare adapter is a documented future alternative,
// not built now.
package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"sync"
	"time"
)

// CertProvider resolves a TLS SNI hostname to the certificate the load
// balancer should present for it. GetCertificate is called from
// tls.Config.GetCertificate on every TLS handshake's ClientHello — it is
// the request hot path (ARCHITECTURE.md §2.5, §2.6's rule applied to TLS
// exactly as it already applies to the registry: never block on Postgres or
// any other network call here).
type CertProvider interface {
	GetCertificate(hostname string) (*tls.Certificate, error)
}

// SelfSigned is Phase 5's CertProvider: generates a self-signed certificate
// for a hostname the first time it's asked for one, and caches it in memory
// for every handshake after that — never touches Postgres or any other
// store, consistent with how registry.Registry itself works. Safe for
// concurrent use: GetCertificate is called concurrently by every TLS
// handshake goroutine.
type SelfSigned struct {
	mu    sync.Mutex
	certs map[string]*tls.Certificate

	// validity is how long a generated cert is valid for. There is no
	// rotation/renewal path in Phase 5 (open decision territory for a real
	// ACME adapter later) — a long validity window keeps that gap from
	// mattering: a self-signed cert needs no CA trust chain to keep working
	// against an InsecureSkipVerify client, which is the only kind of
	// client Phase 5 is built to serve.
	validity time.Duration
}

// NewSelfSigned builds a SelfSigned adapter with an empty cache.
func NewSelfSigned() *SelfSigned {
	return &SelfSigned{
		certs:    make(map[string]*tls.Certificate),
		validity: 10 * 365 * 24 * time.Hour,
	}
}

// GetCertificate implements CertProvider.
func (s *SelfSigned) GetCertificate(hostname string) (*tls.Certificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cert, ok := s.certs[hostname]; ok {
		return cert, nil
	}

	cert, err := generate(hostname, s.validity)
	if err != nil {
		return nil, fmt.Errorf("generating self-signed certificate for %q: %w", hostname, err)
	}
	s.certs[hostname] = cert
	return cert, nil
}

// generate creates a fresh self-signed leaf certificate for hostname,
// valid immediately (with a little clock-skew slack) for validity.
func generate(hostname string, validity time.Duration) (*tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generating serial number: %w", err)
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: hostname},
		DNSNames:              []string{hostname},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(validity),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("creating certificate: %w", err)
	}

	return &tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
	}, nil
}
