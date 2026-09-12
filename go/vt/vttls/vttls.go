/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vttls

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// SslMode indicates the type of SSL mode to use. This matches
// the MySQL SSL modes as mentioned at:
// https://dev.mysql.com/doc/refman/8.0/en/connection-options.html#option_general_ssl-mode
type SslMode string

// Disabled disables SSL and connects over plain text
const Disabled SslMode = "disabled"

// Preferred establishes an SSL connection if the server supports it.
// It does not validate the certificate provided by the server.
const Preferred SslMode = "preferred"

// Required requires an SSL connection to the server.
// It does not validate the certificate provided by the server.
const Required SslMode = "required"

// VerifyCA requires an SSL connection to the server.
// It validates the CA against the configured CA certificate(s).
const VerifyCA SslMode = "verify_ca"

// VerifyIdentity requires an SSL connection to the server.
// It validates the CA against the configured CA certificate(s) and
// also validates the certificate based on the hostname.
// This is the setting you want when you want to connect safely to
// a MySQL server and want to be protected against man-in-the-middle
// attacks.
const VerifyIdentity SslMode = "verify_identity"

// String returns the string representation, part of the Value interface
// for allowing this to be retrieved for a flag.
func (mode *SslMode) String() string {
	return string(*mode)
}

// Type returns the value type, part of the pflag Value interface
// for allowing this to be used as a generic flag.
func (mode *SslMode) Type() string {
	return "SslMode"
}

// Set updates the value of the SslMode pointer, part of the Value interface
// for allowing to update a flag.
func (mode *SslMode) Set(value string) error {
	parsedMode := SslMode(strings.ToLower(value))
	switch parsedMode {
	case "":
		*mode = Preferred
		return nil
	case Disabled, Preferred, Required, VerifyCA, VerifyIdentity:
		*mode = parsedMode
		return nil
	}
	return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Invalid SSL mode specified: %s. Allowed options are disabled, preferred, required, verify_ca, verify_identity", value)
}

// TLSVersionToNumber converts a text description of the TLS protocol
// to the internal Go number representation.
func TLSVersionToNumber(tlsVersion string) (uint16, error) {
	switch strings.ToLower(tlsVersion) {
	case "tlsv1.3":
		return tls.VersionTLS13, nil
	case "", "tlsv1.2":
		return tls.VersionTLS12, nil
	case "tlsv1.1":
		return tls.VersionTLS11, nil
	case "tlsv1.0":
		return tls.VersionTLS10, nil
	default:
		return tls.VersionTLS12, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Invalid TLS version specified: %s. Allowed options are TLSv1.0, TLSv1.1, TLSv1.2 & TLSv1.3", tlsVersion)
	}
}

var onceByKeys = sync.Map{}

// ClientConfig returns the TLS config to use for a client to
// connect to a server with the provided parameters.
func ClientConfig(mode SslMode, cert, key, ca, crl, name string, minTLSVersion uint16) (*tls.Config, error) {
	config := &tls.Config{
		MinVersion: minTLSVersion,
	}

	// Load the client-side cert & key if any.
	if cert != "" && key != "" {
		certificates, err := loadTLSCertificate(cert, key)
		if err != nil {
			return nil, err
		}

		config.Certificates = *certificates
	}

	// Load the server CA if any.
	if ca != "" {
		certificatePool, err := loadx509CertPool(ca)
		if err != nil {
			return nil, err
		}

		config.RootCAs = certificatePool
	}

	// Set the server name if any.
	if name != "" {
		config.ServerName = name
	}

	var checker *crlChecker
	if crl != "" {
		var err error
		checker, err = newCRLChecker(crl, ca)
		if err != nil {
			return nil, err
		}
	}

	// The modes that build the peer's chain themselves verify it
	// against the configured CA, or the system roots without one,
	// resolved once here rather than on every handshake.
	var roots *x509.CertPool
	if mode == VerifyCA || (checker != nil && (mode == Preferred || mode == Required)) {
		var err error
		if roots, err = peerChainRoots(config.RootCAs); err != nil {
			return nil, err
		}
	}

	switch mode {
	case Disabled:
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "can't create config for disabled mode")
	case Preferred, Required:
		config.InsecureSkipVerify = true
		if checker != nil {
			config.VerifyConnection = func(cs tls.ConnectionState) error {
				// Go verifies nothing in these modes, so the chain
				// that the CRLs are held against is built here, to
				// the configured CA or the system roots, as verify_ca
				// does. A peer whose chain cannot be built is
				// rejected: its certificates cannot be checked, and
				// the ones it presents are its own to choose.
				chains, err := verifyPeerChain(roots, cs)
				if err != nil {
					return vterrors.Errorf(vtrpc.Code_UNAUTHENTICATED, "cannot check the revocation of the peer's certificates against the configured CRL, since no chain to a trusted CA could be built for them: %v", err)
				}
				return checker.check(chains)
			}
		}
	case VerifyCA:
		config.InsecureSkipVerify = true
		config.VerifyConnection = func(cs tls.ConnectionState) error {
			// The chains built here are handed to the CRL check,
			// since Go builds none of its own in this mode.
			chains, err := verifyPeerChain(roots, cs)
			if err != nil {
				return err
			}
			if checker == nil {
				return nil
			}
			return checker.check(chains)
		}
	case VerifyIdentity:
		// Go's own verification is the strictest and correct.
		if checker != nil {
			config.VerifyConnection = checker.verifyConnection
		}
	default:
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid mode: %s", mode)
	}

	return config, nil
}

// peerChainRoots returns what the peer's chain is verified against
// in the modes that build it themselves: the configured CA when
// there is one, and the system roots otherwise.
func peerChainRoots(configured *x509.CertPool) (*x509.CertPool, error) {
	if configured != nil {
		return configured, nil
	}
	return x509.SystemCertPool()
}

// verifyPeerChain verifies the certificate chain the peer presented
// against roots and returns the chains it built.
func verifyPeerChain(roots *x509.CertPool, cs tls.ConnectionState) ([][]*x509.Certificate, error) {
	opts := x509.VerifyOptions{
		Roots:         roots,
		Intermediates: x509.NewCertPool(),
	}
	for _, cert := range cs.PeerCertificates[1:] {
		opts.Intermediates.AddCert(cert)
	}
	return cs.PeerCertificates[0].Verify(opts)
}

// ServerConfig returns the TLS config to use for a server to
// accept client connections.
func ServerConfig(cert, key, ca, crl, serverCA string, minTLSVersion uint16) (*tls.Config, error) {
	config := &tls.Config{
		MinVersion: minTLSVersion,
	}

	var certificates *[]tls.Certificate
	var err error

	if serverCA != "" {
		certificates, err = combineAndLoadTLSCertificates(serverCA, cert, key)
	} else {
		certificates, err = loadTLSCertificate(cert, key)
	}

	if err != nil {
		return nil, err
	}
	config.Certificates = *certificates

	// if specified, load ca to validate client,
	// and enforce clients present valid certs.
	if ca != "" {
		certificatePool, err := loadx509CertPool(ca)
		if err != nil {
			return nil, err
		}

		config.ClientCAs = certificatePool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	if crl != "" {
		if ca == "" {
			// Without a CA no client certificate is requested, so
			// there would be nothing to check the CRL against.
			return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "a CRL is configured without a CA: no client certificate is requested without one, so the CRL could not apply")
		}
		checker, err := newCRLChecker(crl, ca)
		if err != nil {
			return nil, err
		}
		config.VerifyConnection = checker.verifyConnection
	}

	return config, nil
}

var certPools = sync.Map{}

func loadx509CertPool(ca string) (*x509.CertPool, error) {
	once, _ := onceByKeys.LoadOrStore(ca, &sync.Once{})

	var err error
	once.(*sync.Once).Do(func() {
		err = doLoadx509CertPool(ca)
	})
	if err != nil {
		return nil, err
	}

	result, ok := certPools.Load(ca)

	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "Cannot find loaded x509 cert pool for ca: %s", ca)
	}

	return result.(*x509.CertPool), nil
}

func doLoadx509CertPool(ca string) error {
	b, err := os.ReadFile(ca)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read ca file: %s", ca)
	}

	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		return vterrors.Errorf(vtrpc.Code_UNKNOWN, "failed to append certificates")
	}

	certPools.Store(ca, cp)

	return nil
}

var caCertificates = sync.Map{}

// loadx509Certificates returns the certificates in the PEM file ca,
// parsed once per path like loadx509CertPool does.
func loadx509Certificates(ca string) ([]*x509.Certificate, error) {
	identifier := tlsCertificatesIdentifier("ca-certificates", ca)
	once, _ := onceByKeys.LoadOrStore(identifier, &sync.Once{})

	var err error
	once.(*sync.Once).Do(func() {
		err = doLoadx509Certificates(ca)
	})
	if err != nil {
		return nil, err
	}

	result, ok := caCertificates.Load(ca)

	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "Cannot find loaded x509 certificates for ca: %s", ca)
	}

	return result.([]*x509.Certificate), nil
}

// doLoadx509Certificates parses the certificates in the PEM file ca,
// skipping the blocks that are not parsable certificates just like
// x509.CertPool.AppendCertsFromPEM does.
func doLoadx509Certificates(ca string) error {
	b, err := os.ReadFile(ca)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read ca file: %s", ca)
	}

	var certificates []*x509.Certificate
	for len(b) > 0 {
		var block *pem.Block
		block, b = pem.Decode(b)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		certificates = append(certificates, certificate)
	}
	if len(certificates) == 0 {
		return vterrors.Errorf(vtrpc.Code_UNKNOWN, "no certificates found in ca file: %s", ca)
	}

	caCertificates.Store(ca, certificates)

	return nil
}

var tlsCertificates = sync.Map{}

func tlsCertificatesIdentifier(tokens ...string) string {
	return strings.Join(tokens, ";")
}

func loadTLSCertificate(cert, key string) (*[]tls.Certificate, error) {
	tlsIdentifier := tlsCertificatesIdentifier(cert, key)
	once, _ := onceByKeys.LoadOrStore(tlsIdentifier, &sync.Once{})

	var err error
	once.(*sync.Once).Do(func() {
		err = doLoadTLSCertificate(cert, key)
	})

	if err != nil {
		return nil, err
	}

	result, ok := tlsCertificates.Load(tlsIdentifier)

	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "Cannot find loaded tls certificate with cert: %s, key: %s", cert, key)
	}

	return result.(*[]tls.Certificate), nil
}

func doLoadTLSCertificate(cert, key string) error {
	tlsIdentifier := tlsCertificatesIdentifier(cert, key)

	var certificate []tls.Certificate
	// Load the server cert and key.
	crt, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to load tls certificate, cert %s, key: %s", cert, key)
	}

	certificate = []tls.Certificate{crt}

	tlsCertificates.Store(tlsIdentifier, &certificate)

	return nil
}

var combinedTLSCertificates = sync.Map{}

func combineAndLoadTLSCertificates(ca, cert, key string) (*[]tls.Certificate, error) {
	combinedTLSIdentifier := tlsCertificatesIdentifier(ca, cert, key)
	once, _ := onceByKeys.LoadOrStore(combinedTLSIdentifier, &sync.Once{})

	var err error
	once.(*sync.Once).Do(func() {
		err = doLoadAndCombineTLSCertificates(ca, cert, key)
	})

	if err != nil {
		return nil, err
	}

	result, ok := combinedTLSCertificates.Load(combinedTLSIdentifier)

	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "Cannot find loaded tls certificate chain with ca: %s, cert: %s, key: %s", ca, cert, key)
	}

	return result.(*[]tls.Certificate), nil
}

func doLoadAndCombineTLSCertificates(ca, cert, key string) error {
	combinedTLSIdentifier := tlsCertificatesIdentifier(ca, cert, key)

	// Read CA certificates chain
	caB, err := os.ReadFile(ca)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read ca file: %s", ca)
	}

	// Read server certificate
	certB, err := os.ReadFile(cert)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read server cert file: %s", cert)
	}

	// Read server key file
	keyB, err := os.ReadFile(key)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read key file: %s", key)
	}

	// Load CA, server cert and key.
	var certificate []tls.Certificate
	crt, err := tls.X509KeyPair(append(certB, caB...), keyB)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to load and merge tls certificate with CA, ca %s, cert %s, key: %s", ca, cert, key)
	}

	certificate = []tls.Certificate{crt}

	combinedTLSCertificates.Store(combinedTLSIdentifier, &certificate)

	return nil
}
