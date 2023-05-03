package ssl

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"

	"vitess.io/vitess/go/vt/log"
)

/*
	This file has been copied over from VTOrc package
*/

// NewTLSConfig returns an initialized TLS configuration suitable for client
// authentication. If caFile is non-empty, it will be loaded.
func NewTLSConfig(caFile string, verifyCert bool, minVersion uint16) (*tls.Config, error) {
	var c tls.Config

	// Set to TLS 1.2 as a minimum.  This is overridden for mysql communication
	c.MinVersion = minVersion

	if verifyCert {
		log.Info("verifyCert requested, client certificates will be verified")
		c.ClientAuth = tls.VerifyClientCertIfGiven
	}
	caPool, err := ReadCAFile(caFile)
	if err != nil {
		return &c, err
	}
	c.ClientCAs = caPool
	return &c, nil
}

// Returns CA certificate. If caFile is non-empty, it will be loaded.
func ReadCAFile(caFile string) (*x509.CertPool, error) {
	var caCertPool *x509.CertPool
	if caFile != "" {
		data, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		caCertPool = x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(data) {
			return nil, errors.New("No certificates parsed")
		}
		log.Infof("Read in CA file: %v", caFile)
	}
	return caCertPool, nil
}

// AppendKeyPair loads the given TLS key pair and appends it to
// tlsConfig.Certificates.
func AppendKeyPair(tlsConfig *tls.Config, certFile string, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
	return nil
}
