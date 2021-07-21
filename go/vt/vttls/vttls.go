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
	"io/ioutil"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Updated list of acceptable cipher suits to address
// Fixed upstream in https://github.com/golang/go/issues/13385
// This removed CBC mode ciphers that are suseptiable to Lucky13 style attacks
func newTLSConfig() *tls.Config {
	return &tls.Config{
		// MySQL Community edition has some problems with TLS1.2
		// TODO: Validate this will not break servers using mysql community edition < 5.7.10
		// MinVersion: tls.VersionTLS12,

		// Default ordering taken from
		// go 1.11 crypto/tls/cipher_suites.go
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
	}
}

var onceByKeys = sync.Map{}

// ClientConfig returns the TLS config to use for a client to
// connect to a server with the provided parameters.
func ClientConfig(cert, key, ca, name string) (*tls.Config, error) {
	config := newTLSConfig()

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

	return config, nil
}

// ServerConfig returns the TLS config to use for a server to
// accept client connections.
func ServerConfig(cert, key, ca, serverCA string) (*tls.Config, error) {
	config := newTLSConfig()

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
	b, err := ioutil.ReadFile(ca)
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
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "Cannot find loaded tls certificate with cert: %s, key%s", cert, key)
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

var combinedTlsCertificates = sync.Map{}

func combineAndLoadTLSCertificates(ca, cert, key string) (*[]tls.Certificate, error) {
	combinedTlsIdentifier := tlsCertificatesIdentifier(ca, cert, key)
	once, _ := onceByKeys.LoadOrStore(combinedTlsIdentifier, &sync.Once{})

	var err error
	once.(*sync.Once).Do(func() {
		err = doLoadAndCombineTLSCertificates(ca, cert, key)
	})

	if err != nil {
		return nil, err
	}

	result, ok := combinedTlsCertificates.Load(combinedTlsIdentifier)

	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "Cannot find loaded tls certificate chain with ca: %s, cert: %s, key: %s", ca, cert, key)
	}

	return result.(*[]tls.Certificate), nil
}

func doLoadAndCombineTLSCertificates(ca, cert, key string) error {
	combinedTlsIdentifier := tlsCertificatesIdentifier(ca, cert, key)

	// Read CA certificates chain
	ca_b, err := ioutil.ReadFile(ca)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read ca file: %s", ca)
	}

	// Read server certificate
	cert_b, err := ioutil.ReadFile(cert)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read server cert file: %s", cert)
	}

	// Read server key file
	key_b, err := ioutil.ReadFile(key)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read key file: %s", key)
	}

	// Load CA, server cert and key.
	var certificate []tls.Certificate
	crt, err := tls.X509KeyPair(append(cert_b, ca_b...), key_b)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to load and merge tls certificate with CA, ca %s, cert %s, key: %s", ca, cert, key)
	}

	certificate = []tls.Certificate{crt}

	combinedTlsCertificates.Store(combinedTlsIdentifier, &certificate)

	return nil
}
