/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vttls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
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

// ClientConfig returns the TLS config to use for a client to
// connect to a server with the provided parameters.
func ClientConfig(cert, key, ca, name string) (*tls.Config, error) {
	config := newTLSConfig()

	// Load the client-side cert & key if any.
	if cert != "" && key != "" {
		crt, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("failed to load cert/key: %v", err)
		}
		config.Certificates = []tls.Certificate{crt}
	}

	// Load the server CA if any.
	if ca != "" {
		b, err := ioutil.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("failed to read ca file: %v", err)
		}
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("failed to append certificates")
		}
		config.RootCAs = cp
	}

	// Set the server name if any.
	if name != "" {
		config.ServerName = name
	}

	return config, nil
}

// ServerConfig returns the TLS config to use for a server to
// accept client connections.
func ServerConfig(cert, key, ca string) (*tls.Config, error) {
	config := newTLSConfig()

	// Load the server cert and key.
	crt, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("failed to load cert/key: %v", err)
	}
	config.Certificates = []tls.Certificate{crt}

	// if specified, load ca to validate client,
	// and enforce clients present valid certs.
	if ca != "" {
		b, err := ioutil.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("failed to read ca file: %v", err)
		}
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("failed to append certificates")
		}
		config.ClientCAs = cp
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return config, nil
}
