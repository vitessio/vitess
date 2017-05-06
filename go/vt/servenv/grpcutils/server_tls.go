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

package grpcutils

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

// TLSServerConfig returns the TLS config to use for a server to
// accept client connections.
func TLSServerConfig(cert, key, ca string) (*tls.Config, error) {
	config := &tls.Config{}

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
			return nil, fmt.Errorf("Failed to read ca file: %v", err)
		}
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("Failed to append certificates")
		}
		config.ClientCAs = cp
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return config, nil
}
