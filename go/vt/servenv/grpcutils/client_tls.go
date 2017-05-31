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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// TLSClientConfig returns the TLS config to use for a client to
// connect to a server with the provided parameters.
func TLSClientConfig(cert, key, ca, name string) (*tls.Config, error) {
	config := &tls.Config{}

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

// ClientSecureDialOption returns the gRPC dial option to use for the
// given client connection. It is either using TLS, or Insecure if
// nothing is set.
func ClientSecureDialOption(cert, key, ca, name string) (grpc.DialOption, error) {
	// No security options set, just return.
	if (cert == "" || key == "") && ca == "" {
		return grpc.WithInsecure(), nil
	}

	// Load the config.
	config, err := TLSClientConfig(cert, key, ca, name)
	if err != nil {
		return nil, err
	}

	// Create the creds server options.
	creds := credentials.NewTLS(config)
	return grpc.WithTransportCredentials(creds), nil
}
