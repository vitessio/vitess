// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttablet

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
)

// SecureListen obtains a listener that accepts
// secure connections
func SecureListen(addr string, certFile, keyFile, caFile string) (l net.Listener, err error) {
	config := tls.Config{}

	// load the server cert / key
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	config.Certificates = []tls.Certificate{cert}

	// load the ca if necessary
	// FIXME(alainjobart) this doesn't quite work yet, have
	// to investigate
	if caFile != "" {
		config.ClientCAs = x509.NewCertPool()

		pemCerts, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		if !config.ClientCAs.AppendCertsFromPEM(pemCerts) {
			return nil, fmt.Errorf("StartHttpsServer failed to parse caFile %v", caFile)
		}

		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tls.Listen("tcp", addr, &config)
}
