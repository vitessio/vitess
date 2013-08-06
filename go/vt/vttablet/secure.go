// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttablet

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/http"

	"github.com/youtube/vitess/go/relog"
)

// SecureListen obtains a listener that accepts
// secure connections
func SecureServe(addr string, certFile, keyFile, caFile string) {
	config := tls.Config{}

	// load the server cert / key
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		relog.Fatal("%s", err)
	}
	config.Certificates = []tls.Certificate{cert}

	// load the ca if necessary
	// FIXME(alainjobart) this doesn't quite work yet, have
	// to investigate
	if caFile != "" {
		config.ClientCAs = x509.NewCertPool()

		pemCerts, err := ioutil.ReadFile(caFile)
		if err != nil {
			relog.Fatal("%s", err)
		}
		if !config.ClientCAs.AppendCertsFromPEM(pemCerts) {
			relog.Fatal("%s", err)
		}

		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	l, err := tls.Listen("tcp", addr, &config)
	if err != nil {
		relog.Fatal("%s", err)
	}
	go http.Serve(l, nil)
}
