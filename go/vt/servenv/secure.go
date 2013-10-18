// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"net/http"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/proc"
)

var (
	secureThrottle  = flag.Int64("secure-accept-rate", 64, "Maximum number of secure connection accepts per second")
	secureMaxBuffer = flag.Int("secure-max-buffer", 1500, "Maximum number of secure accepts allowed to accumulate")
)

// SecureListen obtains a listener that accepts
// secure connections
func SecureServe(addr string, certFile, keyFile, caFile string) {
	config := tls.Config{}

	// load the server cert / key
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("%s", err)
	}
	config.Certificates = []tls.Certificate{cert}

	// load the ca if necessary
	// FIXME(alainjobart) this doesn't quite work yet, have
	// to investigate
	if caFile != "" {
		config.ClientCAs = x509.NewCertPool()

		pemCerts, err := ioutil.ReadFile(caFile)
		if err != nil {
			log.Fatalf("%s", err)
		}
		if !config.ClientCAs.AppendCertsFromPEM(pemCerts) {
			log.Fatalf("%s", err)
		}

		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	l, err := tls.Listen("tcp", addr, &config)
	if err != nil {
		log.Fatalf("%s", err)
	}
	throttled := NewThrottledListener(l, *secureThrottle, *secureMaxBuffer)
	cl := proc.Published(throttled, "SecureConnections", "SecureAccepts")
	go http.Serve(cl, nil)
}
