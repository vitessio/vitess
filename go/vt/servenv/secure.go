// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/proc"
)

var (
	SecurePort      = flag.Int("secure-port", 0, "port for the secure server")
	certFile        = flag.String("cert", "", "cert file")
	keyFile         = flag.String("key", "", "key file")
	caCertFile      = flag.String("ca_cert", "", "ca cert file")
	secureThrottle  = flag.Int64("secure-accept-rate", 64, "Maximum number of secure connection accepts per second")
	secureMaxBuffer = flag.Int("secure-max-buffer", 1500, "Maximum number of secure accepts allowed to accumulate")
)

// serverSecurePort obtains a listener that accepts secure connections.
// All of this is based on *SecurePort being non-zero.
func serveSecurePort() {
	if *SecurePort == 0 {
		log.Info("Not listening on secure port")
		return
	}

	config := tls.Config{}

	// load the server cert / key
	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		log.Fatalf("SecureServe.LoadX509KeyPair(%v, %v) failed: %v", *certFile, *keyFile, err)
	}
	config.Certificates = []tls.Certificate{cert}

	// load the ca if necessary
	// FIXME(alainjobart) this doesn't quite work yet, have
	// to investigate
	if *caCertFile != "" {
		config.ClientCAs = x509.NewCertPool()

		pemCerts, err := ioutil.ReadFile(*caCertFile)
		if err != nil {
			log.Fatalf("SecureServe: cannot read ca file %v: %v", *caCertFile, err)
		}
		if !config.ClientCAs.AppendCertsFromPEM(pemCerts) {
			log.Fatalf("SecureServe: AppendCertsFromPEM failed: %v", err)
		}

		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	l, err := tls.Listen("tcp", fmt.Sprintf(":%d", *SecurePort), &config)
	if err != nil {
		log.Fatalf("Error listening on secure port %v: %v", *SecurePort, err)
	}
	log.Infof("Listening on secure port %v", *SecurePort)
	throttled := NewThrottledListener(l, *secureThrottle, *secureMaxBuffer)
	cl := proc.Published(throttled, "SecureConnections", "SecureAccepts")
	go http.Serve(cl, nil)
}
