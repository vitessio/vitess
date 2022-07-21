/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package base

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

var defaultTimeout = time.Second

// SetupHTTPClient creates a simple HTTP client with timeout
func SetupHTTPClient(httpTimeout time.Duration) *http.Client {
	if httpTimeout == 0 {
		httpTimeout = defaultTimeout
	}
	httpTransport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: false},
		DialContext: (&net.Dialer{
			Timeout:   httpTimeout,
			KeepAlive: httpTimeout,
			DualStack: true,
		}).DialContext,
		ResponseHeaderTimeout: httpTimeout,
	}
	httpClient := &http.Client{Transport: httpTransport}

	return httpClient
}
