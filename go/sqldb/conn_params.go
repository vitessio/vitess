// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqldb defines an interface for low level db connection
package sqldb

// ConnParams contains all the parameters to use to connect to mysql
type ConnParams struct {
	Engine     string `json:"engine"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Uname      string `json:"uname"`
	Pass       string `json:"pass"`
	DbName     string `json:"dbname"`
	UnixSocket string `json:"unix_socket"`
	Charset    string `json:"charset"`
	Flags      uint64 `json:"flags"`

	// The following flags are only used for 'Change Master' command
	// for now (along with flags |= 2048 for CLIENT_SSL)
	SslCa     string `json:"ssl_ca"`
	SslCaPath string `json:"ssl_ca_path"`
	SslCert   string `json:"ssl_cert"`
	SslKey    string `json:"ssl_key"`
}

// capabilityClientSSL is CLIENT_SSL.
// FIXME(alainjobart) when this package is merge with go/mysqlconn,
// use the same constant.
const capabilityClientSSL = 1 << 11

// EnableSSL will set the right flag on the parameters.
func (cp *ConnParams) EnableSSL() {
	cp.Flags |= capabilityClientSSL
}

// SslEnabled returns if SSL is enabled.
func (cp *ConnParams) SslEnabled() bool {
	return (cp.Flags & capabilityClientSSL) > 0
}
