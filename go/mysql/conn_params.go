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

package mysql

import (
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vttls"
)

// ConnParams contains all the parameters to use to connect to mysql.
type ConnParams struct {
	Host       string
	Port       int
	Uname      string
	Pass       string
	DbName     string
	UnixSocket string
	Charset    collations.ID
	Flags      uint64
	Flavor     string

	// The following SSL flags control the SSL behavior.
	//
	// Not setting this value implies preferred mode unless
	// the CapabilityClientSSL bit is set in db_flags. In the
	// flag is set, it ends up equivalent to verify_identity mode.
	SslMode          vttls.SslMode
	SslCa            string
	SslCaPath        string
	SslCert          string
	SslCrl           string
	SslKey           string
	TLSMinVersion    string
	ServerName       string
	ConnectTimeoutMs uint64

	// The following is only set to force the client to connect without
	// using CapabilityClientDeprecateEOF
	DisableClientDeprecateEOF bool

	// EnableQueryInfo sets whether the results from queries performed by this
	// connection should include the 'info' field that MySQL usually returns. This 'info'
	// field usually contains a human-readable text description of the executed query
	// for informative purposes. It has no programmatic value. Returning this field is
	// disabled by default.
	EnableQueryInfo bool

	// FlushDelay is the delay after which buffered response will be flushed to the client.
	FlushDelay time.Duration

	TruncateErrLen int
}

// EnableSSL will set the right flag on the parameters.
func (cp *ConnParams) EnableSSL() {
	cp.SslMode = vttls.VerifyIdentity
}

// SslEnabled returns if SSL is enabled. If the effective
// ssl mode is preferred, it checks the unix socket and
// hostname to see if we're not connecting to local MySQL.
func (cp *ConnParams) SslEnabled() bool {
	mode := cp.EffectiveSslMode()
	// Follow MySQL behavior to not enable SSL if it's
	// preferred but we're using a Unix socket.
	if mode == vttls.Preferred && cp.UnixSocket != "" {
		return false
	}
	return mode != vttls.Disabled
}

// EnableClientFoundRows sets the flag for CLIENT_FOUND_ROWS.
func (cp *ConnParams) EnableClientFoundRows() {
	cp.Flags |= CapabilityClientFoundRows
}

// SslRequired returns whether the connection parameters
// define that SSL is a requirement. If SslMode is set, it uses
// that to determine this, if it's not set it falls back to
// the legacy db_flags behavior.
func (cp *ConnParams) SslRequired() bool {
	mode := cp.EffectiveSslMode()
	return mode != vttls.Disabled && mode != vttls.Preferred
}

// EffectiveSslMode computes the effective SslMode. If SslMode
// is explicitly set, it uses that to determine this, if it's
// not set it falls back to the legacy db_flags behavior.
func (cp *ConnParams) EffectiveSslMode() vttls.SslMode {
	if cp.SslMode == "" {
		if (cp.Flags & CapabilityClientSSL) > 0 {
			return vttls.VerifyIdentity
		}
		// Old behavior is Disabled so keep that for now.
		return vttls.Disabled
	}
	return cp.SslMode
}
