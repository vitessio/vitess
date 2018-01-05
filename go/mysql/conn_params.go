/*
Copyright 2017 Google Inc.

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

// ConnParams contains all the parameters to use to connect to mysql.
type ConnParams struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Uname      string `json:"uname"`
	Pass       string `json:"pass"`
	DbName     string `json:"dbname"`
	UnixSocket string `json:"unix_socket"`
	Charset    string `json:"charset"`
	Flags      uint64 `json:"flags"`

	// The following SSL flags are only used when flags |= 2048
	// is set (CapabilityClientSSL).
	SslCa     string `json:"ssl_ca"`
	SslCaPath string `json:"ssl_ca_path"`
	SslCert   string `json:"ssl_cert"`
	SslKey    string `json:"ssl_key"`
}

// EnableSSL will set the right flag on the parameters.
func (cp *ConnParams) EnableSSL() {
	cp.Flags |= CapabilityClientSSL
}

// SslEnabled returns if SSL is enabled.
func (cp *ConnParams) SslEnabled() bool {
	return (cp.Flags & CapabilityClientSSL) > 0
}

// EnableClientFoundRows sets the flag for CLIENT_FOUND_ROWS.
func (cp *ConnParams) EnableClientFoundRows() {
	cp.Flags |= CapabilityClientFoundRows
}
