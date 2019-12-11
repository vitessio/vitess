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
	"flag"
	"fmt"
	"net"

	"vitess.io/vitess/go/vt/log"
)

var clientcertAuthMethod = flag.String("mysql_clientcert_auth_method", MysqlClearPassword, "client-side authentication method to use. Supported values: mysql_clear_password, dialog.")

type AuthServerClientCert struct {
	Method string
}

// Init is public so it can be called from plugin_auth_clientcert.go (go/cmd/vtgate)
func InitAuthServerClientCert() {
	if flag.CommandLine.Lookup("mysql_server_ssl_ca").Value.String() == "" {
		log.Info("Not configuring AuthServerClientCert because mysql_server_ssl_ca is empty")
		return
	}
	if *clientcertAuthMethod != MysqlClearPassword && *clientcertAuthMethod != MysqlDialog {
		log.Exitf("Invalid mysql_clientcert_auth_method value: only support mysql_clear_password or dialog")
	}
	ascc := &AuthServerClientCert{
		Method: *clientcertAuthMethod,
	}
	RegisterAuthServerImpl("clientcert", ascc)
}

// AuthMethod is part of the AuthServer interface.
func (ascc *AuthServerClientCert) AuthMethod(user string) (string, error) {
	return ascc.Method, nil
}

// Salt is not used for this plugin.
func (ascc *AuthServerClientCert) Salt() ([]byte, error) {
	return NewSalt()
}

// ValidateHash is unimplemented.
func (ascc *AuthServerClientCert) ValidateHash(salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (Getter, error) {
	panic("unimplemented")
}

// Negotiate is part of the AuthServer interface.
func (ascc *AuthServerClientCert) Negotiate(c *Conn, user string, remoteAddr net.Addr) (Getter, error) {
	// This code depends on the fact that golang's tls server enforces client cert verification.
	// Note that the -mysql_server_ssl_ca flag must be set in order for the vtgate to accept client certs.
	// If not set, the vtgate will effectively deny all incoming mysql connections, since they will all lack certificates.
	// For more info, check out go/vt/vtttls/vttls.go
	certs := c.GetTLSClientCerts()
	if len(certs) == 0 {
		return nil, fmt.Errorf("no client certs for connection ID %v", c.ConnectionID)
	}

	if _, err := AuthServerReadPacketString(c); err != nil {
		return nil, err
	}

	commonName := certs[0].Subject.CommonName

	if user != commonName {
		return nil, fmt.Errorf("MySQL connection username '%v' does not match client cert common name '%v'", user, commonName)
	}

	return &StaticUserData{
		username: commonName,
		groups:   certs[0].DNSNames,
	}, nil
}
