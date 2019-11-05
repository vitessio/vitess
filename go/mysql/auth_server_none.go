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
	"net"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// AuthServerNone takes all comers.
// It's meant to be used for testing and prototyping.
// With this config, you can connect to a local vtgate using
// the following command line: 'mysql -P port -h ::'.
// It only uses MysqlNativePassword method.
type AuthServerNone struct{}

// AuthMethod is part of the AuthServer interface.
// We always return MysqlNativePassword.
func (a *AuthServerNone) AuthMethod(user string) (string, error) {
	return MysqlNativePassword, nil
}

// Salt makes salt
func (a *AuthServerNone) Salt() ([]byte, error) {
	return NewSalt()
}

// ValidateHash validates hash
func (a *AuthServerNone) ValidateHash(salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (Getter, error) {
	return &NoneGetter{}, nil
}

// Negotiate is part of the AuthServer interface.
// It will never be called.
func (a *AuthServerNone) Negotiate(c *Conn, user string, remotAddr net.Addr) (Getter, error) {
	panic("Negotiate should not be called as AuthMethod returned mysql_native_password")
}

func init() {
	RegisterAuthServerImpl("none", &AuthServerNone{})
}

// NoneGetter holds the empty string
type NoneGetter struct{}

// Get returns the empty string
func (ng *NoneGetter) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: "userData1"}
}
