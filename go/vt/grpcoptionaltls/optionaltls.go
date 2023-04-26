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
package grpcoptionaltls

import (
	"net"

	"google.golang.org/grpc/credentials"
)

type optionalTLSCreds struct {
	credentials.TransportCredentials
}

func (c *optionalTLSCreds) Clone() credentials.TransportCredentials {
	return New(c.TransportCredentials.Clone())
}

func (c *optionalTLSCreds) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	isTLS, bytes, err := DetectTLS(conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	wc := NewWrappedConn(conn, bytes)
	if isTLS {
		return c.TransportCredentials.ServerHandshake(wc)
	}

	var authInfo = info{
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity},
	}

	return wc, authInfo, nil
}

func New(tc credentials.TransportCredentials) credentials.TransportCredentials {
	return &optionalTLSCreds{TransportCredentials: tc}
}

type info struct {
	credentials.CommonAuthInfo
}

func (info) AuthType() string {
	return "insecure"
}
