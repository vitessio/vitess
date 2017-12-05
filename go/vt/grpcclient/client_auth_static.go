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

package grpcclient

import (
	"encoding/json"
	"flag"
	"io/ioutil"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	credsFile = flag.String("grpc_auth_static_client_creds", "", "when using grpc_static_auth in the server, this file provides the credentials to use to authenticate with server")
	// StaticAuthClientCreds implements client interface to be able to WithPerRPCCredentials
	_ credentials.PerRPCCredentials = (*StaticAuthClientCreds)(nil)
)

// StaticAuthClientCreds holder for client credentials
type StaticAuthClientCreds struct {
	Username string
	Password string
}

// GetRequestMetadata  gets the request metadata as a map from StaticAuthClientCreds
func (c *StaticAuthClientCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"username": c.Username,
		"password": c.Password,
	}, nil
}

// RequireTransportSecurity indicates whether the credentials requires transport security.
// Given that people can use this with or without TLS, at the moment we are not enforcing
// transport security
func (c *StaticAuthClientCreds) RequireTransportSecurity() bool {
	return false
}

// AppendStaticAuth optionally appends static auth credentials if provided.
func AppendStaticAuth(opts []grpc.DialOption) ([]grpc.DialOption, error) {
	if *credsFile == "" {
		return opts, nil
	}
	data, err := ioutil.ReadFile(*credsFile)
	if err != nil {
		return nil, err
	}
	clientCreds := &StaticAuthClientCreds{}
	err = json.Unmarshal(data, clientCreds)
	if err != nil {
		return nil, err
	}
	creds := grpc.WithPerRPCCredentials(clientCreds)
	opts = append(opts, creds)
	return opts, nil
}

func init() {
	RegisterGRPCDialOptions(AppendStaticAuth)
}
