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

package grpcclient

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"vitess.io/vitess/go/vt/servenv"
)

var (
	credsFile string // registered as --grpc_auth_static_client_creds in RegisterFlags
	// StaticAuthClientCreds implements client interface to be able to WithPerRPCCredentials
	_ credentials.PerRPCCredentials = (*StaticAuthClientCreds)(nil)

	clientCreds        *StaticAuthClientCreds
	clientCredsCancel  context.CancelFunc
	clientCredsErr     error
	clientCredsMu      sync.Mutex
	clientCredsSigChan chan os.Signal
)

// StaticAuthClientCreds holder for client credentials.
type StaticAuthClientCreds struct {
	Username string
	Password string
}

// GetRequestMetadata gets the request metadata as a map from StaticAuthClientCreds.
func (c *StaticAuthClientCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"username": c.Username,
		"password": c.Password,
	}, nil
}

// RequireTransportSecurity indicates whether the credentials requires transport security.
// Given that people can use this with or without TLS, at the moment we are not enforcing
// transport security.
func (c *StaticAuthClientCreds) RequireTransportSecurity() bool {
	return false
}

// AppendStaticAuth optionally appends static auth credentials if provided.
func AppendStaticAuth(opts []grpc.DialOption) ([]grpc.DialOption, error) {
	creds, err := getStaticAuthCreds()
	if err != nil {
		return nil, err
	}
	if creds != nil {
		grpcCreds := grpc.WithPerRPCCredentials(creds)
		opts = append(opts, grpcCreds)
	}
	return opts, nil
}

// ResetStaticAuth resets the static auth credentials.
func ResetStaticAuth() {
	clientCredsMu.Lock()
	defer clientCredsMu.Unlock()
	if clientCredsCancel != nil {
		clientCredsCancel()
		clientCredsCancel = nil
	}
	clientCreds = nil
	clientCredsErr = nil
}

// getStaticAuthCreds returns the static auth creds and error.
func getStaticAuthCreds() (*StaticAuthClientCreds, error) {
	clientCredsMu.Lock()
	defer clientCredsMu.Unlock()
	if credsFile != "" && clientCreds == nil {
		var ctx context.Context
		ctx, clientCredsCancel = context.WithCancel(context.Background())
		go handleClientCredsSignals(ctx)
		clientCreds, clientCredsErr = loadStaticAuthCredsFromFile(credsFile)
	}
	return clientCreds, clientCredsErr
}

// handleClientCredsSignals handles signals to reload client creds.
func handleClientCredsSignals(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-clientCredsSigChan:
			if newCreds, err := loadStaticAuthCredsFromFile(credsFile); err == nil {
				clientCredsMu.Lock()
				clientCreds = newCreds
				clientCredsErr = err
				clientCredsMu.Unlock()
			}
		}
	}
}

// loadStaticAuthCredsFromFile loads static auth credentials from a file.
func loadStaticAuthCredsFromFile(path string) (*StaticAuthClientCreds, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	creds := &StaticAuthClientCreds{}
	err = json.Unmarshal(data, creds)
	return creds, err
}

func init() {
	servenv.OnInit(func() {
		clientCredsSigChan = make(chan os.Signal, 1)
		signal.Notify(clientCredsSigChan, syscall.SIGHUP)
		_, _ = getStaticAuthCreds() // preload static auth credentials
	})
	RegisterGRPCDialOptions(AppendStaticAuth)
}
