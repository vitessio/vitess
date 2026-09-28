/*
Copyright 2026 The Vitess Authors.

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
	"crypto/tls"
	"net"
	"sync"

	"google.golang.org/grpc/credentials"

	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttls"
)

// reloadingCreds are TLS transport credentials that build their config
// again once vttls reloaded the files it was built from (see
// vttls.ReloadCachedFiles). A gRPC connection keeps its credentials
// for its lifetime and reconnects with them, so it would otherwise
// present, and verify against, the files it was dialed with forever.
type reloadingCreds struct {
	build func() (*tls.Config, error)

	mu sync.Mutex
	// generation is the vttls.CachedFilesGeneration creds was built
	// at, and failedGeneration the last one creds failed to be built
	// again at, which was logged.
	generation, failedGeneration uint64
	creds                        credentials.TransportCredentials
}

func newReloadingCreds(build func() (*tls.Config, error)) (*reloadingCreds, error) {
	generation := vttls.CachedFilesGeneration()
	config, err := build()
	if err != nil {
		return nil, err
	}
	return &reloadingCreds{build: build, generation: generation, creds: credentials.NewTLS(config)}, nil
}

// current returns the credentials built from what the files hold now.
// When they cannot be built, for instance while a file is being
// replaced, the previous ones stay in use, and building them is tried
// again on the next handshake: nothing may reload the files again once
// the file is back as it was.
func (c *reloadingCreds) current() credentials.TransportCredentials {
	generation := vttls.CachedFilesGeneration()
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		config, err := c.build()
		if err != nil {
			if c.failedGeneration != generation {
				c.failedGeneration = generation
				log.Error(vterrors.Wrapf(err, "cannot build the gRPC client's TLS config from the reloaded files; its connections keep using the previous one, and building it is tried again on their next handshake").Error())
			}
		} else {
			c.creds = credentials.NewTLS(config)
			c.generation = generation
		}
	}
	return c.creds
}

func (c *reloadingCreds) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return c.current().ClientHandshake(ctx, authority, rawConn)
}

func (c *reloadingCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return c.current().ServerHandshake(rawConn)
}

func (c *reloadingCreds) Info() credentials.ProtocolInfo {
	return c.current().Info()
}

func (c *reloadingCreds) Clone() credentials.TransportCredentials {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &reloadingCreds{build: c.build, generation: c.generation, creds: c.creds.Clone()}
}

// OverrideServerName is deprecated in gRPC, and not supported: the
// server name is set with the name the credentials are built with.
func (c *reloadingCreds) OverrideServerName(string) error {
	return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "OverrideServerName is not supported")
}
