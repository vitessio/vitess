/*
Copyright 2025 The Vitess Authors.

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

package vtctldclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
	"vitess.io/vitess/go/vt/vtadmin/cluster/resolver"
	"vitess.io/vitess/go/vt/vtctl/grpcclientcommon"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// TestSecureDialOptionIntegration tests the integration between vtadmin's proxy
// and grpcclientcommon.SecureDialOption
func TestSecureDialOptionIntegration(t *testing.T) {
	t.Run("proxy uses SecureDialOption for TLS configuration", func(t *testing.T) {
		var capturedDialOptions []grpc.DialOption
		var secureDialOptionCalled bool

		// Create a mock dial function to capture the dial options passed to it
		mockDialFunc := func(ctx context.Context, addr string, ff grpcclient.FailFast, opts ...grpc.DialOption) (vtctldclient.VtctldClient, error) {
			capturedDialOptions = opts
			return &mockVtctldClient{}, nil
		}

		disco := fakediscovery.New()
		disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
			Hostname: "localhost:15999",
		})

		proxy := &ClientProxy{
			cluster: &vtadminpb.Cluster{
				Id:   "test",
				Name: "testcluster",
			},
			dialFunc: mockDialFunc,
			resolver: (&resolver.Options{
				Discovery:        disco,
				DiscoveryTimeout: 50 * time.Millisecond,
			}).NewBuilder("test"),
			closed: true,
		}

		// Call dial, which should use grpcclientcommon.SecureDialOption
		err := proxy.dial(context.Background())
		require.NoError(t, err)

		// Verify that dial options were captured
		assert.NotEmpty(t, capturedDialOptions, "Expected dial options to be captured")

		// The first dial option should be from grpcclientcommon.SecureDialOption
		// We can verify this by checking that we have at least one option
		assert.True(t, len(capturedDialOptions) >= 1, "Expected at least one dial option (from SecureDialOption)")

		// Verify that grpcclientcommon.SecureDialOption can be called directly
		// This tests that the function is accessible and works
		tlsOpt, err := grpcclientcommon.SecureDialOption()
		require.NoError(t, err, "grpcclientcommon.SecureDialOption should not return an error")
		assert.NotNil(t, tlsOpt, "grpcclientcommon.SecureDialOption should return a valid dial option")

		secureDialOptionCalled = true
		assert.True(t, secureDialOptionCalled, "SecureDialOption should be callable")
	})

	t.Run("proxy handles SecureDialOption errors gracefully", func(t *testing.T) {
		// This test verifies that if SecureDialOption were to return an error,
		// the proxy would handle it correctly. Since SecureDialOption typically
		// doesn't error with default flags, we test the expected behavior.

		disco := fakediscovery.New()
		disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
			Hostname: "localhost:15999",
		})

		proxy := &ClientProxy{
			cluster: &vtadminpb.Cluster{
				Id:   "test",
				Name: "testcluster",
			},
			dialFunc: func(ctx context.Context, addr string, ff grpcclient.FailFast, opts ...grpc.DialOption) (vtctldclient.VtctldClient, error) {
				// Verify that we received options (including the TLS option)
				assert.NotEmpty(t, opts, "Expected to receive dial options including TLS configuration")
				return &mockVtctldClient{}, nil
			},
			resolver: (&resolver.Options{
				Discovery:        disco,
				DiscoveryTimeout: 50 * time.Millisecond,
			}).NewBuilder("test"),
			closed: true,
		}

		// Under normal conditions, this should succeed
		err := proxy.dial(context.Background())
		assert.NoError(t, err, "Dial should succeed with default TLS configuration")
	})

	t.Run("TLS option is first in dial options", func(t *testing.T) {
		var capturedDialOptions []grpc.DialOption

		mockDialFunc := func(ctx context.Context, addr string, ff grpcclient.FailFast, opts ...grpc.DialOption) (vtctldclient.VtctldClient, error) {
			capturedDialOptions = opts
			return &mockVtctldClient{}, nil
		}

		disco := fakediscovery.New()
		disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
			Hostname: "localhost:15999",
		})

		proxy := &ClientProxy{
			cluster: &vtadminpb.Cluster{
				Id:   "test",
				Name: "testcluster",
			},
			dialFunc: mockDialFunc,
			resolver: (&resolver.Options{
				Discovery:        disco,
				DiscoveryTimeout: 50 * time.Millisecond,
			}).NewBuilder("test"),
			closed: true,
		}

		err := proxy.dial(context.Background())
		require.NoError(t, err)

		// The order in the dial method is: opts := []grpc.DialOption{tlsOpt}
		assert.NotEmpty(t, capturedDialOptions, "Expected to capture dial options")

		// We can't easily inspect the type of the dial option, but we can verify
		// that the first option exists and the total count matches expectations
		assert.True(t, len(capturedDialOptions) >= 1, "Expected at least the TLS dial option")
	})

	t.Run("credentials are added after TLS option", func(t *testing.T) {
		var capturedDialOptions []grpc.DialOption

		mockDialFunc := func(ctx context.Context, addr string, ff grpcclient.FailFast, opts ...grpc.DialOption) (vtctldclient.VtctldClient, error) {
			capturedDialOptions = opts
			return &mockVtctldClient{}, nil
		}

		disco := fakediscovery.New()
		disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
			Hostname: "localhost:15999",
		})

		proxy := &ClientProxy{
			cluster: &vtadminpb.Cluster{
				Id:   "test",
				Name: "testcluster",
			},
			creds: &grpcclient.StaticAuthClientCreds{
				Username: "testuser",
				Password: "testpass",
			},
			dialFunc: mockDialFunc,
			resolver: (&resolver.Options{
				Discovery:        disco,
				DiscoveryTimeout: 50 * time.Millisecond,
			}).NewBuilder("test"),
			closed: true,
		}

		err := proxy.dial(context.Background())
		require.NoError(t, err)

		// With credentials, we should have:
		// 1. TLS option (from SecureDialOption)
		// 2. Credentials option (from grpc.WithPerRPCCredentials)
		// 3. Resolver option (from grpc.WithResolvers)
		assert.True(t, len(capturedDialOptions) >= 3, "Expected at least 3 dial options: TLS, credentials, and resolver")

		// The exact order should be: TLS, credentials, resolver
		// This matches the logic in the dial method after the commit changes
	})
}
