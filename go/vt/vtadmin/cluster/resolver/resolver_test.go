/*
Copyright 2022 The Vitess Authors.

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

package resolver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpcresolver "google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type mockClientConn struct {
	grpcresolver.ClientConn
	Addrs             []grpcresolver.Address
	UpdateStateCalled bool
	ReportedError     error
}

func (cc *mockClientConn) UpdateState(state grpcresolver.State) error {
	cc.UpdateStateCalled = true
	cc.Addrs = state.Addresses
	return nil
}

func (cc *mockClientConn) ReportError(err error) { cc.ReportedError = err }

var testopts = Options{
	DiscoveryTimeout: time.Millisecond * 50,
}

func mustBuild(t *testing.T, b *builder, target grpcresolver.Target, cc grpcresolver.ClientConn, opts grpcresolver.BuildOptions) *resolver {
	t.Helper()

	r, err := b.build(target, cc, opts)
	require.NoError(t, err)

	return r
}

func TestResolveNow(t *testing.T) {
	t.Parallel()

	disco := fakediscovery.New()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: "one",
	})

	cc := mockClientConn{}
	r := mustBuild(t, &builder{disco: disco, opts: testopts}, grpcresolver.Target{
		Authority: "vtctld",
	}, &cc, grpcresolver.BuildOptions{})

	r.ResolveNow(grpcresolver.ResolveNowOptions{})

	assert.ElementsMatch(t, cc.Addrs, []grpcresolver.Address{
		{
			Addr: "one",
		},
	})

	disco.Clear()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: "two",
	}, &vtadminpb.Vtctld{
		Hostname: "three",
	})

	r.ResolveNow(grpcresolver.ResolveNowOptions{})

	assert.ElementsMatch(t, cc.Addrs, []grpcresolver.Address{
		{
			Addr: "two",
		},
		{
			Addr: "three",
		},
	})
}

func TestResolveWithTags(t *testing.T) {
	t.Parallel()

	disco := fakediscovery.New()
	disco.AddTaggedGates([]string{"tag1"}, &vtadminpb.VTGate{
		Hostname: "one",
	})
	disco.AddTaggedGates([]string{"tag2"}, &vtadminpb.VTGate{
		Hostname: "two",
	})

	cc := mockClientConn{}
	r := mustBuild(t, &builder{disco: disco, opts: testopts}, grpcresolver.Target{
		Authority: "vtgate",
	}, &cc, grpcresolver.BuildOptions{})
	r.opts.DiscoveryTags = []string{"tag2"}

	r.ResolveNow(grpcresolver.ResolveNowOptions{})

	assert.ElementsMatch(t, cc.Addrs, []grpcresolver.Address{
		{
			Addr: "two",
		},
	})
}

func TestResolveEmptyList(t *testing.T) {
	t.Parallel()

	disco := fakediscovery.New()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: "one",
	})

	cc := mockClientConn{}
	r := mustBuild(t,
		&builder{disco: disco, opts: testopts}, grpcresolver.Target{
			Authority: "vtgate", // we only have vtctlds
		}, &cc, grpcresolver.BuildOptions{},
	)

	r.ResolveNow(grpcresolver.ResolveNowOptions{})

	assert.Empty(t, cc.Addrs, "ClientConn should have no addresses")
	assert.True(t, cc.UpdateStateCalled, "resolver should still call cc.UpdateState with empty host list")

	disco.AddTaggedGates(nil, &vtadminpb.VTGate{
		Hostname: "gate:one",
	})

	r.ResolveNow(grpcresolver.ResolveNowOptions{})
	assert.ElementsMatch(t, cc.Addrs, []grpcresolver.Address{
		{
			Addr: "gate:one",
		},
	})
}

func TestBuild(t *testing.T) {
	t.Parallel()

	disco := fakediscovery.New()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: "vtctld:one",
	})

	b := &builder{disco: disco, opts: testopts}

	tests := []struct {
		name      string
		target    grpcresolver.Target
		shouldErr bool
		assertion func(t *testing.T, cc *mockClientConn)
	}{
		{
			name: "vtctld",
			target: grpcresolver.Target{
				Authority: "vtctld",
			},
			assertion: func(t *testing.T, cc *mockClientConn) {
				assert.ElementsMatch(t, cc.Addrs, []grpcresolver.Address{
					{
						Addr: "vtctld:one",
					},
				})
			},
		},
		{
			name: "vtgate",
			target: grpcresolver.Target{
				Authority: "vtgate",
			},
			assertion: func(t *testing.T, cc *mockClientConn) {
				assert.Empty(t, cc.Addrs, "resolver should not add addresses to clientconn (no vtgates in discovery)")
				assert.True(t, cc.UpdateStateCalled, "resolver should still call UpdateState on clientconn (no vtgates in discovery)")
			},
		},
		{
			name: "bad authority",
			target: grpcresolver.Target{
				Authority: "unsupported",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cc := &mockClientConn{}
			_, err := b.Build(tt.target, cc, grpcresolver.BuildOptions{})
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			func() {
				t.Helper()
				tt.assertion(t, cc)
			}()
		})
	}
}
