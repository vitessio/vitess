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
	"context"
	"fmt"
	"net/url"
	"sort"
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

	updates chan grpcresolver.State
	errors  chan error

	ctx    context.Context
	cancel context.CancelFunc
}

func newMockClientConn(minExpectedCalls int) *mockClientConn {
	ctx, cancel := context.WithCancel(context.Background())
	return &mockClientConn{
		updates: make(chan grpcresolver.State, minExpectedCalls),
		errors:  make(chan error, minExpectedCalls),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (cc *mockClientConn) close() {
	cc.cancel()
}

func (cc *mockClientConn) assertUpdateWithin(t testing.TB, timeout time.Duration, expected grpcresolver.State, msgAndArgs ...any) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return assert.Fail(t, "failed to receive update", "did not receive update %v within %v: %s", expected, timeout, ctx.Err())
	case actual := <-cc.updates:
		sort.Slice(expected.Addresses, func(i, j int) bool {
			return expected.Addresses[i].Addr < expected.Addresses[j].Addr
		})
		sort.Slice(actual.Addresses, func(i, j int) bool {
			return actual.Addresses[i].Addr < actual.Addresses[j].Addr
		})

		return assert.Equal(t, expected, actual, msgAndArgs...)
	}
}

func (cc *mockClientConn) assertErrorReportedWithin(t testing.TB, timeout time.Duration, msgAndArgs ...any) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return assert.Fail(t, "failed to receive reported error", "did not receive reported error within %v: %s", timeout, ctx.Err())
	case actual := <-cc.errors:
		return assert.Error(t, actual, msgAndArgs...)
	}
}

func (cc *mockClientConn) UpdateState(state grpcresolver.State) error {
	select {
	case <-cc.ctx.Done():
		return fmt.Errorf("failed to update state; clientconn closed: %w", cc.ctx.Err())
	case cc.updates <- state:
		return nil
	default:
		return fmt.Errorf("%w: failed to update; buffer full", assert.AnError)
	}
}

func (cc *mockClientConn) ReportError(err error) {
	select {
	case <-cc.ctx.Done():
	case cc.errors <- err:
	default:
	}
}

var testopts = Options{
	DiscoveryTimeout:     time.Millisecond * 50,
	MinDiscoveryInterval: 0,
}

func mustBuild(t testing.TB, b *builder, target grpcresolver.Target, cc grpcresolver.ClientConn, opts grpcresolver.BuildOptions) *resolver {
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
	testopts := testopts
	testopts.Discovery = disco

	cc := newMockClientConn(1)
	r := mustBuild(t, &builder{opts: testopts}, grpcresolver.Target{
		URL: url.URL{Host: "vtctld"},
	}, cc, grpcresolver.BuildOptions{})

	r.ResolveNow(grpcresolver.ResolveNowOptions{})

	expectedUpdate := grpcresolver.State{
		Addresses: []grpcresolver.Address{{
			Addr: "one",
		}},
	}
	cc.assertUpdateWithin(t, time.Millisecond*100, expectedUpdate)
	cc.close()
	r.Close()

	disco.Clear()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: "two",
	}, &vtadminpb.Vtctld{
		Hostname: "three",
	})

	cc = newMockClientConn(1)
	r = mustBuild(t, &builder{opts: testopts}, grpcresolver.Target{
		URL: url.URL{Host: "vtctld"},
	}, cc, grpcresolver.BuildOptions{})
	r.ResolveNow(grpcresolver.ResolveNowOptions{})

	expectedUpdate.Addresses = []grpcresolver.Address{
		{
			Addr: "two",
		},
		{
			Addr: "three",
		},
	}
	cc.assertUpdateWithin(t, time.Millisecond*100, expectedUpdate)
	cc.close()
	r.Close()
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
	testopts := testopts
	testopts.Discovery = disco

	cc := newMockClientConn(1)
	opts := testopts
	opts.DiscoveryTags = []string{"tag2"}
	r := mustBuild(t, &builder{opts: opts}, grpcresolver.Target{
		URL: url.URL{Host: "vtgate"},
	}, cc, grpcresolver.BuildOptions{})

	expectedUpdate := grpcresolver.State{
		Addresses: []grpcresolver.Address{{
			Addr: "two",
		}},
	}

	r.ResolveNow(grpcresolver.ResolveNowOptions{})
	cc.assertUpdateWithin(t, time.Millisecond*100, expectedUpdate)
	cc.close()
	r.Close()
}

func TestResolveEmptyList(t *testing.T) {
	t.Parallel()

	disco := fakediscovery.New()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: "one",
	})
	testopts := testopts
	testopts.Discovery = disco

	cc := newMockClientConn(1)
	r := mustBuild(t,
		&builder{opts: testopts}, grpcresolver.Target{
			URL: url.URL{Host: "vtgate"}, // we only have vtctlds
		}, cc, grpcresolver.BuildOptions{},
	)

	expectedUpdate := grpcresolver.State{
		Addresses: []grpcresolver.Address{},
	}

	r.ResolveNow(grpcresolver.ResolveNowOptions{})
	cc.assertUpdateWithin(t, time.Millisecond*50, expectedUpdate, "resolver should still call cc.UpdateState with empty host list")
	cc.close()
	r.Close()

	disco.AddTaggedGates(nil, &vtadminpb.VTGate{
		Hostname: "gate:one",
	})

	cc = newMockClientConn(1)
	r = mustBuild(t,
		&builder{opts: testopts}, grpcresolver.Target{
			URL: url.URL{Host: "vtgate"},
		}, cc, grpcresolver.BuildOptions{},
	)

	expectedUpdate.Addresses = []grpcresolver.Address{{
		Addr: "gate:one",
	}}

	r.ResolveNow(grpcresolver.ResolveNowOptions{})
	cc.assertUpdateWithin(t, time.Millisecond*50, expectedUpdate)
	cc.close()
	r.Close()
}

func TestBuild(t *testing.T) {
	t.Parallel()

	disco := fakediscovery.New()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: "vtctld:one",
	})
	testopts := testopts
	testopts.Discovery = disco

	b := &builder{opts: testopts}

	tests := []struct {
		name               string
		target             grpcresolver.Target
		shouldErr          bool
		expectUpdateWithin time.Duration
		expectedUpdate     grpcresolver.State
		msgAndArgs         []any
	}{
		{
			name: "vtctld",
			target: grpcresolver.Target{
				URL: url.URL{Host: "vtctld"},
			},
			expectedUpdate: grpcresolver.State{
				Addresses: []grpcresolver.Address{{
					Addr: "vtctld:one",
				}},
			},
		},
		{
			name: "vtgate",
			target: grpcresolver.Target{
				URL: url.URL{Host: "vtgate"},
			},
			expectedUpdate: grpcresolver.State{Addresses: []grpcresolver.Address{}},
			msgAndArgs:     []any{"resolver should still call UpdateState on clientconn (no vtgates in discovery)"},
		},
		{
			name: "bad authority",
			target: grpcresolver.Target{
				URL: url.URL{Host: "unsupported"},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cc := newMockClientConn(1)
			r, err := b.Build(tt.target, cc, grpcresolver.BuildOptions{})
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			timeout := tt.expectUpdateWithin
			if timeout == 0 {
				timeout = time.Millisecond * 50
			}

			cc.assertUpdateWithin(t, timeout, tt.expectedUpdate, tt.msgAndArgs...)

			cc.close()
			r.Close()
		})
	}
}
