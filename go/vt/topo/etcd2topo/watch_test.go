/*
Copyright 2024 The Vitess Authors.

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

package etcd2topo

import (
	"context"
	"fmt"
	"io"
	"net"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/testfiles"
	"vitess.io/vitess/go/vt/topo"
)

// TestWatchTopoVersion tests how the topo.Version values work within the etcd2topo
// Watch implementation. Today, those logical versions are based on the key's
// ModRevision value, which is a monotonically increasing int64 value. See
// https://github.com/vitessio/vitess/pull/15847 for additional details and the
// current reasoning behing using ModRevision. This can be changed in the future
// but should be done so intentionally, thus this test ensures we don't change the
// behavior accidentally/uinintentionally.
func TestWatchTopoVersion(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	etcdServerAddr, _ := startEtcd(t, testfiles.GoVtTopoEtcd2topoPort, testfiles.GoVtTopoEtcd2topoPeerPort)
	root := "/vitess/test"
	name := "testkey"
	path := path.Join(root, name)
	value := "testval"
	// We use these two variables to ensure that we receive all of the changes in
	// our watch.
	changesMade := atomic.Int64{} // This is accessed across goroutines
	changesSeen := int64(0)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdServerAddr},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	serverRunningCh := make(chan struct{})
	server := &Server{
		cli:     client,
		root:    root,
		running: serverRunningCh,
	}
	defer server.Close()

	// Create the key as the vitess topo server requires that it exist before you
	// can watch it (the lower level etcd watch does not require this).
	client.Put(ctx, path, fmt.Sprintf("%s-%d", value, changesMade.Load()))
	changesMade.Add(1)

	var data <-chan *topo.WatchData
	_, data, err = server.Watch(ctx, name)
	require.NoError(t, err, "Server.Watch() error = %v", err)

	// Coordinate between the goroutines on the delete so that we don't miss
	// N changes when restarting the watch.
	token := make(chan struct{})
	defer close(token)

	// Run a goroutine that updates the key we're watching.
	go func() {
		cur := changesMade.Load() + 1
		batchSize := int64(10)
		for i := cur; i <= cur+batchSize; i++ {
			client.Put(ctx, path, fmt.Sprintf("%s-%d", value, i))
			changesMade.Add(1)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		// Delete the key to ensure that our version continues to be monotonically
		// increasing.
		client.Delete(ctx, path)
		changesMade.Add(1)
		// Let the main goroutine process the delete and restart the watch before
		// we make more changes.
		token <- struct{}{}
		cur = changesMade.Load() + 1
		for i := cur; i <= cur+batchSize; i++ {
			client.Put(ctx, path, fmt.Sprintf("%s-%d", value, i))
			changesMade.Add(1)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	// When using ModRevision as the logical key version, the Revision is initially
	// 1 as we're at the first change of the keyspace (it has been created). This
	// means that the first time we receive a change in the watch, we should expect
	// the key's topo.Version to be 2 as it's the second change to the keyspace.
	// We start with 1 as we increment this every time we receive a change in the
	// watch.
	expectedVersion := int64(1)

	// Consider the test done when we've been watching the key for 10 seconds. We
	// should receive all of the changes made within 1 second but we allow for a lot
	// of extra time to prevent flakiness when the host is very slow for any reason.
	watchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		select {
		case <-watchCtx.Done():
			require.Equal(t, changesMade.Load(), changesSeen, "expected %d changes, got %d", changesMade.Load(), changesSeen)
			return // Success, we're done
		case <-ctx.Done():
			require.FailNow(t, "test context cancelled")
		case <-serverRunningCh:
			require.FailNow(t, "topo server is no longer running")
		case wd := <-data:
			changesSeen++
			expectedVersion++
			if wd.Err != nil {
				if topo.IsErrType(wd.Err, topo.NoNode) {
					// This was our delete. We'll restart the watch.
					// Note that the lower level etcd watch doesn't treat delete as
					// any special kind of change/event, it's another change to the
					// key, but our topo server Watch treats this as an implicit end
					// of the watch and it terminates it.
					// We create the key again as the vitess topo server requires
					// that it exist before watching it.
					client.Put(ctx, path, fmt.Sprintf("%s-%d", value, changesMade.Load()))
					changesMade.Add(1)
					_, data, err = server.Watch(ctx, name)
					require.NoError(t, err, "Server.Watch() error = %v", err)
					<-token // Tell the goroutine making changes to continue
					continue
				}
				require.FailNow(t, "unexpected error in watch data", "error: %v", wd.Err)
			}
			gotVersion := int64(wd.Version.(EtcdVersion))
			require.Equal(t, expectedVersion, gotVersion, "expected version %d, got %d", expectedVersion, gotVersion)
		}
	}
}

// TestWatchAfterCompaction tests that a watch survives resuming from a
// compacted revision. When the client reconnects after the revision it was
// watching from has been compacted, the watch reads the node again and keeps
// going instead of failing.
func TestWatchAfterCompaction(t *testing.T) {
	etcdServerAddr, _ := startEtcd(t, testfiles.GoVtTopoEtcd2topoPort, testfiles.GoVtTopoEtcd2topoPeerPort)
	proxy := newPausableProxy(t, strings.TrimPrefix(etcdServerAddr, "http://"))

	// The watch goes through the proxy, while the test changes etcd directly.
	admin, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdServerAddr},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { admin.Close() })

	put := func(value string) func(ctx context.Context, nodePath string) error {
		return func(ctx context.Context, nodePath string) error {
			_, err := admin.Put(ctx, nodePath, value)
			return err
		}
	}
	tests := []struct {
		name string
		// change is applied while the watch is disconnected, if set.
		change func(ctx context.Context, nodePath string) error
		// deleted means the watch is expected to end with NoNode. Otherwise
		// it is expected to deliver "v2" and then keep delivering changes.
		deleted bool
	}{
		{
			name: "unchanged while disconnected",
		},
		{
			name:   "updated while disconnected",
			change: put("v2"),
		},
		{
			name: "deleted while disconnected",
			change: func(ctx context.Context, nodePath string) error {
				_, err := admin.Delete(ctx, nodePath)
				return err
			},
			deleted: true,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			client, err := clientv3.New(clientv3.Config{
				Endpoints:   []string{proxy.addr()},
				DialTimeout: 5 * time.Second,
			})
			require.NoError(t, err)
			server := &Server{
				cli:     client,
				root:    "/vitess/test",
				running: make(chan struct{}),
			}
			t.Cleanup(server.Close)

			name := "key" + strconv.Itoa(i)
			nodePath := path.Join(server.root, name)
			_, err = admin.Put(ctx, nodePath, "v1")
			require.NoError(t, err)

			current, changes, err := server.Watch(ctx, name)
			require.NoError(t, err)
			require.Equal(t, "v1", string(current.Contents))

			nextChange := func() *topo.WatchData {
				var wd *topo.WatchData
				assert.Eventually(t, func() bool {
					select {
					case wd = <-changes:
						return true
					default:
						return false
					}
				}, 30*time.Second, 10*time.Millisecond)
				require.NotNil(t, wd)
				return wd
			}

			// Disconnect the watch, then compact away the revision it resumes
			// from.
			proxy.setPaused(true)
			if tt.change != nil {
				require.NoError(t, tt.change(ctx, nodePath))
			}
			var resp *clientv3.PutResponse
			for j := range 5 {
				resp, err = admin.Put(ctx, path.Join(server.root, "other"), strconv.Itoa(j))
				require.NoError(t, err)
			}
			_, err = admin.Compact(ctx, resp.Header.Revision)
			require.NoError(t, err)
			proxy.setPaused(false)

			if tt.deleted {
				wd := nextChange()
				assert.True(t, topo.IsErrType(wd.Err, topo.NoNode), "expected NoNode, got %v", wd.Err)
				return
			}

			if tt.change == nil {
				// Wait for the client to reconnect, so that the watch resumes
				// before the node changes.
				_, err = client.Get(ctx, nodePath)
				require.NoError(t, err)
				require.NoError(t, put("v2")(ctx, nodePath))
			}
			wd := nextChange()
			require.NoError(t, wd.Err)
			assert.Equal(t, "v2", string(wd.Contents))

			// The watch keeps delivering changes.
			require.NoError(t, put("v3")(ctx, nodePath))
			wd = nextChange()
			require.NoError(t, wd.Err)
			assert.Equal(t, "v3", string(wd.Contents))
		})
	}
}

// pausableProxy forwards TCP connections to target. While paused, it closes
// all open connections and refuses new ones.
type pausableProxy struct {
	target   string
	listener net.Listener

	mu     sync.Mutex
	paused bool
	conns  map[net.Conn]struct{}
}

func newPausableProxy(t *testing.T, target string) *pausableProxy {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &pausableProxy{
		target:   target,
		listener: listener,
		conns:    make(map[net.Conn]struct{}),
	}
	go p.serve()
	t.Cleanup(func() {
		listener.Close()
		p.setPaused(true)
	})
	return p
}

func (p *pausableProxy) addr() string {
	return p.listener.Addr().String()
}

func (p *pausableProxy) serve() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			conn.Close()
			continue
		}

		p.mu.Lock()
		if p.paused {
			p.mu.Unlock()
			conn.Close()
			upstream.Close()
			continue
		}
		p.conns[conn] = struct{}{}
		p.conns[upstream] = struct{}{}
		p.mu.Unlock()

		pipe := func(dst, src net.Conn) {
			_, _ = io.Copy(dst, src)
			dst.Close()
			src.Close()
		}
		go pipe(conn, upstream)
		go pipe(upstream, conn)
	}
}

func (p *pausableProxy) setPaused(paused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.paused = paused
	if paused {
		for conn := range p.conns {
			conn.Close()
		}
		clear(p.conns)
	}
}
