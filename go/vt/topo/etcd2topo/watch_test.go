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
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/test/utils"
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
	etcdServerAddr, _ := startEtcd(t, 0)
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
