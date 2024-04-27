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

package topo_test

import (
	"context"
	"testing"

	"vitess.io/vitess/go/vt/topo"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestTopoLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()
	conn := ts.GetGlobalConn()
	require.NotNil(t, conn)
	_, err := conn.Create(ctx, "root/key1", []byte("value"))
	if err != nil {
		return
	}
	_, err = conn.Create(ctx, "root/key2", []byte("value"))
	if err != nil {
		return
	}

	var tl1, tl2 topo.ITopoLock

	origCtx := ctx
	tl1 = ts.NewTopoLock("root", "key1", "action1", "name")
	ctx, unlock, err := tl1.Lock(origCtx)
	require.NoError(t, err)
	require.NotNil(t, unlock)

	// locking the same key again, without unlocking, should return an error
	_, _, err2 := tl1.Lock(ctx)
	require.ErrorContains(t, err2, "already held")

	// confirm that the lock can be re-acquired after unlocking
	unlock(&err)
	ctx, unlock, err = tl1.Lock(origCtx)
	require.NoError(t, err)
	require.NotNil(t, unlock)
	defer unlock(&err)

	// locking another key should work
	tl2 = ts.NewTopoLock("root", "key2", "action2", "name")
	_, unlock2, err := tl2.Lock(ctx)
	require.NoError(t, err)
	require.NotNil(t, unlock2)
	defer unlock2(&err)

}
