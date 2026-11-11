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

package zk2topo

import (
	"context"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/testfiles"
	"vitess.io/vitess/go/vt/zkctl"
)

// topo-zk-auth-file is documented as digest:<user>:<placeholder>.
// The placeholder is not a real credential.
const (
	authFileBody  = "digest:topo-user:placeholder-secret\n"
	otherAuthBody = "other-user:placeholder-secret"
)

var (
	tabletRecord  = []byte(`{"mysql_hostname":"real-primary.example","mysql_port":3306}`)
	attackRecord  = []byte(`{"mysql_hostname":"attacker.example","mysql_port":3306}`)
	shardRecord   = []byte(`{"primary_alias":{"cell":"zone1","uid":100}}`)
	attackShard   = []byte(`{"primary_alias":{"cell":"zone1","uid":666}}`)
	vschemaRecord = []byte(`{"sharded":false}`)
	attackVSchema = []byte(`{"sharded":true}`)
)

// TestTopoZkAuthFileRestrictsZNodes is the spec for topo-zk-auth-file.
// A ZooKeeper client that can open a session, including one that authenticates
// as a different digest identity, must not be able to read or change znodes
// the zk2 topo server creates or updates. The identity in the auth file must.
func TestTopoZkAuthFileRestrictsZNodes(t *testing.T) {
	if os.Getenv("VTROOT") == "" {
		t.Fatal("VTROOT must be set so zkctl can start the ZooKeeper server Vitess ships")
	}

	zkd, serverAddr := zkctl.StartLocalZk(testfiles.GoVtTopoZk2topoZkID, testfiles.GoVtTopoZk2topoPort)
	t.Cleanup(func() {
		if err := zkd.Teardown(); err != nil {
			t.Logf("zk teardown: %v", err)
		}
	})

	authPath := path.Join(t.TempDir(), "zk-auth")
	require.NoError(t, os.WriteFile(authPath, []byte(authFileBody), 0o600))
	previousAuthFile := authFile
	authFile = authPath
	t.Cleanup(func() { authFile = previousAuthFile })

	scheme, cred := authMaterial(t, authPath)
	same := dialZK(t, serverAddr)
	require.NoError(t, same.AddAuth(scheme, cred))
	outsider := dialZK(t, serverAddr)
	other := dialZK(t, serverAddr)
	require.NoError(t, other.AddAuth("digest", []byte(otherAuthBody)))

	root := "/vitess/global"
	zs := NewServer(serverAddr, root)
	t.Cleanup(zs.Close)
	ctx := t.Context()

	const (
		tabletRel  = "tablets/zone1-0000000100/Tablet"
		shardRel   = "keyspaces/ks/shards/0/Shard"
		vschemaRel = "keyspaces/ks/VSchema"
	)

	t.Run("create", func(t *testing.T) {
		for _, tc := range []struct {
			rel  string
			body []byte
			bad  []byte
		}{
			{tabletRel, tabletRecord, attackRecord},
			{vschemaRel, vschemaRecord, attackVSchema},
		} {
			_, err := zs.Create(ctx, tc.rel, tc.body)
			require.NoError(t, err)
			got, _, err := zs.Get(ctx, tc.rel)
			require.NoError(t, err)
			require.Equal(t, tc.body, got)

			full := path.Join(root, tc.rel)
			requireCreatedBound(t, same, outsider, other, full, tc.body, tc.bad)

			got, _, err = zs.Get(ctx, tc.rel)
			require.NoError(t, err)
			require.Equal(t, tc.body, got)
		}
	})

	// Update of a missing node falls through to Create.
	t.Run("update creates missing shard", func(t *testing.T) {
		ver, err := zs.Update(ctx, shardRel, shardRecord, nil)
		require.NoError(t, err)
		require.NotNil(t, ver)
		got, _, err := zs.Get(ctx, shardRel)
		require.NoError(t, err)
		require.Equal(t, shardRecord, got)
		requireCreatedBound(t, same, outsider, other, path.Join(root, shardRel), shardRecord, attackShard)
	})

	t.Run("update keeps file protected", func(t *testing.T) {
		updated := []byte(`{"mysql_hostname":"real-primary.example","mysql_port":3307}`)
		current, ver, err := zs.Get(ctx, tabletRel)
		require.NoError(t, err)
		require.Equal(t, tabletRecord, current)
		_, err = zs.Update(ctx, tabletRel, updated, ver)
		require.NoError(t, err)
		requireBound(t, same, outsider, other, path.Join(root, tabletRel), updated, attackRecord)
	})

	// Records written before the auth file existed are world-accessible until
	// an update binds them, and their parents, to the auth-file identity.
	t.Run("update migrates existing world acls", func(t *testing.T) {
		const (
			legacyRoot = "/legacy"
			legacyRel  = "tablets/zone1-0000000100/Tablet"
		)
		seedWorldTree(t, dialZK(t, serverAddr), legacyRoot, legacyRel, tabletRecord)
		legacy := NewServer(serverAddr, legacyRoot)
		t.Cleanup(legacy.Close)

		_, err := legacy.Update(ctx, legacyRel, attackRecord, nil)
		require.NoError(t, err)
		got, _, err := legacy.Get(ctx, legacyRel)
		require.NoError(t, err)
		require.Equal(t, attackRecord, got)
		requireBound(t, same, outsider, other, path.Join(legacyRoot, legacyRel), attackRecord, tabletRecord)
	})

	t.Run("lock", func(t *testing.T) {
		dir := "keyspaces/ks/shards/0"
		ld, err := zs.Lock(ctx, dir, "zone1-0000000100")
		require.NoError(t, err)
		t.Cleanup(func() {
			unlockCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = ld.Unlock(unlockCtx)
		})

		locksDir := path.Join(root, dir, locksPath)
		children, _, err := same.Children(locksDir)
		require.NoError(t, err)
		require.NotEmpty(t, children)
		for _, child := range children {
			node := path.Join(locksDir, child)
			data, _, err := same.Get(node)
			require.NoError(t, err)
			requireCreatedBound(t, same, outsider, other, node, data, []byte("attacker"))
		}
		_, err = outsider.Create(path.Join(locksDir, "attacker"), []byte("x"), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		require.ErrorIs(t, err, zk.ErrNoAuth)
	})

	t.Run("election", func(t *testing.T) {
		part, err := zs.NewLeaderParticipation("cell1", "zone1-0000000100")
		require.NoError(t, err)
		leadCtx, err := part.WaitForLeadership()
		require.NoError(t, err)
		require.NotNil(t, leadCtx)
		// Stop waits for the leadership goroutine to close its done channel.
		// That channel stays open when a watch fires because the proposal
		// changed, which is what an overwrite does, so don't block cleanup on it.
		t.Cleanup(func() {
			stopped := make(chan struct{})
			go func() {
				part.Stop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(2 * time.Second):
				part.(*zkLeaderParticipation).stopCtxCancel()
			}
		})

		electionDir := path.Join(root, electionsPath, "cell1")
		children, _, err := same.Children(electionDir)
		require.NoError(t, err)
		require.NotEmpty(t, children)
		for _, child := range children {
			node := path.Join(electionDir, child)
			data, _, err := same.Get(node)
			require.NoError(t, err)
			requireCreatedBound(t, same, outsider, other, node, data, []byte("attacker"))
		}
		_, err = outsider.Create(path.Join(electionDir, "attacker"), []byte("x"), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		require.ErrorIs(t, err, zk.ErrNoAuth)
		_, _, err = outsider.Children(path.Join(root, electionsPath))
		require.ErrorIs(t, err, zk.ErrNoAuth)
	})
}

// requireCreatedBound is for znodes whose ACL was set by the creating call.
// ZooKeeper starts aversion at 0 and increments it only in setACL, so a
// create that is world-open and tightened afterwards fails this check.
func requireCreatedBound(t *testing.T, same, outsider, other *zk.Conn, fullPath string, want, attack []byte) {
	t.Helper()
	requireBound(t, same, outsider, other, fullPath, want, attack)
	requireUnchangedACL(t, same, fullPath)
	for dir := path.Dir(fullPath); dir != "/" && dir != "."; dir = path.Dir(dir) {
		requireUnchangedACL(t, same, dir)
	}
}

// requireBound checks the auth-file identity can read the znode and that both
// an unauthenticated session and a different digest identity cannot read,
// replace, delete, or re-ACL that znode or create children of its parents.
func requireBound(t *testing.T, same, outsider, other *zk.Conn, fullPath string, want, attack []byte) {
	t.Helper()
	got, _, err := same.Get(fullPath)
	require.NoError(t, err)
	require.Equal(t, want, got)

	for _, client := range []*zk.Conn{outsider, other} {
		data, _, getErr := client.Get(fullPath)
		_, setErr := client.Set(fullPath, attack, -1)
		var overwritten []byte
		if setErr == nil {
			overwritten, _, _ = same.Get(fullPath)
			// Put the caller's bytes back so a later assertion sees the znode
			// the topo server wrote, not the overwrite this check just did.
			_, _ = same.Set(fullPath, want, -1)
		}
		injected := path.Join(path.Dir(fullPath), "injected")
		_, createErr := client.Create(injected, []byte("x"), 0, zk.WorldACL(zk.PermAll))
		if createErr == nil {
			_ = client.Delete(injected, -1)
		}
		if getErr == nil || setErr == nil || createErr == nil {
			t.Fatalf("client reached %s: getErr=%v data=%q setErr=%v overwritten=%q createParentErr=%v", fullPath, getErr, data, setErr, overwritten, createErr)
		}
		require.ErrorIs(t, getErr, zk.ErrNoAuth)
		require.ErrorIs(t, setErr, zk.ErrNoAuth)
		require.ErrorIs(t, createErr, zk.ErrNoAuth)

		err = client.Delete(fullPath, -1)
		require.ErrorIs(t, err, zk.ErrNoAuth)

		_, err = client.SetACL(fullPath, zk.WorldACL(zk.PermAll), -1)
		require.ErrorIs(t, err, zk.ErrNoAuth)
	}
	requireNoWorldACL(t, same, fullPath)

	got, _, err = same.Get(fullPath)
	require.NoError(t, err)
	require.Equal(t, want, got, "znode %s changed", fullPath)

	for dir := path.Dir(fullPath); dir != "/" && dir != "."; dir = path.Dir(dir) {
		requireNoWorldACL(t, same, dir)
		_, _, err := outsider.Children(dir)
		require.ErrorIs(t, err, zk.ErrNoAuth)
		_, err = outsider.Create(path.Join(dir, "injected"), []byte("x"), 0, zk.WorldACL(zk.PermAll))
		require.ErrorIs(t, err, zk.ErrNoAuth)
	}
}

func requireNoWorldACL(t *testing.T, same *zk.Conn, fullPath string) {
	t.Helper()
	acls, _, err := same.GetACL(fullPath)
	require.NoError(t, err, "GetACL %s", fullPath)
	require.NotEmpty(t, acls)
	for _, acl := range acls {
		require.NotEqual(t, "world", acl.Scheme, "znode %s grants world permissions: %+v", fullPath, acls)
		require.NotZero(t, acl.Perms, "znode %s has an empty ACL entry: %+v", fullPath, acls)
	}
}

func requireUnchangedACL(t *testing.T, same *zk.Conn, fullPath string) {
	t.Helper()
	_, stat, err := same.GetACL(fullPath)
	require.NoError(t, err, "GetACL %s", fullPath)
	require.Equal(t, int32(0), stat.Aversion, "ACL on %s was changed after the znode was created", fullPath)
}

func seedWorldTree(t *testing.T, conn *zk.Conn, root, rel string, data []byte) {
	t.Helper()
	t.Cleanup(conn.Close)
	full := path.Join(root, rel)
	var built string
	for _, part := range strings.Split(strings.Trim(full, "/"), "/") {
		built += "/" + part
		payload := []byte(nil)
		if built == full {
			payload = data
		}
		_, err := conn.Create(built, payload, 0, zk.WorldACL(zk.PermAll))
		require.NoError(t, err)
	}
}

func authMaterial(t *testing.T, authPath string) (string, []byte) {
	t.Helper()
	raw, err := os.ReadFile(authPath)
	require.NoError(t, err)
	info := strings.TrimRight(string(raw), "\n")
	scheme, cred, ok := strings.Cut(info, ":")
	require.True(t, ok, "auth file must be <scheme>:<auth>")
	require.NotEmpty(t, scheme)
	require.NotEmpty(t, cred)
	return scheme, []byte(cred)
}

func dialZK(t *testing.T, addr string) *zk.Conn {
	t.Helper()
	conn, events, err := zk.Connect([]string{addr}, 15*time.Second)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	deadline := time.After(20 * time.Second)
	for {
		select {
		case ev := <-events:
			require.NotEqual(t, zk.StateAuthFailed, ev.State)
			if ev.State == zk.StateConnected {
				return conn
			}
		case <-deadline:
			t.Fatal("timed out waiting for zookeeper session")
		}
	}
}
