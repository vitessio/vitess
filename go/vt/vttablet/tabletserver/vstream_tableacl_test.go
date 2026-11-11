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

package tabletserver

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tableaclpb "vitess.io/vitess/go/vt/proto/tableacl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	vstreamSecretMarker = "vstream-acl-secret-marker"
	vstreamAllowedUser  = "secret_owner"
	vstreamDeniedUser   = "app_reader"
	vstreamAllUser      = "all_reader"
	vstreamWriterUser   = "secret_writer"
	vstreamExemptUser   = "exempt_operator"
	vstreamOwnerGroup   = "secret_owners"
)

// TestVStreamStrictTableACL is the spec for strict table ACL on VStream.
// Query execution denies a caller who is not a reader. VStream must do the
// same, for the table whose rows the stream will actually send, before any
// row image is delivered. The checks below fail while VStream skips that.
func TestVStreamStrictTableACL(t *testing.T) {
	ctx := t.Context()
	aclName := fmt.Sprintf("vstream-tableacl-%d", time.Now().UnixNano())
	tableacl.Register(aclName, &simpleacl.Factory{})
	tableacl.SetDefaultACL(aclName)

	env, err := testenv.Init(ctx)
	require.NoError(t, err)
	t.Cleanup(env.Close)

	require.NoError(t, env.Mysqld.ExecuteSuperQueryList(ctx, []string{
		"create table allowed_t (id int primary key, note varchar(128))",
		"create table secret_t (id int primary key, note varchar(128))",
		"insert into allowed_t (id, note) values (1, 'vstream-acl-allowed-marker')",
		"insert into secret_t (id, note) values (1, '" + vstreamSecretMarker + "')",
	}))

	tables := showTables(t, ctx, env)
	require.Contains(t, tables, "secret_t")
	require.Contains(t, tables, "allowed_t")
	require.NoError(t, tableacl.InitFromProto(vstreamACLConfig(tables)))

	strict, stopStrict := startVStreamACLTablet(t, ctx, env, "strict", true, false, vstreamExemptUser)
	target := &querypb.Target{
		Keyspace:   env.KeyspaceName,
		Shard:      env.ShardName,
		TabletType: topodatapb.TabletType_PRIMARY,
	}

	deniedCtx := callerCtx(ctx, vstreamDeniedUser, nil)
	ownerCtx := callerCtx(ctx, vstreamAllowedUser, nil)
	_, err = strict.Execute(deniedCtx, nil, target, "select note from secret_t where id = 1", nil, 0, 0, nil)
	require.Equal(t, vtrpcpb.Code_PERMISSION_DENIED, vterrors.Code(err), "strict SELECT is the baseline this stream must match, got %v", err)
	ownerRows, err := strict.Execute(ownerCtx, nil, target, "select note from secret_t where id = 1", nil, 0, 0, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ownerRows.Rows)

	sqlFilter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{
		Match:  "secret_t",
		Filter: "select * from secret_t",
	}}}
	matchAll := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{
		Match: "/.*",
	}}}
	mixed := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{
		{Match: "allowed_t", Filter: "select * from allowed_t"},
		{Match: "/secret/", Filter: ""},
	}}

	t.Run("sql filter denies a caller who is not a reader", func(t *testing.T) {
		got := runVStream(t, strict, deniedCtx, "", sqlFilter, vstreamSecretMarker, nil)
		requireNoSecret(t, got, vtrpcpb.Code_PERMISSION_DENIED)
	})

	t.Run("sql filter streams the row to a reader", func(t *testing.T) {
		got := runVStream(t, strict, ownerCtx, "", sqlFilter, vstreamSecretMarker, nil)
		requireSaw(t, got, vstreamSecretMarker)
	})

	t.Run("match-all filter denies a caller who cannot read every matched table", func(t *testing.T) {
		got := runVStream(t, strict, deniedCtx, "", matchAll, vstreamSecretMarker, nil)
		requireNoSecret(t, got, vtrpcpb.Code_PERMISSION_DENIED)
	})

	t.Run("match-all filter streams the row to a reader of every matched table", func(t *testing.T) {
		got := runVStream(t, strict, callerCtx(ctx, vstreamAllUser, nil), "", matchAll, vstreamSecretMarker, nil)
		requireSaw(t, got, vstreamSecretMarker)
	})

	t.Run("a filter that matches an allowed table and a denied table does not deliver the denied row", func(t *testing.T) {
		got := runVStream(t, strict, deniedCtx, "", mixed, vstreamSecretMarker, nil)
		requireNoSecret(t, got, vtrpcpb.Code_PERMISSION_DENIED)
	})

	t.Run("a future row is denied for a caller who is not a reader", func(t *testing.T) {
		marker := "vstream-acl-future-denied"
		got := runVStream(t, strict, deniedCtx, "current", sqlFilter, marker, func() {
			require.NoError(t, env.Mysqld.ExecuteSuperQuery(ctx, "insert into secret_t (id, note) values (2, '"+marker+"')"))
		})
		requireNoSecret(t, got, vtrpcpb.Code_PERMISSION_DENIED)
	})

	t.Run("a future row is streamed to a reader", func(t *testing.T) {
		marker := "vstream-acl-future-allowed"
		got := runVStream(t, strict, ownerCtx, "current", sqlFilter, marker, func() {
			require.NoError(t, env.Mysqld.ExecuteSuperQuery(ctx, "insert into secret_t (id, note) values (3, '"+marker+"')"))
		})
		requireSaw(t, got, marker)
	})

	t.Run("membership follows the immediate caller, not the effective caller", func(t *testing.T) {
		// The effective principal is a reader. SELECT ignores it and uses the
		// immediate caller, who is not. The stream has to do the same.
		ctx := callerid.NewContext(ctx,
			&vtrpcpb.CallerID{Principal: vstreamAllowedUser},
			&querypb.VTGateCallerID{Username: vstreamDeniedUser},
		)
		got := runVStream(t, strict, ctx, "", sqlFilter, vstreamSecretMarker, nil)
		requireNoSecret(t, got, vtrpcpb.Code_PERMISSION_DENIED)
	})

	t.Run("a group member of the readers is a reader", func(t *testing.T) {
		ctx := callerCtx(ctx, "nobody", []string{vstreamOwnerGroup})
		got := runVStream(t, strict, ctx, "", sqlFilter, vstreamSecretMarker, nil)
		requireSaw(t, got, vstreamSecretMarker)
	})

	t.Run("a writer who is not a reader is denied", func(t *testing.T) {
		got := runVStream(t, strict, callerCtx(ctx, vstreamWriterUser, nil), "", sqlFilter, vstreamSecretMarker, nil)
		requireNoSecret(t, got, vtrpcpb.Code_PERMISSION_DENIED)
	})

	t.Run("an exempt caller is allowed", func(t *testing.T) {
		got := runVStream(t, strict, callerCtx(ctx, vstreamExemptUser, nil), "", sqlFilter, vstreamSecretMarker, nil)
		requireSaw(t, got, vstreamSecretMarker)
	})

	t.Run("a local context is allowed without a caller", func(t *testing.T) {
		got := runVStream(t, strict, tabletenv.LocalContext(), "", sqlFilter, vstreamSecretMarker, nil)
		requireSaw(t, got, vstreamSecretMarker)
	})

	t.Run("a missing caller is denied when strict", func(t *testing.T) {
		got := runVStream(t, strict, ctx, "", sqlFilter, vstreamSecretMarker, nil)
		requireNoSecret(t, got, vtrpcpb.Code_UNAUTHENTICATED)
	})

	t.Run("a regex stream of a readable table does not deliver a table created later that the caller cannot read", func(t *testing.T) {
		// /sec/ matches secret_t, which this caller may read, and also matches
		// a table that does not exist yet. The stream has to keep sending
		// secret_t. Once sec_late exists, the same filter matches it, and this
		// caller is not a reader of it: that row image must not be delivered,
		// including when the set of tables was resolved before the create.
		allowedMarker := "vstream-acl-regex-allowed"
		lateMarker := "vstream-acl-late-secret"
		filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: "/sec/"}}}
		got := runVStreamWatch(t, strict, ownerCtx, "current", filter, []string{allowedMarker, lateMarker}, func(saw func(string) bool) {
			require.NoError(t, env.Mysqld.ExecuteSuperQuery(ctx, "insert into secret_t (id, note) values (4, '"+allowedMarker+"')"))
			require.Eventually(t, func() bool { return saw(allowedMarker) }, 20*time.Second, 20*time.Millisecond)
			require.NoError(t, env.Mysqld.ExecuteSuperQueryList(ctx, []string{
				"create table sec_late (id int primary key, note varchar(128))",
				"insert into sec_late (id, note) values (1, '" + lateMarker + "')",
			}))
			require.Never(t, func() bool { return saw(lateMarker) }, 10*time.Second, 20*time.Millisecond,
				"row image for sec_late was streamed to a caller who is not a reader of it")
		})
		require.Truef(t, got.saw(allowedMarker), "regex filter did not stream the readable table; stream error was %v", got.err)
		require.Falsef(t, got.saw(lateMarker), "row image for sec_late was streamed to a caller who is not a reader of it; stream error was %v", got.err)
	})

	stopStrict()

	t.Run("dry run lets the denied caller through", func(t *testing.T) {
		dry, stopDry := startVStreamACLTablet(t, ctx, env, "dry", true, true, "")
		defer stopDry()
		got := runVStream(t, dry, deniedCtx, "", sqlFilter, vstreamSecretMarker, nil)
		requireSaw(t, got, vstreamSecretMarker)
	})

	t.Run("strict off lets a non-reader and a missing caller through", func(t *testing.T) {
		loose, stopLoose := startVStreamACLTablet(t, ctx, env, "loose", false, false, "")
		defer stopLoose()
		got := runVStream(t, loose, deniedCtx, "", sqlFilter, vstreamSecretMarker, nil)
		requireSaw(t, got, vstreamSecretMarker)
		got = runVStream(t, loose, ctx, "", sqlFilter, vstreamSecretMarker, nil)
		requireSaw(t, got, vstreamSecretMarker)
	})
}

func vstreamACLConfig(tables []string) *tableaclpb.Config {
	readersOfSecret := []string{vstreamAllowedUser, vstreamAllUser, vstreamOwnerGroup}
	groups := []*tableaclpb.TableGroupSpec{{
		Name:                 "secret",
		TableNamesOrPrefixes: []string{"secret%"},
		Readers:              readersOfSecret,
		Writers:              []string{vstreamWriterUser},
	}, {
		Name:                 "allowed",
		TableNamesOrPrefixes: []string{"allowed_t"},
		Readers:              []string{vstreamDeniedUser, vstreamAllUser},
	}}
	for _, name := range tables {
		if name == "allowed_t" || strings.HasPrefix(name, "secret") {
			continue
		}
		groups = append(groups, &tableaclpb.TableGroupSpec{
			Name:                 name,
			TableNamesOrPrefixes: []string{name},
			Readers:              []string{vstreamAllUser},
		})
	}
	return &tableaclpb.Config{TableGroups: groups}
}

func startVStreamACLTablet(t *testing.T, ctx context.Context, env *testenv.Env, name string, strict, dryRun bool, exempt string) (*TabletServer, func()) {
	t.Helper()
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = env.Dbcfgs
	cfg.StrictTableACL = strict
	cfg.EnableTableACLDryRun = dryRun
	cfg.TableACLExemptACL = exempt
	cfg.EnableOnlineDDL = false
	cfg.HotRowProtection.Mode = tabletenv.Disable
	cfg.TrackSchemaVersions = false

	topoServer := memorytopo.NewServer(ctx, env.Cells...)
	require.NoError(t, topoServer.UpdateSrvKeyspace(ctx, env.Cells[0], env.KeyspaceName, &topodatapb.SrvKeyspace{}))
	counts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	alias := &topodatapb.TabletAlias{Cell: env.Cells[0], Uid: 100}
	tsv := NewTabletServer(ctx, vtenv.NewTestEnv(), "VStreamACL-"+name, cfg, topoServer, alias, counts)
	target := &querypb.Target{
		Keyspace:   env.KeyspaceName,
		Shard:      env.ShardName,
		TabletType: topodatapb.TabletType_PRIMARY,
	}
	require.NoError(t, tsv.StartService(target, env.Dbcfgs, env.Mysqld))
	var once sync.Once
	stop := func() {
		once.Do(func() { tsv.StopService() })
	}
	t.Cleanup(stop)
	return tsv, stop
}

func showTables(t *testing.T, ctx context.Context, env *testenv.Env) []string {
	t.Helper()
	params, err := env.Dbcfgs.DbaWithDB().MysqlParams()
	require.NoError(t, err)
	conn, err := mysql.Connect(ctx, params)
	require.NoError(t, err)
	defer conn.Close()
	qr, err := conn.ExecuteFetch("show tables", 10000, false)
	require.NoError(t, err)
	names := make([]string, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		names = append(names, row[0].ToString())
	}
	return names
}

func callerCtx(ctx context.Context, username string, groups []string) context.Context {
	return callerid.NewContext(ctx, &vtrpcpb.CallerID{Principal: username}, &querypb.VTGateCallerID{
		Username: username,
		Groups:   groups,
	})
}

type vstreamCapture struct {
	mu   sync.Mutex
	seen map[string]bool
	err  error
}

func (c *vstreamCapture) saw(marker string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[marker]
}

func (c *vstreamCapture) observe(evs []*binlogdatapb.VEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ev := range evs {
		row := ev.GetRowEvent()
		if row == nil {
			continue
		}
		for _, change := range row.RowChanges {
			for _, img := range []*querypb.Row{change.Before, change.After} {
				if img == nil {
					continue
				}
				for marker := range c.seen {
					if bytes.Contains(img.Values, []byte(marker)) {
						c.seen[marker] = true
					}
				}
			}
		}
	}
}

// runVStream runs TabletServer.VStream. stopOn ends the stream once that row
// image has been delivered, so a copy does not sit in the binlog phase.
// afterStart runs only after the stream has delivered an event, which is past
// the point where a replicate-from-current stream captured its position. A
// stream that fails before any event does not call afterStart.
func runVStream(t *testing.T, tsv *TabletServer, ctx context.Context, position string, filter *binlogdatapb.Filter, stopOn string, afterStart func()) *vstreamCapture {
	t.Helper()
	return runVStreamWatch(t, tsv, ctx, position, filter, []string{stopOn}, func(saw func(string) bool) {
		if afterStart != nil {
			afterStart()
		}
		if stopOn == "" {
			return
		}
		require.Eventually(t, func() bool { return saw(stopOn) }, 20*time.Second, 20*time.Millisecond)
	})
}

// runVStreamWatch is runVStream with the caller driving what happens after the
// first event. watch is not called when the stream fails before any event.
// Every marker is recorded, including ones added while the stream is running.
func runVStreamWatch(t *testing.T, tsv *TabletServer, ctx context.Context, position string, filter *binlogdatapb.Filter, markers []string, watch func(saw func(string) bool)) *vstreamCapture {
	t.Helper()
	seen := map[string]bool{}
	for _, marker := range markers {
		if marker != "" {
			seen[marker] = false
		}
	}
	cap := &vstreamCapture{seen: seen}

	streamCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	started := make(chan struct{})
	var startOnce sync.Once
	errCh := make(chan error, 1)
	go func() {
		errCh <- tsv.VStream(streamCtx, &binlogdatapb.VStreamRequest{
			Target:   tsv.sm.Target(),
			Position: position,
			Filter:   filter,
		}, func(evs []*binlogdatapb.VEvent) error {
			cap.observe(evs)
			startOnce.Do(func() { close(started) })
			return nil
		})
	}()

	select {
	case <-started:
		if watch != nil {
			watch(cap.saw)
		}
	case err := <-errCh:
		cap.err = err
		return cap
	}
	cancel()
	cap.err = <-errCh
	return cap
}

func requireNoSecret(t *testing.T, got *vstreamCapture, code vtrpcpb.Code) {
	t.Helper()
	require.Falsef(t, got.saw(vstreamSecretMarker) || got.saw("vstream-acl-future-denied"),
		"denied caller received a secret row image; stream error was %v", got.err)
	require.Equal(t, code, vterrors.Code(got.err), "stream error %v", got.err)
}

func requireSaw(t *testing.T, got *vstreamCapture, marker string) {
	t.Helper()
	require.Truef(t, got.saw(marker), "reader did not receive row image %q; stream error was %v", marker, got.err)
}
