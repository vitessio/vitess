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

package cli

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

func TestCatchUpReplicationForBackupClearsLastErrWhenReplicationBecomesHealthy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		oldInitKeyspace := initKeyspace
		oldInitShard := initShard
		initKeyspace = "test_keyspace"
		initShard = "0"
		t.Cleanup(func() {
			initKeyspace = oldInitKeyspace
			initShard = oldInitShard
		})

		ts := memorytopo.NewServer(ctx, "zone1")
		t.Cleanup(ts.Close)
		require.NoError(t, ts.CreateKeyspace(ctx, initKeyspace, &topodatapb.Keyspace{}))
		require.NoError(t, ts.CreateShard(ctx, initKeyspace, initShard))
		primaryAlias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
		require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
			Alias:         primaryAlias,
			Keyspace:      initKeyspace,
			Shard:         initShard,
			Hostname:      "primary.test",
			MysqlHostname: "primary-mysql.test",
			MysqlPort:     3306,
			Type:          topodatapb.TabletType_PRIMARY,
		}))
		_, err := ts.UpdateShardFields(ctx, initKeyspace, initShard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = primaryAlias
			return nil
		})
		require.NoError(t, err)

		restorePos := testCatchupPosition(1)
		primaryPos := testCatchupPosition(3)
		statuses := []replication.ReplicationStatus{
			{
				Position:    restorePos,
				IOState:     replication.ReplicationStateConnecting,
				LastIOError: "Replica I/O for channel '': Error reconnecting to source 'vt_test@192.0.2.10:3306'. This was attempt 1/300, with a delay of 10 seconds between attempts. Message: Can't connect to MySQL server on '192.0.2.10:3306' (111), Error_code: MY-002003",
				SQLState:    replication.ReplicationStateRunning,
			},
		}
		for range int(timeoutWaitingForReplicationStatus.Seconds()) + 1 {
			statuses = append(statuses, replication.ReplicationStatus{
				Position: restorePos,
				IOState:  replication.ReplicationStateRunning,
				SQLState: replication.ReplicationStateRunning,
			})
		}
		statuses = append(
			statuses,
			replication.ReplicationStatus{
				Position: primaryPos,
				IOState:  replication.ReplicationStateRunning,
				SQLState: replication.ReplicationStateRunning,
			},
			replication.ReplicationStatus{
				Position: primaryPos,
				IOState:  replication.ReplicationStateRunning,
				SQLState: replication.ReplicationStateRunning,
			},
		)
		mysqld := &catchupReplicationMysqlDaemon{
			statuses: statuses,
		}

		status, err := catchUpReplicationForBackup(ctx, ts, mysqld, restorePos, primaryPos)

		require.NoError(t, err)
		assert.True(t, status.Position.Equal(primaryPos))
		assert.Equal(t, 1, mysqld.setReplicationSourceCalls)
		assert.Equal(t, 1, mysqld.stopReplicationCalls)
	})
}

type catchupReplicationMysqlDaemon struct {
	mysqlctl.MysqlDaemon

	statuses                  []replication.ReplicationStatus
	statusCalls               int
	setReplicationSourceCalls int
	stopReplicationCalls      int
}

func (m *catchupReplicationMysqlDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	status := m.statuses[m.statusCalls]
	m.statusCalls++
	return status, nil
}

func (m *catchupReplicationMysqlDaemon) SetReplicationSource(ctx context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore bool, startReplicationAfter bool) error {
	m.setReplicationSourceCalls++
	return nil
}

func (m *catchupReplicationMysqlDaemon) StopReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	m.stopReplicationCalls++
	return nil
}

func testCatchupPosition(pos uint64) replication.Position {
	return replication.Position{GTIDSet: replication.FilePosGTID{File: "source-bin.000001", Pos: pos}}
}

func TestResetReplication(t *testing.T) {
	pos, err := replication.DecodePosition("MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-10")
	require.NoError(t, err)

	tests := []struct {
		name        string
		pos         replication.Position
		keepBinlogs bool
		queries     []string
	}{
		{"keep restored binlogs", pos, true, []string{"STOP REPLICA", "FAKE RESET REPLICA ALL"}},
		{"set position", pos, false, []string{"STOP REPLICA", "FAKE RESET REPLICA ALL", "FAKE RESET BINARY LOGS AND GTIDS", "FAKE SET GLOBAL gtid_purged"}},
		{"zero position", replication.Position{}, false, []string{"STOP REPLICA", "FAKE RESET REPLICA ALL", "FAKE RESET ALL REPLICATION"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := mysqlctl.NewFakeMysqlDaemon(fakesqldb.New(t))
			t.Cleanup(fake.Close)
			fake.SetReplicationPositionPos = tt.pos
			fake.ExpectedExecuteSuperQueryList = tt.queries
			require.NoError(t, resetReplication(t.Context(), tt.pos, tt.keepBinlogs, fake))
			assert.Equal(t, len(tt.queries), fake.ExpectedExecuteSuperQueryCurrent)
		})
	}
}

func TestValidateBinlogFlags(t *testing.T) {
	require.NoError(t, validateBinlogFlags(mysqlctl.BackupParams{IncludeBinlogs: true, BackupEngine: "builtin"}, false))
	err := validateBinlogFlags(mysqlctl.BackupParams{IncludeBinlogs: true, BackupEngine: "builtin"}, true)
	require.ErrorContains(t, err, "--restore-with-clone")
	assert.Equal(t, vtrpc.Code_INVALID_ARGUMENT, vterrors.Code(err))
	require.NoError(t, validateBinlogFlags(mysqlctl.BackupParams{}, true))
}

const (
	testPrimaryUUID    = "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9"
	testNewPrimaryUUID = "8bc65cca-3fe4-11ed-bbfb-091034d48b3e"
	testOtherUUID      = "8bc65d0a-3fe4-11ed-9a67-7d0f0fd51f1c"
	testLocalUUID      = "8bc65d48-3fe4-11ed-a47a-77f2e3d1e1a8"
)

// testGTIDPosition parses a MySQL56 GTID set into a position.
func testGTIDPosition(t *testing.T, gtids string) replication.Position {
	t.Helper()
	pos, err := replication.DecodePosition("MySQL56/" + gtids)
	require.NoError(t, err)
	return pos
}

// primaryStatus is the PrimaryStatus of a primary with the given UUID and MySQL56 GTID set.
func primaryStatus(uuid, gtids string) *replicationdatapb.PrimaryStatus {
	return &replicationdatapb.PrimaryStatus{Position: "MySQL56/" + gtids, ServerUuid: uuid}
}

// fakePrimaryStatusTMC serves PrimaryStatus from a function and counts the calls.
type fakePrimaryStatusTMC struct {
	tmclient.TabletManagerClient

	primaryStatus func(tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error)
	calls         int
	// hang makes PrimaryStatus block until its Context is done, like a primary
	// that doesn't answer.
	hang bool
}

func (f *fakePrimaryStatusTMC) PrimaryStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	f.calls++
	if f.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.primaryStatus(tablet)
}

// boundedContext returns a Context that ends an hour into the test, so that a
// check that waits when it shouldn't fails its subtest instead of hanging the
// package. Under synctest the hour passes as soon as every goroutine blocks.
func boundedContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	return ctx
}

// verifyPositionTest is a shard with two tablets that can be its primary, 100 and
// 101, and a fake tmclient that serves their PrimaryStatus.
type verifyPositionTest struct {
	ts   *topo.Server
	tmc  *fakePrimaryStatusTMC
	term int64
}

// newVerifyPositionTest creates the shard, with no primary, and points
// initKeyspace and initShard at it for the test.
func newVerifyPositionTest(t *testing.T, status func(tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error)) *verifyPositionTest {
	t.Helper()
	ctx := t.Context()
	oldInitKeyspace, oldInitShard := initKeyspace, initShard
	initKeyspace, initShard = "test_keyspace", "0"
	t.Cleanup(func() { initKeyspace, initShard = oldInitKeyspace, oldInitShard })

	ts := memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, initKeyspace, &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, initKeyspace, initShard))
	for _, uid := range []uint32{100, 101} {
		require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
			Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: uid},
			Keyspace: initKeyspace,
			Shard:    initShard,
			Type:     topodatapb.TabletType_REPLICA,
		}))
	}
	return &verifyPositionTest{ts: ts, tmc: &fakePrimaryStatusTMC{primaryStatus: status}}
}

// setPrimary makes tablet uid the shard primary, in a new term, with the PRIMARY
// type in its tablet record and REPLICA in the other's.
func (vt *verifyPositionTest) setPrimary(t *testing.T, uid uint32) {
	vt.term++
	term := vt.term
	for _, tablet := range []uint32{100, 101} {
		tabletType := topodatapb.TabletType_REPLICA
		if tablet == uid {
			tabletType = topodatapb.TabletType_PRIMARY
		}
		vt.setType(t, tablet, tabletType)
	}
	_, err := vt.ts.UpdateShardFields(t.Context(), initKeyspace, initShard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = &topodatapb.TabletAlias{Cell: "zone1", Uid: uid}
		si.PrimaryTermStartTime = &vttimepb.Time{Seconds: term}
		return nil
	})
	assert.NoError(t, err)
}

// setType sets the type in tablet uid's record, without changing the shard record.
func (vt *verifyPositionTest) setType(t *testing.T, uid uint32, tabletType topodatapb.TabletType) {
	_, err := vt.ts.UpdateTabletFields(t.Context(), &topodatapb.TabletAlias{Cell: "zone1", Uid: uid}, func(tablet *topodatapb.Tablet) error {
		tablet.Type = tabletType
		return nil
	})
	assert.NoError(t, err)
}

// captureLogs sends the structured log to a buffer for the rest of the test.
func captureLogs(t *testing.T) *bytes.Buffer {
	var logs bytes.Buffer
	old := log.SwapLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { log.SwapLogger(old) })
	return &logs
}

func TestVerifyPosition(t *testing.T) {
	committed := func(gtids string) func(*topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
		return func(*topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
			return primaryStatus(testPrimaryUUID, gtids), nil
		}
	}
	checkData := positionCheck{what: "the data to back up", requireCommitted: true}

	t.Run("a clean position passes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logs := captureLogs(t)
			vt := newVerifyPositionTest(t, committed(testPrimaryUUID+":1-10"))
			vt.setPrimary(t, 100)
			term, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-5"), checkData)
			require.NoError(t, err)
			assert.Equal(t, uint32(100), term.alias.GetUid())
			assert.Equal(t, 1, vt.tmc.calls)
			assert.Contains(t, logs.String(), `msg="Verified a position against the shard primary" check="the data to back up" primary=zone1-0000000100`)
		})
	})

	t.Run("errant GTIDs fail at once", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt := newVerifyPositionTest(t, committed(testPrimaryUUID+":1-10"))
			vt.setPrimary(t, 100)
			_, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-5,"+testOtherUUID+":1-2"), checkData)
			require.ErrorContains(t, err, "the data to back up has errant GTIDs "+testOtherUUID+":1-2 that primary zone1-0000000100")
			assert.Equal(t, vtrpc.Code_FAILED_PRECONDITION, vterrors.Code(err))
			assert.Equal(t, 1, vt.tmc.calls)
		})
	})

	t.Run("the primary's own GTIDs ahead of it pass check 1", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt := newVerifyPositionTest(t, committed(testPrimaryUUID+":1-10"))
			vt.setPrimary(t, 100)
			_, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-12"), positionCheck{what: "restored backup b1"})
			require.NoError(t, err)
			assert.Equal(t, 1, vt.tmc.calls)
		})
	})

	t.Run("uncommitted GTIDs are retried until the primary commits them", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			gtids := []string{testPrimaryUUID + ":1-10", testPrimaryUUID + ":1-12"}
			var vt *verifyPositionTest
			vt = newVerifyPositionTest(t, func(*topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
				return primaryStatus(testPrimaryUUID, gtids[min(vt.tmc.calls, len(gtids))-1]), nil
			})
			vt.setPrimary(t, 100)
			_, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-12"), checkData)
			require.NoError(t, err)
			assert.Equal(t, 2, vt.tmc.calls)
		})
	})

	t.Run("the wait between attempts stops growing at operationTimeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var attempts []time.Time
			vt := newVerifyPositionTest(t, func(*topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
				attempts = append(attempts, time.Now())
				if len(attempts) < 10 {
					return primaryStatus(testPrimaryUUID, testPrimaryUUID+":1-10"), nil
				}
				return primaryStatus(testPrimaryUUID, testPrimaryUUID+":1-12"), nil
			})
			vt.setPrimary(t, 100)
			_, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-12"), checkData)
			require.NoError(t, err)

			var waits []time.Duration
			for i := 1; i < len(attempts); i++ {
				waits = append(waits, attempts[i].Sub(attempts[i-1]))
			}
			s := time.Second
			assert.Equal(t, []time.Duration{s, 2 * s, 4 * s, 8 * s, 16 * s, 32 * s, operationTimeout, operationTimeout, operationTimeout}, waits)
		})
	})

	t.Run("uncommitted GTIDs become errant under a new primary", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var vt *verifyPositionTest
			vt = newVerifyPositionTest(t, func(tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
				if tablet.Alias.Uid == 100 {
					// The primary loses the race: a reparent replaces it.
					vt.setPrimary(t, 101)
					return primaryStatus(testPrimaryUUID, testPrimaryUUID+":1-10"), nil
				}
				return primaryStatus(testNewPrimaryUUID, testPrimaryUUID+":1-10,"+testNewPrimaryUUID+":1"), nil
			})
			vt.setPrimary(t, 100)
			_, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-12"), checkData)
			require.ErrorContains(t, err, "has errant GTIDs "+testPrimaryUUID+":11-12 that primary zone1-0000000101")
			assert.Equal(t, 2, vt.tmc.calls)
		})
	})

	t.Run("a primary that a reparent repointed is checked again", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var vt *verifyPositionTest
			vt = newVerifyPositionTest(t, func(tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
				if tablet.Alias.Uid == 100 {
					// ERS repoints the old primary before it promotes 101: 100
					// becomes a REPLICA, and turning off its semi-sync commits
					// GTIDs 11-12, which 101 never gets. The shard record still
					// names 100.
					vt.setType(t, 100, topodatapb.TabletType_REPLICA)
					return primaryStatus(testPrimaryUUID, testPrimaryUUID+":1-12"), nil
				}
				return primaryStatus(testNewPrimaryUUID, testPrimaryUUID+":1-10,"+testNewPrimaryUUID+":1"), nil
			})
			check := positionCheck{what: "the new backup", requireCommitted: true}
			pos := testGTIDPosition(t, testPrimaryUUID+":1-12")

			vt.setPrimary(t, 100)
			_, err := checkPositionOnce(t.Context(), vt.tmc, vt.ts, pos, check)
			require.ErrorContains(t, err, "shard primary zone1-0000000100 is a REPLICA tablet, as during a reparent, while checking the new backup")
			assert.Equal(t, vtrpc.Code_UNAVAILABLE, vterrors.Code(err))

			// The check waits until the shard record names the new primary.
			vt.setPrimary(t, 100)
			time.AfterFunc(10*time.Second, func() { vt.setPrimary(t, 101) })
			_, err = verifyPosition(boundedContext(t), vt.tmc, vt.ts, pos, check)
			require.ErrorContains(t, err, "the new backup has errant GTIDs "+testPrimaryUUID+":11-12 that primary zone1-0000000101")
		})
	})

	t.Run("a primary replaced during its status call is checked again", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var vt *verifyPositionTest
			vt = newVerifyPositionTest(t, func(tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
				if tablet.Alias.Uid == 100 {
					// A reparent replaces the primary during the call, but the demoted
					// primary still answers, with GTIDs that the new primary lost.
					vt.setPrimary(t, 101)
					return primaryStatus(testPrimaryUUID, testPrimaryUUID+":1-12"), nil
				}
				return primaryStatus(testNewPrimaryUUID, testPrimaryUUID+":1-10,"+testNewPrimaryUUID+":1"), nil
			})
			check := positionCheck{what: "the new backup", requireCommitted: true}
			pos := testGTIDPosition(t, testPrimaryUUID+":1-12")

			vt.setPrimary(t, 100)
			_, err := checkPositionOnce(boundedContext(t), vt.tmc, vt.ts, pos, check)
			require.ErrorContains(t, err, "the shard primary changed from zone1-0000000100 while checking the new backup")
			assert.Equal(t, vtrpc.Code_UNAVAILABLE, vterrors.Code(err))

			vt.setPrimary(t, 100)
			_, err = verifyPosition(boundedContext(t), vt.tmc, vt.ts, pos, check)
			require.ErrorContains(t, err, "the new backup has errant GTIDs "+testPrimaryUUID+":11-12 that primary zone1-0000000101")
			assert.Equal(t, 3, vt.tmc.calls)
		})
	})

	t.Run("a missing primary is retried", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt := newVerifyPositionTest(t, committed(testPrimaryUUID+":1-10"))
			time.AfterFunc(10*time.Second, func() { vt.setPrimary(t, 100) })
			_, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-5"), checkData)
			require.NoError(t, err)
			assert.Equal(t, 1, vt.tmc.calls)
		})
	})

	t.Run("an unreachable primary is retried", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var vt *verifyPositionTest
			vt = newVerifyPositionTest(t, func(*topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
				if vt.tmc.calls == 1 {
					return nil, errors.New("connection refused")
				}
				return primaryStatus(testPrimaryUUID, testPrimaryUUID+":1-10"), nil
			})
			vt.setPrimary(t, 100)
			_, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-5"), checkData)
			require.NoError(t, err)
			assert.Equal(t, 2, vt.tmc.calls)
		})
	})

	t.Run("MariaDB positions pass without a lookup", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// The shard has no primary, so a lookup would wait for one.
			vt := newVerifyPositionTest(t, func(*topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
				return &replicationdatapb.PrimaryStatus{Position: "MariaDB/0-1-10"}, nil
			})
			pos, err := replication.DecodePosition("MariaDB/0-1-5")
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
			t.Cleanup(cancel)
			logs := captureLogs(t)
			_, err = verifyPosition(ctx, vt.tmc, vt.ts, pos, checkData)
			require.NoError(t, err)
			assert.Equal(t, 0, vt.tmc.calls)
			assert.Contains(t, logs.String(), `msg="Not verifying a position that isn't a MySQL56 GTID set" check="the data to back up"`)
		})
	})

	t.Run("an unparsable primary UUID fails at once", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt := newVerifyPositionTest(t, committed(testPrimaryUUID+":1-10"))
			vt.tmc.primaryStatus = func(*topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
				return &replicationdatapb.PrimaryStatus{Position: "MySQL56/" + testPrimaryUUID + ":1-10"}, nil
			}
			vt.setPrimary(t, 100)
			_, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-5"), checkData)
			require.ErrorContains(t, err, "can't parse the server UUID of primary zone1-0000000100")
			assert.Equal(t, 1, vt.tmc.calls)
		})
	})

	t.Run("an attempt against a primary that doesn't answer ends after operationTimeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt := newVerifyPositionTest(t, committed(testPrimaryUUID+":1-10"))
			vt.tmc.hang = true
			vt.setPrimary(t, 100)
			// Two attempts of operationTimeout each, with the waits between them,
			// start a third within three minutes.
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			_, err := verifyPosition(ctx, vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-5"), checkData)
			require.ErrorContains(t, err, "stopped verifying the data to back up")
			assert.Equal(t, 3, vt.tmc.calls)
		})
	})

	t.Run("a new term of the same primary during its status call is checked again", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var vt *verifyPositionTest
			reparented := false
			vt = newVerifyPositionTest(t, func(*topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
				if !reparented {
					// A reparent to the same tablet starts a new term during the call.
					reparented = true
					vt.setPrimary(t, 100)
				}
				return primaryStatus(testPrimaryUUID, testPrimaryUUID+":1-10"), nil
			})
			vt.setPrimary(t, 100)
			check := positionCheck{what: "the new backup", requireCommitted: true}
			_, err := checkPositionOnce(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-5"), check)
			require.ErrorContains(t, err, "the shard primary changed from zone1-0000000100 while checking the new backup")
			assert.Equal(t, vtrpc.Code_UNAVAILABLE, vterrors.Code(err))
		})
	})

	t.Run("an undecodable primary position fails at once", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt := newVerifyPositionTest(t, func(*topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
				return &replicationdatapb.PrimaryStatus{Position: "bogus", ServerUuid: testPrimaryUUID}, nil
			})
			vt.setPrimary(t, 100)
			_, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-5"), checkData)
			require.ErrorContains(t, err, `can't decode the position "bogus" of primary zone1-0000000100`)
			assert.Equal(t, 1, vt.tmc.calls)
		})
	})

	t.Run("checks 1 and 2 run in the VerifyErrantGTIDs phase", func(t *testing.T) {
		var during int64
		err := inVerifyErrantGTIDsPhase(func() error {
			during = phase.Counts()[phaseNameVerifyErrantGTIDs]
			return errors.New("check failed")
		})
		require.EqualError(t, err, "check failed")
		assert.Equal(t, int64(1), during)
		assert.Equal(t, int64(0), phase.Counts()[phaseNameVerifyErrantGTIDs], "the phase must end with the check, even a failed one")
	})

	t.Run("a zero position makes no lookup", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// The shard has no primary, so a lookup would wait for one.
			vt := newVerifyPositionTest(t, committed(testPrimaryUUID+":1-10"))
			ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
			defer cancel()
			term, err := verifyPosition(ctx, vt.tmc, vt.ts, replication.Position{}, checkData)
			require.NoError(t, err)
			assert.Nil(t, term.alias)
			assert.Equal(t, 0, vt.tmc.calls)
		})
	})

	t.Run("the error gives the remedy for each source of errant GTIDs", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt := newVerifyPositionTest(t, committed(testPrimaryUUID+":1-10"))
			vt.setPrimary(t, 100)
			check := positionCheck{
				what:             "the data to back up",
				restorePos:       testGTIDPosition(t, testPrimaryUUID+":1-5,"+testOtherUUID+":1-2"),
				localUUID:        testLocalUUID,
				requireCommitted: true,
			}
			pos := testGTIDPosition(t, testPrimaryUUID+":1-10,"+testOtherUUID+":1-2,"+testLocalUUID+":1,"+testNewPrimaryUUID+":1")
			_, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, pos, check)
			require.ErrorContains(t, err, testOtherUUID+":1-2 came from the restored backup")
			require.ErrorContains(t, err, "take a new backup of a healthy replica, one without them, with vtctldclient Backup")
			require.ErrorContains(t, err, testLocalUUID+":1 were written by this run's --init-backup-sql-queries")
			require.ErrorContains(t, err, "vtbackup applied "+testNewPrimaryUUID+":1 during catch-up")
		})
	})

	t.Run("a clone's errant GTIDs get the donor remedy", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			oldRestoreWithClone := restoreWithClone
			restoreWithClone = true
			t.Cleanup(func() { restoreWithClone = oldRestoreWithClone })
			vt := newVerifyPositionTest(t, committed(testPrimaryUUID+":1-10"))
			vt.setPrimary(t, 100)
			pos := testGTIDPosition(t, testPrimaryUUID+":1-5,"+testOtherUUID+":1")
			_, err := verifyPosition(boundedContext(t), vt.tmc, vt.ts, pos, positionCheck{what: "data cloned from the donor", restorePos: pos})
			require.ErrorContains(t, err, "data cloned from the donor has errant GTIDs "+testOtherUUID+":1")
			require.ErrorContains(t, err, "came from the donor, which has them too")
			// With --clone-from-primary, the next run clones whichever primary the
			// shard has then.
			require.ErrorContains(t, err, "With --clone-from-primary, the donor was a primary that a reparent replaced, and the next run clones the new one")

			// GTIDs applied during catch-up don't lead back to a previous backup:
			// the next run clones the donor again.
			_, err = verifyPosition(boundedContext(t), vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-5,"+testNewPrimaryUUID+":1"), positionCheck{what: "the data to back up", restorePos: testGTIDPosition(t, testPrimaryUUID+":1-5")})
			require.ErrorContains(t, err, "vtbackup applied "+testNewPrimaryUUID+":1 during catch-up")
			require.ErrorContains(t, err, "the next run clones the donor again")
			require.NotContains(t, err.Error(), "starts from the previous backup")
		})
	})

	t.Run("a cancelled wait stops verifying and says what it waited for", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt := newVerifyPositionTest(t, committed(testPrimaryUUID+":1-10"))
			vt.setPrimary(t, 100)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			_, err := verifyPosition(ctx, vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-12"), checkData)
			require.ErrorContains(t, err, "stopped verifying the data to back up")
			require.ErrorContains(t, err, "primary zone1-0000000100 has not committed "+testPrimaryUUID+":11-12")
			require.ErrorContains(t, err, "deadline exceeded")
			assert.NotContains(t, err.Error(), "Code:")
			assert.Equal(t, vtrpc.Code_DEADLINE_EXCEEDED, vterrors.Code(err))
		})
	})

	t.Run("a run cancelled during an attempt stops without announcing a retry", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logs := captureLogs(t)
			vt := newVerifyPositionTest(t, committed(testPrimaryUUID+":1-10"))
			vt.setPrimary(t, 100)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			// The run is cancelled, as by SIGTERM, while it asks the primary.
			vt.tmc.primaryStatus = func(*topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
				cancel()
				return nil, context.Canceled
			}
			_, err := verifyPosition(ctx, vt.tmc, vt.ts, testGTIDPosition(t, testPrimaryUUID+":1-5"), checkData)
			require.ErrorContains(t, err, "stopped verifying the data to back up")
			assert.Equal(t, vtrpc.Code_CANCELED, vterrors.Code(err))
			assert.NotContains(t, logs.String(), "Waiting to retry")
		})
	})
}

// positionMysqlDaemon serves the position and server UUID that verifyNewBackup reads.
type positionMysqlDaemon struct {
	mysqlctl.MysqlDaemon

	pos  replication.Position
	uuid string
}

func (m *positionMysqlDaemon) PrimaryPosition(context.Context) (replication.Position, error) {
	return m.pos, nil
}

func (m *positionMysqlDaemon) GetServerUUID(context.Context) (string, error) {
	return m.uuid, nil
}

func TestVerifyNewBackup(t *testing.T) {
	local := testPrimaryUUID + ":1-11"
	// newBackup runs check 2 for a local position of gtids against primary 100,
	// which has committed testPrimaryUUID:1-11, and returns the shard and check 3.
	newBackup := func(t *testing.T, gtids string) (*verifyPositionTest, func(ctx context.Context, pos replication.Position) error, error) {
		vt := newVerifyPositionTest(t, func(tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
			if tablet.Alias.Uid == 100 {
				return primaryStatus(testPrimaryUUID, local), nil
			}
			// Tablet 101 has lost testPrimaryUUID:11.
			return primaryStatus(testNewPrimaryUUID, testPrimaryUUID+":1-10,"+testNewPrimaryUUID+":1"), nil
		})
		vt.setPrimary(t, 100)
		mysqld := &positionMysqlDaemon{pos: testGTIDPosition(t, gtids), uuid: testLocalUUID}
		params := &mysqlctl.BackupParams{}
		err := verifyNewBackup(boundedContext(t), vt.tmc, vt.ts, mysqld, replication.Position{}, params)
		return vt, params.VerifyPosition, err
	}

	t.Run("check 3 reuses check 2 while the primary is unchanged", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt, verify, err := newBackup(t, local)
			require.NoError(t, err)
			require.NoError(t, verify(boundedContext(t), testGTIDPosition(t, local)))
			assert.Equal(t, 1, vt.tmc.calls)
		})
	})

	t.Run("check 3 reuses check 2 for a position within it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// An incremental backup's position can be a subset of gtid_executed.
			vt, verify, err := newBackup(t, local)
			require.NoError(t, err)
			require.NoError(t, verify(boundedContext(t), testGTIDPosition(t, testPrimaryUUID+":1-9")))
			assert.Equal(t, 1, vt.tmc.calls)
		})
	})

	t.Run("check 3 fails after a reparent that lost a GTID", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt, verify, err := newBackup(t, local)
			require.NoError(t, err)
			vt.setPrimary(t, 101)
			err = verify(boundedContext(t), testGTIDPosition(t, local))
			require.ErrorContains(t, err, "the new backup has errant GTIDs "+testPrimaryUUID+":11 that primary zone1-0000000101")
			assert.Equal(t, 2, vt.tmc.calls)
		})
	})

	t.Run("check 3 passes after a reparent that kept every GTID", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt, verify, err := newBackup(t, testPrimaryUUID+":1-10")
			require.NoError(t, err)
			vt.setPrimary(t, 101)
			require.NoError(t, verify(boundedContext(t), testGTIDPosition(t, testPrimaryUUID+":1-10")))
			assert.Equal(t, 2, vt.tmc.calls)
		})
	})

	t.Run("check 3 checks again in a new term of the same primary", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt, verify, err := newBackup(t, local)
			require.NoError(t, err)
			vt.setPrimary(t, 100)
			require.NoError(t, verify(boundedContext(t), testGTIDPosition(t, local)))
			assert.Equal(t, 2, vt.tmc.calls)
		})
	})

	t.Run("check 3 checks again when the primary has been repointed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt, verify, err := newBackup(t, local)
			require.NoError(t, err)
			// ERS has repointed 100 but not promoted 101 yet, so the shard record
			// still names 100 in its term.
			vt.setType(t, 100, topodatapb.TabletType_REPLICA)
			time.AfterFunc(10*time.Second, func() { vt.setPrimary(t, 101) })
			err = verify(boundedContext(t), testGTIDPosition(t, local))
			require.ErrorContains(t, err, "the new backup has errant GTIDs "+testPrimaryUUID+":11 that primary zone1-0000000101")
		})
	})

	t.Run("check 3 checks a position that check 2 didn't verify", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt, verify, err := newBackup(t, local)
			require.NoError(t, err)
			err = verify(boundedContext(t), testGTIDPosition(t, local+","+testOtherUUID+":1"))
			require.ErrorContains(t, err, "the new backup has errant GTIDs "+testOtherUUID+":1")
			assert.Equal(t, 2, vt.tmc.calls)
		})
	})

	t.Run("check 3 runs the full check when it can't read the shard", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			vt, verify, err := newBackup(t, local)
			require.NoError(t, err)
			// The shard can't be read now, so check 3 can't tell whether the
			// primary is unchanged; the full check then waits for the shard.
			require.NoError(t, vt.ts.DeleteShard(t.Context(), initKeyspace, initShard))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			err = verify(ctx, testGTIDPosition(t, local))
			require.ErrorContains(t, err, "stopped verifying the new backup")
			assert.Equal(t, 1, vt.tmc.calls, "only check 2 reached the primary")
		})
	})

	t.Run("check 2 fails on GTIDs written by init SQL", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			_, verify, err := newBackup(t, local+","+testLocalUUID+":1")
			require.ErrorContains(t, err, "the data to back up has errant GTIDs "+testLocalUUID+":1")
			require.ErrorContains(t, err, "were written by this run's --init-backup-sql-queries")
			assert.Nil(t, verify, "a failed check 2 sets no check 3")
		})
	})
}
