/*
Copyright 2021 The Vitess Authors.

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

package general

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/grpcclient"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
)

// runSQL runs a single statement directly on the tablet's mysqld, optionally
// selecting a database first.
func runSQL(ctx context.Context, t *testing.T, sql string, tablet *vitesst.Tablet, db string) (*sqltypes.Result, error) {
	conn := tabletConn(ctx, t, tablet, db)
	defer conn.Close()
	return conn.ExecuteFetch(sql, 1000, true)
}

// runSQLs runs a list of statements on a single connection to the tablet's
// mysqld.
func runSQLs(ctx context.Context, t *testing.T, sqls []string, tablet *vitesst.Tablet, db string) error {
	conn := tabletConn(ctx, t, tablet, db)
	defer conn.Close()
	for _, sql := range sqls {
		if _, err := conn.ExecuteFetch(sql, 1000, true); err != nil {
			return err
		}
	}
	return nil
}

func tabletConn(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, db string) *mysql.Conn {
	t.Helper()
	params, err := tablet.DBAConnParams(ctx, db)
	require.NoError(t, err)
	conn, err := mysql.Connect(ctx, &params)
	require.NoError(t, err)
	return conn
}

// connectTablet opens a dba connection to the tablet's mysqld, returning nil if
// it cannot connect.
func connectTablet(ctx context.Context, tablet *vitesst.Tablet, db string) *mysql.Conn {
	params, err := tablet.DBAConnParams(ctx, db)
	if err != nil {
		return nil
	}
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		return nil
	}
	return conn
}

// resetBinaryLogsCommand returns the flavor-appropriate statement to reset the
// tablet's binary logs and GTIDs.
func resetBinaryLogsCommand(ctx context.Context, tablet *vitesst.Tablet) (string, error) {
	conn := connectTablet(ctx, tablet, "")
	if conn == nil {
		return "", errors.New("could not connect to tablet mysqld")
	}
	defer conn.Close()
	return conn.ResetBinaryLogsCommand(), nil
}

// shardPrimaryTablet waits until a primary has been elected for the shard and
// returns the matching candidate tablet.
func shardPrimaryTablet(t *testing.T, vc *vtorcCluster, keyspace, shard string, candidates []*vitesst.Tablet) *vitesst.Tablet {
	t.Helper()
	ctx := t.Context()
	start := time.Now()
	for {
		if time.Since(start) > 60*time.Second {
			require.FailNow(t, "failed to elect primary before timeout")
		}
		si, err := vc.ts.GetShard(ctx, keyspace, shard)
		require.NoError(t, err)
		if si.PrimaryAlias == nil {
			time.Sleep(time.Second)
			continue
		}
		for _, tablet := range candidates {
			if tablet.Cell == si.PrimaryAlias.Cell && uint32(tablet.UID) == si.PrimaryAlias.Uid {
				return tablet
			}
		}
		time.Sleep(time.Second)
	}
}

// checkPrimaryTablet waits until the given tablet is the primary in topo and in
// its own health stream.
func checkPrimaryTablet(t *testing.T, vc *vtorcCluster, tablet *vitesst.Tablet, checkServing bool) {
	t.Helper()
	ctx := t.Context()
	start := time.Now()
	for {
		if time.Since(start) > 60*time.Second {
			require.FailNow(t, "failed to elect primary before timeout")
		}
		tabletInfo, err := getTabletRecord(ctx, vc.cluster, tablet.Alias())
		require.NoError(t, err)
		if tabletInfo.GetType() != topodatapb.TabletType_PRIMARY {
			time.Sleep(time.Second)
			continue
		}
		shrs, err := streamTabletHealth(ctx, tablet, 1)
		require.NoError(t, err)
		streamHealthResponse := shrs[0]
		if checkServing && !streamHealthResponse.GetServing() {
			time.Sleep(time.Second)
			continue
		}
		if streamHealthResponse.GetTarget().GetTabletType() != topodatapb.TabletType_PRIMARY {
			time.Sleep(time.Second)
			continue
		}
		return
	}
}

// checkReplication makes sure the table is created, writes succeed and are
// replicated to all replicas, and the topology validates.
func checkReplication(t *testing.T, vc *vtorcCluster, primary *vitesst.Tablet, replicas []*vitesst.Tablet, timeToWait time.Duration) {
	t.Helper()
	ctx := t.Context()
	endTime := time.Now().Add(timeToWait)
	sqlSchema := `
		create table if not exists vt_ks.vt_insert_test (
		id bigint,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB
		`
	for {
		if time.Now().After(endTime) {
			require.FailNow(t, "timedout waiting for keyspace vt_ks to be created by schema engine")
		}
		if _, err := runSQL(ctx, t, sqlSchema, primary, ""); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		vc.lastUsedValue++
		confirmReplication(t, primary, replicas, time.Until(endTime), vc.lastUsedValue)
		validateTopology(t, vc, true, time.Until(endTime))
		return
	}
}

// verifyWritesSucceed inserts more data into the primary and checks it is
// replicated to all replicas.
func verifyWritesSucceed(t *testing.T, vc *vtorcCluster, primary *vitesst.Tablet, replicas []*vitesst.Tablet, timeToWait time.Duration) {
	t.Helper()
	vc.lastUsedValue++
	confirmReplication(t, primary, replicas, timeToWait, vc.lastUsedValue)
}

func confirmReplication(t *testing.T, primary *vitesst.Tablet, replicas []*vitesst.Tablet, timeToWait time.Duration, valueToInsert int) {
	t.Helper()
	ctx := t.Context()
	insertSQL := fmt.Sprintf("insert into vt_insert_test(id, msg) values (%d, 'test %d')", valueToInsert, valueToInsert)
	_, err := runSQL(ctx, t, insertSQL, primary, "vt_ks")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	endTime := time.Now().Add(timeToWait)
	for {
		if time.Now().After(endTime) {
			require.FailNow(t, "timedout waiting for replication, data not yet replicated")
		}
		var lastErr error
		for _, tab := range replicas {
			if replErr := checkInsertedValues(ctx, t, tab, valueToInsert); replErr != nil {
				lastErr = replErr
			}
		}
		if lastErr != nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		return
	}
}

func checkInsertedValues(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, index int) error {
	selectSQL := fmt.Sprintf("select msg from vt_ks.vt_insert_test where id=%d", index)
	qr, err := runSQL(ctx, t, selectSQL, tablet, "")
	if err == nil && len(qr.Rows) == 1 {
		return nil
	}
	return errors.New("data is not yet replicated")
}

// validateTopology runs vtctld Validate and retries until it passes.
func validateTopology(t *testing.T, vc *vtorcCluster, pingTablets bool, timeToWait time.Duration) {
	t.Helper()
	ctx := t.Context()
	endTime := time.Now().Add(timeToWait)
	args := []string{"Validate"}
	if pingTablets {
		args = append(args, "--ping-tablets")
	}
	var lastErr error
	for {
		if time.Now().After(endTime) {
			require.FailNow(t, "time out waiting for validation to pass", "last error: %v", lastErr)
		}
		if _, err := vc.cluster.Vtctld().ExecuteCommandWithOutput(ctx, args...); err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
}

// waitForReplicationToStop waits for replication to stop on the given tablet.
func waitForReplicationToStop(ctx context.Context, t *testing.T, tablet *vitesst.Tablet) error {
	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-timeout:
			return errors.New("timedout: waiting for primary to stop replication")
		default:
			res, err := runSQL(ctx, t, "SHOW REPLICA STATUS", tablet, "")
			if err != nil {
				return err
			}
			if len(res.Rows) == 0 {
				return nil
			}
			time.Sleep(time.Second)
		}
	}
}

// checkSourcePort waits until the replica points its replication source at the
// given tablet.
func checkSourcePort(t *testing.T, replica *vitesst.Tablet, source *vitesst.Tablet, timeToWait time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeToWait)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			require.FailNow(t, "timedout waiting for correct primary to be setup")
			return
		default:
			res, err := runSQL(ctx, t, "SHOW REPLICA STATUS", replica, "")
			require.NoError(t, err)
			if len(res.Rows) == 1 {
				for idx, field := range res.Fields {
					if strings.EqualFold(field.Name, "SOURCE_HOST") {
						if res.Rows[0][idx].ToString() == source.Name() {
							return
						}
					}
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// checkHeartbeatInterval waits until the replica's replication heartbeat
// interval matches the expected value.
func checkHeartbeatInterval(t *testing.T, replica *vitesst.Tablet, heartbeatInterval float64, timeToWait time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeToWait)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			require.FailNow(t, "timed out waiting for correct heartbeat interval to be setup")
			return
		default:
			res, err := runSQL(ctx, t, "select * from performance_schema.replication_connection_configuration", replica, "")
			require.NoError(t, err)
			if len(res.Rows) == 1 {
				for idx, field := range res.Fields {
					if strings.EqualFold(field.Name, "HEARTBEAT_INTERVAL") {
						readVal, err := res.Rows[0][idx].ToFloat64()
						require.NoError(t, err)
						if readVal == heartbeatInterval {
							return
						}
					}
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// makeAPICall performs a GET against the VTOrc HTTP API.
func makeAPICall(t *testing.T, vtorc *vitesst.VTOrc, url string) (int, string, error) {
	t.Helper()
	return vtorc.MakeAPICall(t.Context(), url)
}

// waitForReadOnlyValue waits for the read_only global variable to reach the
// provided value.
func waitForReadOnlyValue(t *testing.T, tablet *vitesst.Tablet, expectValue int64) bool {
	t.Helper()
	ctx := t.Context()
	startTime := time.Now()
	for time.Since(startTime) < 15*time.Second {
		qr, err := runSQL(ctx, t, "select @@global.read_only as read_only", tablet, "")
		require.NoError(t, err)
		require.NotNil(t, qr)
		row := qr.Named().Row()
		require.NotNil(t, row)
		readOnly, err := row.ToInt64("read_only")
		require.NoError(t, err)
		if readOnly == expectValue {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func getVars(t *testing.T, vtorc *vitesst.VTOrc) map[string]any {
	t.Helper()
	vars, err := vtorc.GetVars(t.Context())
	require.NoError(t, err)
	return vars
}

// getSuccessfulRecoveryCount returns the current successful recovery count for
// the given recovery name, keyspace, and shard.
func getSuccessfulRecoveryCount(t *testing.T, vtorc *vitesst.VTOrc, recoveryName, keyspace, shard string) int {
	t.Helper()
	vars := getVars(t, vtorc)
	successfulRecoveriesMap, ok := vars["SuccessfulRecoveries"].(map[string]any)
	if !ok {
		return 0
	}
	mapKey := fmt.Sprintf("%s.%s.%s", recoveryName, keyspace, shard)
	return getIntFromValue(successfulRecoveriesMap[mapKey])
}

// waitForSuccessfulRecoveryCount waits until the given recovery name's count of
// successful runs matches the count expected.
func waitForSuccessfulRecoveryCount(t *testing.T, vtorc *vitesst.VTOrc, recoveryName, keyspace, shard string, countExpected int) {
	t.Helper()
	mapKey := fmt.Sprintf("%s.%s.%s", recoveryName, keyspace, shard)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		vars, err := vtorc.GetVars(t.Context())
		if !assert.NoError(c, err) {
			return
		}
		successfulRecoveriesMap, ok := vars["SuccessfulRecoveries"].(map[string]any)
		require.True(c, ok, "SuccessfulRecoveries metric not yet available")
		successCount := getIntFromValue(successfulRecoveriesMap[mapKey])
		assert.EqualValues(c, countExpected, successCount)
	}, 15*time.Second, time.Second, "timed out waiting for successful recovery count")
}

// waitForSuccessfulPRSCount waits until the shard's count of successful PRS runs
// matches the count expected.
func waitForSuccessfulPRSCount(t *testing.T, vtorc *vitesst.VTOrc, keyspace, shard string, countExpected int) {
	t.Helper()
	mapKey := fmt.Sprintf("%v.%v.success", keyspace, shard)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		vars, err := vtorc.GetVars(t.Context())
		if !assert.NoError(c, err) {
			return
		}
		prsCountsMap, ok := vars["PlannedReparentCounts"].(map[string]any)
		require.True(c, ok, "PlannedReparentCounts metric not yet available")
		successCount := getIntFromValue(prsCountsMap[mapKey])
		assert.EqualValues(c, countExpected, successCount)
	}, 15*time.Second, time.Second, "timed out waiting for successful PRS count")
}

// waitForSuccessfulERSCount waits until the shard's count of successful ERS runs
// matches the count expected.
func waitForSuccessfulERSCount(t *testing.T, vtorc *vitesst.VTOrc, keyspace, shard string, countExpected int) {
	t.Helper()
	mapKey := fmt.Sprintf("%v.%v.success", keyspace, shard)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		vars, err := vtorc.GetVars(t.Context())
		if !assert.NoError(c, err) {
			return
		}
		ersCountsMap, ok := vars["EmergencyReparentCounts"].(map[string]any)
		require.True(c, ok, "EmergencyReparentCounts metric not yet available")
		successCount := getIntFromValue(ersCountsMap[mapKey])
		assert.EqualValues(c, countExpected, successCount)
	}, 15*time.Second, time.Second, "timed out waiting for successful ERS count")
}

// getIntFromValue converts a numeric value from /debug/vars to an int.
func getIntFromValue(val any) int {
	value := reflect.ValueOf(val)
	if value.CanFloat() {
		return int(math.Round(value.Float()))
	}
	if value.CanInt() {
		return int(value.Int())
	}
	return 0
}

// waitForDetectedProblems waits until the given analysis code, alias, keyspace
// and shard count matches the count expected.
func waitForDetectedProblems(t *testing.T, vtorc *vitesst.VTOrc, code, alias, ks, shard string, expect int) {
	t.Helper()
	key := strings.Join([]string{code, alias, ks, shard}, ".")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		vars, err := vtorc.GetVars(t.Context())
		if !assert.NoError(c, err) {
			return
		}
		problems, ok := vars["DetectedProblems"].(map[string]any)
		require.True(c, ok, "DetectedProblems metric not yet available")
		actual, ok := problems[key]
		actual = getIntFromValue(actual)
		assert.True(
			c, ok,
			"The metric DetectedProblems[%s] should exist but does not (all problems: %+v)",
			key, problems,
		)
		assert.EqualValues(
			c, expect, actual,
			"The metric DetectedProblems[%s] should be %v but is %v (all problems: %+v)",
			key, expect, actual, problems,
		)
	}, 15*time.Second, time.Second, "timed out waiting for detected problem(s)")
}

// waitForTabletType waits for the tablet to reach a certain type.
func waitForTabletType(t *testing.T, tablet *vitesst.Tablet, expectedTabletType string) {
	t.Helper()
	require.NoError(t, tablet.WaitForTabletType(t.Context(), 30*time.Second, expectedTabletType))
}

// waitForDrainedTabletInVTOrc waits for VTOrc to see the specified number of
// drained tablets.
func waitForDrainedTabletInVTOrc(t *testing.T, vtorc *vitesst.VTOrc, count int) {
	t.Helper()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		statusCode, res, err := vtorc.MakeAPICall(t.Context(), "/api/database-state")
		assert.NoError(c, err)
		assert.Equal(c, 200, statusCode)
		found := strings.Count(res, fmt.Sprintf(`"tablet_type": "%d"`, topodatapb.TabletType_DRAINED))
		// The database_instance and vitess_tablet tables both store the tablet
		// type, and both should agree in the stable state.
		assert.Equal(c, found, count*2)
	}, 15*time.Second, time.Second, "timed out waiting for drained tablet in VTOrc")
}

// enableGlobalRecoveries enables global recoveries for the given VTOrc.
func enableGlobalRecoveries(t *testing.T, vtorc *vitesst.VTOrc) {
	status, resp, err := vtorc.MakeAPICall(t.Context(), "/api/enable-global-recoveries")
	require.NoError(t, err)
	assert.Equal(t, 200, status)
	assert.Equal(t, "Global recoveries enabled\n", resp)
}

// disableGlobalRecoveries disables global recoveries for the given VTOrc.
func disableGlobalRecoveries(t *testing.T, vtorc *vitesst.VTOrc) {
	status, resp, err := vtorc.MakeAPICall(t.Context(), "/api/disable-global-recoveries")
	require.NoError(t, err)
	assert.Equal(t, 200, status)
	assert.Equal(t, "Global recoveries disabled\n", resp)
}

// semiSyncExtensionLoaded reports which semi-sync plugin flavor is loaded on the
// given tablet's mysqld.
func semiSyncExtensionLoaded(ctx context.Context, tablet *vitesst.Tablet) (mysql.SemiSyncType, error) {
	conn := connectTablet(ctx, tablet, "")
	if conn == nil {
		return mysql.SemiSyncTypeUnknown, errors.New("could not connect to tablet mysqld")
	}
	defer conn.Close()
	return conn.SemiSyncExtensionLoaded()
}

// getDBVar returns the value of a MySQL global variable on the tablet.
func getDBVar(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, name string) string {
	t.Helper()
	qr, err := runSQL(ctx, t, fmt.Sprintf("show variables like '%s'", name), tablet, "")
	require.NoError(t, err)
	if len(qr.Rows) == 0 {
		return ""
	}
	return qr.Rows[0][1].ToString()
}

// isSemiSyncSetupCorrectly checks the replica-side semi-sync setting on the
// tablet.
func isSemiSyncSetupCorrectly(t *testing.T, tablet *vitesst.Tablet, semiSyncVal string) bool {
	t.Helper()
	ctx := t.Context()
	semisyncType, err := semiSyncExtensionLoaded(ctx, tablet)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		return semiSyncVal == getDBVar(ctx, t, tablet, "rpl_semi_sync_replica_enabled")
	case mysql.SemiSyncTypeMaster:
		return semiSyncVal == getDBVar(ctx, t, tablet, "rpl_semi_sync_slave_enabled")
	default:
		assert.Fail(t, "semisync extension not loaded")
		return false
	}
}

// isPrimarySemiSyncSetupCorrectly checks the primary-side semi-sync setting on
// the tablet.
func isPrimarySemiSyncSetupCorrectly(t *testing.T, tablet *vitesst.Tablet, semiSyncVal string) bool {
	t.Helper()
	ctx := t.Context()
	semisyncType, err := semiSyncExtensionLoaded(ctx, tablet)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		return semiSyncVal == getDBVar(ctx, t, tablet, "rpl_semi_sync_source_enabled")
	case mysql.SemiSyncTypeMaster:
		return semiSyncVal == getDBVar(ctx, t, tablet, "rpl_semi_sync_master_enabled")
	default:
		assert.Fail(t, "semisync extension not loaded")
		return false
	}
}

// fullStatusTabletType returns the tablet's served type from its own FullStatus.
func fullStatusTabletType(t *testing.T, vc *vtorcCluster, tablet *vitesst.Tablet) topodatapb.TabletType {
	t.Helper()
	ctx := t.Context()
	proto, err := tablet.TabletProto(ctx)
	require.NoError(t, err)
	status, err := vc.tmClient.FullStatus(ctx, proto)
	require.NoError(t, err)
	return status.TabletType
}

// streamTabletHealth invokes a HealthStream on a tablet and returns the
// responses. It returns an error if the stream ends with fewer than count
// responses.
func streamTabletHealth(ctx context.Context, tablet *vitesst.Tablet, count int) (responses []*querypb.StreamHealthResponse, err error) {
	proto, err := tablet.TabletProto(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := tabletconn.GetDialer()(ctx, proto, grpcclient.FailFast(false))
	if err != nil {
		return nil, err
	}

	i := 0
	err = conn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
		responses = append(responses, shr)
		i++
		if i >= count {
			return io.EOF
		}
		return nil
	})
	switch {
	case err != nil:
		return nil, err
	case len(responses) < count:
		return nil, errors.New("stream ended early")
	}
	return responses, nil
}

// canonicalAlias formats a tablet's alias in the zero-padded form used in
// topology records and VTOrc metric labels.
func canonicalAlias(tablet *vitesst.Tablet) string {
	return topoproto.TabletAliasString(&topodatapb.TabletAlias{Cell: tablet.Cell, Uid: uint32(tablet.UID)})
}

// getTabletRecord fetches a tablet's topology record through vtctld.
func getTabletRecord(ctx context.Context, cluster *vitesst.Cluster, alias string) (*topodatapb.Tablet, error) {
	out, err := cluster.Vtctld().ExecuteCommandWithOutput(ctx, "GetTablet", alias)
	if err != nil {
		return nil, err
	}
	tablet := &topodatapb.Tablet{}
	if err := json2.UnmarshalPB([]byte(out), tablet); err != nil {
		return nil, err
	}
	return tablet, nil
}

// updateTabletFields reads a tablet's cell-local topology record, applies the
// update, and writes it back, retrying on version conflicts. The record lives
// in the cell's topo root, which is opened directly since the cell's in-network
// address is not reachable from the host.
func updateTabletFields(ctx context.Context, etcdAddr, cell string, uid int, update func(*topodatapb.Tablet) error) error {
	ts, err := topo.OpenServer("etcd2", etcdAddr, "/vitess/"+cell)
	if err != nil {
		return err
	}
	defer ts.Close()

	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return err
	}

	alias := &topodatapb.TabletAlias{Cell: cell, Uid: uint32(uid)}
	tabletPath := path.Join(topo.TabletsPath, topoproto.TabletAliasString(alias), topo.TabletFile)
	for {
		data, version, err := conn.Get(ctx, tabletPath)
		if err != nil {
			return err
		}
		tablet := &topodatapb.Tablet{}
		if err := tablet.UnmarshalVT(data); err != nil {
			return err
		}
		if err := update(tablet); err != nil {
			return err
		}
		newData, err := tablet.MarshalVT()
		if err != nil {
			return err
		}
		if _, err := conn.Update(ctx, tabletPath, newData, version); err != nil {
			if topo.IsErrType(err, topo.BadVersion) {
				continue
			}
			return err
		}
		return nil
	}
}

// printVTOrcLogsOnFailure prints the VTOrc logs and dumps cluster diagnostics on
// test failure.
func printVTOrcLogsOnFailure(t *testing.T, ctx context.Context, vc *vtorcCluster) {
	if !t.Failed() {
		return
	}
	for _, vtorc := range vc.vtorcs {
		logs, err := vtorc.Logs(ctx)
		if err != nil {
			continue
		}
		t.Logf("VTOrc %s logs:\n%s", vtorc.Name(), logs)
	}
}
