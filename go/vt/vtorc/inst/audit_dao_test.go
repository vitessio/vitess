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

package inst

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log/logtest"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// TestAuditOperation tests that auditing a operation works as intended based on the configurations.
// This test also verifies that we are able to read the recent audits that are written to the databaes.
func TestAuditOperation(t *testing.T) {
	// Restore original configurations
	originalAuditSysLog := config.GetAuditToSyslog()
	originalAuditLogFile := config.GetAuditFileLocation()
	originalAuditBackend := config.GetAuditToBackend()
	defer func() {
		config.SetAuditToSyslog(originalAuditSysLog)
		config.SetAuditFileLocation(originalAuditLogFile)
		config.SetAuditToBackend(originalAuditBackend)
	}()

	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	defer func() {
		_, err = orcDb.Exec("delete from audit")
		require.NoError(t, err)
		_, err = orcDb.Exec("delete from vitess_tablet")
		require.NoError(t, err)
	}()

	// Store a tablet in the database
	ks := "ks"
	shard := "0"
	hostname := "localhost"
	var port int32 = 100
	tab100 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-1",
			Uid:  100,
		},
		Hostname:      hostname,
		Keyspace:      ks,
		Shard:         shard,
		Type:          topodatapb.TabletType_PRIMARY,
		MysqlHostname: hostname,
		MysqlPort:     port,
	}
	err = SaveTablet(tab100)
	require.NoError(t, err)

	auditType := "test-audit-operation"
	message := "test-message"

	t.Run("audit to backend", func(t *testing.T) {
		config.SetAuditFileLocation("")
		config.SetAuditToSyslog(false)
		config.SetAuditToBackend(true)

		// Auditing should succeed as expected
		err = AuditOperation(auditType, tab100.Alias, message)
		require.NoError(t, err)

		// Check that we can read the recent audits
		audits, err := readRecentAudit(tab100.Alias, 0)
		require.NoError(t, err)
		require.Len(t, audits, 1)
		require.EqualValues(t, 1, audits[0].AuditID)
		require.Equal(t, auditType, audits[0].AuditType)
		require.Equal(t, message, audits[0].Message)
		testRequireTabletAliasEqual(t, tab100.Alias, audits[0].AuditTabletAlias)

		// Check the same for no-filtering
		audits, err = readRecentAudit(nil, 0)
		require.NoError(t, err)
		require.Len(t, audits, 1)
		require.EqualValues(t, 1, audits[0].AuditID)
		require.Equal(t, auditType, audits[0].AuditType)
		require.Equal(t, message, audits[0].Message)
		testRequireTabletAliasEqual(t, tab100.Alias, audits[0].AuditTabletAlias)
	})

	t.Run("audit to File", func(t *testing.T) {
		config.SetAuditToBackend(false)
		config.SetAuditToSyslog(false)

		file, err := os.CreateTemp("", "test-auditing-*")
		require.NoError(t, err)
		defer os.Remove(file.Name())
		config.SetAuditFileLocation(file.Name())

		err = AuditOperation(auditType, tab100.Alias, message)
		require.NoError(t, err)

		// Give a little time for the write to succeed since it happens in a separate go-routine
		// There is no way to wait for that write to complete. This sleep is required to prevent this test from
		// becoming flaky wherein we sometimes read the file before the contents are written.
		time.Sleep(100 * time.Millisecond)
		fileContent, err := os.ReadFile(file.Name())
		require.NoError(t, err)
		require.Contains(t, string(fileContent), "\ttest-audit-operation\tzone-1-0000000100\t[ks:0]\ttest-message")
	})
}

// audit presents a single audit entry (namely in the database)
type audit struct {
	AuditID          int64
	AuditTimestamp   string
	AuditType        string
	AuditTabletAlias *topodatapb.TabletAlias
	Message          string
}

// readRecentAudit returns a list of audit entries order chronologically descending, using page number.
func readRecentAudit(tabletAlias *topodatapb.TabletAlias, page int) ([]audit, error) {
	res := []audit{}
	var args []any
	whereCondition := ``
	if tabletAlias != nil {
		whereCondition = `WHERE alias = ?`
		args = append(args, topoproto.TabletAliasString(tabletAlias))
	}
	query := fmt.Sprintf(`
		SELECT
			audit_id,
			audit_timestamp,
			audit_type,
			alias,
			message
		FROM
			audit
		%s
		ORDER BY
			audit_timestamp DESC
		LIMIT ?
		OFFSET ?`,
		whereCondition,
	)
	args = append(args, config.AuditPageSize, page*config.AuditPageSize)
	err := db.QueryVTOrc(query, args, func(m sqlutils.RowMap) (err error) {
		a := audit{}
		a.AuditID = m.GetInt64("audit_id")
		a.AuditTimestamp = m.GetString("audit_timestamp")
		a.AuditType = m.GetString("audit_type")
		a.Message = m.GetString("message")
		a.AuditTabletAlias, err = topoproto.ParseTabletAlias(m.GetString("alias"))
		if err != nil {
			return err
		}

		res = append(res, a)
		return nil
	})
	return res, err
}

func TestAuditOperationLogs(t *testing.T) {
	originalAuditSysLog := config.GetAuditToSyslog()
	originalAuditLogFile := config.GetAuditFileLocation()
	originalAuditBackend := config.GetAuditToBackend()
	t.Cleanup(func() {
		config.SetAuditToSyslog(originalAuditSysLog)
		config.SetAuditFileLocation(originalAuditLogFile)
		config.SetAuditToBackend(originalAuditBackend)
		db.ClearVTOrcDatabase()
	})
	config.SetAuditFileLocation("")
	config.SetAuditToSyslog(false)
	config.SetAuditToBackend(false)

	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Keyspace: "ks",
		Shard:    "0",
	}
	require.NoError(t, SaveTablet(tablet))

	logs := logtest.Capture(t, slog.LevelInfo)
	require.NoError(t, AuditOperation("test-audit", tablet.Alias, "test-message"))
	record := logs.Record(t, "Audit operation")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "test-audit", record["audit_type"])
	assert.Equal(t, "zone1-0000000100", record["tablet"])
	assert.Equal(t, "ks", record["keyspace"])
	assert.Equal(t, "0", record["shard"])
	assert.Equal(t, "test-message", record["message"])

	// Without a tablet or a message, the record leaves those keys out
	// instead of logging "<nil>" and empty values.
	logs.Reset()
	require.NoError(t, AuditOperation("test-audit", nil, ""))
	record = logs.Record(t, "Audit operation")
	assert.Equal(t, "test-audit", record["audit_type"])
	for _, key := range []string{"tablet", "keyspace", "shard", "message"} {
		assert.NotContains(t, record, key)
	}
	assert.NotContains(t, logs.String(), "<nil>")

	// A tablet that isn't in the backend has no keyspace or shard to log.
	logs.Reset()
	require.NoError(t, AuditOperation("test-audit", &topodatapb.TabletAlias{Cell: "zone1", Uid: 200}, ""))
	record = logs.Record(t, "Audit operation")
	assert.Equal(t, "zone1-0000000200", record["tablet"])
	assert.NotContains(t, record, "keyspace")
	assert.NotContains(t, record, "shard")
}
