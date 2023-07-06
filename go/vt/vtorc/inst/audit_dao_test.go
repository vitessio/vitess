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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// TestAuditOperation tests that auditing a operation works as intended based on the configurations.
// This test also verifies that we are able to read the recent audits that are written to the databaes.
func TestAuditOperation(t *testing.T) {
	// Restore original configurations
	originalAuditSysLog := config.Config.AuditToSyslog
	originalAuditLogFile := config.Config.AuditLogFile
	originalAuditBackend := config.Config.AuditToBackendDB
	defer func() {
		config.Config.AuditToSyslog = originalAuditSysLog
		config.Config.AuditLogFile = originalAuditLogFile
		config.Config.AuditToBackendDB = originalAuditBackend
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

	tab100Alias := topoproto.TabletAliasString(tab100.Alias)
	auditType := "test-audit-operation"
	message := "test-message"

	t.Run("audit to backend", func(t *testing.T) {
		config.Config.AuditLogFile = ""
		config.Config.AuditToSyslog = false
		config.Config.AuditToBackendDB = true

		// Auditing should succeed as expected
		err = AuditOperation(auditType, tab100Alias, message)
		require.NoError(t, err)

		// Check that we can read the recent audits
		audits, err := readRecentAudit(tab100Alias, 0)
		require.NoError(t, err)
		require.Len(t, audits, 1)
		require.EqualValues(t, 1, audits[0].AuditID)
		require.EqualValues(t, auditType, audits[0].AuditType)
		require.EqualValues(t, message, audits[0].Message)
		require.EqualValues(t, tab100Alias, audits[0].AuditTabletAlias)

		// Check the same for no-filtering
		audits, err = readRecentAudit("", 0)
		require.NoError(t, err)
		require.Len(t, audits, 1)
		require.EqualValues(t, 1, audits[0].AuditID)
		require.EqualValues(t, auditType, audits[0].AuditType)
		require.EqualValues(t, message, audits[0].Message)
		require.EqualValues(t, tab100Alias, audits[0].AuditTabletAlias)
	})

	t.Run("audit to File", func(t *testing.T) {
		config.Config.AuditToBackendDB = false
		config.Config.AuditToSyslog = false

		file, err := os.CreateTemp("", "test-auditing-*")
		require.NoError(t, err)
		defer os.Remove(file.Name())
		config.Config.AuditLogFile = file.Name()

		err = AuditOperation(auditType, tab100Alias, message)
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
	AuditTabletAlias string
	Message          string
}

// readRecentAudit returns a list of audit entries order chronologically descending, using page number.
func readRecentAudit(tabletAlias string, page int) ([]audit, error) {
	res := []audit{}
	var args []any
	whereCondition := ``
	if tabletAlias != "" {
		whereCondition = `where alias=?`
		args = append(args, tabletAlias)
	}
	query := fmt.Sprintf(`
		select
			audit_id,
			audit_timestamp,
			audit_type,
			alias,
			message
		from
			audit
		%s
		order by
			audit_timestamp desc
		limit ?
		offset ?
		`, whereCondition)
	args = append(args, config.AuditPageSize, page*config.AuditPageSize)
	err := db.QueryVTOrc(query, args, func(m sqlutils.RowMap) error {
		a := audit{}
		a.AuditID = m.GetInt64("audit_id")
		a.AuditTimestamp = m.GetString("audit_timestamp")
		a.AuditType = m.GetString("audit_type")
		a.AuditTabletAlias = m.GetString("alias")
		a.Message = m.GetString("message")

		res = append(res, a)
		return nil
	})
	return res, err
}
