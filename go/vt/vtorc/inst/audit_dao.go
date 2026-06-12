/*
   Copyright 2014 Outbrain Inc.

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
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
)

var auditOperationCounter = stats.NewCounter("AuditWrite", "Number of audit operations performed")

// AuditOperation creates and writes a new audit entry by given params
func AuditOperation(auditType string, tabletAlias *topodatapb.TabletAlias, message string) error {
	var keyspace, shard string
	if tabletAlias != nil {
		keyspace, shard, _ = GetKeyspaceShardName(tabletAlias)
	}
	tabletAliasString := topoproto.TabletAliasString(tabletAlias)

	auditWrittenToFile := false
	if config.GetAuditFileLocation() != "" {
		auditWrittenToFile = true
		go func() {
			f, err := os.OpenFile(config.GetAuditFileLocation(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o640)
			if err != nil {
				log.Error(err.Error())
				return
			}

			defer f.Close()
			text := fmt.Sprintf("%s\t%s\t%s\t[%s:%s]\t%s\t\n", time.Now().Format("2006-01-02 15:04:05"), auditType, tabletAliasString, keyspace, shard, message)
			if _, err = f.WriteString(text); err != nil {
				log.Error(err.Error())
			}
		}()
	}
	if config.GetAuditToBackend() {
		_, err := db.ExecVTOrc(`INSERT
			INTO audit (
				audit_timestamp,
				audit_type,
				alias,
				keyspace,
				shard,
				message
			) VALUES (
				DATETIME('now'),
				?,
				?,
				?,
				?,
				?
			)`,
			auditType,
			tabletAliasString,
			keyspace,
			shard,
			message,
		)
		if err != nil {
			log.Error(err.Error())
			return err
		}
	}
	logMessage := fmt.Sprintf("auditType:%s alias:%s keyspace:%s shard:%s message:%s", auditType, tabletAliasString, keyspace, shard, message)
	if syslogMessage(logMessage) {
		auditWrittenToFile = true
	}
	if !auditWrittenToFile {
		log.Info(logMessage)
	}
	auditOperationCounter.Add(1)

	return nil
}

// ExpireAudit removes old rows from the audit table
func ExpireAudit() error {
	return ExpireTableData("audit", "audit_timestamp")
}
