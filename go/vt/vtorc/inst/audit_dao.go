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
	"log/syslog"
	"os"
	"time"

	"vitess.io/vitess/go/vt/log"

	"github.com/rcrowley/go-metrics"

	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// syslogWriter is optional, and defaults to nil (disabled)
var syslogWriter *syslog.Writer

var auditOperationCounter = metrics.NewCounter()

func init() {
	_ = metrics.Register("audit.write", auditOperationCounter)
}

// EnableSyslogWriter enables, if possible, writes to syslog. These will execute _in addition_ to normal logging
func EnableAuditSyslog() (err error) {
	syslogWriter, err = syslog.New(syslog.LOG_ERR, "vtorc")
	if err != nil {
		syslogWriter = nil
	}
	return err
}

// AuditOperation creates and writes a new audit entry by given params
func AuditOperation(auditType string, tabletAlias string, message string) error {
	keyspace := ""
	shard := ""
	if tabletAlias != "" {
		keyspace, shard, _ = GetKeyspaceShardName(tabletAlias)
	}

	auditWrittenToFile := false
	if config.Config.AuditLogFile != "" {
		auditWrittenToFile = true
		go func() {
			f, err := os.OpenFile(config.Config.AuditLogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0640)
			if err != nil {
				log.Error(err)
				return
			}

			defer f.Close()
			text := fmt.Sprintf("%s\t%s\t%s\t[%s:%s]\t%s\t\n", time.Now().Format("2006-01-02 15:04:05"), auditType, tabletAlias, keyspace, shard, message)
			if _, err = f.WriteString(text); err != nil {
				log.Error(err)
			}
		}()
	}
	if config.Config.AuditToBackendDB {
		_, err := db.ExecVTOrc(`
			insert
				into audit (
					audit_timestamp, audit_type, alias, keyspace, shard, message
				) VALUES (
					NOW(), ?, ?, ?, ?, ?
				)
			`,
			auditType,
			tabletAlias,
			keyspace,
			shard,
			message,
		)
		if err != nil {
			log.Error(err)
			return err
		}
	}
	logMessage := fmt.Sprintf("auditType:%s alias:%s keyspace:%s shard:%s message:%s", auditType, tabletAlias, keyspace, shard, message)
	if syslogWriter != nil {
		auditWrittenToFile = true
		go func() {
			_ = syslogWriter.Info(logMessage)
		}()
	}
	if !auditWrittenToFile {
		log.Infof(logMessage)
	}
	auditOperationCounter.Inc(1)

	return nil
}

// ExpireAudit removes old rows from the audit table
func ExpireAudit() error {
	return ExpireTableData("audit", "audit_timestamp")
}
