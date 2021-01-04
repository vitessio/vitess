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

	"github.com/rcrowley/go-metrics"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

// syslogWriter is optional, and defaults to nil (disabled)
var syslogWriter *syslog.Writer

var auditOperationCounter = metrics.NewCounter()

func init() {
	metrics.Register("audit.write", auditOperationCounter)
}

// EnableSyslogWriter enables, if possible, writes to syslog. These will execute _in addition_ to normal logging
func EnableAuditSyslog() (err error) {
	syslogWriter, err = syslog.New(syslog.LOG_ERR, "orchestrator")
	if err != nil {
		syslogWriter = nil
	}
	return err
}

// AuditOperation creates and writes a new audit entry by given params
func AuditOperation(auditType string, instanceKey *InstanceKey, message string) error {
	if instanceKey == nil {
		instanceKey = &InstanceKey{}
	}
	clusterName := ""
	if instanceKey.Hostname != "" {
		clusterName, _ = GetClusterName(instanceKey)
	}

	auditWrittenToFile := false
	if config.Config.AuditLogFile != "" {
		auditWrittenToFile = true
		go func() error {
			f, err := os.OpenFile(config.Config.AuditLogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0640)
			if err != nil {
				return log.Errore(err)
			}

			defer f.Close()
			text := fmt.Sprintf("%s\t%s\t%s\t%d\t[%s]\t%s\t\n", time.Now().Format(log.TimeFormat), auditType, instanceKey.Hostname, instanceKey.Port, clusterName, message)
			if _, err = f.WriteString(text); err != nil {
				return log.Errore(err)
			}
			return nil
		}()
	}
	if config.Config.AuditToBackendDB {
		_, err := db.ExecOrchestrator(`
			insert
				into audit (
					audit_timestamp, audit_type, hostname, port, cluster_name, message
				) VALUES (
					NOW(), ?, ?, ?, ?, ?
				)
			`,
			auditType,
			instanceKey.Hostname,
			instanceKey.Port,
			clusterName,
			message,
		)
		if err != nil {
			return log.Errore(err)
		}
	}
	logMessage := fmt.Sprintf("auditType:%s instance:%s cluster:%s message:%s", auditType, instanceKey.DisplayString(), clusterName, message)
	if syslogWriter != nil {
		auditWrittenToFile = true
		go func() {
			syslogWriter.Info(logMessage)
		}()
	}
	if !auditWrittenToFile {
		log.Infof(logMessage)
	}
	auditOperationCounter.Inc(1)

	return nil
}

// ReadRecentAudit returns a list of audit entries order chronologically descending, using page number.
func ReadRecentAudit(instanceKey *InstanceKey, page int) ([]Audit, error) {
	res := []Audit{}
	args := sqlutils.Args()
	whereCondition := ``
	if instanceKey != nil {
		whereCondition = `where hostname=? and port=?`
		args = append(args, instanceKey.Hostname, instanceKey.Port)
	}
	query := fmt.Sprintf(`
		select
			audit_id,
			audit_timestamp,
			audit_type,
			hostname,
			port,
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
	err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		audit := Audit{}
		audit.AuditId = m.GetInt64("audit_id")
		audit.AuditTimestamp = m.GetString("audit_timestamp")
		audit.AuditType = m.GetString("audit_type")
		audit.AuditInstanceKey.Hostname = m.GetString("hostname")
		audit.AuditInstanceKey.Port = m.GetInt("port")
		audit.Message = m.GetString("message")

		res = append(res, audit)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err

}

// ExpireAudit removes old rows from the audit table
func ExpireAudit() error {
	return ExpireTableData("audit", "audit_timestamp")
}
