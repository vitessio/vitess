/*
Copyright 2019 The Vitess Authors.

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

package mysqlctl

import (
	"context"
	"errors"
	"fmt"
)

func (mysqld *Mysqld) CanDisableRedoLog() bool {
	return mysqld.capabilities.hasDisableRedoLogging()
}

func (mysqld *Mysqld) DisableRedoLog(ctx context.Context) error {
	if !mysqld.CanDisableRedoLog() {
		return fmt.Errorf("mysqld >= 8.0.21 required to disable redo_log")
	}
	return mysqld.ExecuteSuperQuery(ctx, "ALTER INSTANCE DISABLE INNODB REDO_LOG")
}

func (mysqld *Mysqld) EnableRedoLog(ctx context.Context) error {
	if !mysqld.CanDisableRedoLog() {
		return fmt.Errorf("mysqld >= 8.0.21 required to enable redo_log")
	}
	return mysqld.ExecuteSuperQuery(ctx, "ALTER INSTANCE ENABLE INNODB REDO_LOG")
}

func (mysqld *Mysqld) IsRedoLogEnabled(ctx context.Context) (bool, error) {
	if !mysqld.CanDisableRedoLog() {
		// redo_log is most likely enabled, but return false anyway. Possible
		// that mysqld is using non-InnoDB engine.
		return false, fmt.Errorf("mysqld >= 8.0.21 required to inspect redo_log status")
	}
	qr, err := mysqld.FetchSuperQuery(ctx, "SHOW GLOBAL STATUS LIKE 'Innodb_redo_log_enabled'")
	if err != nil {
		return false, err
	}
	if len(qr.Rows) != 1 {
		return false, errors.New("no Innodb_redo_log_enabled variable in mysql")
	}
	return qr.Rows[0][1].ToString() == "ON", nil
}
