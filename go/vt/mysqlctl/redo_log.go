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

package mysqlctl

import (
	"context"
	"fmt"
)

func (mysqld *Mysqld) BinaryHasDisableRedoLog() bool {
	return mysqld.capabilities.hasDisableRedoLog()
}

func (mysqld *Mysqld) DisableRedoLog(ctx context.Context) error {
	return mysqld.ExecuteSuperQuery(ctx, "ALTER INSTANCE DISABLE INNODB REDO_LOG")
}

func (mysqld *Mysqld) EnableRedoLog(ctx context.Context) error {
	return mysqld.ExecuteSuperQuery(ctx, "ALTER INSTANCE ENABLE INNODB REDO_LOG")
}

func (mysqld *Mysqld) ProcessCanDisableRedoLog(ctx context.Context) (bool, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SELECT variable_value FROM performance_schema.global_status WHERE variable_name = 'innodb_redo_log_enabled'")
	if err != nil {
		// It's possible that the MySQL process can disable redo logging, but
		// we were unable to connect in order to verify. Let's assume not and
		// let the caller decide if they want to retry.
		return false, err
	}
	if len(qr.Rows) == 0 {
		return false, fmt.Errorf("mysqld >= 8.0.21 required to disable the redo log")
	}
	return true, nil
}
