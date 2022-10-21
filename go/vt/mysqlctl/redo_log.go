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
	"strings"
)

func (mysqld *Mysqld) DisableRedoLog(ctx context.Context) error {
	enabled, err := mysqld.IsRedoLogEnabled(ctx)
	if enabled || err != nil {
		return err
	}
	return mysqld.ExecuteSuperQuery(ctx, "ALTER INSTANCE DISABLE INNODB REDO_LOG")
}

func (mysqld *Mysqld) EnableRedoLog(ctx context.Context) error {
	enabled, err := mysqld.IsRedoLogEnabled(ctx)
	if enabled || err != nil {
		return err
	}
	return mysqld.ExecuteSuperQuery(ctx, "ALTER INSTANCE ENABLE INNODB REDO_LOG")
}

func (mysqld *Mysqld) IsRedoLogEnabled(ctx context.Context) (bool, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SELECT variable_value FROM performance_schema.global_status WHERE variable_name = 'innodb_redo_log_enabled'")
	// If we got an error, don't make assumptions about whether redo log is enabled.
	// It's possible we're dealing with a MySQL >= 8.0.21 server, but failed to connect.
	if err != nil {
		return false, err
	}
	// If the innodb_redo_log_enabled variable isn't present, assume that redo log is enabled.
	if len(qr.Rows) == 0 {
		return true, fmt.Errorf("mysqld >= 8.0.21 required to disable redo_log")
	}
	value := strings.ToLower(qr.Rows[0][0].ToString())
	return (value == "on" || value == "1"), nil
}
