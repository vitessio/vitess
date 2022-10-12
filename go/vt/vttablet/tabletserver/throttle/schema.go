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

package throttle

// import (
// 	"context"
// 	"fmt"
// 	"math"

// 	"vitess.io/vitess/go/sqltypes"
// 	"vitess.io/vitess/go/vt/sqlparser"
// )

// const (
// 	// SchemaMigrationsTableName is used by VExec interceptor to call the correct handler
// 	sqlCreateSidecarDB            = "create database if not exists _vt"
// 	sqlCreateThrottlerConfigTable = `CREATE TABLE IF NOT EXISTS _vt.throttler_config (
// 			anchor int unsigned NOT NULL,
// 			last_updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
// 			enable tinyint unsigned NOT NULL DEFAULT 0,
// 			threshold float NOT NULL DEFAULT 0,
// 			PRIMARY KEY (anchor)
// 		) engine=InnoDB DEFAULT CHARSET=utf8mb4
// 	`
// 	sqlInsertConfigIgnore = `INSERT IGNORE INTO _vt.throttler_config (
// 			anchor,
// 			last_updated,
// 			enable,
// 			threshold
// 		) VALUES (
// 			1,
// 			NOW(),
// 			%a,
// 			%a
// 		)
// 	`
// 	sqlReadConfig      = `SELECT enable, threshold FROM _vt.throttler_config WHERE anchor=1`
// 	sqlUpdateEnabled   = `UPDATE _vt.throttler_config SET enable=%a WHERE anchor=1`
// 	sqlUpdateThreshold = `UPDATE _vt.throttler_config SET threshold=%a WHERE anchor=1`
// 	sqlUpdateConfig    = `INSERT INTO _vt.throttler_config (
// 			anchor,
// 			last_updated,
// 			enable,
// 			threshold
// 		) VALUES (
// 			1,
// 			NOW(),
// 			%a,
// 			%a
// 		)
// 		ON DUPLICATE KEY UPDATE
// 			last_updated=VALUES(last_updated),
// 			enable=VALUES(enable),
// 			threshold=VALUES(threshold)
// 	`
// )

// var ApplyDDL = []string{
// 	sqlCreateSidecarDB,
// 	sqlCreateThrottlerConfigTable,
// }

// func (throttler *Throttler) execQuery(ctx context.Context, query string) (result *sqltypes.Result, err error) {
// 	defer throttler.env.LogError()

// 	conn, err := throttler.pool.Get(ctx, nil)
// 	if err != nil {
// 		return result, err
// 	}
// 	defer conn.Recycle()
// 	return conn.Exec(ctx, query, math.MaxInt32, true)
// }

// func (throttler *Throttler) submitInitialConfig(ctx context.Context, enabled bool, threshold float64) error {
// 	query, err := sqlparser.ParseAndBind(sqlInsertConfigIgnore,
// 		sqltypes.BoolBindVariable(enabled),
// 		sqltypes.Float64BindVariable(threshold),
// 	)
// 	fmt.Printf("=========== ZZZ ParseAndBind: err=%v\n", err)
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Printf("=========== ZZZ submitting: %v\n", query)
// 	_, err = throttler.execQuery(ctx, query)
// 	fmt.Printf("=========== ZZZ submitted: err=%v\n", err)
// 	return err
// }

// func (throttler *Throttler) updateConfig(ctx context.Context, enabled bool, threshold float64) error {
// 	query, err := sqlparser.ParseAndBind(sqlUpdateConfig,
// 		sqltypes.BoolBindVariable(enabled),
// 		sqltypes.Float64BindVariable(threshold),
// 	)
// 	fmt.Printf("=========== ZZZ ParseAndBind: err=%v\n", err)
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Printf("=========== ZZZ submitting: %v\n", query)
// 	_, err = throttler.execQuery(ctx, query)
// 	fmt.Printf("=========== ZZZ submitted: err=%v\n", err)
// 	return err
// }

// func (throttler *Throttler) updateEnabled(ctx context.Context, enabled bool) error {
// 	query, err := sqlparser.ParseAndBind(sqlUpdateEnabled,
// 		sqltypes.BoolBindVariable(enabled),
// 	)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = throttler.execQuery(ctx, query)
// 	return err
// }

// func (throttler *Throttler) updateThreshold(ctx context.Context, threshold float64) error {
// 	query, err := sqlparser.ParseAndBind(sqlUpdateThreshold,
// 		sqltypes.Float64BindVariable(threshold),
// 	)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = throttler.execQuery(ctx, query)
// 	return err
// }

// func (throttler *Throttler) readConfig(ctx context.Context) (*ThrottlerConfig, error) {
// 	rs, err := throttler.execQuery(ctx, sqlReadConfig)
// 	if err != nil {
// 		return nil, err
// 	}
// 	row := rs.Named().Row()
// 	if row == nil {
// 		return nil, nil
// 	}
// 	config := &ThrottlerConfig{
// 		enabled:   row.AsBool("enable", false),
// 		threshold: row.AsFloat64("threshold", 0),
// 	}
// 	return config, nil
// }
