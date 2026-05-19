/*
Copyright 2026 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	// mysqlDiagnosticsTimeout is the timeout to use when running the diagnostics queries on MySQL.
	mysqlDiagnosticsTimeout = 5 * time.Second

	// defaultRedactedText is used when proper redaction fails for whatever reason. The full text is instead
	// replaced with this value.
	defaultRedactedText = "[REDACTED]"
)

// redactableColumns is the set of columns that may contain client SQL and should be redacted.
var redactableColumns = []string{
	"blocking_query",
	"info",
	"processlist_info",
	"trx_query",
	"waiting_query",
}

// diagnosticQuery describes a query to run that collects useful data about the current state of MySQL,
// to help debug notable events, such as when setting `super_read_only=1` stalls.
type diagnosticQuery struct {
	// description explains what the query is reading.
	description string

	// query is the actual query to run on MySQL.
	query string
}

// diagnosticQueries is the collection of queries to run to collect data during notable MySQL events.
var diagnosticQueries = []diagnosticQuery{
	{
		description: "active MySQL sessions",
		query: `select
	id,
	user,
	host,
	db,
	command,
	time,
	state,
	info
from information_schema.processlist
where command != 'Sleep' or info is not null
order by time desc
limit 50`,
	},
	{
		description: "current MySQL read-only settings",
		query: `select
	@@global.read_only as read_only,
	@@global.super_read_only as super_read_only`,
	},
	{
		description: "current MySQL semi-sync counters",
		query: `select
	variable_name,
	variable_value
from performance_schema.global_status
where variable_name like 'Rpl_semi_sync_source_%'
	or variable_name like 'Rpl_semi_sync_master_%'
order by variable_name`,
	},
	{
		description: "active InnoDB transactions",
		query: `select
	trx_id,
	trx_state,
	trx_started,
	trx_wait_started,
	trx_mysql_thread_id,
	trx_query
from information_schema.innodb_trx
order by trx_started
limit 50`,
	},
	{
		description: "current InnoDB row lock waits",
		query: `select
	waiting_trx.trx_mysql_thread_id as waiting_thread_id,
	waiting_trx.trx_started as waiting_trx_started,
	waiting_trx.trx_query as waiting_query,
	blocking_trx.trx_mysql_thread_id as blocking_thread_id,
	blocking_trx.trx_started as blocking_trx_started,
	blocking_trx.trx_query as blocking_query
from performance_schema.data_lock_waits lock_waits
join information_schema.innodb_trx waiting_trx
	on waiting_trx.trx_id = lock_waits.requesting_engine_transaction_id
join information_schema.innodb_trx blocking_trx
	on blocking_trx.trx_id = lock_waits.blocking_engine_transaction_id
limit 50`,
	},
	{
		description: "pending MySQL metadata locks",
		query: `select
	locks.object_type,
	locks.object_schema,
	locks.object_name,
	locks.lock_type,
	locks.lock_duration,
	locks.lock_status,
	threads.processlist_id,
	threads.processlist_user,
	threads.processlist_host,
	threads.processlist_db,
	threads.processlist_command,
	threads.processlist_time,
	threads.processlist_state,
	threads.processlist_info
from performance_schema.metadata_locks locks
left join performance_schema.threads threads
	on locks.owner_thread_id = threads.thread_id
where locks.lock_status = 'PENDING'
order by threads.processlist_time desc
limit 50`,
	},
}

// logMySQLDiagnostics logs useful MySQL diagnostics to help debug notable MySQL events.
func (tm *TabletManager) logMySQLDiagnostics(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mysqlDiagnosticsTimeout)
	defer cancel()

	var parser *sqlparser.Parser
	if tm.Env != nil {
		parser = tm.Env.Parser()
	}

	for _, diagnosticQuery := range diagnosticQueries {
		log.Info(
			"collecting DemotePrimary MySQL diagnostic query",
			slog.String("description", diagnosticQuery.description),
			slog.String("query", diagnosticQuery.query),
		)

		result, err := tm.MysqlDaemon.FetchSuperQuery(ctx, diagnosticQuery.query)
		if err != nil {
			log.Warn(
				"failed to collect DemotePrimary MySQL diagnostic query",
				slog.String("description", diagnosticQuery.description),
				slog.Any("error", err),
			)

			continue
		}

		rowCount := len(result.Rows)
		if rowCount == 0 {
			log.Info(
				diagnosticQuery.description,
				slog.String("description", diagnosticQuery.description),
				slog.Int("row_count", rowCount),
			)

			continue
		}

		// Collect the column names.
		columnNames := make([]string, 0, len(result.Fields))
		for fieldIndex, field := range result.Fields {
			columnName := field.Name
			if columnName == "" {
				columnName = fmt.Sprintf("column_%d", fieldIndex)
			}

			columnNames = append(columnNames, columnName)
		}

		// Log a line for each row returned by the query.
		for rowIndex, row := range result.Rows {
			diagnosticRow := make(map[string]string, len(row))
			for valueIndex, value := range row {
				columnName := fmt.Sprintf("column_%d", valueIndex)
				if valueIndex < len(columnNames) {
					columnName = columnNames[valueIndex]
				}

				diagnosticRow[columnName] = redactIfNecessary(parser, columnName, value.ToString())
			}

			log.Info(
				diagnosticQuery.description,
				slog.String("description", diagnosticQuery.description),
				slog.Any("row", diagnosticRow),
				slog.Int("row_index", rowIndex),
				slog.Int("row_count", rowCount),
			)
		}
	}
}

// redactIfNecessary redacts a column's value if it may contain client SQL.
func redactIfNecessary(parser *sqlparser.Parser, columnName, value string) string {
	if !isRedactableColumn(columnName) {
		return value
	}

	return redactQueryText(parser, value)
}

// isRedactableColumn returns true if the column may contain client SQL.
func isRedactableColumn(columnName string) bool {
	for _, queryTextColumnName := range redactableColumns {
		if strings.EqualFold(columnName, queryTextColumnName) {
			return true
		}
	}

	return false
}

// redactQueryText removes literals from the query text.
func redactQueryText(parser *sqlparser.Parser, queryText string) string {
	if strings.TrimSpace(queryText) == "" || parser == nil {
		return defaultRedactedText
	}

	redactedQueryText, err := parser.RedactSQLQuery(queryText)
	if err != nil {
		return defaultRedactedText
	}

	return redactedQueryText
}
