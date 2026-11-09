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

package sqlerror

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/log/logtest"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	dupEntry = "Duplicate entry '1' for key 't.PRIMARY'"
	secret   = "SECRET_7f3a"
	// fakeMarker is user text that looks like a MySQL error's marker.
	fakeMarker = secret + " (errno 1045) (sqlstate 28000)"
)

func dupEntryError() *SQLError {
	err := NewSQLError(ERDupEntry, SSConstraintViolation, dupEntry)
	err.Query = "insert into t(id, v) values (1, '" + secret + "')"
	return err
}

type hidingError struct{ err error }

func (e hidingError) Error() string { return "request failed" }
func (e hidingError) Unwrap() error { return e.err }

type logValuerError struct{ *SQLError }

func (e logValuerError) LogValue() slog.Value { return slog.StringValue(e.Error()) }

type cyclicError struct{}

func (cyclicError) Error() string   { return "cyclic" }
func (e cyclicError) Unwrap() error { return e }

type cyclicJoin struct{}

func (cyclicJoin) Error() string     { return "cyclic join" }
func (e cyclicJoin) Unwrap() []error { return []error{e} }

func TestMySQLErrno(t *testing.T) {
	sqlErr := dupEntryError()
	// What vtgate gets back from vttablet for sqlErr.
	tabletErr := vterrors.New(vtrpcpb.Code_ALREADY_EXISTS, "target: ks.-80.primary: vttablet: rpc error: code = AlreadyExists desc = "+
		dupEntry+" (errno 1062) (sqlstate 23000) (CallerID: user1): Sql: \"insert into t(id, v) values (:vtg1, :vtg2)\", BindVars: {vtg2: \"type:VARCHAR value:\\\""+secret+"\\\"\"}")
	// A syntax error quotes the statement, here a comment holding a fake marker.
	syntaxErr := NewSQLError(ERSyntaxError, SSClientError, "You have an error in your SQL syntax; near '/* x\n"+fakeMarker+"\n*/ selec 1' at line 1")

	tests := []struct {
		name  string
		value slog.Value
		errno ErrorCode
	}{
		{"vterrors.Wrapf", slog.AnyValue(vterrors.Wrapf(syntaxErr, "copying %s", "t")), ERSyntaxError},
		{"errors.Join, nested, after a fake marker", slog.AnyValue(errors.Join(errors.New(fakeMarker), fmt.Errorf("copying: %w", sqlErr))), ERDupEntry},
		{"wrapper that hides the message", slog.AnyValue(hidingError{sqlErr}), ERDupEntry},
		{"SQLError quoting a fake marker", slog.AnyValue(syntaxErr), ERSyntaxError},
		{"LogValuer", slog.AnyValue(logValuerError{sqlErr}), ERDupEntry},
		{"NewSQLErrorFromError", slog.AnyValue(NewSQLErrorFromError(tabletErr)), ERDupEntry},
		{"fmt.Errorf %v", slog.AnyValue(fmt.Errorf("error in processing binlog event %v", sqlErr)), ERDupEntry},
		{"string value", slog.StringValue(sqlErr.Error()), ERDupEntry},
		{"from vttablet", slog.AnyValue(tabletErr), ERDupEntry},
		{"terse vttablet error", slog.StringValue("(errno 1062) (sqlstate 23000): Sql: \"insert\""), ERDupEntry},
		{"quoted marker skipped", slog.StringValue("could not set state: update t set message='" + fakeMarker + "': " + sqlErr.Error()), ERDupEntry},
		{"broken marker skipped", slog.StringValue("(errno ) " + dupEntry + " (errno 1062) (sqlstate 23000)"), ERDupEntry},
		{"first of an aggregate", slog.AnyValue(vterrors.Aggregate([]error{NewSQLError(ERLockDeadlock, "40001", "Deadlock found"), NewSQLError(ERLockWaitTimeout, "HY000", "Lock wait timeout")})), ERLockDeadlock},
		{"query after the marker not read", slog.StringValue("Incorrect integer value (errno 1366) (sqlstate HY000) during query: insert into t values ('" + fakeMarker + " during query: ')"), ERTruncatedWrongValueForField},
		// User text can change the number, never add text: the first marker
		// with a valid ending wins.
		{"fake marker in a value", slog.StringValue("Duplicate entry '" + fakeMarker + " during query: ' for key 't.PRIMARY' (errno 1062) (sqlstate 23000)"), ERAccessDeniedError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errno, ok := mysqlErrno(tt.value)
			assert.True(t, ok)
			assert.Equal(t, tt.errno, errno)
		})
	}

	none := []struct {
		name  string
		value slog.Value
	}{
		{"Vitess error with a MySQL state", slog.AnyValue(vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadDb, "Unknown database 'x'"))},
		{"nil error", slog.AnyValue(error(nil))},
		{"cyclic Unwrap", slog.AnyValue(cyclicError{})},
		{"cyclic errors.Join", slog.AnyValue(cyclicJoin{})},
		{"errno out of range stops the search", slog.StringValue("x (errno 70000) (sqlstate 23000)\n" + sqlErr.Error())},
	}
	for _, tt := range none {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := mysqlErrno(tt.value)
			assert.False(t, ok)
		})
	}
}

// TestFindSQLErrorBudget checks that the plain errors in an errors.Join count
// against the bound on the errors findSQLError visits.
func TestFindSQLErrorBudget(t *testing.T) {
	errs := make([]error, maxChainErrors, maxChainErrors+1)
	for i := range errs {
		errs[i] = errors.New("plain")
	}
	budget := maxChainErrors
	assert.Nil(t, findSQLError(errors.Join(append(errs, dupEntryError())...), &budget))
	assert.Zero(t, budget)
}

// TestParseErrnoLarge checks that reading a large error takes linear time. The
// bound leaves room for -race on a loaded machine; a quadratic parse of broken
// takes minutes.
func TestParseErrnoLarge(t *testing.T) {
	quoted := strings.Repeat("'"+fakeMarker+"' ", 1<<20/40) + dupEntry + " (errno 1062) (sqlstate 23000)"
	broken := strings.Repeat("(errno 1062) ", 1<<20/13)
	start := time.Now()
	errno, ok := parseErrno(quoted)
	assert.True(t, ok)
	assert.Equal(t, ERDupEntry, errno)
	_, ok = parseErrno(broken)
	assert.False(t, ok)
	assert.Less(t, time.Since(start), 60*time.Second)
}

func TestLogMySQLError(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	log.Warn("insert failed", slog.Any("error", vterrors.Wrapf(dupEntryError(), "copying")), slog.Any("cause", NewSQLError(ERLockDeadlock, "40001", "Deadlock found")))
	log.Error("request failed", slog.Any("error", vterrors.New(vtrpcpb.Code_UNAVAILABLE, "tablet is shutting down")), slog.String("error_code", vtrpcpb.Code_UNAVAILABLE.String()))
	log.Error("spoofed", slog.String("error", "Duplicate entry '"+fakeMarker+"\n' for key 't.PRIMARY' (errno 1062) (sqlstate 23000)"))
	log.Error("own number", slog.Any("error", dupEntryError()), slog.Int("mysql_errno", int(ERLockDeadlock)))
	var nilErr *SQLError
	log.Error("typed nil", slog.Any("error", nilErr))
	log.Error("not a replica", slog.Any("error", NewSQLError(ERNotReplica, SSUnknownSQLState, "no replication status")))
	log.Error("lowest MySQL number", slog.Any("error", NewSQLError(1000, "HY000", "hashchk")))
	log.Info("client error", slog.Any("error", NewSQLErrorf(CRServerLost, SSUnknownSQLState, "%v", errors.New("read tcp 10.0.0.1:5555->10.0.0.2:3306: i/o timeout"))))

	record := logs.Record(t, "insert failed")
	assert.Equal(t, "WARN", record["level"])
	assert.EqualValues(t, ERDupEntry, record["mysql_errno"])
	assert.Contains(t, record["error"], secret)
	assert.Equal(t, []string{"cause", "error", "level", "msg", "mysql_errno", "time"}, slices.Sorted(maps.Keys(record)))

	record = logs.Record(t, "request failed")
	assert.Equal(t, "UNAVAILABLE", record["error_code"])
	assert.NotContains(t, record, "mysql_errno")

	// A fake marker changes the number but adds no text.
	record = logs.Record(t, "spoofed")
	assert.EqualValues(t, ERAccessDeniedError, record["mysql_errno"])
	assert.Equal(t, []string{"error", "level", "msg", "mysql_errno", "time"}, slices.Sorted(maps.Keys(record)))

	// logtest fails on a duplicate key.
	assert.EqualValues(t, ERLockDeadlock, logs.Record(t, "own number")["mysql_errno"])

	assert.NotContains(t, logs.Record(t, "typed nil"), "mysql_errno")
	assert.NotContains(t, logs.Record(t, "not a replica"), "mysql_errno")
	assert.EqualValues(t, 1000, logs.Record(t, "lowest MySQL number")["mysql_errno"])
	// MySQL's client error numbers are public too.
	record = logs.Record(t, "client error")
	assert.Equal(t, "INFO", record["level"])
	assert.EqualValues(t, CRServerLost, record["mysql_errno"])
	assert.NotContains(t, logs.String(), "mysql_error")
}

func BenchmarkLogErrorAttrs(b *testing.B) {
	saved := log.SwapLogger(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	b.Cleanup(func() { log.SwapLogger(saved) })

	tabletErr := vterrors.New(vtrpcpb.Code_UNKNOWN, "target: ks.-80.primary: vttablet: rpc error: code = Unknown desc = "+
		dupEntry+" (errno 1062) (sqlstate 23000) (CallerID: user1): Sql: \"insert into t(id, v) values (:vtg1, :vtg2)\", BindVars: {}")
	large := errors.New(strings.Repeat("'"+fakeMarker+"' (errno 1062) ", 1<<20/48) + dupEntry + " (errno 1062) (sqlstate 23000)")
	cases := []struct {
		name  string
		attrs []slog.Attr
	}{
		{"no error", []slog.Attr{slog.String("table", "t"), slog.Int("rows", 1)}},
		{"plain error", []slog.Attr{slog.String("table", "t"), slog.Any("error", errors.New("connection refused"))}},
		{"wrapped plain error", []slog.Attr{slog.String("table", "t"), slog.Any("error", vterrors.Wrapf(vterrors.Wrapf(errors.New("connection refused"), "dialing %s", "x"), "opening"))}},
		{"SQLError", []slog.Attr{slog.String("table", "t"), slog.Any("error", dupEntryError())}},
		{"wrapped SQLError", []slog.Attr{slog.String("table", "t"), slog.Any("error", vterrors.Wrapf(dupEntryError(), "copying"))}},
		{"from vttablet", []slog.Attr{slog.String("table", "t"), slog.Any("error", tabletErr)}},
		{"1 MB text", []slog.Attr{slog.String("table", "t"), slog.Any("error", large)}},
	}
	for _, hook := range []bool{false, true} {
		for _, c := range cases {
			b.Run(fmt.Sprintf("%s/hook=%t", c.name, hook), func(b *testing.B) {
				if !hook {
					prev := log.SetErrorAttrs(nil)
					b.Cleanup(func() { log.SetErrorAttrs(prev) })
				}
				b.ReportAllocs()
				for b.Loop() {
					log.Error("failed", c.attrs...)
				}
			})
		}
	}
}
