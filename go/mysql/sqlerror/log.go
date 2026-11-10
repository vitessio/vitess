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
	"log/slog"
	"regexp"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/log"
)

func init() {
	log.SetErrorAttrs(addLogAttrs)
}

// errnoAt is errExtract anchored at the start of the text.
var errnoAt = regexp.MustCompile("^" + errExtract.String())

// addLogAttrs adds mysql_errno, the MySQL error number, to a record whose
// error came from MySQL, unless the record already has a mysql_errno.
func addLogAttrs(r *slog.Record, v slog.Value) {
	// slog prints an error whose Error method panics, such as a typed nil, as
	// "!PANIC", so recover from any panic rather than crash where slog didn't.
	defer func() { _ = recover() }()

	set := false
	r.Attrs(func(a slog.Attr) bool {
		set = a.Key == "mysql_errno"
		return !set
	})
	if set {
		return
	}
	errno, ok := mysqlErrno(v)
	// Numbers below 1000 are Vitess's own errors, such as ERNotReplica.
	if !ok || errno < 1000 {
		return
	}
	r.AddAttrs(slog.Int("mysql_errno", int(errno)))
}

// mysqlErrno returns the MySQL error number of v, a logged error or error
// string. An error that is or wraps a SQLError has its number; other errors
// and strings are read with parseErrno.
func mysqlErrno(v slog.Value) (ErrorCode, bool) {
	switch v.Kind() {
	case slog.KindString:
		return parseErrno(v.String())
	case slog.KindAny, slog.KindLogValuer:
		err, ok := v.Any().(error)
		if !ok {
			return 0, false
		}
		budget := maxChainErrors
		if se := findSQLError(err, &budget); se != nil {
			return se.Num, true
		}
		return parseErrno(err.Error())
	}
	return 0, false
}

// maxChainErrors bounds the errors findSQLError visits, so that a chain that
// leads back to itself can't hang the logging call or overflow the stack.
const maxChainErrors = 100

// maxErrnoMarkers bounds the "(errno " markers parseErrno checks, so that
// logging a very long error text costs a bounded amount.
const maxErrnoMarkers = 100

// findSQLError returns the first *SQLError in err's chain, following both
// Unwrap and vterrors' Cause, and visiting at most *budget errors.
func findSQLError(err error, budget *int) *SQLError {
	for err != nil && *budget > 0 {
		*budget--
		switch e := err.(type) {
		case *SQLError:
			return e
		case interface{ Cause() error }:
			err = e.Cause()
		case interface{ Unwrap() error }:
			err = e.Unwrap()
		case interface{ Unwrap() []error }:
			for _, inner := range e.Unwrap() {
				if *budget <= 0 {
					return nil
				}
				if se := findSQLError(inner, budget); se != nil {
					return se
				}
			}
			return nil
		default:
			return nil
		}
	}
	return nil
}

// parseErrno reads the number from the text of an error that crossed an RPC
// or was formatted into a string, in the "(errno N) (sqlstate S)" form
// NewSQLErrorFromError reads. It stops at the first marker followed by what
// Vitess puts after a MySQL error; a marker followed by anything else is
// quoted text, such as an earlier error in a failed query, and is skipped.
// It gives up after maxErrnoMarkers markers, so a very long error costs a
// bounded amount and an error with more quoted markers than that gets no number.
func parseErrno(text string) (ErrorCode, bool) {
	for range maxErrnoMarkers {
		i := strings.Index(text, "(errno ")
		if i < 0 {
			return 0, false
		}
		text = text[i:]
		loc := errnoAt.FindStringSubmatchIndex(text)
		if loc == nil {
			text = text[len("(errno "):]
			continue
		}
		if endsMySQLError(text[loc[1]:]) {
			num, err := strconv.ParseUint(text[loc[2]:loc[3]], 10, 16)
			return ErrorCode(num), err == nil
		}
		text = text[loc[1]:]
	}
	return 0, false
}

// endsMySQLError reports whether rest, the text after an errno and sqlstate
// marker, starts the way it does after a MySQL error: the end of the error or
// of a line, SQLError's " during query: ", or the caller and query vttablet
// adds to a query error.
func endsMySQLError(rest string) bool {
	return rest == "" ||
		strings.HasPrefix(rest, "\n") ||
		strings.HasPrefix(rest, " during query: ") ||
		strings.HasPrefix(rest, " (CallerID: ") ||
		strings.HasPrefix(rest, ": Sql: ")
}
