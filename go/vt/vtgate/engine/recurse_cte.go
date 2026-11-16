/*
Copyright 2024 The Vitess Authors.

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

package engine

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql/collations/resultcharset"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/mysql/sqlmode"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// RecurseCTE is used to represent recursive CTEs
// Seed is used to represent the non-recursive part that initializes the result set.
// It's result are then used to start the recursion on the Term side
// The values being sent to the Term side are stored in the Vars map -
// the key is the bindvar name and the value is the index of the column in the recursive result
type RecurseCTE struct {
	Seed, Term Primitive

	Vars map[string]int
}

var _ Primitive = (*RecurseCTE)(nil)

// maxRecurseDepth caps the number of recursion iterations before we abort.
// TODO: This should be controlled with the cte_max_recursion_depth system variable.
const maxRecurseDepth = 1000

// maxRecursionWarnings is the number of warnings that one execution records
// for the values of its recursions, which is the max_error_count of MySQL.
const maxRecursionWarnings = 1024

// recursionRows returns the rows of a recursion in the character sets of the
// fields of the seed, converted by text, the rowsInFields of the two, which
// is nil when there is nothing to convert.
//
// The columns of a recursive CTE are the ones of its seed, and the result has
// the fields of the seed. The recursion can read another column than the
// seed. For
//
//	with recursive r (id, l) as (
//	  select id, l from a where id = 2
//	  union all select d.id, d.u from r join d on d.l = r.l)
//	select id, l from r
//
// with a latin1 column a.l and a utf8mb4 column d.u, MySQL 8.0.46 returns
// the 'ß' of d.u as DF, in latin1.
//
// A value can have a character that the character set of the column of the
// seed does not have. For a latin1 column of the seed and 'ab中cd' in the
// utf8mb4 column that the recursion reads, MySQL 8.0.46 fails with
//
//	ERROR 1366 (HY000): Incorrect string value: '\xE4\xB8\xADcd' for column 'l' at row 1
//
// when the sql_mode has STRICT_TRANS_TABLES or STRICT_ALL_TABLES. Without
// them it returns 'ab?cd', with that message as a warning for each such
// value. It says "at row 1" for every row. This does the same, by the
// sql_mode of the session, see strictSQLMode. warned counts the warnings of
// the execution.
func (r *RecurseCTE) recursionRows(vcursor VCursor, text *resultcharset.Conversion, rows []sqltypes.Row, seed []*querypb.Field, warned *int) ([]sqltypes.Row, error) {
	if text == nil {
		return rows, nil
	}
	var lost []resultcharset.Lost
	rows = text.RowsLossy(rows, func(l resultcharset.Lost) {
		if len(lost) < maxRecursionWarnings {
			lost = append(lost, l)
		}
	})
	if len(lost) == 0 {
		return rows, nil
	}
	strict := strictSQLMode(vcursor)
	for _, l := range lost {
		var column string
		if l.Column < len(seed) {
			column = seed[l.Column].Name
		}
		message := "Incorrect string value: '" + l.Printable() + "' for column '" + column + "' at row 1"
		if strict {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.TruncatedWrongValueForField, "%s", message)
		}
		if *warned >= maxRecursionWarnings {
			break
		}
		*warned++
		vcursor.Session().RecordWarning(&querypb.QueryWarning{Code: uint32(sqlerror.ERTruncatedWrongValueForField), Message: message})
	}
	return rows, nil
}

// strictSQLMode reports whether the sql_mode of the session has
// STRICT_TRANS_TABLES or STRICT_ALL_TABLES, which TRADITIONAL has too.
//
// vtgate has the sql_mode of a session that set it to another value than the
// one of MySQL. For a session that did not, it takes the default of MySQL 8,
// which is strict: it does not know the global sql_mode of the MySQL servers
// behind it. A SET sql_mode that vtgate ignores, without
// --enable-system-settings, is not the sql_mode of the session.
func strictSQLMode(vcursor VCursor) bool {
	const strict = sqlmode.StrictTransTables | sqlmode.StrictAllTables

	value, set := "", false
	vcursor.Session().GetSystemVariables(func(k, v string) {
		if strings.EqualFold(k, "sql_mode") {
			value, set = v, true
		}
	})
	if !set {
		value = vcursor.SQLMode()
	}

	// The session has the value as it stands in a SET statement: a list of
	// names in quotes, or a number.
	var mode sqlmode.Mode
	var err error
	if unquoted, ok := strings.CutPrefix(value, "'"); ok && set {
		mode, err = sqlmode.Parse(strings.TrimSuffix(unquoted, "'"))
	} else if bits, numErr := strconv.ParseUint(value, 10, 64); numErr == nil && set {
		mode, err = sqlmode.FromBits(bits)
	} else {
		mode, err = sqlmode.Parse(value)
	}
	if err != nil {
		return true
	}
	return mode.Expand()&strict != 0
}

func (r *RecurseCTE) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	// The values of the rows of the seed and of each recursion become bind
	// variables of the next recursion. The rows of a recursion are converted
	// to the character sets of the fields of the seed first, see
	// recursionRows, so the fields of the seed convert the values of both.
	convert := vcursor.ConvertResultCharset()
	res, err := vcursor.ExecutePrimitive(ctx, r.Seed, bindVars, wantfields || convert)
	if err != nil {
		return nil, err
	}

	// recurseRows contains the rows used in the next recursion, with their
	// values in the connection character set. res keeps the rows as they are.
	values := newStatementValues(vcursor, res.Fields)
	recurseRows := values.rows(res.Rows)
	joinVars := make(map[string]*querypb.BindVariable)
	loops := 0
	warned := 0
	for len(recurseRows) > 0 {
		// copy over the results from the previous recursion
		theseRows := recurseRows
		recurseRows = nil
		for _, row := range theseRows {
			for k, col := range r.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(row[col])
			}
			// check if the context is done - we might be in a long running recursion
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			loops++
			if loops > maxRecurseDepth {
				return nil, vterrors.VT09030("")
			}
			rresult, err := vcursor.ExecutePrimitive(ctx, r.Term, combineVars(bindVars, joinVars), convert)
			if err != nil {
				return nil, err
			}
			rows, err := r.recursionRows(vcursor, rowsInFields(vcursor, rresult.Fields, res.Fields), rresult.Rows, res.Fields, &warned)
			if err != nil {
				return nil, err
			}
			recurseRows = append(recurseRows, values.rows(rows)...)
			res.Rows = append(res.Rows, rows...)
		}
	}
	return res, nil
}

func (r *RecurseCTE) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if vcursor.Session().InTransaction() {
		res, err := r.TryExecute(ctx, vcursor, bindVars, wantfields)
		if err != nil {
			return err
		}
		return callback(res)
	}

	// A scatter source delivers its result chunks concurrently, one goroutine
	// per shard, so both the shared frontier and the downstream callback must be
	// serialized.
	var mu sync.Mutex

	// Stream the seed, emitting its rows and collecting them to drive the first
	// recursion level.
	//
	// The values of the rows become bind variables of the next recursion.
	// The rows of a recursion are converted to the character sets of the
	// fields of the seed first, see recursionRows, so the fields of the seed,
	// which come with its first chunk, convert the values of both.
	convert := vcursor.ConvertResultCharset()
	var recurseRows []sqltypes.Row
	var seedValues streamStatementValues
	var seedFields []*querypb.Field
	err := vcursor.StreamExecutePrimitive(ctx, r.Seed, bindVars, wantfields || convert, func(result *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		if seedFields == nil && len(result.Fields) != 0 {
			seedFields = result.Fields
		}
		recurseRows = append(recurseRows, seedValues.of(vcursor, result).rows(result.Rows)...)
		return callback(result)
	})
	if err != nil {
		return err
	}

	// Expand each recursion level iteratively, mirroring the buffered path. Only
	// one Term stream is active at a time: recursing inside the Term callback
	// would nest a live stream per level and exhaust the connection pool.
	joinVars := make(map[string]*querypb.BindVariable)
	loops := 0
	warned := 0
	for len(recurseRows) > 0 {
		// copy over the results from the previous recursion
		theseRows := recurseRows
		recurseRows = nil
		for _, row := range theseRows {
			for k, col := range r.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(row[col])
			}
			// check if the context is done - we might be in a long running recursion
			if err := ctx.Err(); err != nil {
				return err
			}
			loops++
			if loops > maxRecurseDepth {
				return vterrors.VT09030("")
			}
			var text *resultcharset.Conversion
			var haveFields bool
			// rowsErr is the error of recursionRows. The stream below it
			// would return it as the error of a shard, without its MySQL
			// error number, so it is kept, the rest of the stream is
			// read and left out, and it is returned after the stream.
			var rowsErr error
			err := vcursor.StreamExecutePrimitive(ctx, r.Term, combineVars(bindVars, joinVars), convert, func(result *sqltypes.Result) error {
				mu.Lock()
				defer mu.Unlock()
				if !convert {
					recurseRows = append(recurseRows, result.Rows...)
					return callback(result)
				}
				if rowsErr != nil {
					return nil
				}
				if !haveFields && len(result.Fields) != 0 {
					haveFields = true
					text = rowsInFields(vcursor, result.Fields, seedFields)
				}
				rows, err := r.recursionRows(vcursor, text, result.Rows, seedFields, &warned)
				if err != nil {
					rowsErr = err
					return nil
				}
				recurseRows = append(recurseRows, seedValues.values.rows(rows)...)
				// The fields of the recursion were asked for here. The result
				// has the fields of the seed, which were sent with it.
				if len(rows) == 0 {
					return nil
				}
				return callback(&sqltypes.Result{Rows: rows})
			})
			if err != nil {
				return err
			}
			if rowsErr != nil {
				return rowsErr
			}
		}
	}
	return nil
}

func (r *RecurseCTE) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return r.Seed.GetFields(ctx, vcursor, bindVars)
}

func (r *RecurseCTE) NeedsTransaction() bool {
	return r.Seed.NeedsTransaction() || r.Term.NeedsTransaction()
}

func (r *RecurseCTE) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{r.Seed, r.Term}, nil
}

func (r *RecurseCTE) description() PrimitiveDescription {
	other := map[string]any{
		"JoinVars": orderedStringIntMap(r.Vars),
	}

	return PrimitiveDescription{
		OperatorType: "RecurseCTE",
		Other:        other,
		Inputs:       nil,
	}
}
