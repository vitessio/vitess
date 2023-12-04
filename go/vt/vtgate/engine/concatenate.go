/*
Copyright 2020 The Vitess Authors.

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
	"slices"
	"sync"

	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// Concatenate Primitive is used to concatenate results from multiple sources.
var _ Primitive = (*Concatenate)(nil)

// Concatenate specified the parameter for concatenate primitive
type Concatenate struct {
	Sources []Primitive

	// These column offsets do not need to be typed checked - they usually contain weight_string()
	// columns that are not going to be returned to the user
	NoNeedToTypeCheck map[int]any
}

// NewConcatenate creates a Concatenate primitive. The ignoreCols slice contains the offsets that
// don't need to have the same type between sources -
// weight_string() sometimes returns VARBINARY and sometimes VARCHAR
func NewConcatenate(Sources []Primitive, ignoreCols []int) *Concatenate {
	ignoreTypes := map[int]any{}
	for _, i := range ignoreCols {
		ignoreTypes[i] = nil
	}
	return &Concatenate{
		Sources:           Sources,
		NoNeedToTypeCheck: ignoreTypes,
	}
}

// RouteType returns a description of the query routing type used by the primitive
func (c *Concatenate) RouteType() string {
	return "Concatenate"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to
func (c *Concatenate) GetKeyspaceName() string {
	res := c.Sources[0].GetKeyspaceName()
	for i := 1; i < len(c.Sources); i++ {
		res = formatTwoOptionsNicely(res, c.Sources[i].GetKeyspaceName())
	}
	return res
}

// GetTableName specifies the table that this primitive routes to.
func (c *Concatenate) GetTableName() string {
	res := c.Sources[0].GetTableName()
	for i := 1; i < len(c.Sources); i++ {
		res = formatTwoOptionsNicely(res, c.Sources[i].GetTableName())
	}
	return res
}

func formatTwoOptionsNicely(a, b string) string {
	if a == b {
		return a
	}
	return a + "_" + b
}

// errWrongNumberOfColumnsInSelect is an error
var errWrongNumberOfColumnsInSelect = vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.WrongNumberOfColumnsInSelect, "The used SELECT statements have a different number of columns")

// TryExecute performs a non-streaming exec.
func (c *Concatenate) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	res, err := c.execSources(ctx, vcursor, bindVars, true)
	if err != nil {
		return nil, err
	}

	fields, err := c.getFields(res)
	if err != nil {
		return nil, err
	}

	var rows [][]sqltypes.Value
	err = c.coerceAndVisitResults(res, fields, func(result *sqltypes.Result) error {
		rows = append(rows, result.Rows...)
		return nil
	}, evalengine.ParseSQLMode(vcursor.SQLMode()))
	if err != nil {
		return nil, err
	}

	return &sqltypes.Result{
		Fields: fields,
		Rows:   rows,
	}, nil
}

func (c *Concatenate) coerceValuesTo(row sqltypes.Row, fields []*querypb.Field, sqlmode evalengine.SQLMode) error {
	if len(row) != len(fields) {
		return errWrongNumberOfColumnsInSelect
	}

	for i, value := range row {
		if _, found := c.NoNeedToTypeCheck[i]; found {
			continue
		}
		if fields[i].Type != value.Type() {
			newValue, err := evalengine.CoerceTo(value, fields[i].Type, sqlmode)
			if err != nil {
				return err
			}
			row[i] = newValue
		}
	}
	return nil
}

func (c *Concatenate) getFields(res []*sqltypes.Result) (resultFields []*querypb.Field, err error) {
	if len(res) == 0 {
		return nil, nil
	}

	resultFields = res[0].Fields
	columns := make([][]sqltypes.Type, len(resultFields))

	addFields := func(fields []*querypb.Field) error {
		if len(fields) != len(columns) {
			return errWrongNumberOfColumnsInSelect
		}
		for idx, field := range fields {
			columns[idx] = append(columns[idx], field.Type)
		}
		return nil
	}

	for _, r := range res {
		if r == nil || r.Fields == nil {
			continue
		}
		err := addFields(r.Fields)
		if err != nil {
			return nil, err
		}
	}

	// The resulting column types need to be the coercion of all the input columns
	for colIdx, t := range columns {
		if _, found := c.NoNeedToTypeCheck[colIdx]; found {
			continue
		}

		resultFields[colIdx].Type = evalengine.AggregateTypes(t)
	}

	return resultFields, nil
}

func (c *Concatenate) execSources(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) ([]*sqltypes.Result, error) {
	if vcursor.Session().InTransaction() {
		// as we are in a transaction, we need to execute all queries inside a single transaction
		// therefore it needs a sequential execution.
		return c.sequentialExec(ctx, vcursor, bindVars, wantfields)
	}
	// not in transaction, so execute in parallel.
	return c.parallelExec(ctx, vcursor, bindVars, wantfields)
}

func (c *Concatenate) parallelExec(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) ([]*sqltypes.Result, error) {
	results := make([]*sqltypes.Result, len(c.Sources))
	var outerErr error

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i, source := range c.Sources {
		currIndex, currSource := i, source
		vars := copyBindVars(bindVars)
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := vcursor.ExecutePrimitive(ctx, currSource, vars, true)
			if err != nil {
				outerErr = err
				cancel()
			}
			results[currIndex] = result
		}()
	}
	wg.Wait()
	return results, outerErr
}

func (c *Concatenate) sequentialExec(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) ([]*sqltypes.Result, error) {
	results := make([]*sqltypes.Result, len(c.Sources))
	for i, source := range c.Sources {
		currIndex, currSource := i, source
		vars := copyBindVars(bindVars)
		result, err := vcursor.ExecutePrimitive(ctx, currSource, vars, true)
		if err != nil {
			return nil, err
		}
		results[currIndex] = result
	}
	return results, nil
}

// TryStreamExecute performs a streaming exec.
func (c *Concatenate) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) error {
	sqlmode := evalengine.ParseSQLMode(vcursor.SQLMode())
	if vcursor.Session().InTransaction() {
		// as we are in a transaction, we need to execute all queries inside a single connection,
		// which holds the single transaction we have
		return c.sequentialStreamExec(ctx, vcursor, bindVars, callback, sqlmode)
	}
	// not in transaction, so execute in parallel.
	return c.parallelStreamExec(ctx, vcursor, bindVars, callback, sqlmode)
}

func (c *Concatenate) parallelStreamExec(inCtx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, in func(*sqltypes.Result) error, sqlmode evalengine.SQLMode) error {
	// Scoped context; any early exit triggers cancel() to clean up ongoing work.
	ctx, cancel := context.WithCancel(inCtx)
	defer cancel()

	// Mutexes for dealing with concurrent access to shared state.
	var (
		muCallback sync.Mutex                                 // Protects callback
		muFields   sync.Mutex                                 // Protects field state
		condFields = sync.NewCond(&muFields)                  // Condition var for field arrival
		wg         errgroup.Group                             // Wait group for all streaming goroutines
		rest       = make([]*sqltypes.Result, len(c.Sources)) // Collects first result from each source to derive fields
		fields     []*querypb.Field                           // Cached final field types
	)

	// Process each result chunk, considering type coercion.
	callback := func(res *sqltypes.Result, srcIdx int) error {
		muCallback.Lock()
		defer muCallback.Unlock()

		// Check if type coercion needed for this source.
		// We only need to check if fields are not in NoNeedToTypeCheck set.
		needsCoercion := false
		for idx, field := range rest[srcIdx].Fields {
			_, skip := c.NoNeedToTypeCheck[idx]
			if !skip && fields[idx].Type != field.Type {
				needsCoercion = true
				break
			}
		}

		// Apply type coercion if needed.
		if needsCoercion {
			for _, row := range res.Rows {
				if err := c.coerceValuesTo(row, fields, sqlmode); err != nil {
					return err
				}
			}
		}
		return in(res)
	}

	// Start streaming query execution in parallel for all sources.
	for i, source := range c.Sources {
		currIndex, currSource := i, source
		wg.Go(func() error {
			err := vcursor.StreamExecutePrimitive(ctx, currSource, bindVars, true, func(resultChunk *sqltypes.Result) error {
				// Process fields when they arrive; coordinate field agreement across sources.
				if resultChunk.Fields != nil {
					muFields.Lock()

					// Capture the initial result chunk to determine field types later.
					if rest[currIndex] == nil {
						rest[currIndex] = resultChunk

						// If this was the last source to report its fields, derive the final output fields.
						if !slices.Contains(rest, nil) {
							muFields.Unlock()

							// We have received fields from all sources. We can now calculate the output types
							var err error
							fields, err = c.getFields(rest)
							if err != nil {
								return err
							}
							resultChunk.Fields = fields

							defer condFields.Broadcast()
							return callback(resultChunk, currIndex)
						}
					}
					// Wait for fields from all sources.
					for slices.Contains(rest, nil) {
						condFields.Wait()
					}
					muFields.Unlock()
				}

				// Context check to avoid extra work.
				if ctx.Err() != nil {
					return nil
				}
				return callback(resultChunk, currIndex)
			})

			// Error handling and context cleanup for this source.
			if err != nil {
				muFields.Lock()
				if rest[currIndex] == nil {
					// Signal that this source is done, even if by failure, to unblock field waiting.
					rest[currIndex] = &sqltypes.Result{}
				}
				cancel()
				condFields.Broadcast()
				muFields.Unlock()
			}
			return err
		})
	}
	// Wait for all sources to complete.
	return wg.Wait()
}

func (c *Concatenate) sequentialStreamExec(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error, sqlmode evalengine.SQLMode) error {
	// all the below fields ensure that the fields are sent only once.
	results := make([][]*sqltypes.Result, len(c.Sources))

	var mu sync.Mutex
	for idx, source := range c.Sources {
		err := vcursor.StreamExecutePrimitive(ctx, source, bindVars, true, func(resultChunk *sqltypes.Result) error {
			// check if context has expired.
			if ctx.Err() != nil {
				return ctx.Err()
			}

			mu.Lock()
			defer mu.Unlock()
			// This visitor will just accumulate all the results into slices
			results[idx] = append(results[idx], resultChunk)

			return nil
		})
		if err != nil {
			return err
		}
	}

	firsts := make([]*sqltypes.Result, len(c.Sources))
	for i, result := range results {
		firsts[i] = result[0]
	}

	fields, err := c.getFields(firsts)
	if err != nil {
		return err
	}
	for _, res := range results {
		if err = c.coerceAndVisitResults(res, fields, callback, sqlmode); err != nil {
			return err
		}
	}

	return nil
}

func (c *Concatenate) coerceAndVisitResults(
	res []*sqltypes.Result,
	fields []*querypb.Field,
	callback func(*sqltypes.Result) error,
	sqlmode evalengine.SQLMode,
) error {
	for _, r := range res {
		if len(r.Rows) > 0 &&
			len(fields) != len(r.Rows[0]) {
			return errWrongNumberOfColumnsInSelect
		}

		needsCoercion := false
		for idx, field := range r.Fields {
			if fields[idx].Type != field.Type {
				needsCoercion = true
				break
			}
		}
		if needsCoercion {
			for _, row := range r.Rows {
				err := c.coerceValuesTo(row, fields, sqlmode)
				if err != nil {
					return err
				}
			}
		}
		err := callback(r)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetFields fetches the field info.
func (c *Concatenate) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	res, err := c.Sources[0].GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	columns := make([][]sqltypes.Type, len(res.Fields))

	addFields := func(fields []*querypb.Field) {
		for idx, field := range fields {
			columns[idx] = append(columns[idx], field.Type)
		}
	}

	addFields(res.Fields)

	for i := 1; i < len(c.Sources); i++ {
		result, err := c.Sources[i].GetFields(ctx, vcursor, bindVars)
		if err != nil {
			return nil, err
		}
		addFields(result.Fields)
	}

	// The resulting column types need to be the coercion of all the input columns
	for colIdx, t := range columns {
		res.Fields[colIdx].Type = evalengine.AggregateTypes(t)
	}

	return res, nil
}

// NeedsTransaction returns whether a transaction is needed for this primitive
func (c *Concatenate) NeedsTransaction() bool {
	for _, source := range c.Sources {
		if source.NeedsTransaction() {
			return true
		}
	}
	return false
}

// Inputs returns the input primitives for this
func (c *Concatenate) Inputs() ([]Primitive, []map[string]any) {
	return c.Sources, nil
}

func (c *Concatenate) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: c.RouteType()}
}
