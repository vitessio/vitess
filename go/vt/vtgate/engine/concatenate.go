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
	"sync"
	"sync/atomic"

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

	// the following fields are written to only once, and can then be shared between all users of this plan
	typeLoading sync.Once
	typesLoaded atomic.Bool
	fields      []*querypb.Field
	fieldTypes  []evalengine.Type
	typeError   error
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

	err = c.loadTypes(vcursor, res)
	if err != nil {
		return nil, err
	}

	var rows [][]sqltypes.Value
	err = c.coerceAndVisitResults(res, c.fieldTypes, func(result *sqltypes.Result) error {
		rows = append(rows, result.Rows...)
		return nil
	}, evalengine.ParseSQLMode(vcursor.SQLMode()))
	if err != nil {
		return nil, err
	}

	return &sqltypes.Result{
		Fields: c.fields,
		Rows:   rows,
	}, nil
}

func (c *Concatenate) loadTypes(vcursor VCursor, res []*sqltypes.Result) error {
	c.typeLoading.Do(func() {
		c.getFieldTypes(vcursor, res)
	})
	return c.typeError
}

func (c *Concatenate) coerceValuesTo(row sqltypes.Row, fieldTypes []evalengine.Type, sqlmode evalengine.SQLMode) error {
	if len(row) != len(fieldTypes) {
		return errWrongNumberOfColumnsInSelect
	}

	for i, value := range row {
		if _, found := c.NoNeedToTypeCheck[i]; found {
			continue
		}
		if fieldTypes[i].Type() != value.Type() {
			newValue, err := evalengine.CoerceTo(value, fieldTypes[i], sqlmode)
			if err != nil {
				return err
			}
			row[i] = newValue
		}
	}
	return nil
}

func (c *Concatenate) getFieldTypes(vcursor VCursor, res []*sqltypes.Result) {
	if len(res) == 0 {
		return
	}

	typers := make([]evalengine.TypeAggregator, len(res[0].Fields))
	collations := vcursor.Environment().CollationEnv()

	for _, r := range res {
		if r == nil || r.Fields == nil {
			continue
		}
		if len(r.Fields) != len(typers) {
			c.typeError = errWrongNumberOfColumnsInSelect
			return
		}
		for idx, field := range r.Fields {
			if err := typers[idx].AddField(field, collations); err != nil {
				c.typeError = err
				return
			}
		}
	}

	fields := make([]*querypb.Field, 0, len(typers))
	types := make([]evalengine.Type, 0, len(typers))
	for colIdx, typer := range typers {
		f := res[0].Fields[colIdx]

		if _, found := c.NoNeedToTypeCheck[colIdx]; found {
			fields = append(fields, f)
			types = append(types, evalengine.NewTypeFromField(f))
			continue
		}

		t := typer.Type()
		fields = append(fields, t.ToField(f.Name))
		types = append(types, t)
	}
	c.fields = fields
	c.fieldTypes = types
	c.typesLoaded.Store(true)
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
	if vcursor.Session().InTransaction() || !c.typesLoaded.Load() {
		// as we are in a transaction, we need to execute all queries inside a single connection,
		// which holds the single transaction we have
		return c.sequentialStreamExec(ctx, vcursor, bindVars, callback, sqlmode)
	}
	// not in transaction, so execute in parallel.
	return c.parallelStreamExec(ctx, vcursor, bindVars, callback, sqlmode)
}

// parallelStreamExec runs and returns the sub queries in parallel
// it assumes the field types have been loaded
func (c *Concatenate) parallelStreamExec(
	inCtx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	in func(*sqltypes.Result) error,
	sqlmode evalengine.SQLMode,
) error {
	// Scoped context; any early exit triggers cancel() to clean up ongoing work.
	ctx, cancel := context.WithCancel(inCtx)
	defer cancel()

	// Process each result chunk, considering type coercion.
	callback := func(res *sqltypes.Result, srcIdx int) error {
		if len(res.Rows) == 0 {
			return in(res)
		}
		// Check if type coercion needed for this source.
		// We only need to check if fields are not in NoNeedToTypeCheck set.
		needsCoercion := false
		if len(res.Fields) < len(c.fieldTypes) {
			// if we didn't get enough fields, we'll always coerce
			needsCoercion = true
		} else {
			for idx, field := range c.fieldTypes {
				_, skip := c.NoNeedToTypeCheck[idx]
				if !skip && field.Type() != res.Fields[idx].Type {
					needsCoercion = true
					break
				}
			}
		}

		// Apply type coercion if needed.
		// TODO: we should be able to do this only once as well, and remember if we need coercing here or not
		if needsCoercion {
			for _, row := range res.Rows {
				if err := c.coerceValuesTo(row, c.fieldTypes, sqlmode); err != nil {
					return err
				}
			}
		}
		return in(res)
	}
	var wg errgroup.Group
	// Start streaming query execution in parallel for all sources.
	for i, source := range c.Sources {
		currIndex, currSource := i, source
		wg.Go(func() error {
			err := vcursor.StreamExecutePrimitive(ctx, currSource, bindVars, true, func(resultChunk *sqltypes.Result) error {
				// Context check to avoid extra work.
				if ctx.Err() != nil {
					return nil
				}
				return callback(resultChunk, currIndex)
			})

			// Error handling and context cleanup for this source.
			if err != nil {
				cancel()
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

	c.typeLoading.Do(func() {
		firsts := make([]*sqltypes.Result, len(c.Sources))
		for i, result := range results {
			firsts[i] = result[0]
		}
		c.getFieldTypes(vcursor, firsts)
	})
	if c.typeError != nil {
		return c.typeError
	}

	for _, res := range results {
		if err := c.coerceAndVisitResults(res, c.fieldTypes, callback, sqlmode); err != nil {
			return err
		}
	}

	return nil
}

func (c *Concatenate) coerceAndVisitResults(
	res []*sqltypes.Result,
	fieldTypes []evalengine.Type,
	callback func(*sqltypes.Result) error,
	sqlmode evalengine.SQLMode,
) error {
	for _, r := range res {
		if len(r.Rows) > 0 &&
			len(fieldTypes) != len(r.Rows[0]) {
			return errWrongNumberOfColumnsInSelect
		}

		needsCoercion := false
		for idx, field := range r.Fields {
			if fieldTypes[idx].Type() != field.Type {
				needsCoercion = true
				break
			}
		}
		if needsCoercion {
			for _, row := range r.Rows {
				err := c.coerceValuesTo(row, fieldTypes, sqlmode)
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
	sourceFields := make([][]*querypb.Field, 0, len(c.Sources))
	for _, src := range c.Sources {
		f, err := src.GetFields(ctx, vcursor, bindVars)
		if err != nil {
			return nil, err
		}
		sourceFields = append(sourceFields, f.Fields)
	}

	fields := make([]*querypb.Field, 0, len(sourceFields[0]))
	collations := vcursor.Environment().CollationEnv()

	for colIdx := 0; colIdx < len(sourceFields[0]); colIdx++ {
		var typer evalengine.TypeAggregator
		for _, src := range sourceFields {
			if err := typer.AddField(src[colIdx], collations); err != nil {
				return nil, err
			}
		}
		name := sourceFields[0][colIdx].Name
		fields = append(fields, typer.Field(name))
	}
	return &sqltypes.Result{Fields: fields}, nil
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
