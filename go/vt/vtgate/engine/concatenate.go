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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
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
func (c *Concatenate) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	res, err := c.execSources(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	fields, err := c.getFields(res)
	if err != nil {
		return nil, err
	}

	var rowsAffected uint64
	var rows [][]sqltypes.Value

	for _, r := range res {
		rowsAffected += r.RowsAffected

		if len(rows) > 0 &&
			len(r.Rows) > 0 &&
			len(rows[0]) != len(r.Rows[0]) {
			return nil, errWrongNumberOfColumnsInSelect
		}

		rows = append(rows, r.Rows...)
	}

	return &sqltypes.Result{
		Fields:       fields,
		RowsAffected: rowsAffected,
		Rows:         rows,
	}, nil
}

func (c *Concatenate) getFields(res []*sqltypes.Result) ([]*querypb.Field, error) {
	if len(res) == 0 {
		return nil, nil
	}

	var fields []*querypb.Field
	for _, r := range res {
		if r.Fields == nil {
			continue
		}
		if fields == nil {
			fields = r.Fields
			continue
		}

		err := c.compareFields(fields, r.Fields)
		if err != nil {
			return nil, err
		}
	}
	return fields, nil
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

func (c *Concatenate) parallelExec(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) ([]*sqltypes.Result, error) {
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
			result, err := vcursor.ExecutePrimitive(ctx, currSource, vars, wantfields)
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

func (c *Concatenate) sequentialExec(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) ([]*sqltypes.Result, error) {
	results := make([]*sqltypes.Result, len(c.Sources))
	for i, source := range c.Sources {
		currIndex, currSource := i, source
		vars := copyBindVars(bindVars)
		result, err := vcursor.ExecutePrimitive(ctx, currSource, vars, wantfields)
		if err != nil {
			return nil, err
		}
		results[currIndex] = result
	}
	return results, nil
}

// TryStreamExecute performs a streaming exec.
func (c *Concatenate) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if vcursor.Session().InTransaction() {
		// as we are in a transaction, we need to execute all queries inside a single transaction
		// therefore it needs a sequential execution.
		return c.sequentialStreamExec(ctx, vcursor, bindVars, wantfields, callback)
	}
	// not in transaction, so execute in parallel.
	return c.parallelStreamExec(ctx, vcursor, bindVars, wantfields, callback)
}

func (c *Concatenate) parallelStreamExec(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var seenFields []*querypb.Field
	var outerErr error

	var fieldsSent bool
	var cbMu, fieldsMu sync.Mutex
	var wg, fieldSendWg sync.WaitGroup
	fieldSendWg.Add(1)

	for i, source := range c.Sources {
		wg.Add(1)
		currIndex, currSource := i, source

		go func() {
			defer wg.Done()
			err := vcursor.StreamExecutePrimitive(ctx, currSource, bindVars, wantfields, func(resultChunk *sqltypes.Result) error {
				// if we have fields to compare, make sure all the fields are all the same
				if currIndex == 0 {
					fieldsMu.Lock()
					if !fieldsSent {
						defer fieldSendWg.Done()
						defer fieldsMu.Unlock()
						seenFields = resultChunk.Fields
						fieldsSent = true
						// No other call can happen before this call.
						return callback(resultChunk)
					}
					fieldsMu.Unlock()
				}
				fieldSendWg.Wait()
				if resultChunk.Fields != nil {
					err := c.compareFields(seenFields, resultChunk.Fields)
					if err != nil {
						return err
					}
				}
				// This to ensure only one send happens back to the client.
				cbMu.Lock()
				defer cbMu.Unlock()
				select {
				case <-ctx.Done():
					return nil
				default:
					return callback(resultChunk)
				}
			})
			// This is to ensure other streams complete if the first stream failed to unlock the wait.
			if currIndex == 0 {
				fieldsMu.Lock()
				if !fieldsSent {
					fieldsSent = true
					fieldSendWg.Done()
				}
				fieldsMu.Unlock()
			}
			if err != nil {
				outerErr = err
				ctx.Done()
			}
		}()

	}
	wg.Wait()
	return outerErr
}

func (c *Concatenate) sequentialStreamExec(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	// all the below fields ensure that the fields are sent only once.
	var seenFields []*querypb.Field
	var fieldsMu sync.Mutex
	var fieldsSent bool

	for idx, source := range c.Sources {
		err := vcursor.StreamExecutePrimitive(ctx, source, bindVars, wantfields, func(resultChunk *sqltypes.Result) error {
			// if we have fields to compare, make sure all the fields are all the same
			if idx == 0 {
				fieldsMu.Lock()
				defer fieldsMu.Unlock()
				if !fieldsSent {
					fieldsSent = true
					seenFields = resultChunk.Fields
					return callback(resultChunk)
				}
			}
			if resultChunk.Fields != nil {
				err := c.compareFields(seenFields, resultChunk.Fields)
				if err != nil {
					return err
				}
			}
			// check if context has expired.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return callback(resultChunk)

		})
		if err != nil {
			return err
		}
	}
	return nil
}

// GetFields fetches the field info.
func (c *Concatenate) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	// TODO: type coercions
	res, err := c.Sources[0].GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	for i := 1; i < len(c.Sources); i++ {
		result, err := c.Sources[i].GetFields(ctx, vcursor, bindVars)
		if err != nil {
			return nil, err
		}
		err = c.compareFields(res.Fields, result.Fields)
		if err != nil {
			return nil, err
		}
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
func (c *Concatenate) Inputs() []Primitive {
	return c.Sources
}

func (c *Concatenate) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: c.RouteType()}
}

func (c *Concatenate) compareFields(fields1 []*querypb.Field, fields2 []*querypb.Field) error {
	if len(fields1) != len(fields2) {
		return errWrongNumberOfColumnsInSelect
	}
	for i, field1 := range fields1 {
		if _, found := c.NoNeedToTypeCheck[i]; found {
			continue
		}
		field2 := fields2[i]
		if field1.Type != field2.Type {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "merging field of different types is not supported, name: (%v, %v) types: (%v, %v)", field1.Name, field2.Name, field1.Type, field2.Type)
		}
	}
	return nil
}
