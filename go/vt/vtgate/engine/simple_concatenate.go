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
	"sync"

	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// SimpleConcatenate Primitive is used to concatenate results from multiple sources.
// It does no type checking or coercing - it just concatenates results together, assuming
// the inputs are already correctly typed, and it uses the first source for column names
var _ Primitive = (*SimpleConcatenate)(nil)

type SimpleConcatenate struct {
	Sources []Primitive
}

// NewSimpleConcatenate creates a SimpleConcatenate primitive.
func NewSimpleConcatenate(Sources []Primitive) *SimpleConcatenate {
	return &SimpleConcatenate{Sources: Sources}
}

// RouteType returns a description of the query routing type used by the primitive
func (c *SimpleConcatenate) RouteType() string {
	return "SimpleConcatenate"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to
func (c *SimpleConcatenate) GetKeyspaceName() string {
	res := c.Sources[0].GetKeyspaceName()
	for i := 1; i < len(c.Sources); i++ {
		res = formatTwoOptionsNicely(res, c.Sources[i].GetKeyspaceName())
	}
	return res
}

// GetTableName specifies the table that this primitive routes to.
func (c *SimpleConcatenate) GetTableName() string {
	res := c.Sources[0].GetTableName()
	for i := 1; i < len(c.Sources); i++ {
		res = formatTwoOptionsNicely(res, c.Sources[i].GetTableName())
	}
	return res
}

// TryExecute performs a non-streaming exec.
func (c *SimpleConcatenate) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	if vcursor.Session().InTransaction() {
		// as we are in a transaction, we need to execute all queries inside a single transaction
		// therefore it needs a sequential execution.
		return c.sequentialExec(ctx, vcursor, bindVars)
	}
	// not in transaction, so execute in parallel.
	return c.parallelExec(ctx, vcursor, bindVars)
}

// TryStreamExecute performs a streaming exec.
func (c *SimpleConcatenate) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) error {
	if vcursor.Session().InTransaction() {
		// as we are in a transaction, we need to execute all queries inside a single connection,
		// which holds the single transaction we have
		return c.sequentialStreamExec(ctx, vcursor, bindVars, callback)
	}
	// not in transaction, so execute in parallel.
	return c.parallelStreamExec(ctx, vcursor, bindVars, callback)
}

func (c *SimpleConcatenate) parallelExec(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
) (*sqltypes.Result, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       errgroup.Group
		rows     sync.Mutex
		fieldsWg sync.WaitGroup
		fields   []*querypb.Field
		result   *sqltypes.Result
	)

	fieldsWg.Add(1)

	for i, source := range c.Sources {
		vars := copyBindVars(bindVars)
		// the first source will be used to get the fields
		// the other sources will run in parallel
		wg.Go(func() error {
			chunk, err := vcursor.ExecutePrimitive(ctx, source, vars, true)
			if err != nil {
				cancel()
				return err
			}
			if i == 0 {
				fields = chunk.Fields
				fieldsWg.Done()
			} else {
				fieldsWg.Wait()
				chunk.Fields = fields
			}

			rows.Lock()
			defer rows.Unlock()
			result.Rows = append(result.Rows, chunk.Rows...)
			return nil
		})
	}
	err := wg.Wait()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *SimpleConcatenate) sequentialExec(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
) (result *sqltypes.Result, err error) {
	for _, src := range c.Sources {
		vars := copyBindVars(bindVars)
		chunk, err := vcursor.ExecutePrimitive(ctx, src, vars, true)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = &sqltypes.Result{
				Fields:              chunk.Fields,
				SessionStateChanges: chunk.SessionStateChanges,
				StatusFlags:         chunk.StatusFlags,
				Info:                chunk.Info,
			}
		}
		result.Rows = append(result.Rows, chunk.Rows...)
	}
	return
}

// parallelStreamExec executes the sources in parallel and streams the results.
func (c *SimpleConcatenate) parallelStreamExec(
	inCtx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	callback func(*sqltypes.Result) error,
) error {
	// Scoped context; any early exit triggers cancel() to clean up ongoing work.
	ctx, cancel := context.WithCancel(inCtx)
	defer cancel()

	// Mutex for dealing with concurrent access to shared state.
	var (
		muCallback sync.Mutex
		wg         errgroup.Group
		fieldsWg   sync.WaitGroup
		fields     []*querypb.Field
	)

	fieldsWg.Add(1)
	// Start streaming query execution in parallel for all source
	for i, source := range c.Sources {
		wg.Go(func() error {
			return vcursor.StreamExecutePrimitive(ctx, source, bindVars, true, func(chunk *sqltypes.Result) error {
				// Context check to avoid extra work.
				if ctx.Err() != nil {
					return nil
				}
				if i == 0 {
					fields = chunk.Fields
					fieldsWg.Done()
				} else {
					fieldsWg.Wait()
					chunk.Fields = fields
				}

				muCallback.Lock()
				defer muCallback.Unlock()
				return callback(chunk)
			})
		})
	}
	// Wait for all sources to complete.
	return wg.Wait()
}

func (c *SimpleConcatenate) sequentialStreamExec(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	callback func(*sqltypes.Result) error,
) error {
	var fields []*querypb.Field
	for i, source := range c.Sources {
		err := vcursor.StreamExecutePrimitive(ctx, source, bindVars, true, func(resultChunk *sqltypes.Result) error {
			// check if context has expired.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if i == 0 && fields == nil {
				fields = resultChunk.Fields
			} else {
				resultChunk.Fields = fields
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
func (c *SimpleConcatenate) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return c.Sources[0].GetFields(ctx, vcursor, bindVars)
}

// NeedsTransaction returns whether a transaction is needed for this primitive
func (c *SimpleConcatenate) NeedsTransaction() bool {
	for _, source := range c.Sources {
		if source.NeedsTransaction() {
			return true
		}
	}
	return false
}

// Inputs returns the input primitives for this
func (c *SimpleConcatenate) Inputs() ([]Primitive, []map[string]any) {
	return c.Sources, nil
}

func (c *SimpleConcatenate) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: c.RouteType()}
}
