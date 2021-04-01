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
	"sync"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Concatenate Primitive is used to concatenate results from multiple sources.
var _ Primitive = (*Concatenate)(nil)

//Concatenate specified the parameter for concatenate primitive
type Concatenate struct {
	Sources []Primitive
}

//RouteType returns a description of the query routing type used by the primitive
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

var errWrongNumberOfColumnsInSelect = vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.WrongNumberOfColumnsInSelect, "The used SELECT statements have a different number of columns")

// Execute performs a non-streaming exec.
func (c *Concatenate) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	res, err := c.execSources(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	fields, err := c.getFields(res)
	if err != nil {
		return nil, err
	}

	var rowsAffected uint64 = 0
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
	var resFields []*querypb.Field
	for _, r := range res {
		fields := r.Fields
		if fields == nil {
			continue
		}
		if resFields == nil {
			resFields = fields
			continue
		}
		err := compareFields(fields, resFields)
		if err != nil {
			return nil, err
		}
	}
	return resFields, nil
}
func (c *Concatenate) execSources(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) ([]*sqltypes.Result, error) {
	results := make([]*sqltypes.Result, len(c.Sources))
	g, restoreCtx := vcursor.ErrorGroupCancellableContext()
	defer restoreCtx()
	for i, source := range c.Sources {
		currIndex, currSource := i, source
		g.Go(func() error {
			result, err := currSource.Execute(vcursor, bindVars, wantfields)
			if err != nil {
				return err
			}
			results[currIndex] = result
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// StreamExecute performs a streaming exec.
func (c *Concatenate) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var seenFields []*querypb.Field
	var fieldset sync.WaitGroup
	var cbMu sync.Mutex

	g, restoreCtx := vcursor.ErrorGroupCancellableContext()
	defer restoreCtx()
	fieldsSent := false
	fieldset.Add(1)

	for i, source := range c.Sources {
		currIndex, currSource := i, source

		g.Go(func() error {
			err := currSource.StreamExecute(vcursor, bindVars, wantfields, func(resultChunk *sqltypes.Result) error {
				// if we have fields to compare, make sure all the fields are all the same
				if currIndex == 0 && !fieldsSent {
					defer fieldset.Done()
					seenFields = resultChunk.Fields
					fieldsSent = true
					// No other call can happen before this call.
					return callback(resultChunk)
				}
				fieldset.Wait()
				if resultChunk.Fields != nil {
					err := compareFields(seenFields, resultChunk.Fields)
					if err != nil {
						return err
					}
				}
				// This to ensure only one send happens back to the client.
				cbMu.Lock()
				defer cbMu.Unlock()
				select {
				case <-vcursor.Context().Done():
					return nil
				default:
					return callback(resultChunk)
				}
			})
			// This is to ensure other streams complete if the first stream failed to unlock the wait.
			if currIndex == 0 && !fieldsSent {
				fieldset.Done()
			}
			return err
		})

	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

// GetFields fetches the field info.
func (c *Concatenate) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	res, err := c.Sources[0].GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	for i := 1; i < len(c.Sources); i++ {
		result, err := c.Sources[i].GetFields(vcursor, bindVars)
		if err != nil {
			return nil, err
		}
		err = compareFields(result.Fields, res.Fields)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

//NeedsTransaction returns whether a transaction is needed for this primitive
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

func compareFields(fields1 []*querypb.Field, fields2 []*querypb.Field) error {
	if len(fields1) != len(fields2) {
		return errWrongNumberOfColumnsInSelect
	}
	for i, field2 := range fields2 {
		field1 := fields1[i]
		if field1.Type != field2.Type {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "merging field of different types is not supported, name: (%v, %v) types: (%v, %v)", field1.Name, field2.Name, field1.Type, field2.Type)
		}
	}
	return nil
}
