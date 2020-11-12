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

	"vitess.io/vitess/go/mysql"

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
	return formatTwoOptionsNicely(c.Sources[0].GetKeyspaceName(), c.Sources[1].GetKeyspaceName())
}

// GetTableName specifies the table that this primitive routes to.
func (c *Concatenate) GetTableName() string {
	return formatTwoOptionsNicely(c.Sources[0].GetTableName(), c.Sources[1].GetTableName())
}

func formatTwoOptionsNicely(a, b string) string {
	if a == b {
		return a
	}
	return a + "_" + b
}

// Execute performs a non-streaming exec.
func (c *Concatenate) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	lhs, rhs, err := c.execSources(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	fields, err := c.getFields(lhs.Fields, rhs.Fields)
	if err != nil {
		return nil, err
	}

	if len(lhs.Rows) > 0 &&
		len(rhs.Rows) > 0 &&
		len(lhs.Rows[0]) != len(rhs.Rows[0]) {
		return nil, mysql.NewSQLError(mysql.ERWrongNumberOfColumnsInSelect, "21000", "The used SELECT statements have a different number of columns")
	}

	return &sqltypes.Result{
		Fields:       fields,
		RowsAffected: lhs.RowsAffected + rhs.RowsAffected,
		Rows:         append(lhs.Rows, rhs.Rows...),
	}, nil
}

func (c *Concatenate) getFields(a, b []*querypb.Field) ([]*querypb.Field, error) {
	switch {
	case a != nil && b != nil:
		err := compareFields(a, b)
		if err != nil {
			return nil, err
		}
		return a, nil
	case a != nil:
		return a, nil
	case b != nil:
		return b, nil
	}

	return nil, nil
}
func (c *Concatenate) execSources(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, *sqltypes.Result, error) {
	results := make([]*sqltypes.Result, 2)
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
		return nil, nil, vterrors.Wrap(err, "Concatenate.Execute")
	}
	return results[0], results[1], nil
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
	lhs, err := c.Sources[0].GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	rhs, err := c.Sources[1].GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	err = compareFields(lhs.Fields, rhs.Fields)
	if err != nil {
		return nil, err
	}

	return lhs, nil
}

//NeedsTransaction returns whether a transaction is needed for this primitive
func (c *Concatenate) NeedsTransaction() bool {
	return c.Sources[0].NeedsTransaction() || c.Sources[1].NeedsTransaction()
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
		return mysql.NewSQLError(mysql.ERWrongNumberOfColumnsInSelect, "21000", "The used SELECT statements have a different number of columns")
	}
	for i, field2 := range fields2 {
		field1 := fields1[i]
		if field1.Type != field2.Type {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column field type does not match for name: (%v, %v) types: (%v, %v)", field1.Name, field2.Name, field1.Type, field2.Type)
		}
	}
	return nil
}
