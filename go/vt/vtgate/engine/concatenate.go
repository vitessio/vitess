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
	LHS, RHS Primitive
}

//RouteType returns a description of the query routing type used by the primitive
func (c *Concatenate) RouteType() string {
	return "Concatenate"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to
func (c *Concatenate) GetKeyspaceName() string {
	return formatTwoOptionsNicely(c.LHS.GetKeyspaceName(), c.RHS.GetKeyspaceName())
}

// GetTableName specifies the table that this primitive routes to.
func (c *Concatenate) GetTableName() string {
	return formatTwoOptionsNicely(c.LHS.GetTableName(), c.RHS.GetTableName())
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
	var lhs, rhs *sqltypes.Result
	g := vcursor.ErrorGroupCancellableContext()
	g.Go(func() error {
		result, err := c.LHS.Execute(vcursor, bindVars, wantfields)
		if err != nil {
			return err
		}
		lhs = result
		return nil
	})
	g.Go(func() error {
		result, err := c.RHS.Execute(vcursor, bindVars, wantfields)
		if err != nil {
			return err
		}
		rhs = result
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, nil, vterrors.Wrap(err, "Concatenate.Execute")
	}
	return lhs, rhs, nil
}

// StreamExecute performs a streaming exec.
func (c *Concatenate) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var seenFields []*querypb.Field
	var fieldset sync.WaitGroup

	g := vcursor.ErrorGroupCancellableContext()
	fieldset.Add(1)
	var cbMu, visitFieldsMu sync.Mutex

	visitFields := func(fields []*querypb.Field) (unlocked bool, err error) {
		visitFieldsMu.Lock()
		defer visitFieldsMu.Unlock()
		if seenFields == nil {
			seenFields = fields
			return true, nil
		}
		return false, compareFields(fields, seenFields)
	}

	visitor := func(resultChunk *sqltypes.Result) error {
		if resultChunk.Fields != nil {
			var err error
			unlocked, err := visitFields(resultChunk.Fields)
			if err != nil {
				return err
			}
			if unlocked {
				defer fieldset.Done()
				// nothing else will be sent to the client until we send out this first one
				return callback(resultChunk)
			}
		}
		fieldset.Wait()

		// This to ensure only one send happens back to the client.
		cbMu.Lock()
		defer cbMu.Unlock()

		select {
		case <-vcursor.Context().Done():
			return nil
		default:
			return callback(resultChunk)
		}
	}
	g.Go(func() error {
		return c.LHS.StreamExecute(vcursor, bindVars, wantfields, visitor)
	})
	g.Go(func() error {
		return c.RHS.StreamExecute(vcursor, bindVars, wantfields, visitor)
	})
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

// GetFields fetches the field info.
func (c *Concatenate) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	lhs, err := c.LHS.GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	rhs, err := c.RHS.GetFields(vcursor, bindVars)
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
	return c.LHS.NeedsTransaction() || c.RHS.NeedsTransaction()
}

// Inputs returns the input primitives for this
func (c *Concatenate) Inputs() []Primitive {
	return []Primitive{c.LHS, c.RHS}
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
