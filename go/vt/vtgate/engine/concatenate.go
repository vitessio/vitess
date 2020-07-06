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
	"sort"
	"strings"
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
	ksMap := map[string]interface{}{}
	for _, source := range c.Sources {
		ksMap[source.GetKeyspaceName()] = nil
	}
	var ksArr []string
	for ks := range ksMap {
		ksArr = append(ksArr, ks)
	}
	sort.Strings(ksArr)
	return strings.Join(ksArr, "_")
}

// GetTableName specifies the table that this primitive routes to.
func (c *Concatenate) GetTableName() string {
	var tabArr []string
	for _, source := range c.Sources {
		tabArr = append(tabArr, source.GetTableName())
	}
	return strings.Join(tabArr, "_")
}

// Execute performs a non-streaming exec.
func (c *Concatenate) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result := &sqltypes.Result{}
	var wg sync.WaitGroup
	qrs := make([]*sqltypes.Result, len(c.Sources))
	errs := make([]error, len(c.Sources))
	for i, source := range c.Sources {
		wg.Add(1)
		go func(i int, source Primitive) {
			defer wg.Done()
			qrs[i], errs[i] = source.Execute(vcursor, bindVars, wantfields)
		}(i, source)
	}
	wg.Wait()
	for i := 0; i < len(c.Sources); i++ {
		if errs[i] != nil {
			return nil, vterrors.Wrap(errs[i], "Concatenate.Execute")
		}
		qr := qrs[i]
		if result.Fields == nil {
			result.Fields = qr.Fields
		}
		err := compareFields(result.Fields, qr.Fields)
		if err != nil {
			return nil, err
		}
		if len(qr.Rows) > 0 {
			result.Rows = append(result.Rows, qr.Rows...)
			if len(result.Rows[0]) != len(qr.Rows[0]) {
				return nil, mysql.NewSQLError(mysql.ERWrongNumberOfColumnsInSelect, "21000", "The used SELECT statements have a different number of columns")
			}
			result.RowsAffected += qr.RowsAffected
		}
	}
	return result, nil
}

// StreamExecute performs a streaming exec.
func (c *Concatenate) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var seenFields []*querypb.Field
	var fieldset sync.WaitGroup
	fieldsSent := false

	g := vcursor.ErrorGroupCancellableContext()
	fieldset.Add(1)
	var mu sync.Mutex
	for i, source := range c.Sources {
		i, source := i, source
		g.Go(func() error {
			err := source.StreamExecute(vcursor, bindVars, wantfields, func(resultChunk *sqltypes.Result) error {
				// if we have fields to compare, make sure all the fields are all the same
				if i == 0 && !fieldsSent {
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
				mu.Lock()
				defer mu.Unlock()
				select {
				case <-vcursor.Context().Done():
					return nil
				default:
					return callback(resultChunk)
				}
			})
			// This is to ensure other streams complete if the first stream failed to unlock the wait.
			if i == 0 && !fieldsSent {
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
	firstQr, err := c.Sources[0].GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	for i, source := range c.Sources {
		if i == 0 {
			continue
		}
		qr, err := source.GetFields(vcursor, bindVars)
		if err != nil {
			return nil, err
		}
		err = compareFields(firstQr.Fields, qr.Fields)
		if err != nil {
			return nil, err
		}
	}
	return firstQr, nil
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
