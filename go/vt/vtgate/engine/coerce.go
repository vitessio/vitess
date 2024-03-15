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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// Coerce is used to change types of incoming columns
type Coerce struct {
	Source Primitive
	Types  []*evalengine.Type
}

var _ Primitive = (*Coerce)(nil)

func (c *Coerce) RouteType() string {
	return c.Source.RouteType()
}

func (c *Coerce) GetKeyspaceName() string {
	return c.Source.GetKeyspaceName()
}

func (c *Coerce) GetTableName() string {
	return c.Source.GetTableName()
}

func (c *Coerce) GetFields(
	ctx context.Context,
	vcursor VCursor,
	bvars map[string]*querypb.BindVariable,
) (*sqltypes.Result, error) {
	res, err := c.Source.GetFields(ctx, vcursor, bvars)
	if err != nil {
		return nil, err
	}
	c.setFields(res)

	return res, nil
}

func (c *Coerce) setFields(res *sqltypes.Result) {
	for i, t := range c.Types {
		if t == nil {
			continue
		}

		typ := t.Type()
		field := res.Fields[i]
		_, flags := sqltypes.TypeToMySQL(typ)
		if t.Nullable() {
			flags |= int64(querypb.MySqlFlag_NOT_NULL_FLAG)
		}

		field.Type = typ
		field.Flags = uint32(flags)
		field.Charset = uint32(t.Collation())
		field.ColumnLength = uint32(t.Size())
		field.Decimals = uint32(t.Scale())
	}
}

func (c *Coerce) NeedsTransaction() bool {
	return c.Source.NeedsTransaction()
}

func (c *Coerce) TryExecute(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	_ bool,
) (*sqltypes.Result, error) {
	sqlmode := evalengine.ParseSQLMode(vcursor.SQLMode())

	res, err := vcursor.ExecutePrimitive(ctx, c.Source, bindVars, true)
	if err != nil {
		return nil, err
	}

	for _, row := range res.Rows {
		err := c.coerceValuesTo(row, sqlmode)
		if err != nil {
			return nil, err
		}
	}

	c.setFields(res)
	return res, nil
}

func (c *Coerce) coerceValuesTo(row sqltypes.Row, sqlmode evalengine.SQLMode) error {
	for i, value := range row {
		typ := c.Types[i]
		if typ == nil {
			// this column does not need to be coerced
			continue
		}

		newValue, err := evalengine.CoerceTo(value, *typ, sqlmode)
		if err != nil {
			return err
		}
		row[i] = newValue
	}
	return nil
}

func (c *Coerce) TryStreamExecute(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
	callback func(*sqltypes.Result) error,
) error {
	sqlmode := evalengine.ParseSQLMode(vcursor.SQLMode())

	return vcursor.StreamExecutePrimitive(ctx, c.Source, bindVars, wantfields, func(result *sqltypes.Result) error {
		for _, row := range result.Rows {
			err := c.coerceValuesTo(row, sqlmode)
			if err != nil {
				return err
			}
		}
		c.setFields(result)
		return callback(result)
	})
}

func (c *Coerce) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{c.Source}, nil
}

func (c *Coerce) description() PrimitiveDescription {
	var cols []string
	for _, typ := range c.Types {
		cols = append(cols, typ.Type().String())
	}
	return PrimitiveDescription{
		OperatorType: "Coerce",
		Other: map[string]any{
			"Fields": cols,
		},
	}
}
